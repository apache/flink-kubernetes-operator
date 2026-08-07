---
title: "Controllers"
weight: 3
type: docs
aliases:
- /internals/reconciliation-flow/controllers.html
- /concepts/controller-flow.html
- /docs/concepts/controller-flow/
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Controllers

This page walks the machinery that turns a spec change into cluster actions: the reconcile pass, the observers that refresh the status, the validation gate, the reconcilers that act, the diff logic that decides between them, the deletion path, and the resource model they all operate on. The concept-level view of this loop lives under [Architecture → Reconciliation Loop]({{< ref "docs/concepts/architecture#reconciliation-loop" >}}).

For the job-carrying resources, every reconcile pass moves through the same logical phases: observe the status of the currently deployed resource, validate the incoming spec, reconcile any required changes based on the new spec and the observed status, and repeat. All phases execute on every pass, and even a failed validation does not stop reconciliation, it changes what reconciliation targets, as described under [Validation](#validation).

## Reconcile Pass

The Java Operator SDK (JOSDK) requires one controller per resource type, and the operator registers four:

| Resource                             | Controller                                                                                                                         |
|--------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `FlinkDeployment`, `FlinkSessionJob` | The validate, observe, reconcile pipeline this page walks                                                                          |
| `FlinkStateSnapshot`                 | The same pipeline in [trigger-and-observe form](#snapshot-reconciliation), registered only when its CRD is installed               |
| `FlinkBlueGreenDeployment`           | A state machine orchestrating two child `FlinkDeployment` resources, covered under [Blue/Green Controller](#blue-green-controller) |

A pass over a job-carrying resource, end to end:

```
 controller pass
   ├─ restore the status ─► retrieve it from the per-resource cache
   ├─ observe            ─► refresh the status from the cluster, patch it to Kubernetes
   ├─ validate           ─► reject an invalid spec, the last accepted spec stands in
   ├─ reconcile          ─► run the autoscaler, diff the spec, act on the outcome
   └─ patch the status   ─► schedule the next pass
```

One naming caveat helps when reading the code against JOSDK documentation: what the JOSDK calls a reconciler is the interface behind this whole flow. These docs call that layer the controller, and reserve reconciler for the acting part described under [Reconcilers](#reconcilers).

### Triggers and Concurrency

A reconcile pass for a resource is triggered when:

- The resource itself is created, updated, or deleted, with updates filtered by generation and deletions taking the [cleanup path](#deletion-and-cleanup).
- An event on a watched secondary resource maps back to the owning resource, a status change on the JobManager Deployment for example, through the mappings described under [Startup → Informers]({{< ref "docs/internals/startup#informers" >}}).
- The retry timer fires after a failed pass or one that rescheduled itself. Failures back off exponentially per the `kubernetes.operator.retry.*` options: initial interval 5 seconds, multiplier 1.5, at most 15 attempts, and an optional interval cap.
- The periodic reconcile interval fires, per resource, every 60 seconds by default (`kubernetes.operator.reconcile.interval`).

Physically every trigger funnels through the same path: the event source hands the event to the JOSDK event processor, which tracks per-resource state and dispatches the pass onto the reconciliation pool. The steady-state latency from event to running reconcile is typically tens of milliseconds.

The pool is bounded and shared across all four controllers, sized by `kubernetes.operator.reconcile.parallelism` (50 threads by default, -1 for unbounded). Within a single resource only one pass is ever in flight: events arriving mid-pass are folded into a single follow-up pass, while different resources reconcile in parallel up to the pool size.

## Observers

Observation faces reality first: every pass queries the Flink REST API and the Kubernetes API, refreshing `status.jobStatus`, `status.jobManagerDeploymentStatus`, the snapshot progress, and the cluster health before any decision is made.

The observer always works with the previously deployed configuration, the observe config, rather than the incoming spec: user configuration can change the REST client settings, ports for example, so the running cluster must be accessed with the configuration it was actually deployed with. This is why the operator distinguishes observe and deploy configuration, both defined under [Startup → Configuration]({{< ref "docs/internals/startup#configuration" >}}).

An observer never changes or submits resources, acting is the reconciler's responsibility: the required actions depend not only on the current cluster state but also on any new spec changes submitted in the meantime, which only the reconciliation phase sees together.

### Requests

What observation actually calls, all against the observe config:

| Request                                   | Purpose                                                                                        |
|-------------------------------------------|------------------------------------------------------------------------------------------------|
| `GET /jobs/overview`                      | The job's state, resolved from the listing by job id                                           |
| `GET /jobs/:jobid/exceptions`             | New root exceptions, reported as Kubernetes events                                             |
| `GET /jobs/:jobid/checkpoints`            | Checkpoint statistics, recording the last checkpoint in terminal states and the checkpoint age |
| `GET /jobs/:jobid/savepoints/:triggerid`  | Progress of a pending savepoint trigger                                                        |
| `GET /jobs/:jobid/checkpoints/:triggerid` | Progress of a pending checkpoint trigger                                                       |
| `POST /savepoint-disposal`                | Disposal of savepoints rotated out of the history                                              |
| Kubernetes API                            | The JobManager Deployment, its pods, and their events                                          |

### Steps

1. Observation is skipped when there is nothing to observe: before the first deployment and during a rollback.
2. In the `UPGRADING` state, the observer resolves whether the upgrade already went through, the in-flight submission may have succeeded without its status update. The cluster is matched against the target spec through the deployment generation annotation, or the deterministic job id for session jobs:
   - a match corrects the status to `DEPLOYED`,
   - no match skips the rest of the observation, the new deployment is not up yet.
3. For a `FlinkDeployment`, the Kubernetes side comes next: the JobManager Deployment and its pods, observed after an upgrade and whenever the REST endpoints are unreachable, recorded in `status.jobManagerDeploymentStatus`. An unhealthy JobManager clears a previously observed running job status, a job cannot be running without one.
4. With a healthy cluster, the job is observed over REST: its state, new root exceptions, and pending snapshot progress. In a globally terminal state (`FINISHED`, `FAILED`, `CANCELED`) the last checkpoint information is recorded for later `last-state` upgrades, only then, earlier a newer checkpoint could still appear. An unreachable job is re-checked against the cluster and recorded as `RECONCILING` while undeterminable.
5. Bookkeeping closes the pass: a healthy running resource clears its recorded errors, and a no-longer-running job clears pending snapshot triggers so they can be retried.
6. The status is patched to Kubernetes before any reconciliation action. The ordering is deliberate: the recorded savepoint of a terminally failed job must survive the operator failing between observing and acting, once the cluster is deleted it cannot be observed again.

### Snapshot Observation

In the legacy status-based tracking, pending savepoint triggers and the savepoint history live in `status.jobStatus`, updated only for running and terminal jobs, a failing or restarting job means the savepoint failed and needs retriggering.

- Pending triggers are polled by trigger id and recorded into the history on completion.
- The history is bounded: beyond the configured size, old savepoints are disposed through the cluster, no storage credentials needed.
- A job that stops running clears its pending trigger information, aborting the snapshot from the operator's perspective.

With `FlinkStateSnapshot` resources enabled, snapshots are tracked on dedicated resources instead, observed through their own controller, as documented under [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}).

## Validation

After observation has updated the status, the incoming spec is validated against the same rules the [admission webhook]({{< ref "docs/internals/webhook" >}}) applies. A failed validation triggers an error event and resets the operator's in-memory view to the last successfully submitted spec, the resource in Kubernetes stays untouched, only the copy reconciliation works with. Two special cases:

- A spec rejected while an upgrade was in flight is reset with the desired job state forced to running, so the interrupted upgrade still completes.
- Before the first successful deployment there is nothing to fall back to, the pass ends after recording the error.

This is what keeps reconciliation running even against an invalid submission: the operator continues stabilizing the previously desired state, so failures on the deployed resources are still repaired while the invalid spec remains rejected. The user-facing outcomes are described under [Job Management → Starting]({{< ref "docs/managing/job-management#starting" >}}).

### Rules

Every check runs against the effective configuration, the operator defaults merged with the spec's own `flinkConfiguration`, evaluated in order with the first failure winning. For a `FlinkDeployment`:

| Check                          | Rules                                                                                                                                                                                                                                                |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Name                           | Lowercase DNS-style, at most 45 characters                                                                                                                                                                                                           |
| Flink version                  | Defined and supported, and never changed while suspended in `last-state` mode                                                                                                                                                                        |
| Configuration                  | The operator-managed keys are forbidden: `kubernetes.namespace`, `kubernetes.cluster-id`, `high-availability.cluster-id`                                                                                                                             |
| Ingress                        | A resolvable `template` when an ingress is declared                                                                                                                                                                                                  |
| Log configuration              | Only `log4j-console.properties` and `logback-console.xml` entries                                                                                                                                                                                    |
| Job                            | Stateful upgrade modes need a checkpoint directory, and `savepoint` mode, manual triggering, periodic savepoints, and the last-state checkpoint max age all need a savepoint directory. TaskManager replicas or job parallelism must be at least `1` |
| JobManager                     | Memory must be defined and parse, replicas at least `1`, and standby replicas require high availability                                                                                                                                              |
| TaskManager                    | Memory must be defined and parse, replicas at least `1`                                                                                                                                                                                              |
| Resources                      | Memory and ephemeral storage quantities must parse                                                                                                                                                                                                   |
| Spec transitions               | No switch between session and application, or between Native and Standalone mode, and a changed `savepointRedeployNonce` requires an `initialSavepointPath`                                                                                          |
| Service account and autoscaler | A service account must be set, and the autoscaler options are cross-checked                                                                                                                                                                          |

For a `FlinkSessionJob`:

| Check                | Rules                                                                                                                           |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------|
| Cluster name         | `spec.deploymentName` follows the lowercase DNS-style rule                                                                      |
| Job                  | The job spec must not be empty                                                                                                  |
| Jar URI              | Checked against the operator's allowed schemes and host restrictions                                                            |
| Spec transitions     | `deploymentName` cannot change after the first deployment                                                                       |
| Target cluster       | When visible, an actual session cluster with no job of its own, whose name matches `spec.deploymentName`                        |
| Merged configuration | When the cluster is visible, the job and autoscaler rules above are re-checked against the merged cluster and job configuration |

And for a `FlinkStateSnapshot`:

| Check              | Rules                                                                                              |
|--------------------|----------------------------------------------------------------------------------------------------|
| Kind               | Exactly one of `savepoint` and `checkpoint` declared                                               |
| Existing savepoint | A savepoint marked `alreadyExists` needs nothing else                                              |
| Job reference      | Required otherwise, and while the snapshot is `TRIGGER_PENDING` the referenced resource must exist |

Custom validators extend the set through the SPI, as described under [Plugins → Custom Flink Resource Validators]({{< ref "docs/deployment/plugins#custom-flink-resource-validators" >}}).

## Reconcilers

The reconcilers act on the desired state through a shared skeleton: a common diff-and-decide base, a job layer on top adding savepoints, upgrades, and restarts, and per-resource specializations for application clusters, session clusters, and session jobs, all executing against the cluster through one service abstraction. Snapshot resources stand outside this hierarchy, following a simpler trigger-and-observe lifecycle of their own, described under [Snapshot Reconciliation](#snapshot-reconciliation).

Observation records a point-in-time view, and by the time the reconciler acts, the cluster may already have drifted: a running job can fail at any moment, while only some observed states are stable, a terminally failed or completed job stays that way. The reconciler logic must always account for this possible drift, and most of its complications follow from exactly that.

### Base Reconciler Steps

The base flow forks on whether the resource has ever been deployed:

```
 reconcile
   ├─ readiness check  ─► a pending operation, a manual savepoint for example, is not interrupted
   ├─ first deployment ─► deploy, restore from initialSavepointPath when set, done
   └─ every later pass
        ├─ run the autoscaler
        └─ diff the spec
             ├─ SCALE                       ─► in-place when possible, otherwise the upgrade logic
             ├─ UPGRADE, SAVEPOINT_REDEPLOY ─► the upgrade logic
             └─ IGNORE                      ─► rollback checks, savepoints, recovery, restarts
```

- The first deployment is the only flow reading `initialSavepointPath` besides a savepoint redeploy, and a resource submitted in `suspended` state is not deployed at all until resumed.
- The autoscaler runs before the diff, its decisions mutate the desired spec so the very same pass picks them up, as described under [Autoscaler → Operator Integration]({{< ref "docs/internals/autoscaler#operator-integration" >}}).
- An in-place scale patches the TaskManager replica count in Standalone reactive mode, or applies the new per-vertex resource requirements over the REST API with the adaptive scheduler. When neither is possible, the change follows the upgrade logic.
- The diff classification is covered under [Spec Diff and Upgrade Decisions](#spec-diff-and-upgrade-decisions), and without any spec change the pass ensures full reconciliation: rolling back a spec that failed to stabilize, triggering savepoints, recovering missing deployments, restarting unhealthy clusters.

### Deploy Operations

Deployments start clusters and jobs that may immediately produce data and checkpoints, so recognizing whether a deployment succeeded is critical, while the operator process itself may fail at any point. To make the deployment status always recoverable, the to-be-deployed spec is written into the status with the `UPGRADING` state before the deployment is attempted, and the deployed Kubernetes Deployment resources are annotated with the resource generation to make the exact attempt identifiable. Session jobs, where no annotation is possible, encode the same information into a specially generated job id.

### Job Upgrades

The job layer executes stateful updates safely for the job-carrying resources. Scale-type changes are already applied in place by the base flow where possible. This is also how autoscaler decisions avoid restarts, as described under [Job Management → Scaling Without a Spec Change]({{< ref "docs/managing/job-management#scaling-without-a-spec-change" >}}). Everything else lands here, and an `UPGRADE` change executes the full upgrade flow:

```
 upgrade
   ├─ suspend the running job, per the upgrade decision below
   ├─ mark UPGRADING in the status
   └─ restore with the new spec, from HA metadata, the recorded savepoint, or empty state
```

A cancellation that completes asynchronously ends the pass early at the second step, and the upgrade continues on a later pass once the job reaches its terminal state, picking up any spec changes that arrived in the meantime.

A `SAVEPOINT_REDEPLOY` change bypasses this machinery: the job is cancelled without any state capture and redeployed from the spec's `initialSavepointPath`, as described under [Job Management → Redeploying from a Savepoint]({{< ref "docs/managing/job-management#redeploying-from-a-savepoint" >}}).

#### Upgrade Decision

The configured upgrade mode is always respected where state is at stake, with deliberate flexibility for unhealthy jobs: the upgrade decision resolves how the job is actually suspended and restored, given the user's request and the current cluster state.

| Situation                             | Decision                                                                                                                                                                                                                                                                                             |
|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stateless` mode                      | Cancel the running job, restart from empty state                                                                                                                                                                                                                                                     |
| Healthy running job, `savepoint` mode | Suspend with a savepoint, exactly as configured                                                                                                                                                                                                                                                      |
| Job already in a terminal state       | Restore from the savepoint or checkpoint recorded in the status. Missing information fails the upgrade into [Job Management → Manual Recovery]({{< ref "docs/managing/job-management#manual-recovery" >}})                                                                                           |
| `savepoint` mode, job not running     | Fall back to a `last-state` style upgrade when the fallback is enabled, otherwise wait                                                                                                                                                                                                               |
| `last-state` mode, running job        | Hand the state over through HA metadata, or cancel and restore from the latest retained checkpoint: for session jobs, when HA metadata is unavailable, or when explicitly configured. A latest checkpoint older than the configured maximum age waits for a pending one or falls back to a savepoint |
| Flink version change                  | Never through HA metadata: a savepoint when a directory is configured, otherwise cancel and restore from the retained checkpoint, otherwise wait                                                                                                                                                     |

{{< hint info >}}
The operator never changes a stateful mode to `stateless` on its own, that would mean silent state loss.
{{< /hint >}}

### Application Specifics

Application clusters need extra care during deploy, upgrade, and cancel operations:

- Random JobResultStore path: to prevent terminated applications from restarting on JobManager failover, job result cleanup is disabled and every deployment gets a unique job result store path, with the manual cleanup that entails, as described under [High Availability → JobResultStore Cleanup]({{< ref "docs/deployment/leader-election#jobresultstore-cleanup" >}}).
- Random job ids: Flink derives deterministic job ids from the cluster id, which is the resource name, and that would make checkpoint paths collide whenever a job restarts from empty state. The operator generates a random job id instead.
- Terminal JobManager cleanup: JobManager processes are not terminated automatically after shutdown or failure, keeping the terminal state observable, and the operator cleans up the JobManager deployment once the terminal state has been recorded, after a configurable TTL.

### Snapshot Reconciliation

Snapshot resources follow the trigger-and-observe lifecycle instead of the diff-driven flow above. The reconciler acts only in the `TRIGGER_PENDING` state, working through a first-match ladder:

```
 snapshot pass
   ├─ a savepoint marked alreadyExists  ─► completed immediately, nothing is triggered
   ├─ the referenced job is not running ─► the snapshot is abandoned (ABANDONED)
   └─ otherwise                         ─► the savepoint or checkpoint is triggered over
                                           REST and the trigger id recorded (IN_PROGRESS)
```

Progress is polled by the observer through the trigger-status endpoints in the [Requests](#requests) table, landing the resource in `COMPLETED` or `FAILED`. A failed attempt returns to `TRIGGER_PENDING` with an exponential delay, 10 seconds doubling per failure, until the failure count exceeds the spec's `backoffLimit`, with a negative limit retrying forever. The user-facing lifecycle is documented under [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}).

## Spec Diff and Upgrade Decisions

What a reconciler does is decided by the spec diff: the incoming spec is compared against the last reconciled one in a reflective walk over every field. Fields carrying the api module's `@SpecDiff` annotation classify as the annotation says, every other field classifies as `UPGRADE`, and nested spec types are walked field by field while opaque values such as pod templates compare as a whole. The per-field results aggregate to the strongest one, `IGNORE` < `SCALE` < `UPGRADE` < `SAVEPOINT_REDEPLOY`, and that single type, together with the configured `upgradeMode`, shapes the resulting action.

| Classified           | Changes                                                                                                                                                                                                                                   |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `IGNORE`             | The on-demand fields `upgradeMode`, `initialSavepointPath`, `allowNonRestoredState`, `autoscalerResetNonce`, the legacy trigger nonces, and the `job.autoscaler.*`, `parallelism.default`, and `kubernetes.operator.*` configuration keys |
| `SCALE`              | `job.parallelism` and `taskManager.replicas` in Standalone mode, the `pipeline.jobvertex-parallelism-overrides` configuration key in Native mode                                                                                          |
| `UPGRADE`            | Every field without its own rule: image, Flink version, pod templates, the job's `jarURI` and `state`, any unlisted `flinkConfiguration` key, and `restartNonce` (ignored when cleared back to null)                                      |
| `SAVEPOINT_REDEPLOY` | `savepointRedeployNonce` (ignored when cleared back to null)                                                                                                                                                                              |

Two rules refine the classification:

- Mode-conditional annotations count only in their deployment mode, elsewhere the field falls back to `UPGRADE`. The scale vectors differ exactly this way: replica and parallelism changes in Standalone mode, the vertex parallelism overrides with the adaptive scheduler in Native mode.
- The `IGNORE` fields are the ones consumed on demand rather than deployed, a changed `upgradeMode` matters at the next upgrade rather than at the moment of the edit, and an all-`IGNORE` diff runs the no-change branch of the [base flow](#base-reconciler-steps).

The user-facing view of this mechanism is documented under [Spec Diffing]({{< ref "docs/custom-resource/overview#spec-diffing" >}}).

## Status Updates

The JOSDK updates a resource's status only at the end of a reconciliation, which is not enough here: the operator often must persist status mid-flow, most importantly the deployment information written before a deploy attempt. The resource status effectively serves as a write-ahead log for critical actions, guaranteeing recoverability when the operator fails mid-operation.

The mechanism is a per-controller status cache that is also the write path:

- The JOSDK caches are bypassed, every pass starts by restoring the status from this cache, and a write is skipped entirely when nothing changed.
- Updates are patches on the status subresource without a resource version lock, going through even when the spec changed concurrently, with transient Kubernetes errors retried.
- The scheme rests on the operator being the sole status writer: external tampering, or a second operator instance on the same resources, breaks that assumption.

## Deletion and Cleanup

A deletion trigger runs the controller's cleanup path instead of the reconcile pipeline. The job-carrying and snapshot resources carry a finalizer, so the Kubernetes object stays visible until their cleanup completes, while `FlinkBlueGreenDeployment` has no finalizer of its own.

```
 cleanup pass
   ├─ restore the status ─► retrieve it from the per-resource cache, mark the lifecycle DELETING
   ├─ observe            ─► best effort, a failure does not stop the cleanup
   ├─ clean up           ─► the per-resource work in the table below
   ├─ blocked            ─► keep the finalizer, patch the status, retry at the reconcile interval
   └─ done               ─► mark DELETED, drop the cached status, remove the finalizer
```

| Resource                      | Cleanup                                                                                                                                                                                                                                                                                                                                                     |
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Application `FlinkDeployment` | A running job is cancelled, with a savepoint first when `kubernetes.operator.job.savepoint-on-deletion` is enabled. Before the first deployment, or with the job already terminal, the cluster and its HA metadata are deleted directly                                                                                                                     |
| Session `FlinkDeployment`     | Blocked with a warning event while managed session jobs exist (`kubernetes.operator.session.deletion.block-on-session-jobs`), then while unmanaged jobs run in the cluster (`kubernetes.operator.session.deletion.block-on-unmanaged-jobs`), both `true` by default. Once clear, the cluster and its HA metadata are deleted                                |
| `FlinkSessionJob`             | The job is cancelled in its session cluster, honoring the same savepoint option. An unhealthy cluster without HA releases the job uncancelled, with HA enabled the cancellation waits for the cluster, and a cluster already being deleted skips it, unless that cluster blocks on its session jobs, where cancelling is exactly what unblocks the deletion |
| `FlinkStateSnapshot`          | Checkpoints and savepoints without `disposeOnDelete` detach immediately. A disposable savepoint waits out an in-progress trigger and is disposed through the referenced job's cluster, while failed, pending, and abandoned snapshots are released without disposal                                                                                         |
| `FlinkBlueGreenDeployment`    | Nothing of its own: the child deployments cascade through owner references into the application cleanup                                                                                                                                                                                                                                                     |

Two details round out deletion: the JobManager and TaskManager Deployments are deleted with Foreground propagation by default (`kubernetes.operator.resource.deletion.propagation`), and the unmanaged-jobs check runs only while the managed one is also enabled, so disabling `block-on-session-jobs` disables both.

## Resource Model

All four custom resources are defined in the `flink-kubernetes-operator-api` module, layered on the Fabric8 Kubernetes client API. `FlinkDeployment` and `FlinkSessionJob` share a common base, which is why their `spec` and `status` structures look so alike: `AbstractFlinkSpec` on the spec side, and `CommonStatus` with the shared observed fields (`jobStatus`, `error`, `observedGeneration`, `lifecycleState`) on the status side. The shared specs are what the [spec diff](#spec-diff-and-upgrade-decisions) walks. `FlinkStateSnapshot` and `FlinkBlueGreenDeployment` stand on their own, one tracks a single snapshot operation, the other orchestrates two child deployments.

The spec side in detail, with the fields each level contributes:

```
org.apache.flink.kubernetes.operator.api.spec
   │
   ├─ AbstractFlinkSpec            job (JobSpec), restartNonce, flinkConfiguration
   │     │
   │     ├─ FlinkDeploymentSpec    image, imagePullPolicy, flinkVersion, mode, serviceAccount,
   │     │                         jobManager (JobManagerSpec), taskManager (TaskManagerSpec),
   │     │                         ingress (IngressSpec), podTemplate, logConfiguration
   │     │
   │     └─ FlinkSessionJobSpec    deploymentName
   │
   ├─ FlinkStateSnapshotSpec       jobReference, savepoint | checkpoint (mutually exclusive),
   │                               backoffLimit
   │
   └─ FlinkBlueGreenDeploymentSpec template (wraps a full FlinkDeploymentSpec), configuration,
                                   ingress
```

`JobSpec` carries the job-level fields (`jarURI`, `parallelism`, `upgradeMode`, `state`, `initialSavepointPath`, `savepointRedeployNonce`), `JobManagerSpec` and `TaskManagerSpec` pair resources and replicas with a pod template, and `IngressSpec` (`template`, `className`, `annotations`, `labels`, `tls`) declares the REST endpoint exposure. On the snapshot side, the `savepoint` variant carries `path`, `formatType`, `alreadyExists`, and `disposeOnDelete`, while `checkpoint` has no fields of its own. The blue/green `configuration` map holds the `kubernetes.operator.bluegreen.*` options.

This static structure is what every section above observes, diffs, and acts on. The exhaustive field list lives in the [Reference]({{< ref "docs/custom-resource/reference" >}}).

## Blue/Green Controller

The blue/green controller is the one that manages other custom resources rather than a Flink cluster directly: it drives the blue/green state machine, orchestrating the savepoint-driven transitions between the two child deployments, with `deploymentReadyTimestamp` and `abortTimestamp` gating the state changes. The user-facing behavior is documented under [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}}).

The machine always drives toward one of the two terminal states, `ACTIVE_BLUE` or `ACTIVE_GREEN`, in which a single environment runs and serves. A detected change moves through the intermediate states, deploying the new version into the inactive environment, validating it, retiring the previous deployment after the deletion delay, and switching over. The states, their transitions, and their user-visible meaning are documented under [Blue/Green Deployments → Deployment States]({{< ref "docs/managing/bluegreen-deployments#deployment-states" >}}), and the walkthrough below traces one full Blue to Green cycle with its failure and recovery paths:

```
 blue to green cycle
   ├─ INITIALIZING_BLUE ───────► the very first pass deploys Blue, then ACTIVE_BLUE
   ├─ ACTIVE_BLUE ─────────────► a TRANSITION change on a ready cluster: every upgrade
   │                             mode except stateless triggers a savepoint first
   ├─ SAVEPOINTING_BLUE ───────► the savepoint is polled to completion, then back to
   │                             ACTIVE_BLUE, which re-reads the still-unreconciled change
   ├─ TRANSITIONING_TO_GREEN ──► Green is deployed from the new spec and the savepoint,
   │    │                        abortTimestamp set from the abort grace period
   │    ├─ Green ready ────────► deploymentReadyTimestamp stamped, the deletion delay
   │    │                        waited out, Blue deleted, the Ingress repointed, then
   │    │                        ACTIVE_GREEN
   │    └─ not ready in time ──► Green suspended, the timestamps reset, back to
   │                             ACTIVE_BLUE with the job status FAILING
   └─ ACTIVE_GREEN ────────────► the next TRANSITION runs the mirrored cycle back to Blue
```

Four details refine the cycle:

- The abort grace period gates only the transitioning phase, a pending savepoint is waited on indefinitely.
- A transition requested while the active cluster is not ready is marked failing without being started.
- An abort of the very first deployment falls back to `INITIALIZING_BLUE`, there is no previous active environment yet.
- Finalization reschedules immediately, so a spec change that arrived mid-transition is picked up on the very next pass.

### State Handlers

The controller delegates every state to a dedicated handler:

- A handler per state, resolved from a registry on each pass together with a context carrying the resource, its status, and the Blue and Green child deployments.
- The handlers share two services, one for the deployment, savepoint, and transition operations, and one for the Kubernetes-level work: managing the child deployments, their owner references, and readiness checks.
- Four handlers cover the machine: the first deployment, both active states, both savepointing states, and both transitions, including the abort back to the previous active state when the grace period expires.

### Spec Difference Types

Blue/green spec changes are classified into a diff type of their own, evaluated in precedence order:

1. `SUSPEND` and `RESUME`: a `spec.job.state` flip between running and suspended, acted on in place on the active deployment.
2. `SAVEPOINT_REDEPLOY`: a changed `savepointRedeployNonce`, redeploying from the spec's `initialSavepointPath` instead of a fresh savepoint.
3. `IGNORE`: the nested specs are identical, nothing happens.
4. Otherwise the nested `FlinkDeploymentSpec` diff decides: changes the [spec diff](#spec-diff-and-upgrade-decisions) classifies as ignorable become `PATCH_CHILD`, applied in place on the active child, and everything else becomes `TRANSITION`, the full blue/green cycle. The user-facing view is documented under [Blue/Green Deployments → Spec Change Behavior]({{< ref "docs/managing/bluegreen-deployments#spec-change-behavior" >}}).

### Secondary Resources

The controller watches what it owns: the Blue and Green child `FlinkDeployment` resources through an informer with owner-reference mapping, so any status change on a child triggers a reconciliation, and, when operator ingress management is enabled, the single active Ingress it maintains. Completing a transition repoints that Ingress from the old environment's REST service to the new one. With ingress management disabled, traffic switching is left to external tooling.
