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

This page walks the machinery that turns a spec change into cluster actions: the classes the operator reasons with, the controllers that receive events, the observers that refresh the status, the reconcilers that act, and the diff logic that decides between them. The concept-level view of this loop lives under [Architecture → Reconciliation Loop]({{< ref "docs/concepts/architecture#reconciliation-loop" >}}).

As implemented in `FlinkDeploymentController` and `FlinkSessionJobController`, every reconcile pass moves through the same logical phases: observe the status of the currently deployed resource, validate the incoming spec, reconcile any required changes based on the new spec and the observed status, and repeat. All phases execute on every pass, and even a failed validation does not stop reconciliation, it changes what reconciliation targets, as described under [Validation](#validation).

## Resource Class Hierarchy

All four custom resources are defined in the `flink-kubernetes-operator-api` module, layered on the fabric8 kubernetes-client API.

- `CustomResource` (from the kubernetes-client API, abstract) is the common root. It implements `HasMetadata` and `KubernetesResource`, which is what gives every resource its `metadata` plus the typed `spec` and `status` pair.
- `AbstractFlinkResource` (abstract) extends `CustomResource` and is the shared base for `FlinkDeployment` and `FlinkSessionJob`. That shared base is why those two have so much `spec` and `status` structure in common.
- `FlinkStateSnapshot` and `FlinkBlueGreenDeployment` extend `CustomResource` directly, outside the abstract layer, because their lifecycles differ. One tracks a single snapshot operation, the other orchestrates two child `FlinkDeployment`s.

The `spec` and `status` types follow the same split. On the `spec` side, `AbstractFlinkSpec` is the base for `FlinkDeploymentSpec` and `FlinkSessionJobSpec`, while `FlinkStateSnapshotSpec` and `FlinkBlueGreenDeploymentSpec` stand on their own. On the `status` side, `CommonStatus` is the base for `FlinkDeploymentStatus` and `FlinkSessionJobStatus` and carries the common observed fields (`jobStatus`, `error`, `observedGeneration`, `lifecycleState`), while the other two statuses stand alone. The shared specs implement `Diffable`, which is what the diff machinery described under [Spec Diff and Upgrade Decisions](#spec-diff-and-upgrade-decisions) operates on.

| Resource class | Extends | Spec base | Status base |
|---|---|---|---|
| `FlinkDeployment` | `AbstractFlinkResource` | `AbstractFlinkSpec` | `CommonStatus` |
| `FlinkSessionJob` | `AbstractFlinkResource` | `AbstractFlinkSpec` | `CommonStatus` |
| `FlinkStateSnapshot` | `CustomResource` | standalone | standalone |
| `FlinkBlueGreenDeployment` | `CustomResource` | standalone | standalone |

### Spec Hierarchy

The spec side in detail, with the fields each level contributes:

```
org.apache.flink.kubernetes.operator.api.spec
   │
   ├─ AbstractFlinkSpec             job (JobSpec), restartNonce, flinkConfiguration
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

This static structure is what every controller reads, and the rest of this page covers how it is observed, diffed, and acted on. The exhaustive field list lives in the [Reference]({{< ref "docs/custom-resource/reference" >}}).

## Controllers

The Java Operator SDK (JOSDK) requires one `Reconciler`, and therefore one controller, per resource type, and the operator registers four:

```
io.javaoperatorsdk.operator.api.reconciler.Reconciler<P>
   │
   ├─ FlinkDeploymentController           P = FlinkDeployment
   ├─ FlinkSessionJobController           P = FlinkSessionJob
   ├─ FlinkStateSnapshotController        P = FlinkStateSnapshot, registered only when the CRD is installed
   └─ FlinkBlueGreenDeploymentController  P = FlinkBlueGreenDeployment
```

The first three follow the pipeline this page walks: they receive the resource events, run the observers and reconcilers described in the following sections, and coordinate the status and event writes through `StatusRecorder` and `EventRecorder`. The fourth is a state machine orchestrating two child `FlinkDeployment` resources under one parent, covered under [Blue/Green Controller](#blue-green-controller).

One naming caveat helps when reading the code against JOSDK documentation: in the JOSDK, `Reconciler` names the interface behind the whole control flow. The operator calls those classes controllers, and reserves its own `Reconciler` interface for the acting part of the flow described under [Reconcilers](#reconcilers).

### Reconcile Triggers and Concurrency

A reconcile pass for a resource is triggered when:

- The resource itself is created, updated, or deleted, with updates filtered by generation and deletions taking the cleanup path.
- An event on a watched secondary resource maps back to the owning resource, a status change on the JobManager Deployment for example.
- The retry timer fires after a failed pass or one that rescheduled itself.
- The periodic reconcile interval fires, per resource, every 60 seconds by default (`kubernetes.operator.reconcile.interval`).

Physically every trigger funnels through the same path: the event source hands the event to the JOSDK `EventProcessor`, which tracks per-resource state and dispatches the pass onto the reconcile thread pool. The steady-state latency from event to running reconcile is typically tens of milliseconds.

The pool is bounded and shared across all four controllers, sized by `kubernetes.operator.reconcile.parallelism` (50 threads by default, -1 for unbounded). Within a single resource only one pass is ever in flight: events arriving mid-pass are coalesced and picked up by the next pass, while different resources reconcile in parallel up to the pool size.

## Observers

The observers face reality first: `AbstractFlinkDeploymentObserver`, `JobStatusObserver`, `SnapshotObserver`, and `StateSnapshotObserver` query the Flink REST API and the Kubernetes API on every pass of their controller, refreshing `status.jobStatus`, `status.jobManagerDeploymentStatus`, the snapshot progress, and the cluster health before any decision is made.

The observer always works with the previously deployed configuration, the observe config, rather than the incoming spec: user configuration can change the REST client settings, ports for example, so the running cluster must be accessed with the configuration it was actually deployed with. This is why `FlinkConfigManager` and the operator as a whole distinguish observe and deploy configuration throughout the implementation.

An observer never changes or submits resources, acting is the reconciler's responsibility: the required actions depend not only on the current cluster state but also on any new spec changes submitted in the meantime, which only the reconciliation phase sees together.

{{< img src="/img/concepts/observer_classes.svg" alt="Observer class hierarchy" >}}

### Observer Steps

1. Observation is skipped when there is nothing to observe: before the first deployment and during a rollback operation.
2. In the `UPGRADING` reconciliation state, the observer checks whether the upgrade already went through: the reconciler normally moves the state to `DEPLOYED` after a successful submission, but a transient error can prevent exactly that status update, so resource-specific logic checks whether the cluster already matches the target spec, through the deployment generation annotation or deterministically generated job ids. If it did, the status is corrected to `DEPLOYED`, and if it did not, the pass skips observation, the new deployment is not up yet.
3. For `FlinkDeployment` resources the Kubernetes side is observed next, the JobManager Deployment and its pods, once after an upgrade and any time the job REST endpoints are unreachable, recorded in `status.jobManagerDeploymentStatus`. Observing jobs over the Flink REST API only makes sense while the JobManager resources are healthy: any error with the JobManager deployment translates into an error state and clears a previously observed running job status, a job cannot be running without a healthy JobManager.
4. If the cluster looks healthy, the job status is observed next for application and session job resources, querying the job state and pending snapshot progress over the REST API. For globally terminal job states (`FINISHED`, `FAILED`, `CANCELED`) the last available checkpoint information is also recorded for later last-state upgrades, only then, because in a non-terminal state a newer checkpoint could still be created before the upgrade. If the job cannot be accessed, the cluster status is checked again, and while the job state cannot be determined it is recorded as `RECONCILING`.
5. The pass closes with bookkeeping on the observed status: a healthy running resource has previously recorded errors cleared, and a job that is no longer running has pending snapshot trigger information cleared so the trigger can be retried later.
6. Finally, the updated status is sent to Kubernetes before any reconciliation action runs. This ordering is critical: if the observer records the latest savepoint of a terminally failed job and the reconciler then deletes the cluster for an upgrade, an operator failure between the two must not lose that savepoint information, once the cluster is gone it cannot be observed again.

### Snapshot Observation

In the legacy status-based tracking, pending savepoint triggers and the savepoint history are part of `status.jobStatus`, updated only for running and terminal jobs: a failing or restarting job means the savepoint failed and needs retriggering. Pending savepoints are observed through their trigger id and recorded into the history on completion, and when the history exceeds its configured size, old savepoints are disposed through the running cluster's REST API, which needs no storage credentials. If the job becomes unhealthy, pending trigger information is cleared, aborting the savepoint from the operator's perspective. With `FlinkStateSnapshot` resources enabled, snapshots are tracked on dedicated resources instead, observed through their own controller, as documented under [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}).

## Validation

After observation has updated the status, the incoming spec is validated against the same rules the [admission webhook]({{< ref "docs/internals/webhook" >}}) applies. If validation fails, an error event is triggered for the user and the operator resets its in-memory view of the resource to the last successfully submitted spec, the resource in Kubernetes is not modified, only the copy reconciliation works with. A spec rejected while an upgrade was in flight is reset with the desired job state forced to running, so the interrupted upgrade still completes. Before the first successful deployment there is no previous spec to fall back to, so the pass ends after recording the error.

This is what keeps reconciliation running even against an invalid submission: the operator continues stabilizing the previously desired state, so failures on the deployed resources are still repaired while the invalid spec remains rejected. The user-facing outcomes are described under [Job Management → Starting]({{< ref "docs/managing/job-management#starting" >}}).

## Reconcilers

The reconcilers act on the desired state through a shared hierarchy: `AbstractFlinkResourceReconciler` provides the common diff-and-decide skeleton, `AbstractJobReconciler` adds the job lifecycle with savepoints, upgrades, and restarts, and `ApplicationReconciler`, `SessionReconciler`, and `SessionJobReconciler` specialize it per resource, all executing against the cluster through the `FlinkService` abstraction. `StateSnapshotReconciler` stands outside this hierarchy, the snapshot resources follow a simpler trigger-and-observe lifecycle of their own.

Observation records a point-in-time view, and by the time the reconciler acts, the cluster may already have drifted: a running job can fail at any moment, while only some observed states are stable, a terminally failed or completed job stays that way. The reconciler logic must always account for this possible drift, and most of its complications follow from exactly that.

{{< img src="/img/concepts/reconciler_classes.svg" alt="Reconciler class hierarchy" >}}

### Base Reconciler Steps

`AbstractFlinkResourceReconciler` encapsulates the core flow shared by all resource types:

1. Check whether the resource is ready for reconciliation, or whether a pending operation, a manual savepoint for example, should not be interrupted.
2. On the first deployment attempt, deploy, restoring from `initialSavepointPath` when set. The only other flow that reads this field is a savepoint redeploy.
3. Determine whether and how the desired spec changed: `IGNORE`, `SCALE`, `UPGRADE`, or `SAVEPOINT_REDEPLOY`, as described under [Spec Diff and Upgrade Decisions](#spec-diff-and-upgrade-decisions). Only the last three lead to further action.
4. For `SCALE` changes, first attempt an in-place scale through `FlinkService.scale`: in Standalone mode with reactive scheduling the TaskManager replica count is patched directly, and with the adaptive scheduler the new parallelism is applied to the running job by updating its per-vertex resource requirements over the REST API. When an in-place scale is not possible, and for `UPGRADE` and `SAVEPOINT_REDEPLOY` changes always, execute the resource-specific upgrade logic.
5. Without a spec change, ensure the deployed resources are fully reconciled: roll back a previously deployed spec that failed to stabilize, and apply the resource-specific extras, triggering savepoints, recovering missing deployments, restarting unhealthy clusters.

One more actor runs inside this flow: between the readiness check and the diff, `applyAutoscaler` executes the autoscaler, whose decisions mutate the desired spec so the very same pass picks them up as a spec change, as described under [Autoscaler → Integration with the Operator]({{< ref "docs/internals/autoscaler#integration-with-the-operator" >}}).

### Deploy Operations

Deployments start clusters and jobs that may immediately produce data and checkpoints, so recognizing whether a deployment succeeded is critical, while the operator process itself may fail at any point. To make the deployment status always recoverable, the to-be-deployed spec is written into the status with the `UPGRADING` state before the deployment is attempted, and the deployed Kubernetes Deployment resources are annotated with the resource generation to make the exact attempt identifiable. Session jobs, where no annotation is possible, encode the same information into a specially generated job id.

### Job Upgrades

`AbstractJobReconciler` executes stateful job updates safely for the job-carrying resources. Scale-type changes are already applied in place by the base flow where possible. This is also how autoscaler decisions avoid restarts, as described under [Job Management → Scaling Without a Spec Change]({{< ref "docs/managing/job-management#scaling-without-a-spec-change" >}}). Everything else lands here, and an `UPGRADE` change executes the full upgrade flow:

1. If the job is running, suspend it according to the upgrade decision described below.
2. Mark the `UPGRADING` state in the status. A cancellation that completes asynchronously ends the pass early, and the upgrade continues on a later pass once the job reaches its terminal state, picking up any spec changes that arrived in the meantime.
3. Restore the job with the new spec, from the HA metadata, from the savepoint recorded in the status, or from empty state, depending on the decided restore mode.

A `SAVEPOINT_REDEPLOY` change bypasses this machinery: the job is cancelled without any state capture and redeployed from the spec's `initialSavepointPath`, as described under [Job Management → Redeploying from a Savepoint]({{< ref "docs/managing/job-management#redeploying-from-a-savepoint" >}}).

#### Upgrade Mode Availability

The configured upgrade mode is always respected where state is at stake, with deliberate flexibility for unhealthy jobs: `getJobUpgrade` decides how the job is actually suspended and restored, given the user's request and the current cluster state.

- A `stateless` upgrade simply cancels the running job, and a healthy running job in `savepoint` mode suspends with a savepoint, exactly as configured.
- A job already in a terminal state upgrades from the latest savepoint or checkpoint recorded in the status. When that information is missing, the upgrade fails and manual recovery is required, as described under [Job Management → Manual Recovery]({{< ref "docs/managing/job-management#manual-recovery" >}}).
- In `savepoint` mode with a job that is not running, the operator falls back to a `last-state` style upgrade when the fallback is enabled, and otherwise waits for the job to become upgradable again.
- In `last-state` mode with a running job, the state is handed over through the HA metadata or by cancelling the job and restoring from the latest retained checkpoint. Cancellation is used for session jobs, when HA metadata is unavailable, and when it is explicitly configured. When a maximum checkpoint age is configured and the latest completed checkpoint is older, the operator waits for a recent pending checkpoint or falls back to taking a savepoint, so the upgrade does not resume from stale state.
- Because the HA metadata format is not compatible across Flink versions, a version change never upgrades through HA metadata: a running job takes a savepoint when a savepoint directory is configured, otherwise the job is cancelled and restored from its latest retained checkpoint, and when neither is possible the upgrade waits.
- The operator never changes a stateful mode to `stateless` on its own, that would mean silent state loss.

### Application Specifics

Application clusters need extra care during deploy, upgrade, and cancel operations:

- **Random JobResultStore path**: to prevent terminated applications from restarting on JobManager failover, job result cleanup is disabled and every deployment gets a unique job result store path, with the manual cleanup that entails, as described under [High Availability → JobResultStore Cleanup]({{< ref "docs/deployment/leader-election#jobresultstore-cleanup" >}}).
- **Random job ids**: Flink derives deterministic job ids from the cluster id, which is the resource name, and that would make checkpoint paths collide whenever a job restarts from empty state. The operator generates a random job id instead.
- **Terminal JobManager cleanup**: JobManager processes are not terminated automatically after shutdown or failure, keeping the terminal state observable, and the operator cleans up the JobManager deployment once the terminal state has been recorded, after a configurable TTL.

## Spec Diff and Upgrade Decisions

What a reconciler does is decided by the spec diff: `ReflectiveDiffBuilder` walks the spec fields annotated with `@SpecDiff` and classifies every change as `IGNORE`, `SCALE`, `UPGRADE`, or `SAVEPOINT_REDEPLOY`. The strongest classification, together with the configured `upgradeMode`, shapes the resulting action. The user-facing view of this mechanism is documented under [Spec Diffing]({{< ref "docs/custom-resource/overview#spec-diffing" >}}).

## Status Updates

The JOSDK updates a resource's status only at the end of a reconciliation, which is not enough here: the operator often must persist status mid-flow, most importantly the deployment information written before a deploy attempt. The resource status effectively serves as a write-ahead log for critical actions, guaranteeing recoverability when the operator fails mid-operation.

The mechanism lives in `StatusRecorder`, which is both a cache for the latest status and the update path. Because the JOSDK caches are bypassed, every controller pass starts by restoring the status from this cache, and a write is skipped entirely when the status has not changed. Updates are applied as patches on the status subresource without a resource version lock, so a status write goes through even when the spec was updated concurrently, and transient Kubernetes errors are retried. The scheme rests on the operator being the sole writer of the status: external status tampering, or a second operator instance managing the same resources, breaks that assumption.

## Blue/Green Controller

`FlinkBlueGreenDeploymentController` is the one controller that manages other custom resources rather than a Flink cluster directly: it drives the blue/green state machine, orchestrating the savepoint-driven transitions between the two child deployments, with `deploymentReadyTimestamp` and `abortTimestamp` gating the state changes. The user-facing behavior is documented under [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}}).

The machine always drives toward one of the two terminal states, `ACTIVE_BLUE` or `ACTIVE_GREEN`, in which a single environment runs and serves. A detected change moves through the intermediate states, deploying the new version into the inactive environment, validating it, retiring the previous deployment after the deletion delay, and switching over:

```
INITIALIZING_BLUE → ACTIVE_BLUE
ACTIVE_BLUE  → [SAVEPOINTING_BLUE]  → TRANSITIONING_TO_GREEN → ACTIVE_GREEN
ACTIVE_GREEN → [SAVEPOINTING_GREEN] → TRANSITIONING_TO_BLUE  → ACTIVE_BLUE
```

The savepointing states in brackets are entered only when the upgrade mode requires state preservation. The walkthrough below traces one full Blue to Green cycle with its failure and recovery paths:

{{< img src="/img/internals/bluegreen-state-machine.png" alt="Blue/green state machine transition sequence" >}}

Each state and its user-visible meaning is documented under [Blue/Green Deployments → Deployment States]({{< ref "docs/managing/bluegreen-deployments#deployment-states" >}}).

### State Handlers

The controller delegates every state to a dedicated handler. `BlueGreenStateHandlerRegistry` maps each state to its `BlueGreenStateHandler` implementation, and each pass builds a `BlueGreenContext` carrying the resource, its status, the JOSDK context, and the Blue and Green child deployments, and hands it to the handler for the current state. The handlers share `BlueGreenDeploymentService` for the deployment, savepoint, and transition operations, and `BlueGreenKubernetesService` for the Kubernetes-level work, child deployment management, owner references, and readiness checks. Four handlers cover the machine: `InitializingBlueStateHandler` for the first deployment, `ActiveStateHandler` for both active states, `SavepointingStateHandler` for both savepointing states, and `TransitioningStateHandler` for both transitions, including the abort back to the previous active state when the grace period expires. A transition finalizes only after the previous deployment is gone: the handler stamps the readiness timestamp, waits out the deletion delay, deletes the old deployment, and only then repoints the ingress and enters the active state.

### Spec Difference Types

Blue/green spec changes are classified by `FlinkBlueGreenDeploymentSpecDiff` into a `BlueGreenDiffType`, evaluated in precedence order:

1. `SUSPEND` and `RESUME`: a `spec.job.state` flip between running and suspended, acted on in place on the active deployment.
2. `SAVEPOINT_REDEPLOY`: a changed `savepointRedeployNonce`, redeploying from the spec's `initialSavepointPath` instead of a fresh savepoint.
3. `IGNORE`: the nested specs are identical, nothing happens.
4. Otherwise the nested `FlinkDeploymentSpec` diff decides: changes the [spec diff](#spec-diff-and-upgrade-decisions) classifies as ignorable become `PATCH_CHILD`, applied in place on the active child, and everything else becomes `TRANSITION`, the full blue/green cycle. The user-facing view is documented under [Blue/Green Deployments → Spec Change Behavior]({{< ref "docs/managing/bluegreen-deployments#spec-change-behavior" >}}).

### Secondary Resources

The controller watches what it owns: the Blue and Green child `FlinkDeployment` resources through an informer with owner-reference mapping, so any status change on a child triggers a reconciliation, and, when operator ingress management is enabled, the single active Ingress it maintains. Completing a transition repoints that Ingress from the old environment's REST service to the new one. With ingress management disabled, traffic switching is left to external tooling.
