---
title: "Job Management"
weight: 1
type: docs
aliases:
- /custom-resource/job-management.html
- /docs/custom-resource/job-management/
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

# Job Management

The operator manages the complete lifecycle of Flink jobs declared through custom resources. The concepts behind these operations are covered under [Lifecycle Management]({{< ref "docs/concepts/lifecycle-management" >}}). This page documents the concrete controls, the spec fields that drive each operation and the configuration around them, building on the [Custom Resource → Overview]({{< ref "docs/custom-resource/overview" >}}).

{{< hint info >}}
Everything on this page applies to both `FlinkDeployment` and `FlinkSessionJob` resources, with three exceptions: recovering missing deployments and rollbacks exist only for `FlinkDeployment` resources, and restarting unhealthy deployments only for application-mode ones.
{{< /hint >}}

## Starting

A job is started by creating the resource itself. Applying a `FlinkDeployment` with a `spec.job` section, or a `FlinkSessionJob` pointing at a session cluster, deploys and submits the job:

```shell
kubectl apply -f example-deployment.yaml
```

Between the apply and the running job sits the operator's [reconciliation loop]({{< ref "docs/concepts/architecture#reconciliation-loop" >}}), and its validation gate comes first. With the admission webhook enabled, an invalid spec never reaches the operator, it is rejected at apply time. Without it, the operator catches the invalid spec on the first pass: for a first deployment nothing is deployed, the error lands in `status.error`, and the resource sits in the `FAILED` lifecycle state until the spec is fixed, while for a running job the change is set aside and the job keeps running on the last valid spec. The same rules run on every reconciliation pass, so every later spec change and upgrade is gated by them exactly like the first deployment.

The rules are shared between the webhook and the operator, and cover, among others:

- unsupported `flinkVersion` values
- forbidden `flinkConfiguration` keys managed by the operator itself, such as `kubernetes.cluster-id` and `kubernetes.namespace`
- inconsistent job settings, for example a stateful `upgradeMode` without a configured checkpoint or savepoint directory
- disallowed spec transitions, such as switching between session and application clusters, between Native and Standalone mode, or changing `flinkVersion` while suspended in `last-state` mode

The gate is also extensible: custom validators can complement the built-in rules, and with the webhook enabled, mutators can adjust the incoming spec at admission. Out of the box the webhook carries a single default mutator, which only labels session jobs with their target cluster. Both validators and mutators are discovered as plugins, as described under [Plugins → Custom Flink Resource Validators]({{< ref "docs/deployment/plugins#custom-flink-resource-validators" >}}) and [Plugins → Custom Flink Resource Mutators]({{< ref "docs/deployment/plugins#custom-flink-resource-mutators" >}}).

The first deployment starts from empty state, unless `spec.job.initialSavepointPath` names a savepoint to bootstrap the state from.

## Suspending and Resuming

Once running, the desired state of the job is declared through `spec.job.state`, with two supported values:

- `running`: the job is expected to be running and processing data, the default
- `suspended`: processing is temporarily stopped, with the intention of continuing later

Suspending and resuming are therefore plain spec updates:

```yaml
job:
  state: suspended
```

Setting the value to `suspended` stops the job while keeping its state information, and setting it back to `running` resumes the job from where it stopped. Any other spec change while the job is running triggers an [upgrade](#upgrades), and changes made while suspended are recorded and take effect once the job is resumed. In every case, how state survives the stop and restore is decided by the upgrade mode.

These are the desired states: what the resource actually reports back, the observed lifecycle states and the transitions between them, is documented under [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}).

## Deleting

Deletion mirrors creation. Just as starting a job means creating its resource, ending it for good means deleting the resource, there is no `cancelled` or `deleted` value for `spec.job.state`:

```shell
kubectl delete flinkdeployment example-deployment
```

{{< hint warning >}}
Deleting a resource removes its status information together with the tracked checkpoint history. A later redeployment starts from empty state unless a starting snapshot is set explicitly, as described under [Manual Recovery](#manual-recovery).
{{< /hint >}}

To leave a final recovery point behind, `kubernetes.operator.job.savepoint-on-deletion` (default `false`) makes the operator take a savepoint as part of the shutdown when a `FlinkDeployment` or `FlinkSessionJob` is deleted. Each resource savepoints only its own job: a session cluster carries none, its jobs take theirs through their own `FlinkSessionJob` deletions. The savepoint is written to the configured savepoint directory.

Deleting a session cluster is blocked by default while jobs are still running on it. `kubernetes.operator.session.deletion.block-on-session-jobs` (default `true`) blocks the deletion while managed `FlinkSessionJob` resources are running, and `kubernetes.operator.session.deletion.block-on-unmanaged-jobs` (default `true`) blocks it while unmanaged jobs are running, for example jobs submitted through the Flink CLI. The jobs have to be removed first, or the corresponding option disabled.

## Upgrades

When the spec of a `FlinkDeployment` or `FlinkSessionJob` changes, the running job must be upgraded. To do this the operator stops the job (unless already suspended) and redeploys it using the latest spec, with the state carried over from the previous run for stateful applications.

Upgrades are triggered exclusively by changes to the resource spec, with the operator tracking its own resources together with their dependencies. Where resources are linked they react to each other: a session cluster `FlinkDeployment` and the `FlinkSessionJob` resources running on it respond to each other's changes, and the Kubernetes objects the operator creates itself, such as the JobManager Deployment or a managed Ingress, are watched and reconciled in the same way. A `FlinkSessionJob` is only reconciled while the JobManager of its session cluster reports ready, otherwise the operator waits. A session cluster always upgrades as a whole: instead of a rolling upgrade, the operator tears the running cluster down and redeploys it with the new spec.

{{< hint warning >}}
When a referenced Kubernetes resource changes, the running job is not restarted and may keep using the previously deployed values until the next upgrade. To roll such changes out deliberately, trigger a restart through the `restartNonce` field, as described under [Restarting Without a Spec Change](#restarting-without-a-spec-change).
{{< /hint >}}

How state is carried across the restart is controlled through `spec.job.upgradeMode`, with three supported values: `stateless`, `last-state`, and `savepoint`. The setting controls both the stop and the restore mechanism, as detailed in the following table:

|                        | Stateless        | Last State                             | Savepoint                                 |
|------------------------|------------------|----------------------------------------|-------------------------------------------|
| Config Requirement     | None             | Checkpointing Enabled                  | Checkpoint or savepoint directory defined |
| Job Status Requirement | None             | Job or HA metadata accessible          | Job Running                               |
| Suspend Mechanism      | Cancel or delete | Cancel or delete (keeping HA metadata) | Cancel with savepoint                     |
| Restore Mechanism      | Empty state      | Use HA metadata or the latest snapshot | Restore from savepoint                    |
| Production Use         | Not recommended  | Recommended                            | Recommended                               |

{{< hint info >}}
When HA is enabled the `savepoint` upgrade mode may fall back to the `last-state` behavior in cases where the job is in an unhealthy state.
{{< /hint >}}

With `FlinkStateSnapshot` resources enabled, the savepoint taken when suspending in `savepoint` mode is tracked as its own snapshot resource, as described under [Snapshot Management → Upgrade Triggering]({{< ref "docs/managing/snapshot-management#upgrade-triggering" >}}). A `last-state` upgrade creates no snapshot resource, it takes no new snapshot in the first place, reusing the latest checkpoint instead.

The three upgrade modes are intended to support different scenarios:

1. **stateless**: Stateless application upgrades from empty state.
2. **last-state**: Quick upgrades in any application state (even for failing jobs), does not require a healthy job as it always uses the latest checkpoint information. Manual recovery may be necessary if HA metadata is lost. The time the job may fall back when picking up the latest checkpoint can be limited with `kubernetes.operator.job.upgrade.last-state.max.allowed.checkpoint.age`: if the checkpoint is older than the configured value, a savepoint is taken instead for healthy jobs.
3. **savepoint**: Use savepoint for upgrade, providing maximal safety and the possibility to serve as a backup or fork point. The savepoint is created during the upgrade process. Note that the Flink job needs to be running to allow the savepoint to be created. If the job is in an unhealthy state, the last checkpoint is used instead (unless `kubernetes.operator.job.upgrade.last-state-fallback.enabled` is set to `false`). If the last checkpoint is not available, the job upgrade fails.

During stateful upgrades there are always cases that require user intervention to preserve the consistency of the application, covered under [Manual Recovery](#manual-recovery).

Every upgrade mode stops the job briefly. When even that pause is unacceptable, [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}}) run the new version next to the old one instead.

A full example using the `last-state` strategy:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: basic-checkpoint-ha-example
spec:
  image: flink:1.20
  flinkVersion: v1_20
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.savepoints.dir: file:///flink-data/savepoints
    state.checkpoints.dir: file:///flink-data/checkpoints
    high-availability.type: kubernetes
    high-availability.storageDir: file:///flink-data/ha
  serviceAccount: flink
  jobManager:
    resources:
      requests:
        memory: "2048Mi"
        cpu: "1"
  taskManager:
    resources:
      requests:
        memory: "2048Mi"
        cpu: "1"
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          volumeMounts:
            - mountPath: /flink-data
              name: flink-volume
      volumes:
        - name: flink-volume
          hostPath:
            # directory location on host
            path: /tmp/flink
            # this field is optional
            type: Directory
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
    upgradeMode: last-state
    state: running
```

### Restarting Without a Spec Change

Sometimes a Flink deployment has to be restarted to deal with a transient problem. The top-level `spec.restartNonce` field exists for this purpose, setting it to a different value triggers a restart:

```yaml
spec:
  restartNonce: 123
```

Restarts work exactly like other upgrades and follow the semantics of the current `upgradeMode`.

### Scaling Without a Spec Change

Upgrades can also be triggered automatically, with no edit to the resource at all: the [autoscaler]({{< ref "docs/managing/autoscaler" >}}) adjusts the parallelism of a running job on its own, and its scaling decisions are rolled out through the same upgrade cycle, unless the cluster supports applying them in place without a restart.

## Self-Healing

Beyond upgrades, the operator can also repair a job on its own. Each of the following reactions is controlled by its own configuration.

### Recovering Missing Deployments

The operator recovers Flink cluster deployments that were deleted by accident or by an external process. In application mode this requires HA, the job state is restored from the HA metadata, while a session cluster is simply recreated. The behavior is controlled with `kubernetes.operator.jm-deployment-recovery.enabled`, and keeping the default `true` is recommended.

A missing deployment is not something that happens during normal operation and can indicate a deeper problem, so the operator also raises an Error event when it detects one. If a deployment cannot be recovered safely, see [Manual Recovery](#manual-recovery).

### Restarting Unhealthy Deployments

When HA is enabled, the operator can also restart deployments it considers unhealthy. The feature is turned on with `kubernetes.operator.cluster.health-check.enabled` (default `false`) and requires [recovering missing deployments](#recovering-missing-deployments) to be enabled.

A deployment is considered unhealthy when:

- the Flink restart count reaches `kubernetes.operator.cluster.health-check.restarts.threshold` (default `64`) within the time window of `kubernetes.operator.cluster.health-check.restarts.window` (default 2 minutes)
- `kubernetes.operator.cluster.health-check.checkpoint-progress.enabled` is turned on and the number of successful checkpoints does not change within the time window of `kubernetes.operator.cluster.health-check.checkpoint-progress.window` (default 5 minutes)

### Restarting Failed Jobs

With `kubernetes.operator.job.restart.failed` set to `true`, the operator restarts a job that reached the terminal `FAILED` state: the failed job is deleted and redeployed from the latest successful checkpoint. This can be useful when the job is able to reconfigure itself on restart to handle the failure.

## Rollbacks

{{< hint warning >}}
Rollbacks are an experimental feature, currently only supported for `FlinkDeployment` resources.
{{< /hint >}}

The operator supports rolling back a failed upgrade to the last stable spec.

Every upgraded spec starts as unstable. Once the operator observes the new job in a healthy running state, the spec is marked stable, the same distinction the resource reports through its lifecycle state, documented under [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}). If an upgrade is not marked stable within `kubernetes.operator.deployment.readiness.timeout`, a rollback restores the last stable spec.

Rollbacks are enabled with:

```yaml
kubernetes.operator.deployment.rollback.enabled: true
```

HA is currently required for the rollback functionality.

A resource that was suspended before the upgrade is never rolled back to a running state, in that case no rollback is performed.

A rollback restores only the Flink resource itself. A release that also updates surrounding Kubernetes resources, such as ConfigMaps or Services, must keep those changes compatible with both the old and the new spec, since the rollback will not revert them.

### Stability Condition

Currently a job is marked stable as soon as the operator observes it in a running state. This catches obvious errors but is not always enough for more complex issues, more sophisticated stability conditions are expected in the future.

## Failures

Runtime failures are handled inside Flink itself through the job's configured [restart strategy](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/task_failure_recovery/), without operator involvement. The operator reacts once a job fails terminally, with its restart strategy exhausted: the resource enters the `FAILED` lifecycle state ([Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}})), and the failure is recorded in the resource status and reported through Kubernetes events.

A terminally failed job stays failed until acted upon. It can be restarted automatically from the latest checkpoint ([Restarting Failed Jobs](#restarting-failed-jobs)), upgraded with a corrected spec, or restored by hand as described under [Manual Recovery](#manual-recovery). For interpreting the reported errors and the most common failure scenarios, see [Resource Exceptions]({{< ref "docs/operations/debugging/resource-exceptions" >}}).

## Manual Recovery

Some situations require manual intervention to recover a job or to restore it to a user-specified state. In most of them the deployment got into a state where the operator can no longer determine the health of the application or the latest checkpoint information to use for recovery. These cases are not common, but they must be recoverable. There are two ways to restore a job from a target savepoint or checkpoint.

### Redeploying from a Savepoint

A `FlinkDeployment` or `FlinkSessionJob` resource can be redeployed from a target savepoint by using the combination of `spec.job.savepointRedeployNonce` and `spec.job.initialSavepointPath`:

```yaml
job:
  initialSavepointPath: file://redeploy-target-savepoint
  # If not set previously, set to 1, otherwise increment, e.g. 2
  savepointRedeployNonce: 1
```

When the `savepointRedeployNonce` changes, the operator redeploys the job to the savepoint defined in `initialSavepointPath`. The savepoint path must not be empty.

{{< hint warning >}}
Rollbacks are not supported after redeployments.
{{< /hint >}}

### Deleting and Recreating the Resource

Deleting and recreating the custom resource resolves almost any issue by fully resetting the status information. The savepoint history is lost with it, and past periodic savepoints taken before the deletion are not cleaned up afterwards.

1. Locate the latest checkpoint or savepoint metafile in the configured checkpoint or savepoint directory.
2. Delete the resource.
3. Verify that the snapshot from step 1 is present and the resource is fully deleted.
4. Set `spec.job.initialSavepointPath` to the located snapshot path.
5. Recreate the resource.

The operator then starts completely fresh from the given path and can recover fully. Investigating what caused the problem in the first place is still worthwhile.

## State

What the operator knows about a job is reported through the resource status, whose lifecycle states and transitions are documented under [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}). How the status is persisted, and what deleting a resource erases, is covered under [State → Custom Resource Status]({{< ref "docs/operations/state#custom-resource-status" >}}).

## Metrics

The operator exports lifecycle metrics for every resource: how many resources sit in each lifecycle state, and how long transitions such as upgrades and rollbacks take. They are covered under [Metrics]({{< ref "docs/operations/metrics#operator-custom-resource-metrics" >}}).

## Events

Every operation on this page is accompanied by Kubernetes events on the resource, submissions, suspensions, upgrades, rollbacks, restarts, and errors, catalogued under [Events]({{< ref "docs/operations/events#operator-events" >}}).

## Limitations

### Deleting

- Deletion is destructive for the operator's tracked state: a deleted resource's status and checkpoint history are gone, and a later redeployment starts from empty state unless a snapshot path is provided explicitly or a savepoint was taken on deletion.

### Upgrades

- The `savepoint` upgrade mode needs a running job at upgrade time. For unhealthy jobs it falls back to the latest checkpoint only when HA is enabled and the last-state fallback is not disabled.
- The `last-state` upgrade mode depends on accessible HA metadata, and manual recovery may be necessary when it is lost.
- Changing `spec.flinkVersion` while a job is suspended in `last-state` upgrade mode is not allowed, the job must first be restored with the current version and upgraded afterwards.
- Upgrades react only to spec changes: modifications to referenced Kubernetes resources, such as ConfigMaps or Secrets, are invisible to the operator, and rolling them out requires a manual restart through `restartNonce`.
- Session cluster upgrades are full replacements: the cluster is deleted and redeployed on every spec change, a rolling upgrade is not available.
- Simultaneous spec changes to a session cluster `FlinkDeployment` and its `FlinkSessionJob` resources are not coordinated beyond the JobManager readiness check, and can lead to lost or duplicated job submissions. Updating the session cluster first and the session jobs after it stabilizes avoids this.

### Self-Healing

- Restarting unhealthy deployments exists only for application-mode `FlinkDeployment` resources and requires HA. Recovering missing deployments exists for both `FlinkDeployment` modes, and requires HA only in application mode, where the job state is restored through the HA metadata.

### Rollbacks

- Rollbacks are experimental: only `FlinkDeployment` resources are supported, HA is required, and only the Flink resource itself is restored, surrounding Kubernetes resources are not reverted.
- No rollback is performed after a redeployment through `savepointRedeployNonce`, or when the resource was suspended before the upgrade.
- The stability condition behind rollbacks is currently simple, a job counts as stable once observed running, so subtler failures go undetected.
