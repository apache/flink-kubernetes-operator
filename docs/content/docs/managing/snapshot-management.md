---
title: "Snapshot Management"
weight: 2
type: docs
aliases:
  - /custom-resource/snapshots.html
  - /docs/custom-resource/snapshots/
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

# Snapshot Management

{{< hint warning >}}
`FlinkStateSnapshot` resources are an experimental feature. The API and the behavior described on this page may still change between releases.
{{< /hint >}}

A `FlinkStateSnapshot` turns savepoints and checkpoints into first-class Kubernetes resources: snapshots are created, listed, and deleted declaratively, and the operator drives each operation through the same reconcile-and-observe cycle as the other resources. How the two snapshot kinds relate, and their role in the job lifecycle, is described under [Lifecycle Management → Snapshots]({{< ref "docs/concepts/lifecycle-management#snapshots" >}}). Beyond replacing manual snapshot handling it provides:

- **A tracked history**: every savepoint and checkpoint is an object with its own state and error reporting, filterable through labels.
- **Automated housekeeping**: periodic triggering, history cleanup, and savepoint disposal run without manual effort.
- **A restore inventory**: completed snapshots record their location, ready for restores, backups, and job forking.

The resource and its fields are introduced under [Custom Resource → Overview]({{< ref "docs/custom-resource/overview#flinkstatesnapshot" >}}). It supersedes the deprecated `savepointInfo` and `checkpointInfo` status blocks and the `savepointTriggerNonce` and `checkpointTriggerNonce` spec fields, and it is enabled by default through `kubernetes.operator.snapshot.resource.enabled`, disabling it makes the operator fall back to the deprecated status fields for tracking snapshots.

## Creating Snapshots

A snapshot is created like any other resource, by applying a `FlinkStateSnapshot` manifest. Each resource represents a single savepoint or checkpoint operation against the referenced job, and exactly one of the spec blocks `savepoint` or `checkpoint` is set, selecting which of the two is taken. The `savepoint` block has four fields, all optional:

| Field             | Type    | Description                                                                                                                                       |
|-------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `path`            | String  | The target path of the savepoint, defaulting to the savepoint directory configured on the job                                                     |
| `formatType`      | enum    | The savepoint format, `CANONICAL` (default) or `NATIVE`                                                                                           |
| `disposeOnDelete` | Boolean | Dispose of the savepoint data when the resource is deleted, default `true`, see [Savepoint Disposal on Deletion](#savepoint-disposal-on-deletion) |
| `alreadyExists`   | Boolean | Signals that the path already exists, the snapshot is marked `COMPLETED` on the first reconciliation without triggering anything, default `false` |

The `checkpoint` block carries no fields. The exhaustive, generated field list lives in the [Reference]({{< ref "docs/custom-resource/reference" >}}).

### Savepoint

A savepoint snapshot with every field spelled out, referencing its target job and writing to a custom path:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkStateSnapshot
metadata:
  name: example-savepoint
spec:
  backoffLimit: 1  # retry count, -1 for infinite, 0 for no retries (default: -1)
  jobReference:
    kind: FlinkDeployment  # FlinkDeployment or FlinkSessionJob
    name: example-deployment  # name of the resource
  savepoint:
    alreadyExists: false
    disposeOnDelete: true
    formatType: CANONICAL
    path: /flink-data/savepoints-custom
```

### Checkpoint

A checkpoint snapshot of the same job, the `checkpoint` block stays empty since checkpoints take no parameters:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkStateSnapshot
metadata:
  name: example-checkpoint
spec:
  backoffLimit: 1
  jobReference:
    kind: FlinkDeployment
    name: example-deployment
  checkpoint: {}
```

## Snapshot Lifecycle

Every snapshot moves through a small state machine, reported in `status.state`:

| State             | Meaning                                                                                     |
|-------------------|---------------------------------------------------------------------------------------------|
| `TRIGGER_PENDING` | The snapshot is created but not yet processed by the operator                               |
| `IN_PROGRESS`     | The snapshot was triggered on the referenced job and is running                             |
| `COMPLETED`       | The snapshot succeeded and its location is recorded in `status.path`                        |
| `FAILED`          | The snapshot failed and exhausted its retry budget                                          |
| `ABANDONED`       | The referenced job failed, was upgraded, or stopped after triggering, no retry is attempted |

Retries loop between the two live states until the backoff limit is exhausted, and a savepoint marked `alreadyExists` jumps straight to `COMPLETED`:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/managing/snapshot-lifecycle.svg" alt="FlinkStateSnapshot lifecycle states and transitions" >}}

### Snapshot Creation

When a new `FlinkStateSnapshot` resource is created, the operator triggers the savepoint or checkpoint on the referenced job through the Flink REST API in the first reconciliation, recording the resulting trigger id in `status.triggerId`. The following observation phases check every in-progress snapshot and query its state, and once the snapshot completes, its location is recorded in `status.path`.

If the snapshot is a savepoint with `spec.savepoint.alreadyExists` set, the operator instead marks it `COMPLETED` on the first reconciliation and copies the path from the spec to `status.path`.

### Snapshot Errors

An error during snapshot reconciliation or observation is recorded in `status.error` and increments `status.failures`. When the failure count exceeds `spec.backoffLimit`, the snapshot enters the `FAILED` state and is not retried. Below the limit the operator keeps retrying with exponential backoff (10s, 20s, 40s, ...). Every error also generates a Kubernetes event on the snapshot resource, see [Events]({{< ref "docs/operations/events" >}}).

{{< hint info >}}
For checkpoints, after the operator has confirmed a successful completion, it attempts to fetch the final checkpoint path through the Flink REST API. Errors during this step generate a Kubernetes event but do not populate `status.error`, the checkpoint is marked `COMPLETED` with an empty `status.path`.
{{< /hint >}}

### Starting a Job from an Existing Snapshot

A new `FlinkDeployment` or `FlinkSessionJob` starts from an existing snapshot by carrying its path in the job spec. Despite its savepoint-specific name, the field accepts the path of either snapshot type:

```yaml
job:
  initialSavepointPath: <savepoint-path>
```

{{< hint warning >}}
While a job can start from a `FlinkStateSnapshot` of checkpoint type, checkpoint data is owned by Flink and may be deleted by Flink at any time after the checkpoint was triggered.
{{< /hint >}}

### Savepoint Disposal on Deletion

With `spec.savepoint.disposeOnDelete` enabled, the operator disposes the savepoint data on the filesystem when the snapshot resource is deleted. The disposal runs through the Flink REST API, so it requires the referenced Flink job to still be running. Checkpoints have no disposal support.

## Triggering Snapshots

Snapshots enter the system from three sources: upgrades, manual requests, and periodic triggering. Only the upgrade savepoints are required for correct operation, manual and periodic snapshots serve backups, job forking, and similar purposes.

### Upgrade Triggering

Upgrade savepoints are triggered automatically during stateful upgrades, as described under [Job Management → Upgrades]({{< ref "docs/managing/job-management#upgrades" >}}). The savepoint path is also recorded in the `upgradeSavepointPath` job status field, which the operator uses when restoring the job.

### Manual Triggering

A manual snapshot is triggered by creating a `FlinkStateSnapshot` resource directly, as shown under [Creating Snapshots](#creating-snapshots).

The deprecated alternative is setting a new arbitrary value for `savepointTriggerNonce` or `checkpointTriggerNonce` in the job spec:

```yaml
job:
  savepointTriggerNonce: 123
```

Changing the nonce value triggers a new snapshot. With `FlinkStateSnapshot` resources enabled, the operator then creates the matching snapshot resource automatically. With them disabled, pending and completed snapshot information is tracked in the `FlinkDeployment` or `FlinkSessionJob` status instead.

### Periodic Triggering

The operator also triggers snapshots periodically, configured per job:

```yaml
flinkConfiguration:
  kubernetes.operator.periodic.savepoint.interval: 6h
  kubernetes.operator.periodic.checkpoint.interval: 6h
```

There is no guarantee on the timely execution of periodic snapshots, they can be delayed by an unhealthy job status or other interfering operations.

## Snapshot History

The operator keeps track of the snapshot history and cleans old snapshots up. Cleanup covers only the snapshots the operator creates itself, periodic and upgrade ones: manually created `FlinkStateSnapshot` resources are exempt and stay until deleted.

{{< hint info >}}
Snapshot cleanup happens lazily and only while the Flink resource associated with the snapshot is running, so savepoints are likely to live beyond the configured maximum age.
{{< /hint >}}

### Savepoints

Savepoint cleanup is controlled by a maximum age and a maximum count:

```yaml
kubernetes.operator.savepoint.history.max.age: 24h
kubernetes.operator.savepoint.history.max.count: 5
```

With a maximum age set, operator-created `FlinkStateSnapshot` resources of savepoint type are cleaned up based on their `metadata.creationTimestamp`. Snapshots are cleaned up regardless of their status, but the operator always retains at least one completed snapshot for every job.

To also dispose of the savepoint data during cleanup, `kubernetes.operator.savepoint.dispose-on-delete: true` sets `spec.savepoint.disposeOnDelete` on every snapshot resource the operator creates for upgrade, periodic, and nonce-triggered savepoints. Automatic savepoint cleanup is disabled entirely with `kubernetes.operator.savepoint.cleanup.enabled: false`.

### Checkpoints

Operator-created `FlinkStateSnapshot` resources of checkpoint type are always cleaned up, a maximum age cannot be set for them. The number of retained checkpoint resources follows the Flink configuration `state.checkpoints.num-retained`.

{{< hint warning >}}
Checkpoint cleanup is only supported with `FlinkStateSnapshot` resources enabled. It only deletes the `FlinkStateSnapshot` resource, checkpoint data on the filesystem is never deleted.
{{< /hint >}}

### Legacy Savepoints

Legacy savepoints tracked in a `FlinkDeployment` or `FlinkSessionJob` under the deprecated `status.jobStatus.savepointInfo.savepointHistory` are cleaned up as well:

- For the maximum age, a savepoint is cleaned up once its trigger timestamp exceeds the age.
- For the maximum count with `FlinkStateSnapshot` resources disabled, cleanup starts when `savepointHistory` exceeds the count.
- For the maximum count with `FlinkStateSnapshot` resources enabled, cleanup starts when `savepointHistory` and the job's `FlinkStateSnapshot` resources together exceed the count.

## Advanced Snapshot Filtering

At the end of every snapshot reconciliation the operator updates the resource's labels to mirror its latest spec and status. This lets the Kubernetes API server filter snapshots without reading every resource, since filtering custom resources by spec or status fields is not supported by Kubernetes itself. Example queries with label selectors:

```shell
# All checkpoints
kubectl -n flink get flinksnp -l 'snapshot.type=CHECKPOINT'

# All completed or abandoned savepoints
kubectl -n flink get flinksnp -l 'snapshot.state in (COMPLETED,ABANDONED),snapshot.type=SAVEPOINT'

# All snapshots referencing a job
kubectl -n flink get flinksnp -l 'job-reference.kind=FlinkDeployment,job-reference.name=test-job'
```

## State

Everything the operator knows about a snapshot is reported through the resource status, whose fields and state machine are documented under [Custom Resource → Overview]({{< ref "docs/custom-resource/overview#flinkstatesnapshot" >}}), and mirrored into the labels described [above](#advanced-snapshot-filtering). How the status is persisted, and what deleting a resource erases, is covered under [State → Custom Resource Status]({{< ref "docs/operations/state#custom-resource-status" >}}).

## Metrics

The operator exports snapshot metrics per namespace: the number of tracked savepoints and checkpoints, and their counts per state. They are covered under [Metrics]({{< ref "docs/operations/metrics#operator-custom-resource-metrics" >}}).

## Events

Snapshot operations are accompanied by Kubernetes events on the involved resources, savepoint and checkpoint errors, abandonment, validation and cleanup failures, catalogued under [Events]({{< ref "docs/operations/events#snapshots" >}}).

## Limitations

- A `COMPLETED` checkpoint-type resource does not guarantee its data still exists: checkpoint data stays under Flink's control and may be deleted by Flink at any time, and the operator offers no disposal for it either.
- `status.path` is best-effort for checkpoints, a `COMPLETED` checkpoint can carry an empty path.
- Checkpoint-type resources have no maximum-age control, their retention can only follow `state.checkpoints.num-retained`.
- The configured maximum age is not enforced on time: cleanup is lazy and runs only while the associated resource is running, so snapshots regularly outlive it.
- History cleanup applies only to operator-created snapshots: manually created `FlinkStateSnapshot` resources are never cleaned up automatically, the operator does not delete user-created resources.
- Savepoint disposal requires the referenced job to be running: once the job is gone, `disposeOnDelete` cannot act and the savepoint data must be removed manually. A failing disposal also blocks the deletion of the resource, retried at the reconcile interval, so that a requested disposal never leaks data silently. Clearing `disposeOnDelete` on the deleting resource releases the deletion.
- Periodic triggering is interval-based only, cron-style schedules are not supported, and execution carries no timing guarantee, snapshots can be delayed by an unhealthy job or other interfering operations.
