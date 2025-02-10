---
title: "Snapshots"
weight: 3
type: docs
aliases:
  - /custom-resource/snapshots.html
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

# Snapshots

To create, list and delete snapshots you can use the custom resource called FlinkStateSnapshot. 
The operator will use the same controller flow as in the case of FlinkDeployment and FlinkSessionJob to trigger the savepoint/checkpoint and observe its status.

This feature deprecates the old `savepointInfo` and `checkpointInfo` fields found in the Flink resource CR status, alongside with spec fields `initialSavepointPath`, `savepointTriggerNonce` and `checkpointTriggerNonce`. 
It is enabled by default using the configuration option `kubernetes.operator.snapshot.resource.enabled`. 
If you set this to false, the operator will keep using the deprecated status fields to track snapshots.

## Overview

To create a savepoint or checkpoint, exactly one of the spec fields `savepoint` or `checkpoint` must present. 
Furthermore, in case of a savepoint you can signal to the operator that the savepoint already exists using the `alreadyExists` field, and the operator will mark it as a successful snapshot in the next reconciliation phase.

You can also instruct the Operator to start a new FlinkDeployment/FlinkSessionJob from an existing snapshot by using `initialSavepointPath` in the job spec.

## Examples

### Savepoint

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
    alreadyExists: false  # optional (default: false), if true, the path is considered to already exist and state will be COMPLETED on first reconciliation
    disposeOnDelete: true  # optional (default: true), dispose of savepoint when this FlinkStateSnapshot is removed, job needs to be running
    formatType: CANONICAL  # optional (default: CANONICAL), format type of savepoint
    path: /flink-data/savepoints-custom  # optional (default: job savepoint path)
```

### Checkpoint

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

### Start job from existing snapshot

To start a job from an existing snapshot, you need to extract the path then use:

```yaml
 job:
   initialSavepointPath: [savepoint_path]
```

{{< hint warning >}}
While it is possible to start a job from a FlinkStateSnapshot with checkpoint type, checkpoint data is owned by Flink, and might be deleted by Flink anytime after triggering the checkpoint.
{{< /hint >}}


## Snapshot CR lifecycle

### Snapshot creation

When a new FlinkStateSnapshot CR is created, in the first reconciliation phase the operator will trigger the savepoint/checkpoint for the linked deployment via REST API. 
The resulting trigger ID will be added to the CR Status.

In the next observation phase the operator will check all the in-progress snapshots and query their state. 
If the snapshot was successful, the path will be added to the CR Status.

If the triggered snapshot is a savepoint and `spec.savepoint.alreadyExists` is set to true, on the first reconciliation the operator will populate its `status` fields with `COMPLETED` state, and copy the savepoint path found in the spec to `status.path`.

### Snapshot errors

If the operator encountered any errors during snapshot observation/reconciliation, the `error` field will be populated in the CR status and the `failures` field will be incremented by 1. 
If the backoff limit specified in the spec is reached, the snapshot will enter a `FAILED` state, and won't be retried. 
If it's not reached, the Operator will continuously back off retrying the snapshot (10s, 20s, 40s, ...).

In case of any error there will also be a new Event generated for the snapshot resource containing the error message.

{{< hint info >}}
For checkpoints, after the operator has ensured that the checkpoint was successful, it will attempt to fetch its final path via Flink REST API. 
Any errors experienced during this step will generate a Kubernetes event, but will not populate the `error` field, and will mark the checkpoint as `COMPLETED`.
The `path` field will stay empty though.
{{< /hint >}}

### Snapshot abandonment

If the referenced Flink job can't be found or is stopped after triggering a snapshot, the state of the snapshot will be `ABANDONED` and won't be retried.

### Savepoint disposal on deletion

In case of savepoints, if `spec.savepoint.disposeOnDelete` is true, the operator will automatically dispose the savepoint on the filesystem when the CR gets deleted. 
This however requires the referenced Flink resource to be alive, as this operation is done using Flink REST API.

This feature is not available for checkpoints.

## Triggering snapshots

Upgrade savepoints are triggered automatically by the system during the upgrade process as we have seen in the previous sections.
In this case, the savepoint path will also be recorded in the `upgradeSavepointPath` job status field, which the operator will use when restarting the job.

For backup, job forking and other purposes savepoint and checkpoints can be triggered manually or periodically by the operator, however generally speaking these will not be used during upgrades and are not required for the correct operation.

### Manual Checkpoint Triggering

Users can trigger snapshots manually by defining a new (different/random) value to the variable `savepointTriggerNonce` or `checkpointTriggerNonce` in the job specification:

```yaml
 job:
    ...
    savepointTriggerNonce: 123
    checkpointTriggerNonce: 123
    ...
```

Changing the nonce value will trigger a new snapshot. If FlinkStateSnapshot resources are enabled, a new snapshot CR will be automatically created.
If disabled, information about pending and last snapshots is stored in the FlinkDeployment/FlinkSessionJob CR status.

### Periodic Snapshot Triggering

The operator also supports periodic snapshot triggering through the following config option which can be configured on a per job level:

```yaml
 flinkConfiguration:
    ...
    kubernetes.operator.periodic.savepoint.interval: 6h
    kubernetes.operator.periodic.checkpoint.interval: 6h
```

There is no guarantee on the timely execution of the periodic snapshots as they might be delayed by unhealthy job status or other interfering user operation.

### Snapshot History

The operator automatically keeps track of the snapshot history triggered by upgrade, manual and periodic snapshot operations.
This is necessary so cleanup can be performed by the operator for old snapshots.

{{< hint info >}}
Snapshot cleanup happens lazily and only when the Flink resource associated with the snapshot is running.
It is therefore very likely that savepoints live beyond the max age configuration.
{{< /hint >}}

#### Savepoints

Users can control the cleanup behaviour by specifying maximum age and maximum count for savepoints.
If a max age is specified, FlinkStateSnapshot resources of savepoint type will be cleaned up based on the `metadata.creationTimestamp` field.
Snapshots will be cleaned up regardless of their status, but the operator will always keep at least 1 completed FlinkStateSnapshot for every Flink job at all time.

Example configuration:
```
kubernetes.operator.savepoint.history.max.age: 24 h
kubernetes.operator.savepoint.history.max.count: 5
```

To also dispose of savepoint data on savepoint cleanup, set `kubernetes.operator.savepoint.dispose-on-delete: true`.
This config will set `spec.savepoint.disposeOnDelete` to true for FlinkStateSnapshot CRs created by upgrade, periodic and manual savepoints created using `savepointTriggerNonce`.

To disable automatic savepoint cleanup by the operator you can set `kubernetes.operator.savepoint.cleanup.enabled: false`.

#### Checkpoints

FlinkStateSnapshots of checkpoint type will always be cleaned up. It's not possible to set max age for them.
The maxmimum amount of checkpoint resources retained will be deteremined by the Flink configuration `state.checkpoints.num-retained`.

{{< hint warning >}}
Checkpoint cleanup is only supported if FlinkStateSnapshot resources are enabled.
This operation will only delete the FlinkStateSnapshot CR, and will never delete any checkpoint data on the filesystem.
{{< /hint >}}


### Snapshot History For Legacy Savepoints

Legacy savepoints found in FlinkDeployment/FlinkSessionJob CRs under the deprecated `status.jobStatus.savepointInfo.savepointHistory` will be cleaned up:
- For max age, it will be cleaned up when its trigger timestamp exceeds max age
- For max count and FlinkStateSnapshot resources **disabled**, it will be cleaned up when `savepointHistory` exceeds max count
- For max count and FlinkStateSnapshot resources **enabled**, it will be cleaned up when `savepointHistory` + number of FlinkStateSnapshot CRs related to the job exceed max count


## Advanced Snapshot Filtering

At the end of each snapshot reconciliation phase, the operator will update its labels to reflect the latest status and spec of the resources.
This will allow the Kubernetes API server to filter snapshots without having to query all resources, since filtering by status or spec fields of custom resources is not supported in Kubernetes by default.
Example queries with label selectors using `kubectl`:
```shell
# Query all checkpoints
kubectl -n flink get flinksnp -l 'snapshot.type=CHECKPOINT'

# Query all savepoints with states
kubectl -n flink get flinksnp -l 'snapshot.state in (COMPLETED,ABANDONED),snapshot.type=SAVEPOINT'

# Query all savepoints/checkpoints with job reference
kubectl -n flink get flinksnp -l 'job-reference.kind=FlinkDeployment,job-reference.name=test-job'
```