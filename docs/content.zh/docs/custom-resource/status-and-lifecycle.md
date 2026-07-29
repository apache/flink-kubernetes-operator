---
title: "Status and Lifecycle"
weight: 2
type: docs
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

# Status and Lifecycle

Every Flink resource reports what the operator knows about it through its status subresource. This page documents that contract for the two job-carrying resources, `FlinkDeployment` and `FlinkSessionJob`, starting with the lifecycle they move through. `FlinkStateSnapshot` and `FlinkBlueGreenDeployment` follow their own, simpler state models: the snapshot states are documented under [FlinkStateSnapshot]({{< ref "docs/custom-resource/overview#flinkstatesnapshot" >}}) with their mechanics under [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}), and the blue/green transition states under [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}}).

## Flink Resource Lifecycle

Every managed resource moves through a well-defined set of lifecycle states, tracked as `status.lifecycleState` and computed from the fields documented in [Status Structure](#status-structure) below. The chart illustrates the states, their transitions, and what the job is doing in each of them:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/cr-status-and-lifecycle.svg" alt="Flink resource lifecycle states and transitions" >}}

| State          | Description                                                                                      |
|----------------|--------------------------------------------------------------------------------------------------|
| `CREATED`      | The resource was created in Kubernetes but is not yet handled by the operator                    |
| `SUSPENDED`    | The job has been suspended, its state information is retained                                    |
| `UPGRADING`    | The resource is being upgraded to a new spec                                                     |
| `DEPLOYED`     | The resource is deployed and running, but not yet considered stable, it may still be rolled back |
| `STABLE`       | The deployment was observed healthy and is considered stable, it will not be rolled back         |
| `ROLLING_BACK` | The resource is being rolled back to the last stable spec                                        |
| `ROLLED_BACK`  | The resource is deployed with the last stable spec after a failed upgrade                        |
| `FAILED`       | The job terminally failed                                                                        |
| `DELETING`     | The resource is being deleted and the operator is cleaning up                                    |
| `DELETED`      | The resource is deleted                                                                          |

The colors in the figure answer the operational question directly: whether a job is running in that state (provisionally or stably) or not running, whether an operator action is in progress, or whether the resource reached a terminal end. The distinction between `DEPLOYED` and `STABLE` is what powers rollbacks, covered under [Lifecycle Management]({{< ref "docs/concepts/lifecycle-management#self-healing-and-rollback" >}}).

## Status Structure

The status is the operator's write-back half of the contract: users declare the spec, the operator answers here, and nothing else writes it. Its content falls into two groups, observations of the running system (the job, and for deployments the cluster around it) and the operator's own bookkeeping of what it has acted on. `FlinkDeployment` and `FlinkSessionJob` share the same core, extended for deployments with cluster-level information:

| Field                        | Resources         | Description                                                                                                                                                                |
|------------------------------|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `lifecycleState`             | Both              | The [lifecycle state](#flink-resource-lifecycle) of the resource, computed from the rest of the status                                                                     |
| `reconciliationStatus`       | Both              | The operator's bookkeeping of the last acted-on specs, see [Reconciliation Status](#reconciliation-status)                                                                 |
| `jobStatus`                  | Both              | The last observed state of the Flink job, see [Job Status](#job-status)                                                                                                    |
| `error`                      | Both              | The last error the operator encountered for the resource, interpreted under [Debugging → Resource Exceptions]({{< ref "docs/operations/debugging/resource-exceptions" >}}) |
| `observedGeneration`         | Both              | The generation of the resource the operator last observed                                                                                                                  |
| `jobManagerDeploymentStatus` | `FlinkDeployment` | The observed state of the JobManager's Kubernetes Deployment, see [JobManager Deployment Status](#jobmanager-deployment-status)                                            |
| `clusterInfo`                | `FlinkDeployment` | The Flink version and revision of the running cluster, its total CPU and memory, the last observed state size, and the operator's cluster health bookkeeping               |
| `taskManager`                | `FlinkDeployment` | The TaskManager label selector and replica count backing the Kubernetes scale subresource, populated in application mode only                                              |
| `conditions`                 | `FlinkDeployment` | Kubernetes-standard conditions derived from the status, see [Conditions](#conditions)                                                                                      |

### Reconciliation Status

`status.reconciliationStatus` is the operator's memory: the desired state it has already acted on, kept separately from the live spec. Every newly applied spec is diffed against `lastReconciledSpec`, and the diff's verdict decides the action: changes classified as ignorable (see [Spec Diffing]({{< ref "docs/custom-resource/overview#spec-diffing" >}})) trigger no action at all, everything else a scale or an upgrade cycle. `lastReconciledSpec` therefore only ever holds a spec the operator has acted on, ignored changes are not absorbed into it. `lastStableSpec` is what a rollback restores, which makes this block the backbone of upgrades and rollbacks:

| Field                     | Description                                                                                                                          |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `lastReconciledSpec`      | A serialized copy of the spec the operator last successfully acted on                                                                |
| `lastStableSpec`          | A serialized copy of the last spec observed stable, the target of a [rollback]({{< ref "docs/managing/job-management#rollbacks" >}}) |
| `reconciliationTimestamp` | The timestamp of the last reconciliation                                                                                             |
| `state`                   | Where the current spec stands in the upgrade cycle                                                                                   |

The two spec copies form a leader and a follower. Every acted-on spec lands in `lastReconciledSpec` first, and once the deployment is observed healthy the same spec is recorded as `lastStableSpec`. Between an upgrade and its stabilization the two differ, which is exactly the window in which a rollback can still happen, and the moment they match again is what the `STABLE` lifecycle state reports.

The `state` field is the operator's cursor through that cycle. It marks how far the operator has come in acting on the latest spec, from picking it up to having it running, or back on the rollback path, and it moves through four values:

| State          | Meaning                                                       |
|----------------|---------------------------------------------------------------|
| `DEPLOYED`     | The last reconciled spec is currently deployed                |
| `UPGRADING`    | The resource is being upgraded to the latest spec             |
| `ROLLING_BACK` | The resource is being rolled back to the last stable spec     |
| `ROLLED_BACK`  | The resource runs the last stable spec after a failed upgrade |

### Job Status

`status.jobStatus` is the observed truth about the job itself, refreshed from the Flink REST API on every reconciliation pass that finds the cluster reachable. It answers whether the job is actually running, under which ID, and since when, independent of what the spec wishes:

| Field                  | Description                                                                                      |
|------------------------|--------------------------------------------------------------------------------------------------|
| `jobName`              | The name of the job                                                                              |
| `jobId`                | The ID of the job on the Flink cluster                                                           |
| `state`                | The last observed job state as reported by Flink, for example `RUNNING`, `FINISHED`, or `FAILED` |
| `startTime`            | The start time of the job                                                                        |
| `updateTime`           | The time of the last observed state change                                                       |
| `upgradeSavepointPath` | The savepoint recorded for the current upgrade cycle, used when the job is restored              |

{{< hint info >}}
The deprecated `savepointInfo` and `checkpointInfo` blocks are superseded by the `FlinkStateSnapshot` resource, covered under [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}).
{{< /hint >}}

### JobManager Deployment Status

For `FlinkDeployment` resources the operator also tracks the Kubernetes side of the cluster: `status.jobManagerDeploymentStatus` reports what the observer found of the JobManager's Deployment. It separates a broken cluster from a broken job, and it gates communication. Only a `READY` JobManager is asked about job state over REST:

| Value                | Meaning                                                                                    |
|----------------------|--------------------------------------------------------------------------------------------|
| `READY`              | The JobManager is running and ready to receive REST API calls                              |
| `DEPLOYED_NOT_READY` | The JobManager is running but not yet ready to receive REST API calls                      |
| `DEPLOYING`          | The JobManager process is starting up                                                      |
| `MISSING`            | The JobManager deployment was not found                                                    |
| `ERROR`              | The deployment failed terminally, a spec change is required for reconciliation to continue |

The operator exports the number of resources per tracked value as a metric, described under [Metrics → FlinkDeployment JobManager Deployment Status Tracking]({{< ref "docs/operations/metrics#flinkdeployment-jobmanager-deployment-status-tracking" >}}).

### Conditions

Conditions are the ecosystem-facing summary: tooling that understands Kubernetes conventions but not operator-specific fields can read readiness here. `FlinkDeployment` resources expose a single condition of type `Running`, refreshed on every reconciliation pass. In application mode it reports `True` while the observed job state is `RUNNING`, in session mode while the JobManager deployment is `READY`, with the reason and message carrying the underlying state. The `lastTransitionTime` is preserved for as long as the condition status does not change, and the condition plugs into standard Kubernetes tooling:

```shell
kubectl wait --for=condition=Running flinkdeployment/basic-example
```

## Correlating the Statuses

The lifecycle state is the summary and the fields above are its inputs, so the values constrain each other. The table answers, for each lifecycle state, what the other fields can hold:

| Lifecycle State | Reconciliation Status               | Job Status                                 | JobManager Deployment Status                            |
|-----------------|-------------------------------------|--------------------------------------------|---------------------------------------------------------|
| `CREATED`       | `UPGRADING`, no spec reconciled yet | Empty, no job observed yet                 | `MISSING`                                               |
| `SUSPENDED`     | `DEPLOYED` or `ROLLED_BACK`         | `FINISHED`, recorded at shutdown           | `MISSING`                                               |
| `UPGRADING`     | `UPGRADING`                         | Any, the job is being stopped and replaced | Any                                                     |
| `DEPLOYED`      | `DEPLOYED`                          | Any except `FAILED`                        | Typically `DEPLOYING`, `DEPLOYED_NOT_READY`, or `READY` |
| `STABLE`        | `DEPLOYED`                          | Any except `FAILED`, typically `RUNNING`   | Typically `READY`                                       |
| `ROLLING_BACK`  | `ROLLING_BACK`                      | Any                                        | Any                                                     |
| `ROLLED_BACK`   | `ROLLED_BACK`                       | Any except `FAILED`                        | Typically `READY`                                       |
| `FAILED`        | Any                                 | `FAILED`, or any on unrecoverable errors   | `MISSING` or `ERROR` on unrecoverable errors            |
| `DELETING`      | Frozen at the last values           | Frozen at the last values                  | Frozen at the last values                               |
| `DELETED`       | Frozen at the last values           | Frozen at the last values                  | Frozen at the last values                               |

Three details sit outside the table. `DEPLOYED` and `STABLE` share the same field values, what separates them is the reconciliation bookkeeping: the resource is `STABLE` once `lastStableSpec` matches `lastReconciledSpec`. `SUSPENDED` is recognized from the last reconciled spec, whose job state records the suspension. And `FAILED` is reached on three paths: an error before the first deployment, an observed `FAILED` job, or, for `FlinkDeployment` resources, a lost or broken JobManager deployment whose `error` field reports an unrecoverable condition.

The table assumes a job is present, a `FlinkDeployment` in application mode or a `FlinkSessionJob` (which has no `jobManagerDeploymentStatus` of its own). A session-mode `FlinkDeployment` differs in a fixed way: with no `spec.job` there is nothing to fill `jobStatus`, so its fields stay empty, the `SUSPENDED` state does not occur, the spec is marked stable once the running cluster's REST endpoint responds, and `FAILED` can only be reached through the pre-deployment and JobManager paths.

## Observing the Status

The two most important fields are exposed as printer columns, so a plain `kubectl get` already answers the operational question:

```shell
kubectl get flinkdeployment
NAME            JOB STATUS   LIFECYCLE STATE
basic-example   RUNNING      STABLE
```

The full status is part of the resource, available through `kubectl describe` or `kubectl get -o yaml`. How the status is persisted, and what deleting the resource erases, is covered under [State → Custom Resource Status]({{< ref "docs/operations/state#custom-resource-status" >}}).
