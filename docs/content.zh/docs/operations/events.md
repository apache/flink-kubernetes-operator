---
title: "Events"
weight: 4
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

# Events

The Flink Kubernetes Operator emits [Kubernetes Events](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/) to surface notable transitions and conditions for the resources it manages (`FlinkDeployment`, `FlinkSessionJob`, `FlinkStateSnapshot`). `FlinkBlueGreenDeployment` resources emit no events of their own: failures surface through their status and metrics, while their two child deployments emit the standard events below, as described under [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments#events" >}}). Events provide a lightweight, time-ordered audit trail that complements the resource `status` field and the operator logs.

Events can be inspected with `kubectl`:

```bash
kubectl describe flinkdeployment <name>
kubectl get events --field-selector involvedObject.name=<name>
```

Each event carries:

 - **Reason**: a short, machine-readable code identifying what happened (e.g. `Submit`, `Rollback`, `ScalingReport`).
 - **Type**: `Normal` for expected lifecycle transitions and informational signals, `Warning` for errors and conditions that require operator attention.
 - **Component**: the subsystem that produced the event. One of `Operator`, `JobManagerDeployment`, `Job`, `Snapshot`.
 - **Message**: a human-readable description, often including job IDs, vertex IDs, exception text or scaling summaries.

## Retention

Event retention is reporter-specific. Under the default Kubernetes reporter, events follow the standard Kubernetes event TTL (one hour by default, configured on the API server via `--event-ttl`). For longer retention, forward events to an external sink (see [Custom Flink Resource Listeners]({{< ref "docs/deployment/plugins#custom-flink-resource-listeners" >}})), or run the standalone autoscaler with the JDBC reporter (see [Event Reporters](#event-reporters) for the alternatives).

## Deduplication

To keep the volume of emitted events manageable, the operator merges repeated events that share the same reason, type, component and resource into a single record whose `count` is incremented and `lastTimestamp` is refreshed.

 - **Reporter-side merging**: the default for operator events under the Kubernetes reporter. The reporter derives a deterministic event name from the resource, reason and message key, looks up the existing `Event`, and increments its `count` instead of creating a new record.
 - **Per-event keys**: some events use an additional `messageKey` to scope the de-dupe (e.g. `gcPressure` vs `heapUsage` for `MemoryPressure`, `MemoryTuning` for the autotuning recommendation, `jobmanager-exception-<hash>` for `JobException`).
 - **Per-event labels**: `ScalingReport` is de-duplicated by a hash of the recommended parallelism map, so a new event is only emitted when the recommendation changes.
 - **One-shot events**: a few events (e.g. `SpecChanged`) are de-duplicated by resource generation so each generation produces exactly one event.
 - **Time-based interval**: all autoscaler and autotuning events use a time-based deduplication window controlled by [`job.autoscaler.scaling.event.interval`]({{< ref "docs/deployment/configuration#autoscaler-configuration" >}}) (default `30 min`).

## Operator Events

These events are emitted by the core reconcile and observe loops that manage Flink resources on Kubernetes. They cover the full lifecycle from initial submission through upgrades, rollbacks, snapshots and cleanup.

### Deployment Lifecycle

| Reason        | Type   | Component            | Description                                                                                                                                                                                                            |
|---------------|--------|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Submit`      | Normal | JobManagerDeployment | A `FlinkDeployment` or `FlinkSessionJob` is being submitted to the cluster (`Starting deployment`). Emitted once per deployment attempt.                                                                               |
| `SpecChanged` | Normal | JobManagerDeployment | The reconciler detected a difference between the desired and last reconciled spec. See [message formats](#operator-message-formats). **Dedup:** by resource generation, so each generation produces exactly one event. |
| `Suspended`   | Normal | JobManagerDeployment | The existing deployment is being suspended before an upgrade or spec-driven redeploy (`Suspending existing deployment.`).                                                                                              |
| `Rollback`    | Normal | JobManagerDeployment | A failed upgrade is being rolled back to the previous stable spec (`Rolling back failed deployment.`). Triggered automatically when [rollback]({{< ref "docs/managing/job-management#rollbacks" >}}) is enabled.       |
| `Scaling`     | Normal | Job                  | In-place parallelism change applied to a running job via the Flink rescale API, without restart (`In-place scaling triggered`).                                                                                        |
| `Cleanup`     | Normal | Operator             | The resource is being deleted (`Cleaning up FlinkDeployment` or `Cleaning up FlinkSessionJob`). The operator is tearing down associated Kubernetes objects and Flink state.                                            |

### Job Status

| Reason                | Type    | Component                 | Description                                                                                                                                                                                                                                                                                  |
|-----------------------|---------|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `JobStatusChanged`    | Normal  | Job                       | The Flink job transitioned to a new state (e.g. `RUNNING`, `FINISHED`, `FAILED`, `CANCELED`). See [message formats](#operator-message-formats).                                                                                                                                              |
| `JobException`        | Warning | Job                       | The job manager reported an exception (root cause or vertex-level). Annotations record the task name, task manager id and exception timestamp. See [message formats](#operator-message-formats). **Dedup:** by a hash of the formatted message (`messageKey = jobmanager-exception-<hash>`). |
| `Missing`             | Warning | Job, JobManagerDeployment | The job (`Job Not Found`) or the JobManager deployment (`Missing JobManager deployment`) expected to exist on the cluster could not be found. May indicate a lost JobManager pod or a failed submission.                                                                                     |
| `RecoverDeployment`   | Warning | Job                       | The operator detected that the JobManager deployment was lost and is attempting to recover it from the last reconciled state (`Recovering lost deployment`).                                                                                                                                 |
| `RestartUnhealthyJob` | Warning | Job                       | The cluster health check failed (e.g. restart-count threshold exceeded) and the operator is restarting the job. The message is the error returned by the [health check]({{< ref "docs/operations/health" >}}), or `Restarting unhealthy job` as a fallback.                                  |
| `Error`               | Warning | Operator, Job             | A generic reconciliation error. The message is the exception message. Used as a fallback when no more specific reason applies (e.g. an unhandled exception during reconcile, or a failed-job result returned by Flink).                                                                      |

### Snapshots

| Reason              | Type    | Component          | Description                                                                                                                                                                                                                                                                                                                                                 |
|---------------------|---------|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SavepointError`    | Warning | Operator, Snapshot | A triggered savepoint failed. The message is a fixed phrase indicating periodic vs. nonce-triggered failure; the Flink-returned root cause is recorded in `status.error`, not in the event message. Emitted by the periodic savepoint trigger, the upgrade path, and the `FlinkStateSnapshot` controller. See [message formats](#operator-message-formats). |
| `CheckpointError`   | Warning | Operator, Snapshot | A triggered checkpoint failed. Same shape as `SavepointError`: the message is the templated phrase, the root cause lives in `status.error`. Emitted by the periodic checkpoint trigger and the `FlinkStateSnapshot` controller. See [message formats](#operator-message-formats).                                                                           |
| `SnapshotAbandoned` | Warning | Snapshot           | A `FlinkStateSnapshot` was abandoned because its referenced `FlinkDeployment`/`FlinkSessionJob` is no longer reconcilable (deleted or not running). See [message formats](#operator-message-formats).                                                                                                                                                       |

### Validation and Configuration

| Reason                    | Type    | Component          | Description                                                                                                                                                                                                                                                              |
|---------------------------|---------|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ValidationError`         | Warning | Operator, Snapshot | The submitted spec failed one of the registered validators (e.g. invalid `upgradeMode`, conflicting fields, malformed `FlinkStateSnapshot`). The message is the validator-specific error string. Reconciliation is skipped until the spec is fixed.                      |
| `UnsupportedFlinkVersion` | Warning | Operator           | The `spec.flinkVersion` is not supported by this operator release (`Flink version <version> is not supported by this operator version`). See [Compatibility Guarantees]({{< ref "docs/deployment/compatibility" >}}).                                                    |
| `IngressManagement`       | Warning | Operator           | The resource defines `spec.ingress` but ingress management is disabled in the operator (`kubernetes.operator.ingress.manage`). The ingress section is ignored (`Ingress management is turned off but ingress set in spec`).                                              |
| `CleanupFailed`           | Warning | Operator, Snapshot | Resource cleanup could not complete. Common causes: a session `FlinkDeployment` still has child `FlinkSessionJob` or non-terminated jobs, a job cancellation timed out, or a savepoint required during cleanup failed. See [message formats](#operator-message-formats). |

### Operator Message Formats

For events whose message follows a structured template, the verbatim formats are listed below.

#### SpecChanged

```
<DiffType> change(s) detected (<diff>), starting reconciliation.
```
Where `<DiffType>` is one of `UPGRADE`, `SCALE`, `SAVEPOINT_REDEPLOY`.

#### JobStatusChanged

```
Job status changed from <old> to <new>
```
On the first observation (no previous state), the operator instead emits:

```
Job status changed to <state>
```

#### JobException

```
<root exception class>: <message>
<first N lines of the stack trace>
... (<N> more lines)
```
The number of stack-trace lines included is controlled by the operator configuration; the trailing `... (<N> more lines)` suffix is only present when the trace was truncated.

#### SavepointError

One of (depending on the trigger type that initiated the savepoint):

```
Periodic savepoint failed
```

```
Savepoint failed for savepointTriggerNonce: <nonce>
```

The root cause returned by Flink is stored in `status.error`, not in the event message.

#### CheckpointError

One of (depending on the trigger type that initiated the checkpoint):

```
Periodic checkpoint failed
```

```
Checkpoint failed for checkpointTriggerNonce: <nonce>
```

The root cause returned by Flink is stored in `status.error`, not in the event message.

#### SnapshotAbandoned

One of (depending on why the snapshot's secondary resource is no longer reconcilable):

```
Secondary resource <jobReference> for savepoint <snapshotName> was not found
```

```
Secondary resource <jobReference> for savepoint <snapshotName> is not running
```

Note: the literal word `savepoint` is used in both templates even when the snapshot is a checkpoint.

#### CleanupFailed

For a session `FlinkDeployment` whose child resources are still present:

```
The session jobs [<sessionJobName1>, <sessionJobName2>, ...] should be deleted first
```

For a session cluster with non-terminated unmanaged jobs (when `kubernetes.operator.session.block-on-unmanaged-jobs` is enabled):

```
The session cluster has non terminated jobs [<jobId1>, <jobId2>, ...] that should be cancelled first
```

For a `FlinkStateSnapshot` whose cleanup throws:

```
<exception message>
```

## Autoscaler Events

These events are emitted by the [Autoscaler]({{< ref "docs/managing/autoscaler" >}}) when it evaluates scaling metrics and applies or rejects parallelism changes. They are attached to the same `FlinkDeployment` or `FlinkSessionJob` resource as the autoscaled job. Unless noted otherwise, every autoscaler event is also subject to the time-based deduplication window controlled by [`job.autoscaler.scaling.event.interval`]({{< ref "docs/deployment/configuration#autoscaler-configuration" >}}) (default `30 min`). The per-event **Dedup** notes below describe any *additional* keys or labels layered on top.

| Reason                 | Type    | Component | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|------------------------|---------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ScalingReport`        | Normal  | Operator  | The autoscaler produced a parallelism recommendation. The message is built from a header indicating whether scaling execution is enabled or disabled, followed by one summary entry per vertex. See [message formats](#autoscaler-message-formats). **Dedup:** interval + hash of the recommended parallelism (a new event is emitted only when the recommendation changes).                                                                                                                                                  |
| `IneffectiveScaling`   | Normal  | Operator  | A previous scaling action did not produce the expected throughput increase (actual increase below [`job.autoscaler.scaling.effectiveness.threshold`]({{< ref "docs/deployment/configuration#autoscaler-configuration" >}})). When [`job.autoscaler.scaling.effectiveness.detection.enabled`]({{< ref "docs/deployment/configuration#autoscaler-configuration" >}}) is `true` the autoscaler blocks further scale-ups on that vertex. **Dedup:** interval + per-vertex `messageKey` (`ineffective<vertex><expectedIncrease>`). |
| `ScalingLimited`       | Warning | Operator  | The recommended parallelism for a vertex could not be applied as-is because of key-group and partition alignment constraints, the configured `parallelismUpperLimit`, or the source partition count, and the autoscaler fell back to the closest aligned value. Emitted only by the deprecated legacy alignment modes (`scaling.key-group.partitions.adjust.mode`); the current [alignment modes]({{< ref "docs/managing/autoscaler#parallelism-alignment" >}}) never block. **Dedup:** interval + per-vertex `messageKey` (`ScalingLimited<vertex><newParallelism>`).                                                                                                                                                                          |
| `MemoryPressure`       | Normal  | Operator  | The job is under memory pressure. Emitted when GC pressure exceeds [`job.autoscaler.memory.gc-pressure.threshold`]({{< ref "docs/deployment/configuration#autoscaler-configuration" >}}) or heap usage exceeds [`job.autoscaler.memory.heap-usage.threshold`]({{< ref "docs/deployment/configuration#autoscaler-configuration" >}}). Scaling is paused entirely while this condition holds. **Dedup:** interval, with `messageKey` distinguishing `gcPressure` from `heapUsage`.                                                |
| `ResourceQuotaReached` | Warning | Operator  | The recommended scaling action would push the cluster beyond the configured resource quota ([`job.autoscaler.quota.cpu`]({{< ref "docs/deployment/configuration#autoscaler-configuration" >}}), [`job.autoscaler.quota.memory`]({{< ref "docs/deployment/configuration#autoscaler-configuration" >}})). The action is rejected. **Dedup:** by interval only.                                                                                                                                                |
| `AutoscalerError`      | Warning | Operator  | The autoscaler failed with an unexpected exception during a scaling evaluation. The message is the exception text, or an abbreviated stack trace (max 2048 chars) when no message is available. **Dedup:** none.                                                                                                                                                                                                                                                                                                              |

### Autoscaler Message Formats

#### ScalingReport

The header depends on whether scaling execution is enabled, gated by config, or gated by an excluded period. One of:

```
Scaling execution enabled, begin scaling vertices: <entry1>, <entry2>, ...
```

```
Scaling execution disabled by config job.autoscaler.scaling.enabled:false, recommended parallelism change: <entry1>, <entry2>, ...
```

```
Scaling execution disabled by config job.autoscaler.excluded.periods:<periods>, recommended parallelism change: <entry1>, <entry2>, ...
```

The header is followed by one entry per vertex:

```
{ Vertex ID <id> | Parallelism <old> -> <new> | Processing capacity <old> -> <new> | Target data rate <rate> }
```

#### IneffectiveScaling

```
Ineffective scaling detected for <vertex> (expected increase: <X>, actual increase <Y>). Blocking of ineffective scaling decisions is <enabled|disabled>
```

#### ScalingLimited

```
Scaling limited detected for <vertex> (expected parallelism: <X>, actual parallelism <Y>). Scaling limited due to numKeyGroupsOrPartitions : <N>，upperBoundForAlignment(maxParallelism or parallelismUpperLimit): <U>, parallelismLowerLimit: <L>.
```

#### MemoryPressure

```
GC Pressure <value> is above the allowed limit for scaling operations. Please adjust the available memory manually.
```

```
Heap Usage <value> is above the allowed limit for scaling operations. Please adjust the available memory manually.
```

#### ResourceQuotaReached

```
Resource usage is above the allowed limit for scaling operations. Please adjust the resource quota manually.
```

## Autotuning Events

[Scaling Autotuning]({{< ref "docs/managing/autoscaler#scaling-autotuning" >}}) is part of the autoscaler and uses TaskManager memory metrics to recommend a tuned memory configuration. The event is emitted regardless of whether tuning is enabled, which allows previewing recommendations before opting in via `job.autoscaler.memory.tuning.enabled`.

| Reason                         | Type   | Component | Description                                                                                                                                                                                                                                                                                                                                                                                              |
|--------------------------------|--------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Configuration recommendation` | Normal | Operator  | Memory tuning produced a new TaskManager memory configuration. The message lists the recommended values for managed memory, network memory, JVM overhead and metaspace, and indicates whether the recommendation is auto-applied. **Dedup:** `messageKey` `MemoryTuning`, on the `job.autoscaler.scaling.event.interval` window. |

### Autotuning Message Format

#### Configuration recommendation

```
Memory tuning recommends the following configuration (automatic tuning is <enabled|disabled>):
<formatted_config>
```

## Event Reporters

Autoscaler events are routed through an `AutoScalerEventHandler`. The chosen implementation depends on how the autoscaler is deployed. When the autoscaler runs inside the operator on Kubernetes, the Kubernetes handler is wired in directly. When the standalone autoscaler runs outside Kubernetes, configuration picks one of two built-in alternatives (`LOGGING` or `JDBC`). The interface is internal, with no extension point for custom handlers. Operator-side events go through the Kubernetes reporter.

| Reporter   | Scope                                                               | Backend                                                     | Selected via                                                                                              |
|------------|---------------------------------------------------------------------|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| Kubernetes | Operator events + autoscaler events when running inside Kubernetes. | Native Kubernetes `Event` objects on the involved resource. | Default (and only) reporter when the operator is deployed in a Kubernetes cluster.                        |
| Logging    | Autoscaler events from the standalone autoscaler.                   | SLF4J logger.                                               | `autoscaler.standalone.event-handler.type: LOGGING` (default for the standalone autoscaler).              |
| JDBC       | Autoscaler events from the standalone autoscaler.                   | Relational database (any JDBC-compatible engine).           | `autoscaler.standalone.event-handler.type: JDBC` + the `autoscaler.standalone.jdbc.*` connection options. |

### Kubernetes

The default reporter writes each event as a native Kubernetes `Event` object in the same namespace as the involved resource, with the resource set as the event's `involvedObject`. When the operator runs in-cluster, both operator events and autoscaler events go through this reporter. Events are visible through:

```bash
# All events on a single resource
kubectl describe flinkdeployment <name>
kubectl describe flinksessionjob <name>
kubectl describe flinkstatesnapshot <name>

# Filter by reason, type or component
kubectl get events --field-selector involvedObject.name=<name>,reason=ScalingReport
kubectl get events --field-selector type=Warning -n <namespace>
```

See [Retention](#retention) and [Deduplication](#deduplication) for how long events stick around and how repeats are folded.

### Logging

The Logging reporter is the default for the [standalone autoscaler]({{< ref "docs/managing/autoscaler#standalone-autoscaler" >}}) (`flink-autoscaler-standalone`). Each autoscaler event is written to the autoscaler's SLF4J logger at `INFO` level, including the job key, type, reason, message, message key and deduplication interval. Lifecycle and retention are then handled by the surrounding logging framework (file appenders, log rotation, log shippers, etc.).

Enabled with:

```yaml
autoscaler.standalone.event-handler.type: LOGGING
```

This reporter only handles autoscaler events. It has no effect on operator events (which only exist when the operator itself is running in-cluster).

Unlike the Kubernetes and JDBC reporters, the Logging reporter does not deduplicate, and every event becomes a separate log line. The `messageKey` and deduplication interval are emitted as informational fields, but any folding is left to the surrounding logging stack.

### JDBC

The JDBC reporter persists autoscaler events in a relational database, which is the recommended option for the standalone autoscaler in production. Events are written through `JdbcEventInteractor`, and old records are periodically purged by a background cleaner thread.

Enabled with:

```yaml
autoscaler.standalone.event-handler.type: JDBC
autoscaler.standalone.jdbc.url: jdbc:mysql://<host>:<port>/<db>
autoscaler.standalone.jdbc.username: <user>
autoscaler.standalone.jdbc.password-env-variable: JDBC_PWD
```

Retention is controlled by `autoscaler.standalone.jdbc.event-handler.ttl` (default `90 d`). Setting it to `0` disables TTL-based pruning so events are kept indefinitely.

Deduplication semantics are equivalent to the Kubernetes reporter, but implemented at the storage layer: when an autoscaler event is emitted with a non-`null` interval, the handler looks up the latest stored event with the same `jobKey`/`reason`/`eventKey` and, if it falls inside the interval, increments its `count` and updates the message instead of inserting a new row.

### Extensibility

In addition to the built-in reporters, every event is also forwarded to any registered [`FlinkResourceListener`]({{< ref "docs/deployment/plugins#custom-flink-resource-listeners" >}}). This SPI allows publishing events to external systems (logs, message buses, monitoring backends) without modifying the operator.
