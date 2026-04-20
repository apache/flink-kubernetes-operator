---
title: "Metrics and Logging"
weight: 4
type: docs
aliases:
   - /operations/metrics-logging.html
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

# Metrics and Logging

## Metrics

The Flink Kubernetes Operator (Operator) extends the [Flink Metric System](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/) that allows gathering and exposing metrics to centralized monitoring solutions.

Different operator metrics can be turned on/off individually using the configuration. For details check the [metrics config reference]({{< ref "docs/operations/configuration#system-metrics-configuration" >}}).

### Scope

Every metric emitted by the operator is published under a metric group. The operator defines three scopes, each with a configurable scope format (see the [Configuration]({{< ref "docs/operations/configuration" >}}) page for the full list of scope variables):

| Scope     | Configuration option                                         | Default format                                                                               |
|-----------|--------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| System    | `kubernetes.operator.metrics.scope.k8soperator.system`       | `<host>.k8soperator.<namespace>.<name>.system`                                               |
| Namespace | `kubernetes.operator.metrics.scope.k8soperator.resourcens`   | `<host>.k8soperator.<namespace>.<name>.namespace.<resourcens>.<resourcetype>`                |
| Resource  | `kubernetes.operator.metrics.scope.k8soperator.resource`     | `<host>.k8soperator.<namespace>.<name>.resource.<resourcens>.<resourcename>.<resourcetype>`  |

The variables above refer to, in order of appearance:

- `<host>`: the operator pod host.
- `<namespace>`: the operator pod's namespace.
- `<name>`: the operator pod's name.
- `<resourcens>`: the namespace of the managed custom resource.
- `<resourcename>`: the name of the managed custom resource.
- `<resourcetype>`: the type of the managed custom resource.

In all three scopes, any subgroups added at metric registration time are appended to both the scope components and the logical scope in registration order.

#### How Metric Identifiers Are Built

Flink internally distinguishes two views of a metric's identity, and reporters consume them differently:
1. **Scope components**: produced by substituting variables (`<host>`, `<namespace>`, ...) in the configured scope format with their current values. Literal segments such as `k8soperator`, `system`, `namespace`, `resource` are carried through verbatim.
2. **Logical scope**: built from the operator's metric-group chain, using each group's fixed name:
   - System metrics: `k8soperator`
   - Namespace metrics: `k8soperator.namespace`
   - Resource metrics: `k8soperator.namespace.resource`

Reporters then assemble the reported series as follows:

- **Non-labeling reporters** (SLF4J, JMX, Graphite, …) build the identifier from the **scope components + metric name**, joined with `.`. Every scope-format segment is kept inline (literals verbatim, variables substituted with their current values):
- **Labeling reporters** (Prometheus, Datadog, InfluxDB, …) build the metric name from the **logical scope + metric name**, and expose the scope variables (`host`, `namespace`, `name`, `resourcens`, `resourcename`, `resourcetype`) as labels/tags.

{{< hint info >}}
Labeling reporters drop the scope-format literals (`system`, `namespace`, `resource`): any `k8soperator`, `namespace` or `resource` segment in their metric names comes from the operator metric group's name, not from the literal. Customizing the scope formats therefore only affects non-labeling reporter output and the label/tag values for the scope variables.
{{< /hint >}}

#### Concrete Example

**System scope (Prometheus)**

The `FlinkDeployment` `Lifecycle.State.<State>`:
```
flink_k8soperator_FlinkDeployment_Lifecycle_State_STABLE_TimeSeconds{
    host="<operator-pod-host>",
    name="<operator-pod-name>",
    namespace="<operator-pod-namespace>"
}
```
Decomposed:
- `flink`: fixed prefix hard-coded by Flink's Prometheus reporter (builtin in `flink-metrics-prometheus`); applied to every metric it exports and not configurable.
- `k8soperator`: from the operator metric group's name (not from the literal `k8soperator` in the scope format).
- `FlinkDeployment_Lifecycle_State_STABLE`: subgroups added by the lifecycle tracker.
- `TimeSeconds`: metric name.
- `<host>`, `<namespace>`, `<name>`: attached as labels.

**System scope (SLF4J / JMX)**

The `FlinkDeployment` `Lifecycle.State.<State>`:
```
<host>.k8soperator.<namespace>.<name>.system.FlinkDeployment.Lifecycle.State.STABLE.TimeSeconds
```
Here the `system` literal from the scope format appears inline, and all variables are substituted into the identifier.

**Namespace scope (Prometheus)**

The `FlinkDeployment` `Lifecycle.State.<State>.Count` gauge:
```
flink_k8soperator_namespace_Lifecycle_State_STABLE_Count{
    host="<operator-pod-host>",
    name="<operator-pod-name>",
    namespace="<operator-pod-namespace>"
    resourcens="<cr-k8s-namespace>",
    resourcetype="FlinkDeployment"
}
```

**Resource scope (Prometheus)**

The `FlinkDeployment` autoscaler metric `AutoScaler.<jobVertexID>.TRUE_PROCESSING_RATE.Current`:
```
flink_k8soperator_namespace_resource_AutoScaler_<jobVertexID>_TRUE_PROCESSING_RATE_Current{
    host="<operator-pod-host>",
    name="<operator-pod-name>",
    namespace="<operator-pod-namespace>"
    resourcens="<cr-k8s-namespace>",
    resourcename="<cr-k8s-name>",
    resourcetype="FlinkDeployment"
}
```

### Operator Custom Resource Metrics
The Operator gathers aggregate metrics about managed custom resources (`FlinkDeployment`, `FlinkSessionJob`, `FlinkBlueGreenDeployment`).

The Metrics column in the tables below lists only the path suffix that follows the scope prefix. The full identifier, and whether variables appear inline or as labels, depends on the configured scope format and on the reporter in use.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Resource type</th>
      <th class="text-left" style="width: 20%">Metrics</th>
      <th class="text-left" style="width: 32%">Description</th>
      <th class="text-left" style="width: 8%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="4"><strong>System</strong></th>
      <td rowspan="2">FlinkBlueGreenDeployment</td>
      <td>Lifecycle.State.&lt;State&gt;.TimeSeconds</td>
      <td>Time spent in lifecycle state &lt;State&gt; for a given FlinkBlueGreenDeployment resource.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>Lifecycle.Transition.&lt;Transition&gt;.TimeSeconds</td>
      <td>Time statistics for blue-green lifecycle state transitions.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td rowspan="2">FlinkDeployment, FlinkSessionJob</td>
      <td>Lifecycle.State.&lt;State&gt;.TimeSeconds</td>
      <td>Time spent in lifecycle state &lt;State&gt; for a given resource.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>Lifecycle.Transition.&lt;Transition&gt;.TimeSeconds</td>
      <td>Time statistics for selected lifecycle state transitions.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <th rowspan="17"><strong>Namespace</strong></th>
      <td rowspan="5">FlinkBlueGreenDeployment</td>
      <td>BlueGreenState.&lt;State&gt;.Count</td>
      <td>Number of managed FlinkBlueGreenDeployment resources currently in state &lt;State&gt; per namespace.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Failures</td>
      <td>Monotonically-increasing count of transitions into the <code>FAILING</code> state for all FlinkBlueGreenDeployment resources in the namespace.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>JobStatus.&lt;Status&gt;.Count</td>
      <td>Number of managed FlinkBlueGreenDeployment resources currently in JobStatus &lt;Status&gt; per namespace.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Lifecycle.State.&lt;State&gt;.TimeSeconds</td>
      <td>Time spent in lifecycle state &lt;State&gt; for a given FlinkBlueGreenDeployment resource, aggregated per namespace.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>Lifecycle.Transition.&lt;Transition&gt;.TimeSeconds</td>
      <td>Time statistics for blue-green lifecycle state transitions, aggregated per namespace.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td rowspan="4">FlinkDeployment</td>
      <td>FlinkMinorVersion.&lt;FlinkMinorVersion&gt;.Count</td>
      <td>Number of managed FlinkDeployment resources per Flink minor version per namespace.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>FlinkVersion.&lt;FlinkVersion&gt;.Count</td>
      <td>Number of managed FlinkDeployment resources per Flink version per namespace.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>JmDeploymentStatus.&lt;Status&gt;.Count</td>
      <td>Number of managed FlinkDeployment resources per JobManager deployment status per namespace.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>ResourceUsage.Cpu/Memory/StateSize</td>
      <td>Total resources used per namespace. <code>Cpu</code> / <code>Memory</code> aggregate cluster requests across deployments; <code>StateSize</code> aggregates the last observed checkpoint state size reported via the JobManager REST API.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td rowspan="4">FlinkDeployment, FlinkSessionJob</td>
      <td>Count</td>
      <td>Number of managed resources per namespace.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Lifecycle.State.&lt;State&gt;.Count</td>
      <td>Number of managed resources currently in lifecycle state &lt;State&gt; per namespace.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Lifecycle.State.&lt;State&gt;.TimeSeconds</td>
      <td>Time spent in lifecycle state &lt;State&gt; for a given resource, aggregated per namespace.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td>Lifecycle.Transition.&lt;Transition&gt;.TimeSeconds</td>
      <td>Time statistics for selected lifecycle state transitions, aggregated per namespace.</td>
      <td>Histogram</td>
    </tr>
    <tr>
      <td rowspan="4">FlinkStateSnapshot</td>
      <td>Checkpoint.Count</td>
      <td>Total number of tracked checkpoint snapshots per namespace (across all states).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Checkpoint.State.&lt;SnapshotState&gt;.Count</td>
      <td>Number of checkpoint snapshots currently in state &lt;SnapshotState&gt; per namespace.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Savepoint.Count</td>
      <td>Total number of tracked savepoint snapshots per namespace (across all states).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>Savepoint.State.&lt;SnapshotState&gt;.Count</td>
      <td>Number of savepoint snapshots currently in state &lt;SnapshotState&gt; per namespace.</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <th rowspan="5"><strong>Resource</strong></th>
      <td rowspan="5">FlinkDeployment, FlinkSessionJob</td>
      <td>AutoScaler.scalings</td>
      <td>Number of scaling events triggered by the autoscaler for the resource.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>AutoScaler.errors</td>
      <td>Number of autoscaler evaluation errors for the resource.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>AutoScaler.balanced</td>
      <td>Number of autoscaler evaluations for the resource that concluded no scaling was needed.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>AutoScaler.jobVertexID.&lt;jobVertexID&gt;.&lt;ScalingMetric&gt;.Current</td>
      <td>Latest observed value of &lt;ScalingMetric&gt; for the given job vertex (see <a href="#scaling-metrics">scaling metrics</a> list below).</td>
      <td>Gauge</td>
    </tr>
    <tr>
      <td>AutoScaler.jobVertexID.&lt;jobVertexID&gt;.&lt;ScalingMetric&gt;.Average</td>
      <td>Metric-window average of &lt;ScalingMetric&gt; for the given job vertex. Only emitted for scaling metrics that support averaging.</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

**Variables used in the metric names** are described in the dedicated subsections below.

#### FlinkDeployment Version and Resource Usage

These namespace-level metrics provide a fleet-wide view of the `FlinkDeployment` resources managed by the operator. Version gauges help track Flink version adoption across a cluster (e.g. to plan upgrades or spot workloads still running on deprecated versions), while resource-usage gauges surface aggregate CPU, memory and state-size consumption per namespace, which is useful for capacity planning, quota/cost monitoring and alerting on unexpected growth.

- `FlinkVersion.<FlinkVersion>.Count` / `FlinkMinorVersion.<FlinkMinorVersion>.Count`: number of managed `FlinkDeployment` resources per Flink version (or its `X.Y` minor prefix) per namespace. The version string is reported by the JobManager REST API.
- `ResourceUsage.Cpu` / `ResourceUsage.Memory`: total CPU / memory requests aggregated across all `FlinkDeployment` resources in the namespace.
- `ResourceUsage.StateSize`: last observed checkpoint state size aggregated across all `FlinkDeployment` resources in the namespace, reported via the JobManager REST API.

#### FlinkDeployment / FlinkSessionJob Lifecycle metrics

Based on the resource status the operator monitors [resource lifecycle states]({{< ref "docs/concepts/architecture#flink-resource-lifecycle" >}}) for `FlinkDeployment` and `FlinkSessionJob` resources.

The number of resources and time spend in each of these states at any given time is tracked by the `Lifecycle.<State>.Count` and `Lifecycle.<State>.TimeSeconds` metrics, where `<State>` is one of: `CREATED`, `SUSPENDED`, `UPGRADING`, `DEPLOYED`, `STABLE`, `ROLLING_BACK`, `ROLLED_BACK`, `FAILED`.

In addition to the simple counts we further track a few selected state transitions via the `Lifecycle.Transition.<Transition>.TimeSeconds` histograms, where `<Transition>` is one of:

- `Upgrade`: End-to-end resource upgrade time from stable to stable.
- `Resume`: Time from suspended to stable.
- `Suspend`: Time for any suspend operation.
- `Stabilization`: Time from deployed to stable state.
- `Rollback`: Time from deployed to rolled_back state if the resource was rolled back.
- `Submission`: Flink resource submission time.

#### FlinkBlueGreenDeployment Lifecycle metrics

`FlinkBlueGreenDeployment` resources have their own lifecycle states that track the blue-green deployment process. The `Lifecycle.State.<State>.TimeSeconds` and `BlueGreenState.<State>.Count` metrics use a dedicated set of `<State>` values: `INITIALIZING_BLUE`, `ACTIVE_BLUE`, `SAVEPOINTING_BLUE`, `TRANSITIONING_TO_GREEN`, `ACTIVE_GREEN`, `SAVEPOINTING_GREEN`, `TRANSITIONING_TO_BLUE`.

The `Lifecycle.Transition.<Transition>.TimeSeconds` histograms use the following `<Transition>` values:

- `InitialDeployment`: Time from leaving `INITIALIZING_BLUE` to reaching `ACTIVE_BLUE` (first deployment).
- `BlueToGreen`: Time from leaving `ACTIVE_BLUE` to reaching `ACTIVE_GREEN` (actual transition duration).
- `GreenToBlue`: Time from leaving `ACTIVE_GREEN` to reaching `ACTIVE_BLUE` (actual transition duration).

Transition metrics measure the actual transition time - from when the deployment leaves the source stable state until it reaches the target stable state. This excludes time spent running stably before the transition was initiated.

State time metrics track how long a resource spends in each state, which helps identify bottlenecks in the deployment pipeline.

#### FlinkDeployment / FlinkSessionJob JobStatus Tracking

These namespace-level gauges summarize the JobManager deployment status reported by the operator for every `FlinkDeployment` it manages. They complement the lifecycle metrics by exposing the underlying JobManager health (is the JM pod ready, still deploying, missing, in error?), which is useful for dashboards that correlate controller-level lifecycle transitions with Kubernetes-level deployment readiness, and for alerting on stuck `DEPLOYING` / `MISSING` / `ERROR` states.

The `JmDeploymentStatus.<Status>.Count` gauge tracks how many `FlinkDeployment` resources are currently in each JobManager deployment status per namespace. The `<Status>` values reported are: `READY`, `DEPLOYED_NOT_READY`, `DEPLOYING`, `MISSING`, `ERROR`.

#### FlinkBlueGreenDeployment JobStatus Tracking

In addition to BlueGreenState tracking, `FlinkBlueGreenDeployment` resources also expose JobStatus metrics that track the Flink job state. The `JobStatus.<Status>.Count` gauge uses `<Status>` values: `RUNNING`, `FAILING`, `SUSPENDED`, `FAILED`, `RECONCILING`.

**JobStatus Gauges**: Current count of deployments per JobStatus. These gauges go up and down as deployments transition between states.

**Failures Counter**: Historical count that increments each time a deployment transitions TO the FAILING state. This counter:
- Never decrements (accumulates total failures since operator start).
- Increments on each new transition to FAILING (even if the same deployment fails multiple times).
- Persists across deployment recoveries (provides historical failure tracking).
- Useful for calculating failure rates and setting up alerts.

Example: A deployment goes RUNNING → FAILING → RUNNING → FAILING. The FAILING gauge shows 0 or 1 (current state), while the Failures counter shows 2 (historical events).

#### FlinkStateSnapshot State Tracking

These namespace-level metrics expose the progress and health of checkpoint and savepoint `FlinkStateSnapshot` resources managed by the operator. Tracking the distribution of snapshots across states helps detect stuck or failing snapshot pipelines (e.g. a spike in `FAILED` or long-lived `IN_PROGRESS` / `TRIGGER_PENDING` counts), validate that periodic checkpoints and savepoints are completing as expected, and power alerts on snapshot reliability and retention.

The `Checkpoint.State.<SnapshotState>.Count` and `Savepoint.State.<SnapshotState>.Count` gauges track how many `FlinkStateSnapshot` resources are currently in each snapshot state per namespace. The `<SnapshotState>` values reported are: `COMPLETED`, `FAILED`, `IN_PROGRESS`, `TRIGGER_PENDING`, `ABANDONED`.

#### Scaling metrics

These resource-scoped metrics expose the autoscaler's view of each managed job at job-vertex granularity. Counters (`scalings`, `errors`, `balanced`) track the autoscaler's activity over time, which is useful to alert on scaling storms or persistent evaluation errors, while the per-vertex `Current` / `Average` gauges surface the exact inputs the autoscaler uses to decide whether to scale (processing rate, lag, load, parallelism bounds, thresholds). Together they make it possible to debug scaling decisions, tune autoscaler configuration, and build dashboards that correlate traffic patterns with parallelism changes.

The `<ScalingMetric>` placeholder in the resource-scoped `AutoScaler.jobVertexID.<jobVertexID>.<ScalingMetric>.{Current,Average}` gauges takes one of the following values (per job vertex). `<jobVertexID>` is the hex id of a Flink job vertex as reported by the JobManager REST API.

| Name                        | Description                                                                                              | `.Average` emitted? |
|-----------------------------|----------------------------------------------------------------------------------------------------------|---------------------|
| `CATCH_UP_DATA_RATE`        | Additional processing rate needed to catch up with the backlog within the catch-up window (records/sec). | No                  |
| `EXPECTED_PROCESSING_RATE`  | Expected processing rate after a scale-up.                                                               | No                  |
| `LAG`                       | Total number of pending records at the source.                                                           | No                  |
| `LOAD`                      | Subtask load (busy-time ratio, `0` idle, `1` fully utilized).                                            | Yes                 |
| `MAX_PARALLELISM`           | Configured max parallelism of the job vertex.                                                            | No                  |
| `NUM_SOURCE_PARTITIONS`     | Number of source partitions (for source vertices).                                                       | No                  |
| `PARALLELISM`               | Current job vertex parallelism.                                                                          | No                  |
| `RECOMMENDED_PARALLELISM`   | Parallelism recommended by the autoscaler for the next scaling decision.                                 | No                  |
| `SCALE_DOWN_RATE_THRESHOLD` | Lower bound of the target data rate range.                                                               | No                  |
| `SCALE_UP_RATE_THRESHOLD`   | Upper bound of the target data rate range.                                                               | No                  |
| `TARGET_DATA_RATE`          | Target processing rate derived from source inputs (records/sec).                                         | Yes                 |
| `TRUE_PROCESSING_RATE`      | Processing rate at full capacity (records/sec).                                                          | Yes                 |

### Kubernetes Client Metrics

The Operator gathers various metrics related to Kubernetes API server access.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 12%">Scope</th>
      <th class="text-left" style="width: 30%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 10%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="10"><strong>System</strong></th>
      <td>KubeClient.HttpRequest.&lt;RequestMethod&gt;.Count</td>
      <td>Number of HTTP requests sent to the Kubernetes API Server per request method. &lt;RequestMethod&gt; can take values from: GET, POST, PUT, PATCH, DELETE, etc.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpRequest.Count</td>
      <td>Number of HTTP requests sent to the Kubernetes API Server.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpRequest.Failed.Count</td>
      <td>Number of failed HTTP requests that have no response from the Kubernetes API Server.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpRequest.Failed.NumPerSecond</td>
      <td>Number of failed HTTP requests sent to the Kubernetes API Server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpRequest.NumPerSecond</td>
      <td>Number of HTTP requests sent to the Kubernetes API Server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.&lt;ResponseCode&gt;.Count</td>
      <td>Number of HTTP responses received from the Kubernetes API Server per response code. &lt;ResponseCode&gt; can take values from: 200, 404, 503, etc.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.&lt;ResponseCode&gt;.NumPerSecond</td>
      <td>Number of HTTP responses received from the Kubernetes API Server per response code per second. &lt;ResponseCode&gt; can take values from: 200, 404, 503, etc.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.Count</td>
      <td>Number of HTTP responses received from the Kubernetes API Server.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.NumPerSecond</td>
      <td>Number of HTTP responses received from the Kubernetes API Server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.TimeNanos</td>
      <td>Latency statistics obtained from the HTTP responses received from the Kubernetes API Server.</td>
      <td>Histogram</td>
    </tr>
  </tbody>
</table>

#### Kubernetes client metrics by Http Response Code

It's possible to publish additional metrics by Http response code received from API server by setting `kubernetes.operator.kubernetes.client.metrics.http.response.code.groups.enabled` to `true`.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 12%">Scope</th>
      <th class="text-left" style="width: 30%">Metrics</th>
      <th class="text-left" style="width: 48%">Description</th>
      <th class="text-left" style="width: 10%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="10"><strong>System</strong></th>
      <td>KubeClient.HttpResponse.1xx.Count</td>
      <td>Number of HTTP Code 1xx responses (informational) received from the Kubernetes API Server.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.1xx.NumPerSecond</td>
      <td>Number of HTTP Code 1xx responses (informational) received from the Kubernetes API Server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.2xx.Count</td>
      <td>Number of HTTP Code 2xx responses (success) received from the Kubernetes API Server.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.2xx.NumPerSecond</td>
      <td>Number of HTTP Code 2xx responses (success) received from the Kubernetes API Server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.3xx.Count</td>
      <td>Number of HTTP Code 3xx responses (redirection) received from the Kubernetes API Server.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.3xx.NumPerSecond</td>
      <td>Number of HTTP Code 3xx responses (redirection) received from the Kubernetes API Server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.4xx.Count</td>
      <td>Number of HTTP Code 4xx responses (client error) received from the Kubernetes API Server.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.4xx.NumPerSecond</td>
      <td>Number of HTTP Code 4xx responses (client error) received from the Kubernetes API Server per second.</td>
      <td>Meter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.5xx.Count</td>
      <td>Number of HTTP Code 5xx responses (server error) received from the Kubernetes API Server.</td>
      <td>Counter</td>
    </tr>
    <tr>
      <td>KubeClient.HttpResponse.5xx.NumPerSecond</td>
      <td>Number of HTTP Code 5xx responses (server error) received from the Kubernetes API Server per second.</td>
      <td>Meter</td>
    </tr>
  </tbody>
</table>

### JVM Metrics

The Operator gathers metrics about the JVM process and exposes it similarly to core Flink [System metrics](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics/#system-metrics). The list of metrics are not repeated in this document.

### JOSDK Metrics

The Flink operator also forwards metrics created by the [Java Operator SDK](https://javaoperatorsdk.io/) (JOSDK) framework itself under the `JOSDK` metric name prefix. Some of these metrics are on system, namespace and resource level.

The full list of emitted metrics (reconciliation counts and timings, event-source / controller-level gauges, etc.) is not repeated here as it is owned by the JOSDK project and may evolve across versions. Refer to the upstream [JOSDK metrics documentation](https://javaoperatorsdk.io/docs/documentation/operations/metrics/) for the authoritative description of the available metrics and their semantics.

## Metric Reporters

The well known [Metric Reporters](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/metric_reporters) are shipped in the operator image and are ready to use. The following reporters are bundled by default: SLF4J, Prometheus, JMX, Graphite, InfluxDB, Datadog, StatsD, Dropwizard and OpenTelemetry.

Any other Flink-compatible `MetricReporterFactory` can be added by dropping its plugin jar into `/opt/flink/plugins/<name>/` in a custom image.

### Operator-scoped Metric Configuration

To keep operator metrics separate from the Flink-job metrics managed by the operator, the operator accepts the standard Flink `metrics.*` keys under its own `kubernetes.operator.metrics.*` prefix. At startup the `kubernetes.operator.` prefix is stripped and the remainder is forwarded to the operator's Flink metric registry.

Reporter options follow Flink's schema verbatim and are therefore not repeated on the [Configuration]({{< ref "docs/operations/configuration" >}}) page. Only operator-specific toggles (JVM / JOSDK / Kubernetes client metrics, resource & lifecycle metrics, scope formats) are listed there.

### Examples
#### Slf4j
The default metrics reporter in the operator is Slf4j. It does not require any external monitoring systems, and it is enabled in the `values.yaml` file by default, mainly for demonstrating purposes.
```yaml
defaultConfiguration:
   create: true
   append: true
   flink-conf.yaml: |+
      kubernetes.operator.metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory
      kubernetes.operator.metrics.reporter.slf4j.interval: 5 MINUTE
```
To use a more robust production grade monitoring solution the configuration needs to be changed.

#### Prometheus
The following example shows how to enable the Prometheus metric reporter:
```yaml
defaultConfiguration:
   create: true
   append: true
   flink-conf.yaml: |+
      # Prometheus is pull-based; no interval is needed.
      kubernetes.operator.metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
      kubernetes.operator.metrics.reporter.prom.port: 9999
```
Some metric reporters, including the Prometheus, need a port to be exposed on the container. This can be achieved be defining a value for the otherwise empty `metrics.port` variable.
Either in the `values.yaml` file:
```yaml
metrics:
   port: 9999
```
or using the option `--set metrics.port=9999` in the command line.

#### Configuring Reporters on a FlinkDeployment

Reporters for a managed Flink cluster are configured directly on the `FlinkDeployment` resource under `spec.flinkConfiguration`. These keys are consumed by the Flink cluster itself (JobManager / TaskManagers) and therefore use the plain `metrics.reporter.*` prefix, without `kubernetes.operator.`, which is reserved for the operator JVM:

```yaml
spec:
   ...
   flinkConfiguration:
      metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
      metrics.reporter.prom.port: 9249-9250
```

### Monitoring the Operator with Prometheus
The Prometheus Operator among other options provides an elegant, declarative way to specify how group of pods should be monitored using custom resources.

To install the Prometheus operator via Helm run:

```shell
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/kube-prometheus-stack
```
The Grafana dashboard can be accessed through port-forwarding:
```shell
kubectl port-forward deployment/prometheus-grafana 3000
```
The credentials for the dashboard can be retrieved by:
```bash
kubectl get secret prometheus-grafana -o jsonpath="{.data.admin-user}" | base64 --decode ; echo
kubectl get secret prometheus-grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo
```

To enable the operator metrics in Prometheus create a `pod-monitor.yaml` file with the following content:
```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
   name: flink-kubernetes-operator
   labels:
      release: prometheus
spec:
   selector:
      matchLabels:
         app.kubernetes.io/name: flink-kubernetes-operator
   podMetricsEndpoints:
      - port: metrics
```
and apply it on your Kubernetes environment:
```shell
kubectl create -f pod-monitor.yaml
```
Once the custom resource is created in the Kubernetes environment the operator metrics are ready to explore [http://localhost:3000/explore](http://localhost:3000/explore).


## Logging
The Operator controls the logging behaviour for Flink applications and the Operator itself using configuration files mounted externally via ConfigMaps. [Configuration files](https://github.com/apache/flink-kubernetes-operator/tree/main/helm/flink-kubernetes-operator/conf) with default values are shipped in the Helm chart. It is recommended to review and adjust them if needed in the `values.yaml` file before deploying the Operator in production environments.

The operator supports two logging frameworks: **Log4j2** (default) and **Logback**. Both sets of configuration files are shipped in the Helm chart and Docker image. The active framework is selected at install time via the `logging.framework` Helm value, either in `values.yaml` or on the command line:

```shell
# Use the default Log4j2 framework (explicit, same as omitting the flag)
helm install flink-operator helm/flink-kubernetes-operator --set logging.framework=log4j2

# Switch to Logback
helm install flink-operator helm/flink-kubernetes-operator --set logging.framework=logback
```

This controls three things:
1. Which logging configuration files are mounted into `/opt/flink/conf/`
2. Which JVM system property is passed to select the framework (`log4j.configurationFile` or `logback.configurationFile`)
3. Which SLF4J binding JAR is placed on the classpath at runtime

{{< hint info >}}
Logging in the operator is intentionally succinct and does not include contextual information such as namespace or name of the FlinkDeployment objects.
We rely on the MDC provided by the operator-sdk to access this information and use it directly in the log layout.

See the [Java Operator SDK docs](https://javaoperatorsdk.io/docs/features#contextual-info-for-logging-with-mdc) for more detail.
{{< /hint >}}

To learn more about accessing the job logs or changing the log level dynamically check the corresponding [section](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#logging) of the core documentation.

### Log4j2

Log4j2 is the default logging framework. To append/override the default log configuration properties for the operator and Flink deployments define the `log4j-operator.properties` and `log4j-console.properties` keys respectively:

```yaml
defaultConfiguration:
   create: true
   append: true
   log4j-operator.properties: |+
      # Flink Operator Logging Overrides
      # rootLogger.level = DEBUG
      # The monitor interval in seconds to enable log4j automatic reconfiguration
      # monitorInterval = 30
   log4j-console.properties: |+
      # Flink Deployment Logging Overrides
      # rootLogger.level = DEBUG
      # The monitor interval in seconds to enable log4j automatic reconfiguration
      # monitorInterval = 30
```

### Logback

When using Logback, the equivalent configuration keys are `logback-operator.xml` and `logback-console.xml`:

```yaml
logging:
  framework: logback

defaultConfiguration:
  create: true
  append: true
  logback-operator.xml: |+
    <!-- Flink Operator Logback Overrides -->
    <!-- <configuration>
           <appender name="ConsoleAppender" class="ch.qos.logback.core.ConsoleAppender">
             <encoder>
               <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level %-60logger{26} %X - %msg%n</pattern>
             </encoder>
           </appender>
           <root level="DEBUG">
             <appender-ref ref="ConsoleAppender" />
           </root>
         </configuration> -->
  logback-console.xml: |+
    <!-- Flink Deployment Logback Overrides -->
    <!-- <configuration>
           <appender name="ConsoleAppender" class="ch.qos.logback.core.ConsoleAppender">
             <encoder>
               <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level %-60logger{26} %X - %msg%n</pattern>
             </encoder>
           </appender>
           <root level="DEBUG">
             <appender-ref ref="ConsoleAppender" />
           </root>
         </configuration> -->
```

{{< hint warning >}}
**Logback XML overrides replace the entire default configuration.** Unlike Log4j2 `.properties` files where user entries are appended, XML files cannot be concatenated (two `<configuration>` root elements produce invalid XML). Ensure your custom `logback-operator.xml` or `logback-console.xml` is complete and self-contained. Logback supports only XML-based configuration and does not provide native support for `.properties` configuration files.
{{< /hint >}}

### Logging Library Version Overrides

The operator ships with **Logback 1.2.x** and **SLF4J 1.7.x**. These versions are bundled in the Docker image and the SLF4J 1.7.x API is shaded into the operator JAR.

{{< hint warning >}}
**Upgrading to Logback 1.4+/1.5+ or SLF4J 2.x is not supported.** SLF4J 2.x uses a `ServiceLoader`-based binding mechanism that is incompatible with the SLF4J 1.7.x API shaded inside the operator. Replacing the JARs at runtime will result in `ClassNotFoundException: org.slf4j.impl.StaticLoggerBinder`.
{{< /hint >}}

### FlinkDeployment Logging Configuration

Users have the freedom to override the default logging settings on a per-deployment level by putting the entire log configuration into `spec.logConfiguration`.

For Log4j2:

```yaml
spec:
   ...
   logConfiguration:
      log4j-console.properties: |+
         rootLogger.level = DEBUG
         rootLogger.appenderRef.file.ref = LogFile
         ...
```

For Logback:

```yaml
spec:
  ...
  logConfiguration:
    logback-console.xml: |+
      <configuration>
        <appender name="ConsoleAppender" class="ch.qos.logback.core.ConsoleAppender">
          <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss,SSS} %-5level %-60.60logger{60} %X - %msg%n</pattern>
          </encoder>
        </appender>
        <root level="DEBUG">
          <appender-ref ref="ConsoleAppender" />
        </root>
      </configuration>
```
