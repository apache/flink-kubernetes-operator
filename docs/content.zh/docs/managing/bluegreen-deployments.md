---
title: "Blue/Green Deployments"
weight: 3
type: docs
aliases:
- /concepts/bluegreen-controller-flow.html
- /docs/concepts/bluegreen-controller-flow/
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

# Blue/Green Deployments

{{< hint warning >}}
Blue/green deployments are an experimental feature. The API and the behavior described on this page may still change between releases.
{{< /hint >}}

A `FlinkBlueGreenDeployment` runs a Flink application as two alternating deployments, blue and green, and upgrades it without stopping the pipeline, as described under [Zero-Downtime Upgrades]({{< ref "docs/concepts/zero-downtime-upgrades" >}}). Beyond the uninterrupted processing itself it provides:

- **State preservation**: the application state is carried across versions through savepoints.
- **Safe rollback capability**: the previous version keeps running until the new one proves itself, and a failed upgrade is aborted without touching the serving deployment.
- **Validation before the switch**: a new deployment becomes active only after it has been observed running and stable.

How the two deployments hand over is set by `spec.transitionMode`. The default, `BASIC`, switches at the deployment level: once the new deployment is stable the old one is retired, and while both run they write the same records. `ADVANCED` switches at the record level: a gate injected into both jobs decides which of them writes each record, so the hand-over neither duplicates nor drops the records that arrive within the job's watermark. It is described under [Advanced Transition Mode](#advanced-transition-mode). The mode is fixed at the first deployment: changing it later is rejected.

## Requirements

- **Application Mode**: blue/green deployments manage application clusters, so the template must define a `spec.job`, as described under [Application Mode]({{< ref "docs/deployment/overview#application-mode" >}}).
- **State handoff**: for every upgrade mode except `stateless` the transition hands the state over through a savepoint, so checkpointing and a savepoint directory (`state.savepoints.dir`) must be configured just like for regular [stateful upgrades]({{< ref "docs/managing/job-management#upgrades" >}}). This currently applies to `last-state` as well.
- **Savepoint format**: the transition savepoint is taken in the format set by `kubernetes.operator.savepoint.format.type`, `CANONICAL` by default. On Flink 2.x, operators using the async state API cannot take a canonical savepoint: the attempt fails the job over, the transition fails with an error that names the setting, and such a job needs `NATIVE`. A native savepoint can only be restored with the state backend that took it, so a transition that changes `state.backend.type` is rejected while the format is `NATIVE`. Changing the format itself is patched onto the running deployment, without a transition.
- **ADVANCED transition mode**: further requirements, among them a Java 17 JobManager and event-time watermarks, are listed under [ADVANCED Requirements](#advanced-requirements).

## Creating a Blue/Green Deployment

Converting an existing `FlinkDeployment` requires three changes to the resource definition:

{{< img src="/img/managing/BlueGreenConfigurationQuickstart.png" alt="Blue/Green migration quick start" >}}

1. **Change the resource kind** from `FlinkDeployment` to `FlinkBlueGreenDeployment`.
2. **Optionally add a `configuration` block** at the top level of the spec with the blue/green specific settings, listed further down under Configuration.
3. **Wrap the existing spec in a template**: everything that used to live under `spec` moves under `spec.template.spec`, unchanged.

A minimal resource looks like this:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  name: basic-bg-example
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: "10 min"
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: "15s"
    kubernetes.operator.bluegreen.deployment-deletion.delay: "0ms"
  template:
    spec:
      image: flink:1.20
      flinkVersion: v1_20
      flinkConfiguration:
        taskmanager.numberOfTaskSlots: "2"
        execution.checkpointing.interval: "10 s"
        state.checkpoints.dir: s3://flink-data/checkpoints
        state.savepoints.dir: s3://flink-data/savepoints
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
      job:
        jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
        parallelism: 2
        upgradeMode: savepoint
```

From here on the resource behaves as described on the concepts page: the first deployment comes up as blue, and every later spec change transitions the application to the other color.

## Deployment States

The current phase of a blue/green resource is reported in `status.blueGreenState`:

| State                    | Meaning                                                                    |
|--------------------------|----------------------------------------------------------------------------|
| `INITIALIZING_BLUE`      | First deployment of the resource, the blue deployment is being created     |
| `ACTIVE_BLUE`            | Steady state, the blue deployment is running and serving                   |
| `SAVEPOINTING_BLUE`      | A savepoint is being taken from blue to seed the upcoming green deployment |
| `TRANSITIONING_TO_GREEN` | The green deployment is starting while blue keeps serving                  |
| `ACTIVE_GREEN`           | Steady state, the green deployment is running and serving                  |
| `SAVEPOINTING_GREEN`     | A savepoint is being taken from green to seed the upcoming blue deployment |
| `TRANSITIONING_TO_BLUE`  | The blue deployment is starting while green keeps serving                  |

The savepointing states appear only for stateful upgrade modes, and the resource always settles in one of the two active states:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/managing/bluegreen-deployment-states.svg" alt="Blue/green deployment states and transitions" >}}

A transition is easiest to follow by watching the resource and its children side by side:

```shell
# The blue/green resource and the state of the currently active job
kubectl get flinkbgdep basic-bg-example
kubectl get flinkbgdep basic-bg-example -o jsonpath='{.status.blueGreenState}'

# The child deployments appearing and disappearing during a transition
kubectl get flinkdep
```

How the controller drives these states internally, including the abort and error paths, is documented under [Blue/Green Controller]({{< ref "docs/internals/controllers#blue-green-controller" >}}).

## Spec Change Behavior

Not every spec change causes a transition. The controller compares the desired template against the last reconciled one and reacts proportionally:

| Spec change                                                                                                                                                                                   | Behavior                                                                                                         |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| No effective change to the `spec.template`                                                                                                                                                    | Nothing, the active deployment keeps running                                                                     |
| `job.state` changed between `running` and `suspended`                                                                                                                                         | The active deployment is suspended or resumed in place, no transition                                            |
| `flinkConfiguration` keys under `job.autoscaler.*`, `parallelism.default` or `kubernetes.operator.*`, and job fields such as `upgradeMode`, `initialSavepointPath` or `allowNonRestoredState` | Patched onto the active deployment in place, no transition                                                       |
| `savepointRedeployNonce` changed                                                                                                                                                              | A full transition that restores the new deployment from `initialSavepointPath` instead of taking a new savepoint |
| Any other template change                                                                                                                                                                     | A full blue/green transition                                                                                     |

Changes that arrive while a transition is already running are applied to the incoming deployment, so the transition completes directly with the newest spec. The one exception is suspending: a `job.state: suspended` request during a transition is deferred until the transition completes, and the then-active deployment is suspended afterwards.

## Kubernetes Resources

The resource materializes as up to two child `FlinkDeployment` resources named `<name>-blue` and `<name>-green`, each a complete Flink cluster with the standard object tree shown in the [Deployment Overview]({{< ref "docs/deployment/overview" >}}). In steady state only the active child exists, during transitions both do. The children are owned by the blue/green resource, so deleting it removes both together with everything below them. They are managed entirely by the controller and should not be edited directly, manual changes are overwritten when the controller next reconciles the template.

In the `ADVANCED` transition mode the resource also owns a ConfigMap named `<name>-configmap`, through which the controller and the gates in the two jobs coordinate the hand-over, described under [Advanced Transition Mode](#advanced-transition-mode).

For reaching the Flink Web UI and REST API there are two ingress layers, both created only while the operator manages ingress resources (`kubernetes.operator.ingress.manage`, enabled by default):

- An ingress defined at the top level of the spec (`spec.ingress`) is owned by the blue/green resource and always routes to the REST service of the currently active deployment.
- An ingress defined inside the template (`spec.template.spec.ingress`) is created per child, with the color prefixed to its ingress template so that the blue and green endpoints do not collide.

Traffic switching happens through the active ingress: when a transition completes, its backend is updated from the old to the new REST service, for example from `basic-bg-example-blue-rest` to `basic-bg-example-green-rest`, and the ingress controller shifts connections to the new target service without downtime. When ingress management is disabled, no ingress is created or updated and traffic management is expected to be handled externally.

## Health Monitoring and Recovery

A new deployment is considered successful when its job reports `RUNNING` and the child resource reaches the `STABLE` lifecycle state ([Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}})). During a transition the controller re-checks these conditions on the rescheduling interval, and the new deployment has the length of the abort grace period to meet them.

If the grace period expires first, the transition is aborted:

- The incoming deployment is suspended and kept for inspection, it is not deleted.
- The previous deployment simply keeps serving, it was never stopped during the transition.
- `status.blueGreenState` returns to the previous active state and the resource status reports the failure.

The abort mechanism ensures that a failed deployment does not disrupt production traffic, while everything needed to investigate what went wrong with the attempted deployment stays in place.

In the `ADVANCED` transition mode the gate adds a deadline of its own and one more step to an abort, both covered under [Aborts](#aborts).

Recovery is then an ordinary next upgrade: inspect the suspended child for the root cause, correct the template, and apply it. The next spec change starts a fresh transition that redeploys the same color from the corrected spec.

## Advanced Transition Mode

{{< hint warning >}}
Like blue/green deployments as a whole, the `ADVANCED` transition mode is experimental.
{{< /hint >}}

In the `ADVANCED` transition mode the operator injects a gate operator into the jobs of both deployments. Both jobs read the same input, so every record reaches both gates, and the gates decide which of the two jobs writes it. The jobs coordinate through the resource's ConfigMap, and the controller retires the old deployment only once its gate has handed everything over.

### How the Hand-Over Works

The gates use two kinds of time:

- **The job's watermark**, produced by its `WatermarkStrategy` (or the `WATERMARK FOR` clause in SQL), drives the gates. When the new deployment is ready, the old deployment's gate proposes its own watermark plus the deployment deletion delay as the cutover point. Every subtask of that gate may propose one, the first written to the ConfigMap wins and all of them use it. The old deployment stops writing once its watermark passes the cutover point, and the new deployment writes every record once its own watermark passes it.
- **Each record's timestamp**, read by the extraction strategy the job configures, decides who writes the record until then: the old deployment writes records older than the cutover point, the new deployment the rest.

What this gives you:

- Every record that arrives within the job's watermark is written exactly once.
- Late records are written by the old deployment before the crossing and by the new deployment after it. Only late records arriving right around the crossing can be written twice or dropped, because the watermarks of the two jobs do not advance in lockstep.
- A record without a timestamp (a null field or column, or an extractor returning `null`) counts as older than the cutover point: the old deployment writes it before the crossing, the new deployment after.

Both kinds of time must be the same event time: the extracted timestamp must be the epoch milliseconds of the time the watermark is built from. The gates never use the wall clock. A job without event-time watermarks never gets a cutover point, and the transition is aborted once the gate timeout expires.

#### Teardown

The first subtask of the old gate whose watermark passes the cutover point signals that the old deployment may be retired, but its other subtasks may still have records to write: without a shuffle in front of the gate, each subtask follows only its own upstream. Each gate subtask therefore reports a `bluegreenGateHandOverDone` gauge, 1 once its watermark has passed the cutover point, or once it has received no record for 30 seconds since it learned the cutover point. The controller reads the minimum of that gauge over all subtasks through the Flink REST API and waits until it is 1.

The old deployment is then suspended with a savepoint, whatever its upgrade mode, and deleted once stopped. The savepoint makes sinks that commit on checkpoints, such as transactional Kafka or file sinks, commit what the old deployment wrote since its last checkpoint: deleting a running job would cancel it and discard that output, and the new deployment does not write those records again. Without a savepoint directory the old deployment is deleted directly, with a warning in the operator log.

#### Aborts

The gate phase, from the moment the new deployment is ready until every subtask of the old gate is done, is bounded by `kubernetes.operator.bluegreen.gate.timeout`. If the hand-over does not complete in that time, because a watermark stalls or the new deployment keeps failing for example, the transition is aborted as described under [Health Monitoring and Recovery](#health-monitoring-and-recovery), and the gate of the deployment that keeps serving is reset so that it passes every record again. What else happens depends on how far the hand-over got:

- Before the cutover point is set, the old deployment was still writing every record and simply carries on.
- Once it is set, the old deployment had left the records from it on to the new one, which may not have written or committed them. The old deployment is therefore redeployed from the savepoint the new deployment started from, through its `savepointRedeployNonce`, so those records are written again. Records written since the transition started may be written twice.

### ADVANCED Requirements

- **A Java 17 JobManager**: the gate is injected by a Java agent compiled to Java 17 bytecode and loaded at JobManager startup, so a Java 11 JobManager fails to start with `UnsupportedClassVersionError`. `BASIC` deployments are not affected.
- **Event-time watermarks**: the cutover point and both crossings come from the job's watermark.
- **The `savepoint` or `last-state` upgrade mode**: the new deployment must start from the transition savepoint of the old one, and an abort after the cutover point redeploys the old deployment from it. `stateless` is rejected. On Flink 2.x, a job using the async state API needs that savepoint in the native format, see [Requirements](#requirements).
- **The gate client in the job JAR**: the agent provides only the injection hook, the gate classes are loaded from the job. Add `org.apache.flink:flink-kubernetes-operator-bluegreen-client` at the operator's version, bundled into the job JAR rather than `provided`, so that the TaskManagers can load it. The client links against Flink internals and is built per Flink major version: the default artifact targets Flink 1.x, and the `flink2` classifier targets Flink 2.x. The `examples/flink-bluegreen-advanced-example` job bundles it.
- **`OPERATOR_IMAGE` on the operator**: the agent is copied out of the operator image by an init container, so the operator must know its own image. The Helm chart sets it, and the operator rejects `ADVANCED` deployments without it.

### Enabling It

Set the transition mode, a gate strategy, and exactly one way to read each record's timestamp:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  name: advanced-bg-example
spec:
  transitionMode: ADVANCED
  configuration:
    kubernetes.operator.bluegreen.gate.timeout: "10 min"
  template:
    spec:
      flinkConfiguration:
        bluegreen.gate.strategy: WATERMARK
        bluegreen.gate.watermark.field-path: eventTime
        # rest of the flinkConfiguration as in the BASIC example
      job:
        jarURI: local:///opt/flink/usrlib/my-job.jar
        upgradeMode: savepoint
      # rest of the template as in the BASIC example
```

The operator then adds the `-javaagent` flag to the JobManager JVM options, an init container that copies the agent out of the operator image, and `bluegreen.gate.injection.enabled: "true"`. At submission the agent inserts the gate into the job graph, and the JobManager log shows `[BlueGreen Agent] Gate injection applied to StreamGraph`.

### Reading Each Record's Timestamp

A job sets exactly one of the following, in its `flinkConfiguration`. A spec with none or more than one is rejected, and the job fails at startup when the chosen strategy cannot read the records at the gate.

| Key                                        | Reads                                                                                                                                                             |
|--------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bluegreen.gate.watermark.field-path`      | A `long` field of the record, by dot-notation path, for example `eventTime` or `metadata.timestamp`. Public or private fields and getters are resolved, so protobuf classes work. Not for `RowData` records. |
| `bluegreen.gate.watermark.field-index`     | A column of a `RowData` record, by 0-based position. The column must be `BIGINT` (epoch milliseconds), `TIMESTAMP(p)` or `TIMESTAMP_LTZ(p)`.                     |
| `bluegreen.gate.watermark.extractor-class` | Anything else: the fully qualified name of a class implementing `org.apache.flink.kubernetes.operator.bluegreen.client.WatermarkExtractor<T>`, with a no-argument constructor, fully serializable, returning the epoch milliseconds or `null` when the record has none. |

The value is used as it is, without unit conversion: a field in microseconds or seconds needs an extractor class that converts it. The path, position or extractor applies to the record at the gate, so it must match the type flowing there.

### Gate Placement

The gate is placed at a single point of the job graph, set by `bluegreen.gate.injection.position`:

| Position                | Gate placement                                     | Works when                 |
|-------------------------|----------------------------------------------------|----------------------------|
| `BEFORE_SINK` (default) | In front of the operator that writes the records   | The job has exactly 1 sink   |
| `AFTER_SOURCE`          | Behind the source                                  | The job has exactly 1 source |

For a sink with a committer, the operator that writes the records is the sink's writer, not the committer behind it. Some sinks end in an operator that receives no records, the legacy Apache Iceberg `FlinkSink` for example, which runs as a writer, a committer and a no-op sink. The job then fails at startup until `bluegreen.gate.injection.before-uid` names the operator to gate by its uid, `<uidPrefix>-writer` for that sink.

The gate joins the operator chain next to it: it copies the parallelism, the maximum parallelism and the slot sharing group of the operator it sits in front of, and keeps the partitioning of the edge it replaces. If it cannot chain, because chaining is disabled for the job or the neighbouring operator starts its own chain, it runs as a separate operator, the JobManager log names it, and every record costs one more serialization.

For topologies that neither position fits, the gate can be placed in the job code instead. Set `bluegreen.gate.injection.enabled: "false"`, leave out the three extraction keys, and add the gate to the stream that matters:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<MyEvent> gated =
        events.process(
                        WatermarkGateProcessFunction.create(
                                env.getConfiguration().toMap(),
                                (WatermarkExtractor<MyEvent>) MyEvent::getEventTime))
                .name("BlueGreen-Gate");
```

The extractor passed in code is then the gate's extraction strategy, which is why the extraction keys must not be set.

### SQL and Table API Jobs

Records in SQL and Table API jobs are `RowData`: values by position, without field names. They use `bluegreen.gate.watermark.field-index`:

- **Which column**: the one the table's `WATERMARK FOR` clause is defined on, or one holding the same time.
- **Which position**: its position in the row entering the gate. With `BEFORE_SINK` that is the column order of the table written by `INSERT INTO`. `BEFORE_SINK` is the safer choice, with `AFTER_SOURCE` the planner may pass on only the columns the query uses, so positions can change when the query changes.
- **Which types**: the gate reads the column types from the job and checks the position and the type at startup.
- **One sink**: a job with several `INSERT INTO` statements has several sinks, so `BEFORE_SINK` does not apply to it.

DataStream jobs that build `RowData` themselves, for example to feed a sink that takes `RowData`, can use `field-index` too, but the stream entering the gate must declare its row type with `returns(InternalTypeInfo.of(rowType))`. Without it Flink falls back to a generic serializer that carries no column types, and the job fails at startup. An extractor class reads the column itself and needs no declared type. Jobs that convert a table into a `DataStream<Row>` carry `Row` records, not `RowData`, and need an extractor class.

### Gate Configuration

The gate is configured in the template's `flinkConfiguration`:

| Key                                        | Default       | Description                                                                                         |
|--------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------|
| `bluegreen.gate.strategy`                  | (none)        | The gate strategy, required in `ADVANCED` mode. `WATERMARK` is the only one available.              |
| `bluegreen.gate.watermark.field-path`      | (none)        | See [Reading Each Record's Timestamp](#reading-each-records-timestamp), exactly one of the three.   |
| `bluegreen.gate.watermark.field-index`     | (none)        | See [Reading Each Record's Timestamp](#reading-each-records-timestamp), exactly one of the three.   |
| `bluegreen.gate.watermark.extractor-class` | (none)        | See [Reading Each Record's Timestamp](#reading-each-records-timestamp), exactly one of the three.   |
| `bluegreen.gate.injection.enabled`         | `true`        | `false` places no gate, the job adds it in code, see [Gate Placement](#gate-placement).              |
| `bluegreen.gate.injection.position`        | `BEFORE_SINK` | `BEFORE_SINK` or `AFTER_SOURCE`, see [Gate Placement](#gate-placement).                             |
| `bluegreen.gate.injection.before-uid`      | (none)        | With `BEFORE_SINK`, the uid of the operator to place the gate in front of.                          |

The gate timeout is configured with the other blue/green options in the resource's `configuration`, listed under [Configuration](#configuration). It must be longer than the deployment deletion delay, which the cutover point includes.

## Configuration

Blue/green behavior is configured on the resource itself, in the `configuration` map at the top level of the spec. These options are not read from the operator configuration, and the defaults apply when they are unset:

| Key                                                                | Default | Description                                                                                |
|--------------------------------------------------------------------|---------|--------------------------------------------------------------------------------------------|
| `kubernetes.operator.bluegreen.abort.grace-period`                 | 10 min  | Maximum time the new deployment is given to become stable before the transition is aborted |
| `kubernetes.operator.bluegreen.reconciliation.reschedule-interval` | 15 s    | How often the controller re-checks progress during savepointing and transitions            |
| `kubernetes.operator.bluegreen.deployment-deletion.delay`          | 0 ms    | Extra time the old deployment is kept running after the new one becomes stable             |
| `kubernetes.operator.bluegreen.gate.timeout`                       | (none)  | `ADVANCED` mode only: maximum time the gate is given to hand over once the new deployment is ready, the abort grace period when unset. Must be longer than the deployment deletion delay |

The same options are listed with their formal defaults under [Blue/Green Deployment Configuration]({{< ref "docs/deployment/configuration#bluegreen-configuration" >}}).

## State

Everything that drives a transition is kept in the status subresource: the transition state, the last reconciled spec, and the timestamps and savepoint trigger id that gate teardown and abort. The status fields are introduced under [FlinkBlueGreenDeployment]({{< ref "docs/custom-resource/overview#flinkbluegreendeployment" >}}) and listed in full in the [Reference]({{< ref "docs/custom-resource/reference" >}}), and the two child deployments report their own status like any other resource, as documented under [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}).

## Metrics

The operator exports blue/green metrics alongside its other resource metrics: how many resources sit in each transition state and job status per namespace, and counters for failed transitions. They are covered under [Metrics]({{< ref "docs/operations/metrics#flinkbluegreendeployment-lifecycle-metrics" >}}).

In the `ADVANCED` transition mode every gate subtask also reports the `bluegreenGateHandOverDone` gauge as a job metric, described under [Teardown](#teardown).

## Events

The blue/green resource itself does not emit Kubernetes events, failures surface through its status and metrics instead. The two child deployments emit the standard operator events, submissions, upgrades, and errors, catalogued under [Events]({{< ref "docs/operations/events#operator-events" >}}).

## Limitations

- Only application clusters are supported: the template must define a `spec.job`. Session Mode is not supported.
- In the `BASIC` transition mode both jobs process and emit the same records during the overlap, so delivery across the transition is at-least-once. The `ADVANCED` mode coordinates the two jobs record by record instead.
- The `ADVANCED` gate is injected by rewriting the job graph through Flink internals, so it is tested against each supported Flink version and may need changes for new ones.
- The automatic gate placement supports a single sink or a single source, other topologies place the gate in code.
- In the `ADVANCED` mode, late records arriving right around the crossing of the cutover point can be written twice or dropped, and an abort after the cutover point may write records twice.
- The state handoff is currently always savepoint-based: `last-state` transitions also take a savepoint instead of reusing the latest checkpoint information.
- Suspend requests are deferred while a transition is in progress.
- A Flink 2.x job using the async state API takes only native savepoints, so a transition cannot change its state backend.
