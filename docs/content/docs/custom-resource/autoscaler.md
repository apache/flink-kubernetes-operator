---
title: "Autoscaler"
weight: 4
type: docs
aliases:
- /custom-resource/autoscaler.html
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

# Autoscaler

The operator provides a job autoscaler functionality that collects various metrics from running Flink jobs and automatically scales individual job vertexes (chained operator groups) to eliminate backpressure and satisfy the utilization target set by the user.
By adjusting parallelism on a job vertex level (in contrast to job parallelism) we can efficiently autoscale complex and heterogeneous streaming applications.

Key benefits to the user:
 - Better cluster resource utilization and lower operating costs
 - Automatic parallelism tuning for even complex streaming pipelines
 - Automatic adaptation to changing load patterns
 - Detailed utilization metrics for performance debugging

## Overview

The autoscaler relies on the metrics exposed by the Flink metric system for the individual tasks. The metrics are queried directly from the Flink job.

Collected metrics:
 - Backlog information at each source
 - Incoming data rate at the sources (e.g. records/sec written into the Kafka topic)
 - Record processing rate at each job vertex
 - Busy and backpressured time at each job vertex

{{< hint info >}}
Please note that we are not using any container memory / CPU utilization metrics directly here. High utilization will be reflected in the processing rate and busy time metrics of the individual job vertexes.
{{< /hint >}}

The algorithm starts from the sources and recursively computes the required processing capacity (target data rate) for each operator in the pipeline. At the source vertices, target data rate is equal to incoming data rate (from the Kafka topic).

For downstream operators we compute the target data rate as the sum of the input (upstream) operators output data rate along the given edge in the processing graph.

{{< img src="/img/custom-resource/autoscaler_fig1.png" alt="Computing Target Data Rates" >}}

Users configure the target utilization percentage of the operators in the pipeline, e.g. keep the all operators between 60% - 80% busy. The autoscaler then finds a parallelism configuration such that the output rates of all operators match the input rates of all their downstream operators at the targeted utilization.

In this example we see an upscale operation:

{{< img src="/img/custom-resource/autoscaler_fig2.png" alt="Scaling Up" >}}

Similarly as load decreases, the autoscaler adjusts individual operator parallelism levels to match the current rate over time.

{{< img src="/img/custom-resource/autoscaler_fig3.png" alt="Scaling Down" >}}

The autoscaler approach is based on [Three steps is all you need: fast, accurate, automatic scaling decisions for distributed streaming dataflows](https://www.usenix.org/system/files/osdi18-kalavri.pdf) by Kalavri et al.

## Executing rescaling operations

By default, the autoscaler uses the built-in job upgrade mechanism from the operator to perform the rescaling as detailed in [Job Management and Stateful upgrades]({{< ref "docs/custom-resource/job-management" >}}).

### Flink 1.18 and in-place scaling support

The upcoming Flink 1.18 release brings very significant improvements to the speed of scaling operations through the new resource requirements rest endpoint.
This allows the autoscaler to scale vertices in-place without performing a full job upgrade cycle.

To try this experimental feature, please use the currently available Flink 1.18 snapshot base image to build you application docker image.
Furthermore make sure you set Flink version to `v1_18` in your FlinkDeployment yaml and enable the adaptive scheduler which is required for this feature.

```
jobmanager.scheduler: adaptive
```

## Job Requirements and Limitations

### Requirements

The autoscaler currently only works with [Flink 1.17](https://hub.docker.com/_/flink) and later flink images, or after backporting the following fixes to your 1.15/1.16 Flink images:

 - Job vertex parallelism overrides (must have)
   - [Add option to override job vertex parallelisms during job submission](https://github.com/apache/flink/commit/23ce2281a0bb4047c64def9af7ddd5f19d88e2a9)
   - [Change ForwardPartitioner to RebalancePartitioner on parallelism changes](https://github.com/apache/flink/pull/21443) (consists of 5 commits)
   - [Fix logic for determining downstream subtasks for partitioner replacement](https://github.com/apache/flink/commit/fb482fe39844efda33a4c05858903f5b64e158a3)
 - [Support timespan for busyTime metrics](https://github.com/apache/flink/commit/a7fdab8b23cddf568fa32ee7eb804d7c3eb23a35) (good to have)

For session job auto-scaling, a latest custom build of Flink 1.19 or 1.18 is required that contains the fix for [FLINK-33534](https://issues.apache.org/jira/browse/FLINK-33534).

### Limitations

By default, the autoscaler can work for all job vertices in the processing graph.

However, source scaling requires that the sources:

   - Use the new [Source API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) that exposes the busy time metric (must have, most common connectors already do)
   - Expose the [standardized connector metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics) for accessing backlog information (good to have, extra capacity will be added for catching up with backlog)

In the current state the autoscaler works best with Kafka sources, as they expose all the standardized metrics. It also comes with some additional benefits when using Kafka such as automatically detecting and limiting source max parallelism to the number of Kafka partitions.


## Configuration guide

Depending on your environment and job characteristics there are a few very important configurations that will affect how well the autoscaler works.

Key configuration areas:
 - Job and per operator max parallelism
 - Stabilization and metrics collection intervals
 - Target utilization and flexible boundaries
 - Target catch-up duration and restart time

The defaults might work reasonably well for many applications, but some tuning may be required in this early stage of the autoscaler module.

{{< hint info >}}
The autoscaler also supports a passive/metrics-only mode where it only collects and evaluates scaling related performance metrics but does not trigger any job upgrades.
This can be used to gain confidence in the module without any impact on the running applications.

To disable scaling actions, set: `job.autoscaler.scaling.enabled: "false"`
{{< /hint >}}

### Job and per operator max parallelism

When computing the scaled parallelism, the autoscaler always considers the max parallelism settings for each job vertex to ensure that it doesn't introduce unnecessary data skew.
The computed parallelism will always be a divisor of the max parallelism number.

To ensure flexible scaling it is therefore recommended to choose max parallelism settings that have a [lot of divisors](https://en.wikipedia.org/wiki/Highly_composite_number) instead of relying on the Flink provided defaults.
You can then use the `pipeline.max-parallelism` to configure this for your pipeline.

Some good numbers for max-parallelism are: 120, 180, 240, 360, 720 etc.

It is also possible to set maxParallelism on a per operator level, which can be useful if we want to avoid scaling some sources/sinks beyond a certain number.

### Stabilization and metrics collection intervals

The autoscaler always looks at average metrics in the collection time window defined by `job.autoscaler.metrics.window`.
The size of this window determines how small fluctuations will affect the autoscaler. The larger the window, the more smoothing and stability we get, but we may be slower to react to sudden load changes.
We suggest you experiment with setting this anywhere between 3-60 minutes for best experience.

To allow jobs to stabilize after recovery users can configure a stabilization window by setting `job.autoscaler.stabilization.interval`.
During this time period no metrics will be collected and no scaling actions will be taken.

### Target utilization and flexible boundaries

In order to provide stable job performance and some buffer for load fluctuations, the autoscaler allows users to set a target utilization level for the job (`job.autoscaler.target.utilization`).
A target of `0.6` means we are targeting 60% utilization/load for the job vertexes.

In general, it's not recommended to set target utilization close to 100% as performance usually degrades as we reach capacity limits in most real world systems.

In addition to the utilization target we can set a utilization boundary, that serves as extra buffer to avoid immediate scaling on load fluctuations.
Setting `job.autoscaler.target.utilization.boundary: "0.2"` means that we allow 20% deviation from the target utilization before triggering a scaling action.

### Target catch-up duration and restart time

When taking scaling decisions the operator need to account for the extra capacity required to catch up the backlog created during scaling operations.
The amount of extra capacity is determined automatically by the following 3 configs:

 - `job.autoscaler.catch-up.duration` : Time to job is expected to catch up after scaling
 - `job.autoscaler.restart.time` : Time it usually takes to restart the application
 - `job.autoscaler.restart.time-tracking.enabled` : Whether to use the actual observed rescaling restart times instead of the fixed 'job.autoscaler.restart.time' configuration. It's disabled by default.

The maximum restart duration over a number of samples will be used when `job.autoscaler.restart.time-tracking.enabled` is set to true, 
but the target catch-up duration depends on the users SLO.

By lowering the catch-up duration the autoscaler will have to reserve more extra capacity for the scaling actions.
We suggest setting this based on your actual objective, such us 10,30,60 minutes etc.

### Basic configuration example
```yaml
...
flinkVersion: v1_17
flinkConfiguration:
    job.autoscaler.enabled: "true"
    job.autoscaler.stabilization.interval: 1m
    job.autoscaler.metrics.window: 5m
    job.autoscaler.target.utilization: "0.6"
    job.autoscaler.target.utilization.boundary: "0.2"
    job.autoscaler.restart.time: 2m
    job.autoscaler.catch-up.duration: 5m
    pipeline.max-parallelism: "720"
```

### Advanced config parameters

The autoscaler also exposes various more advanced config parameters that affect scaling actions:

 - Minimum time before scaling down after scaling up a vertex
 - Maximum parallelism change when scaling down
 - Min/max parallelism

The list of options will likely grow to cover more complex scaling scenarios.

For a detailed config reference check the [general configuration page]({{< ref "docs/operations/configuration#autoscaler-configuration" >}})

## Extensibility of Autoscaler

The Autoscaler exposes a set of interfaces for storing autoscaler state, handling autoscaling events,
and executing scaling decisions. How these are implemented is specific to the orchestration framework
used (e.g. Kubernetes), but the interfaces are designed to be as generic as possible. The following
are the introduction of these generic interfaces:

- **AutoScalerEventHandler** : Handling autoscaler events, such as: ScalingReport,
  AutoscalerError, etc. `LoggingEventHandler` is the default implementation, it logs events.
- **AutoScalerStateStore** : Storing all state during scaling. `InMemoryAutoScalerStateStore` is
  the default implementation, it's based on the Java Heap, so the state will be discarded after
  process restarts. We will implement persistent State Store in the future, such as : `JdbcAutoScalerStateStore`.
- **ScalingRealizer** : Applying scaling actions.
- **JobAutoScalerContext** : Including all details related to the current job.

## Autoscaler Standalone

**Flink Autoscaler Standalone** is an implementation of **Flink Autoscaler**, it runs as a separate java
process. It computes the reasonable parallelism of all job vertices by monitoring the metrics, such as:
processing rate, busy time, etc.

Flink Autoscaler Standalone rescales flink job in-place by rest api of
[Externalized Declarative Resource Management](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/elastic_scaling/#externalized-declarative-resource-management).
`RescaleApiScalingRealizer` is the default implementation of `ScalingRealizer`, it uses the Rescale API
to apply parallelism changes.

Kubernetes Operator is well integrated with Autoscaler, we strongly recommend using Kubernetes Operator
directly for the kubernetes flink jobs, and only flink jobs in non-kubernetes environments use Autoscaler
Standalone.

### How To Use Autoscaler Standalone

Currently, `Flink Autoscaler Standalone` only supports a single Flink cluster. It can be any type of
Flink cluster, includes:

- Flink Standalone Cluster
- MiniCluster
- Flink yarn session cluster
- Flink yarn application cluster
- Flink kubernetes session cluster
- Flink kubernetes application cluster
- etc

You can start a Flink Streaming job with the following ConfigOptions.

```
# Enable Adaptvie scheduler to play the in-place rescaling.
jobmanager.scheduler : adaptive

# Enable autoscale and scaling
job.autoscaler.enabled : true
job.autoscaler.scaling.enabled : true
job.autoscaler.stabilization.interval : 1m
job.autoscaler.metrics.window : 3m
```

> Note: In-place rescaling is only supported since Flink 1.18. Flink jobs before version
> 1.18 cannot be scaled automatically, but you can view the `ScalingReport` in Log.
> `ScalingReport` will show the recommended parallelism for each vertex.

After the flink job starts, please start the StandaloneAutoscaler process by the
following command. Please download released autoscaler-standalone jar from
[here](https://repo.maven.apache.org/maven2/org/apache/flink/flink-autoscaler-standalone/) first.

```
java -cp flink-autoscaler-standalone-{{< version >}}.jar \
org.apache.flink.autoscaler.standalone.StandaloneAutoscalerEntrypoint \
--autoscaler.standalone.fetcher.flink-cluster.host localhost \
--autoscaler.standalone.fetcher.flink-cluster.port 8081
```

Updating the `autoscaler.standalone.fetcher.flink-cluster.host` and `autoscaler.standalone.fetcher.flink-cluster.port`
based on your flink cluster. In general, the host and port are the same as Flink WebUI.

All autoscaler related options can be set at autoscaler standalone level, and the configuration at job-level can 
override the default value provided in the autoscaler standalone, such as:

- job.autoscaler.enabled
- job.autoscaler.metrics.window
- etc

### Using the JDBC Autoscaler State Store & Event Handler

A driver dependency is required to connect to a specified database. Here are drivers currently supported,
please download JDBC driver and initialize database and table first.

| Driver     | Group Id                   | Artifact Id            | JAR                                                                             | Schema                  |
|:-----------|:---------------------------|:-----------------------|:--------------------------------------------------------------------------------|-------------------------|
| MySQL      | `mysql`                    | `mysql-connector-java` | [Download](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/)    | [Table DDL](https://github.com/apache/flink-kubernetes-operator/blob/main/flink-autoscaler-plugin-jdbc/src/main/resources/schema/mysql_schema.sql)     |
| PostgreSQL | `org.postgresql`           | `postgresql`           | [Download](https://jdbc.postgresql.org/download/)                               | [Table DDL](https://github.com/apache/flink-kubernetes-operator/blob/main/flink-autoscaler-plugin-jdbc/src/main/resources/schema/postgres_schema.sql)  |
| Derby      | `org.apache.derby`         | `derby`                | [Download](http://db.apache.org/derby/derby_downloads.html)                     | [Table DDL](https://github.com/apache/flink-kubernetes-operator/blob/main/flink-autoscaler-plugin-jdbc/src/main/resources/schema/derby_schema.sql)     |

```
JDBC_DRIVER_JAR=./mysql-connector-java-8.0.30.jar
# export the password of jdbc state store & jdbc event handler
export JDBC_PWD=123456

java -cp flink-autoscaler-standalone-{{< version >}}.jar:${JDBC_DRIVER_JAR} \
org.apache.flink.autoscaler.standalone.StandaloneAutoscalerEntrypoint \
--autoscaler.standalone.fetcher.flink-cluster.host localhost \
--autoscaler.standalone.fetcher.flink-cluster.port 8081 \
--autoscaler.standalone.state-store.type jdbc \
--autoscaler.standalone.event-handler.type jdbc \
--autoscaler.standalone.jdbc.url jdbc:mysql://localhost:3306/flink_autoscaler \
--autoscaler.standalone.jdbc.username root
```

### Load StandaloneAutoscaler configuration through configuration file

Can specify the running configuration of StandaloneAutoscaler in the configuration file. The config.yaml and
flink-conf.yaml files in the environment variable `FLINK_CONF_DIR` directory will be loaded by default.
NOTE: main parameters will overwrite the configuration in the configuration file

All supported options for autoscaler standalone can be viewed
[here]({{< ref "docs/operations/configuration#autoscaler-standalone-configuration" >}}).

### Extensibility of autoscaler standalone

Please click [here]({{< ref "docs/custom-resource/autoscaler#extensibility-of-autoscaler" >}})
to check out extensibility of generic autoscaler.

`Autoscaler Standalone` isn't responsible for job management, so it doesn't have job information.
`Autoscaler Standalone` defines the `JobListFetcher` interface in order to get the
`JobAutoScalerContext` of the job. It has a control loop that periodically calls
`JobListFetcher#fetch` to fetch the job list and scale these jobs.

Currently `FlinkClusterJobListFetcher` is the only implementation of the `JobListFetcher`
interface, that's why `Flink Autoscaler Standalone` only supports a single Flink cluster so far.
We will implement `YarnJobListFetcher` in the future, `Flink Autoscaler Standalone` will call
`YarnJobListFetcher#fetch` to fetch job list from yarn cluster periodically.

## Metrics

The operator reports detailed jobvertex level metrics about the evaluated Flink job metrics that are collected and used in the scaling decision.

This includes:
 - Utilization, input rate, target rate metrics
 - Scaling thresholds
 - Parallelism and max parallelism changes over time

These metrics are reported under the Kubernetes Operator Resource metric group:

```
[resource_prefix].Autoscaler.[jobVertexID].[ScalingMetric].Current/Average
```
