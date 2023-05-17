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

The operator provides a job autoscaler functionality that collects various metrics from running Flink jobs and automatically scales individual job vertexes (chained operator groups) to eliminate backpressure and satisfy the utilization and catch-up duration target set by the user.
By adjusting parallelism on a job vertex level (in contrast to job parallelism) we can efficiently autoscale complex and heterogeneous streaming applications.

Key benefits to the user:
 - Better cluster resource utilization and lower operating costs
 - Automatic parallelism tuning for even complex streaming pipelines
 - Automatic adaptation to changing load patterns
 - Detailed utilization metrics for performance debugging

Job requirements:
 - The autoscaler currently only works with the latest [Flink 1.17](https://hub.docker.com/_/flink) or after backporting the following fixes to your 1.15/1.16 Flink image
   - [Job vertex parallelism overrides](https://github.com/apache/flink/commit/23ce2281a0bb4047c64def9af7ddd5f19d88e2a9) (must have)
   - [Support timespan for busyTime metrics](https://github.com/apache/flink/commit/a7fdab8b23cddf568fa32ee7eb804d7c3eb23a35) (good to have)
 - All sources must use the new [Source API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) (most common connectors already do)
 - Source scaling requires sources to expose the [standardized connector metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics) for accessing backlog information (source scaling can be disabled)

In the current state the autoscaler works best with Kafka sources, as they expose all the standardized metrics. It also comes with some additional benefits when using Kafka such as automatically detecting and limiting source max parallelism to the number of Kafka partitions.

{{< hint info >}}
The autoscaler also supports a passive/metrics-only mode where it only collects and evaluates scaling related performance metrics but does not trigger any job upgrades.
This can be used to gain confidence in the module without any impact on the running applications.

To disable scaling actions, set: `kubernetes.operator.job.autoscaler.scaling.enabled: "false"`
{{< /hint >}}

## Configuration guide

Depending on your environment and job characteristics there are a few very important configurations that will affect how well the autoscaler works.

Key configuration areas:
 - Job and per operator max parallelism
 - Stabilization and metrics collection intervals
 - Target utilization and flexible boundaries
 - Target catch-up duration and restart time

The defaults might work reasonably well for many applications, but some tuning may be required in this early stage of the autoscaler module.

### Job and per operator max parallelism

When computing the scaled parallelism, the autoscaler always considers the max parallelism settings for each job vertex to ensure that it doesn't introduce unnecessary data skew.
The computed parallelism will always be a divisor of the max parallelism number.

To ensure flexible scaling it is therefore recommended to choose max parallelism settings that have a [lot of divisors](https://en.wikipedia.org/wiki/Highly_composite_number) instead of relying on the Flink provided defaults.
You can then use the `pipeline.max-parallelism` to configure this for your pipeline.

Some good numbers for max-parallelism are: 120, 180, 240, 360, 720 etc.

It is also possible to set maxParallelism on a per operator level, which can be useful if we want to avoid scaling some sources/sinks beyond a certain number.

### Stabilization and metrics collection intervals

The autoscaler always looks at average metrics in the collection time window defined by `kubernetes.operator.job.autoscaler.metrics.window`.
The size of this window determines how small fluctuations will affect the autoscaler. The larger the window, the more smoothing and stability we get, but we may be slower to react to sudden load changes.
We suggest you experiment with setting this anywhere between 3-60 minutes for best experience.

To allow jobs to stabilize after recovery users can configure a stabilization window by setting `kubernetes.operator.job.autoscaler.stabilization.interval`.
During this time period no metrics will be collected and no scaling actions will be taken.

### Target utilization and flexible boundaries

In order to provide stable job performance and some buffer for load fluctuations, the autoscaler allows users to set a target utilization level for the job (`kubernetes.operator.job.autoscaler.target.utilization`).
A target of `0.6` means we are targeting 60% utilization/load for the job vertexes.

In general, it's not recommended to set target utilization close to 100% as performance usually degrades as we reach capacity limits in most real world systems.

In addition to the utilization target we can set a utilization boundary, that serves as extra buffer to avoid immediate scaling on load fluctuations.
Setting `kubernetes.operator.job.autoscaler.target.utilization.boundary: "0.2"` means that we allow 20% deviation from the target utilization before triggering a scaling action.

### Target catch-up duration and restart time

When taking scaling decisions the operator need to account for the extra capacity required to catch up the backlog created during scaling operations.
The amount of extra capacity is determined automatically by the following 2 configs:

 - `kubernetes.operator.job.autoscaler.restart.time` : Time it usually takes to restart the application
 - `kubernetes.operator.job.autoscaler.catch-up.duration` : Time to job is expected to catch up after scaling

In the future the autoscaler may be able to automatically determine the restart time, but the target catch-up duration depends on the users SLO.

By lowering the catch-up duration the autoscaler will have to reserve more extra capacity for the scaling actions.
We suggest setting this based on your actual objective, such us 1, 5, 10 minutes etc.

### Basic configuration example
```yaml
...
flinkVersion: v1_17
flinkConfiguration:
    kubernetes.operator.job.autoscaler.enabled: "true"
    kubernetes.operator.job.autoscaler.stabilization.interval: 1m
    kubernetes.operator.job.autoscaler.metrics.window: 5m
    kubernetes.operator.job.autoscaler.target.utilization: "0.6"
    kubernetes.operator.job.autoscaler.target.utilization.boundary: "0.2"
    kubernetes.operator.job.autoscaler.restart.time: 2m
    kubernetes.operator.job.autoscaler.catch-up.duration: 5m
    pipeline.max-parallelism: "720"
```

### Advanced config parameters

The autoscaler also exposes various more advanced config parameters that affect scaling actions:

 - Minimum time before scaling down after scaling up a vertex
 - Maximum parallelism change when scaling down
 - Min/max parallelism

The list of options will likely grow to cover more complex scaling scenarios.

For a detailed config reference check the [general configuration page]({{< ref "docs/operations/configuration#autoscaler-configuration" >}})

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
