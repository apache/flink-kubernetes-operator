---
title: "Autoscaler"
weight: 5
type: docs
aliases:
  - /internals/autoscaler-flow/overview.html
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

This page is the deep walkthrough of how the autoscaler reaches a scaling decision: every metric it reads, every formula it computes, the exact conditions it branches on, and how the result is fed back into the running job. It is organized by idea rather than by call order, so each section builds the vocabulary for the next. The user-facing view, requirements, enabling, and tuning, lives under [Autoscaler]({{< ref "docs/managing/autoscaler" >}}), and its three configuration phases, collecting, evaluating, and executing, map onto the sense, understand and decide, and execute stages walked below.

## Operator Integration

The autoscaler reads from Flink directly but never acts on it directly. Metrics and topology are collected straight from the job's REST API, as walked under [Sense](#sense-the-windowed-metric-history), while every decision leaves the autoscaler only as a mutation of the resource's desired spec, written into `spec.flinkConfiguration`:

- a **parallelism** decision becomes `pipeline.jobvertex-parallelism-overrides`,
- a **memory** decision (from memory tuning) becomes the memory config entries plus an adjusted `spec.taskManager.resource.memory`.

In the operator, the autoscaler runs as a step of the reconcile cycle itself, right before the spec diff is computed. The same cycle therefore picks the mutated spec up and rolls it out through the operator's normal diff and apply machinery, as described under [Controllers]({{< ref "docs/internals/controllers#base-reconciler-steps" >}}):

```
 reconcile cycle
   ├─ run the autoscaler  ─► decision written into spec.flinkConfiguration
   └─ reconcile the now-changed spec, by diff type:
        parallelism-only change   ─► SCALE     ─► in-place rescale          (no restart)
        memory / other change     ─► UPGRADE   ─► stop + redeploy + restore (restart)
```

- Parallelism is applied in place, without a restart. A spec change that only touches parallelism overrides is a `SCALE` diff. In native mode the new per-vertex resource requirements are sent to Flink's adaptive scheduler with `PUT /jobs/:jobid/resource-requirements`, and in standalone mode reactive scaling adjusts the TaskManager replicas. The operator then aligns the TaskManager count with the new total slot demand.
- Memory changes require a restart, because they change the container. The diff is an `UPGRADE`: the job is stopped with `last-state` or `savepoint` (per `upgradeMode`), new TaskManagers come up with the new memory, and the job is restored. If a cycle changes both, the restart dominates and carries the new parallelism with it.

The autoscaler is stateful across cycles. In the operator that state lives in a Kubernetes ConfigMap per resource, labeled `component=autoscaler`, cached in memory during a cycle and flushed back at its end. It holds the collected metric history, the scaling history, the scaling-tracking bookkeeping, the parallelism overrides, the config overrides, and any delayed scale-down, all catalogued under [State → Autoscaler State]({{< ref "docs/operations/state#autoscaler-state" >}}). How state is stored, how events are published, and how decisions are realized are the autoscaler's pluggable seams, and everything between them is shared. This is what lets the same scaling core run embedded in the operator or as the standalone service, which swaps the ConfigMap for a JDBC or in-memory store, as described under [Autoscaler → Standalone Autoscaler]({{< ref "docs/managing/autoscaler#standalone-autoscaler" >}}).

{{< hint info >}}
The autoscaler is not a separate actuator but a step in the reconcile loop, expressing its decision as desired state and letting the operator carry it out.
{{< /hint >}}

## Scaling Model

A Flink job is a chain of operators, each running with some parallelism, a number of parallel subtasks. Too little and the operator falls behind, too much and resources are wasted. The autoscaler keeps that number right, once per reconcile cycle, by solving a single relation for every operator:

```
[parallelism] ◄─ such that ─► [capacity ≈ demand ÷ target utilization]
```

Two of those terms are very different in nature, and the distinction runs through the whole page:

- **Demand is measured.** Records flow through the job, Flink counts them, the autoscaler reads the counters. No guessing.
- **Capacity is estimated.** How fast an operator could go flat out cannot be read directly, because the operator only ever runs at its current rate. The autoscaler estimates it two different ways and chooses between them. This estimate, the **true processing rate**, is the brain of the autoscaler and gets its own section.

One rule governs every number below: everything is averaged over a window (`job.autoscaler.metrics.window`, default 15 minutes). The autoscaler reasons about sustained trends, not instantaneous readings, so a transient spike never drives a decision. Almost every quantity on this page is a rate or an average computed over that window.

The loop, end to end:

```
   ┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
   └────► sense ───────────────► understand ───────────────► decide ─────────────────► execute ──────┘ 
   (metrics collection)   (demand, capacity, band)   (parallelism per vertex)   (write back into spec)
```

The rest of the page walks this loop stage by stage: [Sense](#sense-the-windowed-metric-history), [Understand](#understand-demand-and-the-utilization-band) together with [Estimate Capacity](#estimate-capacity-the-true-processing-rate), [Decide](#decide-from-ratio-to-parallelism), and [Execute and Apply](#execute-and-apply).

## Sense: The Windowed Metric History

Each reconcile cycle appends one sample to a windowed, persisted history: scaling metrics per vertex plus global cluster metrics.

### Requests

Five kinds of REST requests produce the sample, in order:

| # | Request                                                            | Scope              | Yields                                                          |
|---|--------------------------------------------------------------------|--------------------|-----------------------------------------------------------------|
| 1 | `GET /jobs/:jobid`                                                 | one                | Job state and start time, execution plan, per-vertex details    |
| 2 | `GET /jobs/:jobid/vertices/:vertexid/subtasks/metrics`             | one per source     | Full metric-name listing, scanned for partition patterns        |
| 3 | `GET /jobs/:jobid/vertices/:vertexid/subtasks/metrics`             | per vertex, cached | The metric names worth collecting, filtered from the listing    |
| 4 | `GET /jobs/:jobid/vertices/:vertexid/subtasks/metrics?get=<names>` | one per vertex     | Values aggregated across subtasks as `min`, `max`, `sum`, `avg` |
| 5 | `GET /jobmanager/metrics` and `GET /taskmanagers/metrics`          | one each           | Task slots, heap, managed and metaspace memory, GC time         |

### Responses

The job details of request 1 are the backbone, nearly every later stage leans on a piece of the response:

| Job Details Element             | Drives                                                                              |
|---------------------------------|-------------------------------------------------------------------------------------|
| Edges between vertices          | The demand propagation in [Understand](#understand-demand-and-the-utilization-band) |
| Vertices without inputs         | The sources, where backlog detection and the capacity floor apply                   |
| Ship strategies                 | A `HASH` input marks the vertex for key-group alignment                             |
| Parallelism and max parallelism | The scale factor and its clamps in [Decide](#decide-from-ratio-to-parallelism)      |
| Slot sharing groups             | The TaskManager count checked by the capacity and quota gates                       |
| Accumulated IO counters         | The cumulative rate estimates behind demand and capacity                            |
| Execution states                | Finished vertices, excluded from scaling with `job.autoscaler.vertex.exclude.ids`   |
| Job state and start time        | Restart detection and the stabilization window                                      |

The same response drives restart detection: a job that entered `RUNNING` after the oldest stored sample has restarted, the old averages are not comparable, so the history is cleared, and the observed vertex set prunes the scaling history of vertices that left the graph.

The per-source name listings of request 2 are scanned for connector partition patterns: distinct matches of Kafka's `KafkaSourceReader.topic.<topic>.partition.<id>.currentOffset` and Pulsar's `PulsarConsumer.<topic>-partition-<id>` names become `NUM_SOURCE_PARTITIONS`, no values are fetched.

The filtered names of request 3 are cached per job: while the vertex set is unchanged only sources are re-listed, their lag and rate metrics can appear late, and a topology change refreshes every vertex. A required metric missing during stabilization retries as not ready, afterwards it is an error.

The raw Flink metrics fetched by the value queries, requests 4 and 5:

| Raw Flink Metric                                    | Scope        | Purpose                                                                                           |
|-----------------------------------------------------|--------------|---------------------------------------------------------------------------------------------------|
| `busyTimeMsPerSecond`                               | all vertices | Busy fraction, drives `LOAD` and the busy-time estimate. Required, a vertex without it is skipped |
| `backPressuredTimeMsPerSecond`                      | sources      | Blocked fraction, drives the backpressure estimate                                                |
| `Source__*.numRecordsInPerSecond`                   | sources      | Ingest rate of the source operator                                                                |
| `Source__*.numRecordsIn`, `Source__*.numRecordsOut` | sources      | Cumulative counters behind the rate deltas                                                        |
| `pendingRecords`                                    | sources      | Lag, drives backlog detection                                                                     |
| `numRecordsInPerSecond`                             | non-sources  | Throughput gauge, when available                                                                  |
| `taskSlotsTotal`, `taskSlotsAvailable`              | JobManager   | Slot usage for the capacity gate                                                                  |
| `Status.JVM.Memory.Heap.Used` and `.Max`            | TaskManagers | Heap signals for the memory-pressure gate and tuning                                              |
| `Status.Flink.Memory.Managed.Used`                  | TaskManagers | Memory tuning input                                                                               |
| `Status.JVM.Memory.Metaspace.Used`                  | TaskManagers | Memory tuning input                                                                               |
| `Status.JVM.GarbageCollector.All.TimeMsPerSecond`   | TaskManagers | GC pressure, queried when the cluster exposes it                                                  |

The `Source__*` names are operator-scoped: a source task has no in-graph input, so its plain task metrics would show nothing, while the source operator's scope reflects what the connector actually ingests. Non-source counters are not queried separately, their cumulative numbers ride the job details of request 1.

{{< hint info >}}
Finished vertices are excluded at every layer: their metric names and values are not queried, they are added to the scaling exclude list, and each sample stores synthetic zero metrics for them, keeping the demand propagation well-defined for graphs where a bounded source has completed.
{{< /hint >}}

### Stored Scaling Metrics

Derived from those raw metrics, the cycle's sample stores, per vertex:

| Scaling Metric              | Definition                                                                                                                     |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `LAG`                       | `pendingRecords`, else `0`                                                                                                     |
| `LOAD`                      | `max(0, busyTimeMsPerSecond) / 1000`, in `[0, 1]`                                                                              |
| `ACCUMULATED_BUSY_TIME`     | accumulated busy time from the job details                                                                                     |
| `NUM_RECORDS_IN`            | first available of `numRecordsIn`, source `numRecordsIn`, source `numRecordsOut`                                               |
| `NUM_RECORDS_OUT`           | `numRecordsOut`                                                                                                                |
| `NUM_RECORDS_IN_PER_SECOND` | per-second gauge (sum across subtasks), recorded for non-source vertices                                                       |
| `NUM_SOURCE_PARTITIONS`     | distinct connector partitions counted from the source's metric names                                                           |
| `OBSERVED_TPR`              | the backpressure-derived capacity estimate, sources only, see [Estimate Capacity](#estimate-capacity-the-true-processing-rate) |

And globally:

| Scaling Metric          | Definition                                          |
|-------------------------|-----------------------------------------------------|
| `NUM_TASK_SLOTS_USED`   | `taskSlotsTotal − taskSlotsAvailable`               |
| `GC_PRESSURE`           | max GC `TimeMsPerSecond / 1000` across TaskManagers |
| `HEAP_MEMORY_USED`      | max heap used across TaskManagers                   |
| `HEAP_MAX_USAGE_RATIO`  | max heap `used / max` across TaskManagers           |
| `MANAGED_MEMORY_USED`   | max across TaskManagers                             |
| `METASPACE_MEMORY_USED` | max across TaskManagers                             |

### Windowing

Samples count toward the window only after `job.autoscaler.stabilization.interval` has passed since the job started. Until the configured `metrics.window` has elapsed past the first counted sample, the history is not fully collected and scaling is skipped, the recommended parallelism stays `null`. Once full, samples older than the window are trimmed every cycle.

{{< hint info >}}
Collection itself does not wait for stabilization, samples are gathered and persisted from the very first cycle. For sources this is deliberate: the catch-up right after a restart is when `OBSERVED_TPR` is measurable, and those early observations seed the fallback average that samples collected during window filling inherit when their own observation is unusable, as described under [Backpressure Estimate](#backpressure-estimate).
{{< /hint >}}

{{< hint warning >}}
Rates are deltas over time, so with a single sample the autoscaler does nothing. Combined with windowing this is why `metrics.window` must exceed the reconcile interval: samples arrive once per reconcile loop, and if the window is shorter than that interval it is trimmed back to one sample every loop and the autoscaler silently never scales.
{{< /hint >}}

## Understand: Demand and the Utilization Band

For each vertex the evaluator derives demand and the band of acceptable rates. Capacity is involved enough to get [its own section](#estimate-capacity-the-true-processing-rate).

### Backlog Detection

The job is processing backlog when any source would need longer than the configured threshold to clear its current lag at its current ingest rate:

```
LAG / inputRate  >  job.autoscaler.backlog-processing.lag-threshold   (default 5 minutes)
```

`inputRate` is the windowed ingest rate defined under [Target Data Rate](#target-data-rate). The flag is job-wide: a single source over the threshold puts every vertex into the backlog regime of the [Utilization Band](#utilization-band), and a source whose rate is unavailable can never trigger it.

### Target Data Rate

The building blocks, computed over the window:

| Term                | Definition                                                                                |
|---------------------|-------------------------------------------------------------------------------------------|
| `inputRate`         | `(last − first NUM_RECORDS_IN) / (last − first ts) × 1000`, records per second            |
| `lagRate`           | `(last − first LAG) / (last − first ts) × 1000`, growing lag positive, shrinking negative |
| `last_LAG`          | the latest `LAG` sample                                                                   |
| `outputRatio(edge)` | edge output rate / upstream vertex input rate                                             |

The edge output rate behind `outputRatio(edge)` is resolved by the shape of the downstream vertex, in reliability order:

| Edge Shape                                                                         | Edge Output Rate                                                                                                      |
|------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| The downstream has a single input                                                  | The downstream's own `NUM_RECORDS_IN` rate, the most reliable measure                                                 |
| Every other input of the downstream comes from an upstream with exactly one output | The downstream's `NUM_RECORDS_IN` rate minus those upstreams' `NUM_RECORDS_OUT` rates                                 |
| Otherwise                                                                          | The upstream's `NUM_RECORDS_OUT` rate, the least reliable, it overstates the edge when the upstream has other outputs |

The ratio is `0` when either the upstream's input rate or the edge rate is not positive: an idle input would otherwise produce an enormous ratio and trigger a rapid scale-up.

Sources measure demand directly from these, while non-sources have no external input, so their demand is propagated through the graph edge by edge, each edge scaled by its own output ratio:

| Scaling Metric       | Sources                                                                    | Non-Sources                                                           |
|----------------------|----------------------------------------------------------------------------|-----------------------------------------------------------------------|
| `TARGET_DATA_RATE`   | `max(0, inputRate + lagRate)`, `NaN` with insufficient data                | `Σ over input edges ( input TARGET_DATA_RATE × outputRatio(edge) )`   |
| `CATCH_UP_DATA_RATE` | `last_LAG / catch-up.duration` (default 30 minutes), `NaN` outside backlog | `Σ over input edges ( input CATCH_UP_DATA_RATE × outputRatio(edge) )` |

### Utilization Band

Scaling is hysteresis, not a target point. The current rate is acceptable while it sits inside `[SCALE_DOWN_RATE_THRESHOLD, SCALE_UP_RATE_THRESHOLD]`:

| Threshold                   | Formula                                                                                                           |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------|
| `SCALE_UP_RATE_THRESHOLD`   | `CATCH_UP_DATA_RATE + TARGET_DATA_RATE / upperUtilization`                                                        |
| `SCALE_DOWN_RATE_THRESHOLD` | `CATCH_UP_DATA_RATE + (TARGET_DATA_RATE × restartTime) / catch-up.duration + TARGET_DATA_RATE / lowerUtilization` |

The two utilization bounds:

| Bound              | During Backlog           | Otherwise                                                                                            |
|--------------------|--------------------------|------------------------------------------------------------------------------------------------------|
| `upperUtilization` | `1.0`                    | `utilization.max`, or `utilization.target + utilization.target.boundary` (default `0.7 + 0.3 = 1.0`) |
| `lowerUtilization` | `0`, scale-down disabled | `utilization.min`, or `utilization.target − utilization.target.boundary` (default `0.7 − 0.3 = 0.4`) |

The restart-cost term `(TARGET_DATA_RATE × restartTime) / catch-up.duration` reserves capacity for the records that pile up during a restart, and is dropped when `catch-up.duration = 0`. `restartTime` is the configured `job.autoscaler.restart.time` (default 5 minutes), or the observed restart duration when `restart.time-tracking.enabled` is set, capped by `restart.time-tracking.limit`.

Two edge rules close the contract: an `upperUtilization` of `0` becomes `+∞`, the vertex never scales up, and both thresholds are `NaN` when either `TARGET_DATA_RATE` or `CATCH_UP_DATA_RATE` is `NaN`.

### Global Evaluation

The global signals of the latest sample pass through as evaluated metrics:

| Global Metric           | Evaluated As                 | Consumer                 |
|-------------------------|------------------------------|--------------------------|
| `GC_PRESSURE`           | current                      | The memory-pressure gate |
| `HEAP_MAX_USAGE_RATIO`  | current and windowed average | The memory-pressure gate |
| `HEAP_MEMORY_USED`      | current and windowed average | Memory tuning            |
| `MANAGED_MEMORY_USED`   | current and windowed average | Memory tuning            |
| `METASPACE_MEMORY_USED` | current and windowed average | Memory tuning            |
| `NUM_TASK_SLOTS_USED`   | current                      | The capacity gate        |

## Estimate Capacity: The True Processing Rate

This is the core. `TRUE_PROCESSING_RATE` is how fast a vertex could process at full utilization, and it is the denominator of the scale factor, so its accuracy drives every decision. The autoscaler computes it two ways and selects one.

### Busy-Time Estimate

If a vertex achieves rate `R` while busy a fraction `b` of the time, then at full busyness it could do `R / b`:

```
busyTimeTpr = inputRateForTpr / (busyTimeAvg / 1000)
```

Both sides of the ratio follow `job.autoscaler.metrics.busy-time.aggregator`, and each row pairs a denominator with its matching numerator:

| `busy-time.aggregator`         | `busyTimeAvg` (denominator)                            | `inputRateForTpr` (numerator)                                                                       |
|--------------------------------|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `MAX` or `MIN` (default `MAX`) | windowed mean of `LOAD` × 1000                         | windowed mean of `NUM_RECORDS_IN_PER_SECOND`, falling back to the windowed rate of `NUM_RECORDS_IN` |
| `AVG`                          | windowed rate of `ACCUMULATED_BUSY_TIME` / parallelism | windowed rate of `NUM_RECORDS_IN`                                                                   |

The matching is deliberate: `busyTimeTpr = rate / busy` and `rate ≈ capacity × busy`, so when numerator and denominator use the same estimator their shared sampling weighting cancels and the ratio recovers `capacity`. Under the default `MAX` the denominator is a per-second-gauge mean, so the numerator must be one too, and under `AVG` both are cumulative rates. Mixing a cumulative numerator with a per-second-mean denominator leaves the denominator's sampling error in the result, which matters under bursty, non-uniform sampling.

### Backpressure Estimate

Busy time gets unreliable under sustained backpressure, so the second estimate uses the backpressure signal directly. While a vertex is blocked it could have been processing, so divide the achieved rate by the fraction of time it was not blocked:

```
OBSERVED_TPR = numRecordsInPerSecond / (1 − backPressuredTimeMsPerSecond / 1000)
```

It is computed for sources only, and only while the observation is meaningful, a rate reflects capacity rather than a lack of input only while the source is catching up:

```
catchingUp = LAG ≥ numRecordsInPerSecond × job.autoscaler.observed-true-processing-rate.lag-threshold   (default 30s)
```

What each sample records:

| Observation State                           | `OBSERVED_TPR` of the Sample                                                      |
|---------------------------------------------|-----------------------------------------------------------------------------------|
| Catching up, backpressure below `1000 ms/s` | The formula above                                                                 |
| Idle (`numRecordsInPerSecond = 0`)          | `+∞`, allowing scale-down                                                         |
| Fully backpressured (`≥ 1000 ms/s`)         | `NaN`                                                                             |
| Otherwise unusable                          | The historical average of past values, `NaN` below `min-observations` (default 2) |

### Estimate Selection

`observedTprAvg` is the windowed average of `OBSERVED_TPR`, subject to `min-observations`, and the first matching rule wins:

| Condition                                                                | Selected Estimate |
|--------------------------------------------------------------------------|-------------------|
| `observedTprAvg` is `NaN`                                                | `busyTimeTpr`     |
| `busyTimeTpr` is `NaN` or `+∞`                                           | `OBSERVED_TPR`    |
| `busyTimeTpr > observedTprAvg × (1 + switch-threshold)` (default `0.15`) | `OBSERVED_TPR`    |
| Otherwise                                                                | `busyTimeTpr`     |

The selection trusts the optimistic `busyTimeTpr` unless it claims more than roughly 15% more capacity than the conservative `OBSERVED_TPR` floor. It is value-based and applies to any vertex that has an `OBSERVED_TPR`, which in the current implementation means sources.

What actually drives the switch rule becomes visible with `busy + backpressured + idle ≈ 1` and the same `R` on both sides:

```
busyTimeTpr / OBSERVED_TPR = (1 − backpressured) / busy = (busy + idle) / busy = 1 + idle/busy
```

So the switch (`busyTimeTpr > observed × (1 + threshold)`) is really `idle/busy > threshold`, driven by idle time relative to busy time, not by backpressure directly. But high backpressure shrinks `busy`, which amplifies `idle/busy`, so heavily backpressured sources fall back to `OBSERVED_TPR` far more readily. A vertex with zero idle stays on `busyTimeTpr` no matter how backpressured it is, because the two estimates coincide there.

{{< hint info >}}
Most vertices, most of the time, use `busyTimeTpr`. `OBSERVED_TPR` takes over for heavily backpressured sources with non-trivial idle, where busy time overstates capacity. And `TRUE_PROCESSING_RATE` is a windowed average, not an instantaneous rate, deliberately, for stability.
{{< /hint >}}

## Decide: From Ratio to Parallelism

For each non-excluded vertex with valid metrics, `TRUE_PROCESSING_RATE` not `NaN` and at least one threshold not `NaN`, the new parallelism is derived through a fixed pipeline: scale factor, scalability correction, caps and bounds, alignment, then the guards and stability checks.

### Scale Factor

The ratio of required capacity, demand at the target utilization plus catch-up, to actual capacity:

```
scaleFactor = ( CATCH_UP_DATA_RATE
              + (TARGET_DATA_RATE × restartTime) / job.autoscaler.catch-up.duration
              + TARGET_DATA_RATE / job.autoscaler.utilization.target )
              / TRUE_PROCESSING_RATE
```

Dividing by `utilization.target` adds headroom: a lower target scales to higher parallelism so the same throughput runs less saturated.

### Scalability Correction

With `job.autoscaler.observed-scalability.enabled`, the scale factor is divided by a per-vertex coefficient `α` that captures sub-linear scaling:

```
scaleFactor = scaleFactor / α
```

`α` is fitted by least squares over the vertex's own scaling history, assuming the linear model `R_i = β · α · P_i`:

```
α = Σ(P_i · R_i) / (β · Σ(P_i²))
```

| Symbol | Meaning                                                                                                                          |
|--------|----------------------------------------------------------------------------------------------------------------------------------|
| `P_i`  | Parallelism of a past scaling in the history                                                                                     |
| `R_i`  | Average true processing rate observed at `P_i`                                                                                   |
| `β`    | Baseline per-subtask rate, taken at the smallest parallelism in the history, an actual parallelism-1 observation when one exists |

An `α` of `1` is perfect linear scaling, and below `1` the vertex scales sub-linearly, inflating the required parallelism. Three guards keep the correction conservative, the upper clamp of `1.0` in particular means a super-linear fit is never trusted:

| Guard                                                                    | Effect                                                                          |
|--------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| Fewer than `observed-scalability.min-observations` entries (default `3`) | `α = 1.0`                                                                       |
| `β` unavailable                                                          | `α = 1.0`                                                                       |
| Fit result                                                               | Clamped to `[observed-scalability.coefficient-min, 1.0]` (default `[0.5, 1.0]`) |

### Caps and Bounds

The corrected factor is capped before application, and the resulting parallelism clamped after:

```
newParallelism = ceil(currentParallelism × scaleFactor)
```

| Bound             | Interval                                                                       | Default         |
|-------------------|--------------------------------------------------------------------------------|-----------------|
| Factor cap        | `[1 − scale-down.max-factor, 1 + scale-up.max-factor]`                         | `[0.4, 100001]` |
| Parallelism clamp | `[min(current, vertex.min-parallelism), max(current, vertex.max-parallelism)]` | `[1, 200]`      |

The clamp always includes the current parallelism, so tightening the configured bounds never forces a rescale by itself, and the job's `maxParallelism` caps the result within the alignment step.

### Parallelism Alignment

For sources and HASH-partitioned vertices, the clamped parallelism is aligned to the number of key groups or source partitions, `N`, by the mode selected through `job.autoscaler.scaling.parallelism-alignment.mode`. The aligner scans the region of the scaling direction, upward from the target:

| Direction  | Search Region                                        |
|------------|------------------------------------------------------|
| Scale-up   | `[target, upperAlignLimit]`                          |
| Scale-down | `[target, min(currentParallelism, upperAlignLimit)]` |

`upperAlignLimit` is `min(N, maxParallelism, vertex.max-parallelism)`. The first accepted value wins: an exact divisor of `N` in every mode, and in the default `BALANCED` mode also any `p` that reduces the biggest per-subtask share (`N / p < N / target` in whole key groups or partitions). When nothing in the region is accepted, the computed target is used unchanged, alignment never blocks a scaling.

The user-facing mode semantics and the search figure live under [Autoscaler → Executing Scaling Actions]({{< ref "docs/managing/autoscaler#executing-scaling-actions" >}}), custom modes plug in as described under [Custom Parallelism Alignment Modes]({{< ref "docs/deployment/plugins#custom-parallelism-alignment-modes" >}}), and only the deprecated legacy adjust modes keep the old blocking behavior with its `ScalingLimited` event.

### Direction Guards

A change is only emitted with the matching direction:

| Direction  | Emitted When                                       |
|------------|----------------------------------------------------|
| Scale-up   | `TRUE_PROCESSING_RATE < SCALE_UP_RATE_THRESHOLD`   |
| Scale-down | `TRUE_PROCESSING_RATE > SCALE_DOWN_RATE_THRESHOLD` |

### Effectiveness Detection

Once the final `newParallelism` is known, after capping and alignment, the expected post-scale capacity is recorded:

```
EXPECTED_PROCESSING_RATE = TRUE_PROCESSING_RATE × (newParallelism / currentParallelism)
```

It uses the applied ratio, computed after the constraints, not the raw pre-constraint `scaleFactor`. Otherwise a constraint that changed the applied parallelism, alignment or the bounds, would make a perfectly effective scaling look ineffective.

The next cycle compares the actual gain against the promised one:

```
effectiveness = (TRUE_PROCESSING_RATE_now  − TRUE_PROCESSING_RATE_prev)
              / (EXPECTED_PROCESSING_RATE_prev − TRUE_PROCESSING_RATE_prev)
```

If `effectiveness` falls below `job.autoscaler.scaling.effectiveness.threshold` (default `0.1`), the last scale-up delivered almost none of its promised gain. An event is always published, and the next scale-up is blocked only with `job.autoscaler.scaling.effectiveness.detection.enabled = true`. This is what stops pouring parallelism into a vertex that does not actually scale, one bottlenecked on an external system for example.

### Delayed Scale-Down

With `job.autoscaler.scale-down.interval` set (default 1 hour), a scale-down is recorded instead of executed, and when the interval elapses, the largest parallelism recommended during it is applied. Releasing capacity slowly is safe, reclaiming it prematurely risks immediately needing it back. The delayed state is persisted in the state store and cleared after a scaling action.

### Balanced Gate

While the direction guards act per vertex, this gate is job-wide. A vertex is outside its band when `TRUE_PROCESSING_RATE` falls outside `[SCALE_DOWN_RATE_THRESHOLD, SCALE_UP_RATE_THRESHOLD]`, and after all vertices are processed, if no vertex is outside its band, the cycle applies nothing, even if some vertices computed a different recommendation, and logs that all vertex processing rates are within target. This is the `balanced` path.

{{< hint info >}}
Thrashing is the autoscaler's real enemy, and four mechanisms form one design against it: the windowed averages of [Sense](#sense-the-windowed-metric-history), the utilization band of [Understand](#utilization-band), and the effectiveness detection and delayed scale-down above.
{{< /hint >}}

## Execute and Apply

The execution stage assembles the per-vertex decisions into a plan, runs the gates, and persists the outcome.

### Gates

Evaluated in order, any gate blocks the whole cycle:

| Gate                                | Blocks When                                                                                                                                                                                  |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Memory pressure                     | `GC_PRESSURE` or the windowed average of `HEAP_MAX_USAGE_RATIO` is above its threshold. Both thresholds default to `1.0`, the gate only takes effect once lowered                            |
| Scaling disabled or excluded period | `job.autoscaler.scaling.enabled` is `false`, or the time falls within the cron-like `job.autoscaler.excluded.periods`                                                                        |
| Cluster capacity                    | `NUM_TASK_SLOTS_USED` is unavailable, or the new total slots would exceed what the cluster can hold                                                                                          |
| Resource quota                      | Per slot sharing group, `TM_CPU × newTMCount` above `quota.cpu` or `TM_Memory × newTMCount` above `quota.memory`. Skipped when the change reduces TaskManagers, scale-down is always allowed |

Memory tuning, when enabled, is computed before the capacity and quota gates, so both validate the TaskManager profile that would actually be deployed, as covered under [Memory Tuning](#memory-tuning).

### Outcome

The state store receives the scaling history, per vertex and timestamp the current and new parallelism plus the evaluated metrics, the scaling tracking, the parallelism overrides, and the config overrides. The `scalings` counter increments on a change, `balanced` on a no-op, `errors` in the exception handler, and decisions, recommendations, and blocking reasons surface as Kubernetes events, catalogued under [Autoscaler Events]({{< ref "docs/operations/events#autoscaler-events" >}}).

The decision itself is written into the spec, and the reconcile cycle rolls it out as described under [Operator Integration](#operator-integration):

| Decision                                              | Rollout                                                          | Restart |
|-------------------------------------------------------|------------------------------------------------------------------|---------|
| Parallelism, native mode with the adaptive scheduler  | `PUT /jobs/:jobid/resource-requirements` against the running job | No      |
| Parallelism, standalone mode with reactive scheduling | The TaskManager replica count is patched                         | No      |
| Parallelism, in-place scaling not possible            | The regular upgrade cycle, honoring the configured `upgradeMode` | Yes     |
| Memory, with or without a parallelism change          | An `UPGRADE` rollout, the restart carries both changes           | Yes     |

## Memory Tuning

Beyond parallelism, the autoscaler can right-size TaskManager memory from observed usage. The user-facing view of the feature is [Autotuning]({{< ref "docs/managing/autotuning" >}}), this section covers the budget computation, which parses the declared memory configuration and sizes the pools in a fixed order, each allocation drawing down a shared budget that caps the remainder.

### Budget Order

```
budget = total process memory − framework off-heap − task off-heap − JVM overhead   (the reserved pools are not tuned)
```

| Order | Pool         | Sizing                                                                                                                                                                 |
|-------|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1     | Network      | Recomputed from the topology, the new parallelisms, and the buffer configuration, then pinned by setting `taskmanager.memory.network.min` and `.max` to the same value |
| 2     | Metaspace    | `avg(METASPACE_MEMORY_USED) × (1 + memory.tuning.overhead)`, budgeted before heap                                                                                      |
| 3     | Heap         | `avg(HEAP_MEMORY_USED) × (1 + memory.tuning.overhead)`                                                                                                                 |
| 4     | Managed      | `0` if unused, all remaining budget with `memory.tuning.maximize-managed-memory`, otherwise the original size                                                          |
| 5     | Heap rescale | The heap follows the parallelism change with factor `numTMsBefore / numTMsAfter`: scaling up shrinks the per-TaskManager heap, scaling down grows it                   |

### Configuration Overrides

The outcome is a set of configuration overrides rolled out with the scaling action:

| Change      | Keys                                                                                                                                                                                                                  |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Set         | `taskmanager.memory.process.size`, `taskmanager.memory.managed.fraction`, `taskmanager.memory.jvm-overhead.fraction`, `taskmanager.memory.jvm-metaspace.size`, the pinned `taskmanager.memory.network.min` and `.max` |
| Set to zero | `taskmanager.memory.framework.heap.size`                                                                                                                                                                              |
| Removed     | `taskmanager.memory.flink.size`, `taskmanager.memory.task.heap.size`, `taskmanager.memory.managed.size`                                                                                                               |

All of it lands on the resource itself: removed keys are dropped from and set keys written into `spec.flinkConfiguration`, and when the resulting total process memory differs from the declared TaskManager memory, `spec.taskManager.resource` is updated to match. That container change is what turns the rollout into an `UPGRADE`, as described under [Operator Integration](#operator-integration), and without a TaskManager memory declared on the spec, the container adjustment is skipped with a warning.

A recommendation event is published on every executed scaling action, memory tuning is computed only as part of one, so the recommendations stay visible even in dry-run mode. The change is only applied, the restart path, when `job.autoscaler.memory.tuning.enabled = true`, otherwise no overrides are produced.

## Quick Reference

### Capacity and Demand

| Quantity               | Formula                                                                                                | Scope                      |
|------------------------|--------------------------------------------------------------------------------------------------------|----------------------------|
| `OBSERVED_TPR`         | `numRecordsInPerSecond / (1 − backPressuredTimeMsPerSecond/1000)`                                      | sources, while catching up |
| `busyTimeTpr`          | `inputRateForTpr / (busyTimeAvg/1000)`, numerator and `busyTimeAvg` both follow `busy-time.aggregator` | all vertices               |
| `TRUE_PROCESSING_RATE` | `busyTimeTpr`, switching to `OBSERVED_TPR` when `busyTimeTpr > OBSERVED_TPR × (1 + switch-threshold)`  | all vertices               |
| `TARGET_DATA_RATE`     | sources: `max(0, inputRate + lagRate)`, non-sources: `Σ edges (input × outputRatio)`                   | all vertices               |
| `CATCH_UP_DATA_RATE`   | `last_LAG / catch-up.duration` while in backlog, else `NaN` (propagated for non-sources)               | all vertices               |

### Thresholds and Decision

| Quantity                    | Formula                                                                                                                                                                                               |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SCALE_UP_RATE_THRESHOLD`   | `CATCH_UP_DATA_RATE + TARGET_DATA_RATE / upperUtilization`                                                                                                                                            |
| `SCALE_DOWN_RATE_THRESHOLD` | `CATCH_UP_DATA_RATE + (TARGET_DATA_RATE × restartTime)/catch-up.duration + TARGET_DATA_RATE / lowerUtilization`                                                                                       |
| `scaleFactor`               | `(CATCH_UP + (TARGET × restartTime)/catch-up.duration + TARGET / utilization.target) / TRUE_PROCESSING_RATE`, `÷ α` if enabled, then capped to `[1 − scale-down.max-factor, 1 + scale-up.max-factor]` |
| `newParallelism`            | `ceil(currentParallelism × scaleFactor)`, clamped to `[min(current, vertex.min), max(current, vertex.max)]`, then partition-aligned                                                                   |
| `EXPECTED_PROCESSING_RATE`  | `TRUE_PROCESSING_RATE × (newParallelism / currentParallelism)` (applied ratio)                                                                                                                        |
| `α`                         | `Σ(P_i·R_i) / (β·Σ(P_i²))` ∈ `[coefficient-min, 1.0]`, `1.0` with insufficient history                                                                                                                |
| `effectiveness`             | `(TPR_now − TPR_prev) / (EXPECTED_prev − TPR_prev)`, ineffective if `< effectiveness.threshold`                                                                                                       |

### Utilization Bounds

| Bound              | During Backlog           | Otherwise                                                                                            |
|--------------------|--------------------------|------------------------------------------------------------------------------------------------------|
| `upperUtilization` | `1.0`                    | `utilization.max`, or `utilization.target + utilization.target.boundary` (default `0.7 + 0.3 = 1.0`) |
| `lowerUtilization` | `0`, scale-down disabled | `utilization.min`, or `utilization.target − utilization.target.boundary` (default `0.7 − 0.3 = 0.4`) |

### Where a Change Lands

| Change      | Mechanism                                                                                                                         | Disruptive |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------|------------|
| Parallelism | `pipeline.jobvertex-parallelism-overrides` in the spec, `SCALE` diff, in-place rescale (`resource-requirements` REST or reactive) | No         |
| Memory      | memory config + TaskManager resource in the spec, `UPGRADE` diff, restart (`last-state` or `savepoint`)                           | Yes        |

### Key Relations

| Condition                                                                                          | Outcome                                                                         |
|----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| The metric window is not yet full                                                                  | No decisions at all, the recommended parallelism stays `null`                   |
| `LAG / inputRate > backlog-processing.lag-threshold` on any source                                 | Job-wide backlog regime, scale-down disabled, upper utilization raised to `1.0` |
| `busyTimeTpr > OBSERVED_TPR × (1 + switch-threshold)`, equivalently `idle/busy > switch-threshold` | The conservative observed floor is used as capacity                             |
| `SCALE_DOWN_RATE_THRESHOLD ≤ TRUE_PROCESSING_RATE ≤ SCALE_UP_RATE_THRESHOLD`                       | Within the band, no change                                                      |
| `TRUE_PROCESSING_RATE < SCALE_UP_RATE_THRESHOLD`                                                   | Scale up, capacity short of demand                                              |
| `TRUE_PROCESSING_RATE > SCALE_DOWN_RATE_THRESHOLD`                                                 | Scale down, excess capacity                                                     |
| No vertex is outside its band after all are processed                                              | The cycle applies nothing, the `balanced` path                                  |
| `effectiveness < effectiveness.threshold` after a scale-up                                         | The next scale-up is blocked, with detection enabled                            |

## Rules of Thumb and Typical Scenarios

Rules of thumb:

- Keep `metrics.window` well above the reconcile interval, or the window never holds two samples and the autoscaler silently never scales.
- `busyTimeTpr` for almost everything, `OBSERVED_TPR` for heavily backpressured sources. The switch is driven by `idle/busy`, which backpressure amplifies.
- Catch-up is additive: required capacity is steady-state demand plus backlog catch-up, so a shorter `catch-up.duration` scales more aggressively, and scale-down is disabled entirely while the job is catching up.
- Scale-downs are patient: with `scale-down.interval` set (default 1 hour), reductions apply only after the interval elapses, at the largest parallelism recommended during it.
- Alignment nudges, it does not pin: the default `BALANCED` mode snaps to a divisor of the partition count when one is within reach but accepts mildly skewed values, and when nothing aligns, the computed target proceeds unchanged. Strict divisor-only alignment is `EVENLY_SPREAD`.
- Effectiveness needs the applied ratio: `EXPECTED_PROCESSING_RATE` is computed after alignment and clamping, so constraints do not masquerade as ineffective scalings.

Typical scenarios:

- A steady CPU-bound operator runs near 100% busy with little idle, so `busyTimeTpr ≈ OBSERVED_TPR` and it scales up cleanly on the busy-time estimate.
- A non-source operator backpressured by a slow external system never receives an `OBSERVED_TPR`, the floor exists only for sources, so it stays on `busyTimeTpr`. What protects it from over-scaling is effectiveness detection: capacity poured into an externally bottlenecked vertex delivers no gain, and the follow-up scale-up is blocked when detection is enabled.
- A source recovering from a backlog has lag above the threshold, so `OBSERVED_TPR` is computed and the catch-up term inflates demand for an aggressive scale-up, while scale-down stays disabled until the backlog clears.
- A skewed source (one hot subtask) is hard to size from aggregate metrics and is guided by partition alignment, so the autoscaler stays deliberately conservative.
