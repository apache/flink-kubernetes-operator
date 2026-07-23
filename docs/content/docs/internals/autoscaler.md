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

This page is the deep walkthrough of how the autoscaler reaches a scaling decision: every metric it reads, every formula it computes, the exact conditions it branches on, and how the result is fed back into the running job. It is organized by idea rather than by call order, so each section builds the vocabulary for the next. The user-facing view, requirements, enabling, and tuning, lives under [Autoscaler]({{< ref "docs/managing/autoscaler" >}}), and its three configuration phases, collecting, evaluating, and executing, map onto the sense, understand and decide, and act stages walked below.

## The Core Relation

A Flink job is a chain of operators, each running with some parallelism, a number of parallel subtasks. Too little and the operator falls behind, too much and resources are wasted. The autoscaler keeps that number right, once per reconcile cycle, by solving a single relation for every operator:

```
parallelism such that   capacity ≈ demand ÷ target utilization
```

Two of those terms are very different in nature, and the distinction runs through the whole page:

- **Demand is measured.** Records flow through the job, Flink counts them, the autoscaler reads the counters. No guessing.
- **Capacity is estimated.** How fast an operator could go flat out cannot be read directly, because the operator only ever runs at its current rate. The autoscaler estimates it two different ways and chooses between them. This estimate, the **true processing rate**, is the brain of the autoscaler and gets its own section.

One rule governs every number below: everything is averaged over a window (`job.autoscaler.metrics.window`, default 15 minutes). The autoscaler reasons about sustained trends, not instantaneous readings, so a transient spike never drives a decision. Almost every quantity on this page is a rate or an average computed over that window.

The loop, end to end:

```
   sense ───► understand ───► decide ───► act ───► (next cycle)
   metrics    demand,          parallelism  write back
   → history  capacity, band   per vertex   into the spec
```

## Integration with the Operator

Before the mechanics, the integration, because it shapes everything else and is the part most easily misunderstood.

The autoscaler never talks to Flink directly. In the operator it runs inside the reconcile loop (`AbstractFlinkResourceReconciler.applyAutoscaler()` calling `JobAutoScalerImpl.scale()`), and its only output is a mutation of the resource's desired spec. The `ScalingRealizer` writes the decision into `spec.flinkConfiguration`:

- a **parallelism** decision becomes `pipeline.jobvertex-parallelism-overrides`,
- a **memory** decision (from memory tuning) becomes the memory config entries plus an adjusted `spec.taskManager.resource.memory`.

The same reconcile cycle then notices the spec changed and reconciles it through the operator's normal diff and apply machinery, as described under [Controllers]({{< ref "docs/internals/controllers#base-reconciler-steps" >}}):

```
 reconcile cycle
   ├─ applyAutoscaler()  → autoscaler decides → ScalingRealizer writes overrides into spec.flinkConfiguration
   └─ reconcile the now-changed spec, by diff type:
        parallelism-only change → SCALE  → in-place rescale          (no restart)
        memory / other change   → UPGRADE → stop + redeploy + restore (restart)
```

- **Parallelism is applied in place, without a restart.** A spec change that only touches parallelism overrides is a `SCALE` diff. In native mode `NativeFlinkService.scale()` builds a `JobVertexResourceRequirements` per vertex and sends it to Flink's adaptive scheduler with `PUT /jobs/:jobid/resource-requirements`, and in standalone mode reactive scaling adjusts the replicas. The operator then aligns the TaskManager count with the new total slot demand.
- **Memory changes require a restart**, because they change the container. The diff is an `UPGRADE`: the job is stopped with `last-state` or `savepoint` (per `upgradeMode`), new TaskManagers come up with the new memory, and the job is restored. If a cycle changes both, the restart dominates and carries the new parallelism with it.

The autoscaler is stateful across cycles. In the operator that state is a Kubernetes ConfigMap per resource (labeled `component=autoscaler`), accessed through `ConfigMapStore`, which caches the ConfigMap in memory during a cycle and flushes it back at the end. It holds the collected metric history, the scaling history, the scaling-tracking bookkeeping, the parallelism overrides, the config overrides, and any delayed scale-down, all catalogued under [State → Autoscaler State]({{< ref "docs/operations/state#autoscaler-state" >}}). The standalone runtime swaps this for a JDBC or in-memory store, the only substantive difference between the two runtimes, as described under [Autoscaler → Standalone Autoscaler]({{< ref "docs/managing/autoscaler#standalone-autoscaler" >}}).

{{< hint info >}}
The mental model: the autoscaler is not a separate actuator. It is a step in the reconcile loop that expresses its decision as desired state and lets the operator carry it out.
{{< /hint >}}

## Architecture

```
 ScalingMetricCollector ──► ScalingMetricEvaluator ──► ScalingExecutor ──► ScalingRealizer ──► spec
   (raw metrics, REST)       (capacity, demand,         (decide + gates)    (write back)
                              thresholds)                    │
                                                     JobVertexScaler   MemoryTuning

      AutoScalerStateStore (ConfigMap / JDBC / in-memory)  •  AutoScalerEventHandler (events)
```

| Component                                        | Responsibility                                                                                                              |
|--------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `JobAutoScalerImpl`                              | Orchestrates one cycle and drives the `scalings`, `errors`, and `balanced` counters (`AutoscalerFlinkMetrics`).             |
| `ScalingMetricCollector` / `RestApiMetricsCollector` | Queries the Flink REST API for topology and per-vertex, JobManager, and TaskManager metrics, folds them into the windowed `CollectedMetricHistory`. |
| `ScalingMetricEvaluator`                         | Turns the history into evaluated signals: true processing rate, target and catch-up data rate, scale-up and scale-down thresholds, load, lag. |
| `ScalingExecutor`                                | Decides whether to scale, runs the memory-pressure, quota, and excluded-period gates, drives memory tuning, persists the outcome. |
| `JobVertexScaler`                                | The per-vertex parallelism computation: scale factor, scalability correction, capping, alignment, effectiveness, delayed scale-down. |
| `MemoryTuning` / `MemoryScaling`                 | Computes an optional TaskManager memory layout.                                                                             |
| `ScalingRealizer` (SPI)                          | Writes the decision back into the spec.                                                                                     |
| `AutoScalerStateStore` (SPI)                     | Cross-cycle state (ConfigMap, JDBC, or in-memory).                                                                          |
| `AutoScalerEventHandler` (SPI)                   | Publishes user-facing events, catalogued under [Autoscaler Events]({{< ref "docs/operations/events#autoscaler-events" >}}). |

The three SPIs are what let the same `flink-autoscaler` core run embedded in the operator or as the standalone service.

## Sense: The Windowed Metric History

The collector's job is to produce, every cycle, an up-to-date `CollectedMetricHistory`: a time-indexed series of `CollectedMetrics` per vertex, plus global metrics.

**Topology.** `GET /jobs/:jobid` yields the `JobTopology`, a set of `VertexInfo` (id, slot-sharing group, `inputs` and `outputs` with their `ShipStrategy` such as `FORWARD`, `HASH`, or `REBALANCE`, parallelism, max parallelism, finished flag, and per-vertex `IOMetrics`: `numRecordsIn`, `numRecordsOut`, `accumulatedBusyTime`). Source partition counts (Kafka, Pulsar) are parsed from metric names and stored as `numSourcePartitions`, which later drives partition alignment. Vertices in `job.autoscaler.vertex.exclude.ids` and finished vertices are marked excluded.

**Restart and staleness.** If the job entered `RUNNING` after the oldest stored sample, it restarted. The old windowed averages are not comparable to the new run, so the history is cleared.

**Per-vertex metric values.** `GET /jobs/:jobid/vertices/:id/subtasks/metrics` lists available names, then `?get=<list>` fetches values. Each metric becomes an `AggregatedMetric` across the vertex's subtasks with `min`, `max`, `sum`, and `avg`. The set collected:

- `busyTimeMsPerSecond` for all vertices. It is required, a vertex without it is skipped for scaling.
- `backPressuredTimeMsPerSecond`, the source-task `numRecordsInPerSecond`, and `pendingRecords` (lag) for sources.
- source `numRecordsIn` / `numRecordsOut` counters, and the plain `numRecordsInPerSecond` gauge for non-source vertices when available.

**Cluster metrics.** `GET /jobmanager/metrics` yields `taskSlotsTotal` and `taskSlotsAvailable`. `GET /taskmanagers/metrics` yields heap, managed, metaspace, and GC-time metrics, aggregated across TaskManagers.

**Raw metrics become scaling metrics (`CollectedMetrics`).** Per vertex:

| Metric                      | Definition                                                                             |
|-----------------------------|----------------------------------------------------------------------------------------|
| `LAG`                       | `pendingRecords`, else `0`                                                             |
| `LOAD`                      | `max(0, busyTimeMsPerSecond) / 1000`, in `[0, 1]`                                      |
| `ACCUMULATED_BUSY_TIME`     | from `IOMetrics`                                                                       |
| `NUM_RECORDS_IN`            | first available of `numRecordsIn`, source `numRecordsIn`, source `numRecordsOut`       |
| `NUM_RECORDS_OUT`           | `numRecordsOut`                                                                        |
| `NUM_RECORDS_IN_PER_SECOND` | per-second gauge (sum across subtasks), recorded for non-source vertices               |
| `OBSERVED_TPR`              | the backpressure-derived capacity estimate, sources only, see [Estimate Capacity](#estimate-capacity-the-true-processing-rate) |

Global: `NUM_TASK_SLOTS_USED = total − available`, `GC_PRESSURE = TimeMsPerSecond / 1000`, `HEAP_MEMORY_USED`, `HEAP_MAX_USAGE_RATIO = used / max`, `MANAGED_MEMORY_USED` (max across TaskManagers), `METASPACE_MEMORY_USED` (max across TaskManagers).

`CollectedMetrics` is persisted to the state store each cycle.

**The window.** Until the window is full (the configured `metrics.window` has elapsed since the first post-stabilization sample), the history is marked not fully collected and scaling is skipped, the recommended parallelism stays `null`. Once full, the history is marked fully collected and samples older than the window are trimmed.

{{< hint warning >}}
Rates are deltas over time, so with a single sample the autoscaler does nothing. Combined with windowing this is why `metrics.window` must exceed the reconcile interval: samples arrive once per reconcile loop, and if the window is shorter than that interval it is trimmed back to one sample every loop and the autoscaler silently never scales.
{{< /hint >}}

## Understand: Demand and the Utilization Band

For each vertex the evaluator derives demand and the band of acceptable rates. Capacity is involved enough to get [its own section](#estimate-capacity-the-true-processing-rate).

### Backlog Detection

A source is processing backlog when clearing its lag at the current rate would take longer than the threshold:

```
LAG / observedInputRate  >  job.autoscaler.backlog-processing.lag-threshold   (default 5 minutes)
```

where `observedInputRate = (last − first NUM_RECORDS_IN) / (last − first ts)`.

### Target Data Rate

**Sources** measure demand directly:

```
inputRate = (last − first NUM_RECORDS_IN) / (last − first ts) × 1000          (records/s)
lagRate   = (last − first LAG)            / (last − first ts) × 1000          (lag growing +, shrinking −)
TARGET_DATA_RATE   = max(0, inputRate + lagRate)            (NaN if insufficient data)
CATCH_UP_DATA_RATE = last_LAG / job.autoscaler.catch-up.duration   (default 30m), else NaN if not in backlog
```

**Non-sources** have no external input rate, so demand is propagated through the graph edge by edge, each edge scaled by its own output ratio:

```
outputRatio(edge)  = edge output rate / upstream vertex input rate
TARGET_DATA_RATE   = Σ over input edges ( input's TARGET_DATA_RATE   × outputRatio(edge) )
CATCH_UP_DATA_RATE = Σ over input edges ( input's CATCH_UP_DATA_RATE × outputRatio(edge) )
```

### The Utilization Band

Scaling is hysteresis, not a target point. The current rate is acceptable while it sits inside `[SCALE_DOWN_RATE_THRESHOLD, SCALE_UP_RATE_THRESHOLD]`:

```
SCALE_UP_RATE_THRESHOLD   = CATCH_UP_DATA_RATE + TARGET_DATA_RATE / upperUtilization
SCALE_DOWN_RATE_THRESHOLD = CATCH_UP_DATA_RATE
                          + (TARGET_DATA_RATE × restartTime) / job.autoscaler.catch-up.duration
                          + TARGET_DATA_RATE / lowerUtilization
```

with:

- `upperUtilization` = `1.0` during backlog, else `job.autoscaler.utilization.max` (or `utilization.target + utilization.target.boundary`, default `0.7 + 0.3 = 1.0`). If `0`, it becomes `+∞` and the vertex never scales up.
- `lowerUtilization` = `0` during backlog (so `SCALE_DOWN_RATE_THRESHOLD = +∞`, scale-down is disabled while catching up), else `job.autoscaler.utilization.min` (or `utilization.target − utilization.target.boundary`, default `0.7 − 0.3 = 0.4`).
- the restart-cost term `(TARGET_DATA_RATE × restartTime) / catch-up.duration` reserves capacity for records that pile up during a restart. It is dropped when `catch-up.duration = 0`. `restartTime` is the configured `job.autoscaler.restart.time` (default 5 minutes), or the observed restart duration when `restart.time-tracking.enabled` is set, capped by `restart.time-tracking.limit`.

Both thresholds are `NaN` if either `TARGET_DATA_RATE` or `CATCH_UP_DATA_RATE` is `NaN`.

### Global Evaluation

`GC_PRESSURE` (current), `HEAP_MAX_USAGE_RATIO` (current and average), `HEAP_MEMORY_USED`, `MANAGED_MEMORY_USED`, `METASPACE_MEMORY_USED` (current and average), `NUM_TASK_SLOTS_USED`. These feed the memory-pressure gate and memory tuning.

## Estimate Capacity: The True Processing Rate

This is the core. `TRUE_PROCESSING_RATE` is how fast a vertex could process at full utilization, and it is the denominator of the scale factor, so its accuracy drives every decision. The autoscaler computes it two ways and selects one.

### The Busy-Time Estimate

If a vertex achieves rate `R` while busy a fraction `b` of the time, then at full busyness it could do `R / b`:

```
busyTimeTpr = inputRateForTpr / (busyTimeAvg / 1000)
```

The denominator `busyTimeAvg` (in ms/s) depends on `job.autoscaler.metrics.busy-time.aggregator`:

```
AVG          : busyTimeAvg = getRate(ACCUMULATED_BUSY_TIME) / parallelism      (cumulative, time-integral)
MAX or MIN   : busyTimeAvg = getAverage(LOAD) × 1000                            (mean of the per-second busy gauge)
   (default = MAX)
```

The numerator `inputRateForTpr` is matched to that aggregator so the ratio is internally consistent:

```
AVG          : getRate(NUM_RECORDS_IN)              (cumulative rate, like the denominator)
MAX or MIN   : getAverage(NUM_RECORDS_IN_PER_SECOND) (per-second gauge mean, like the denominator)
   fallback to getRate(NUM_RECORDS_IN) when the per-second gauge is unavailable
```

Why match the estimator? `busyTimeTpr = rate / busy` and `rate ≈ capacity × busy`, so if numerator and denominator use the same estimator their shared sampling weighting cancels and the ratio recovers `capacity`. Under the default `MAX` the denominator is a per-second-gauge mean, so the numerator must be one too, and under `AVG` both are cumulative rates. Mixing a cumulative numerator with a per-second-mean denominator leaves the denominator's sampling error in the result, which matters under bursty, non-uniform sampling.

### The Backpressure Estimate

Busy time gets unreliable under sustained backpressure, so the second estimate uses the backpressure signal directly. While a vertex is blocked it could have been processing, so divide the achieved rate by the fraction of time it was not blocked:

```
OBSERVED_TPR = numRecordsInPerSecond / (1 − backPressuredTimeMsPerSecond / 1000)
```

It is computed for sources only, and only while the observation is meaningful, a rate reflects capacity rather than a lack of input only while the source is catching up:

```
catchingUp = LAG ≥ numRecordsInPerSecond × job.autoscaler.observed-true-processing-rate.lag-threshold   (default 30s)
```

If `numRecordsInPerSecond = 0` the estimate is `+∞` (idle, allow scale down), and if `backPressuredTimeMsPerSecond ≥ 1000` the formula is `NaN`. When the current observation is unusable, it falls back to the historical average of past `OBSERVED_TPR` values, provided at least `observed-true-processing-rate.min-observations` (default 2) exist, else `NaN`.

### Choosing Between Them

`observedTprAvg = getAverage(OBSERVED_TPR)` over the window (subject to `min-observations`). Then:

```
if observedTprAvg is NaN                                              → busyTimeTpr
else if busyTimeTpr is NaN or +∞                                     → OBSERVED_TPR
else if busyTimeTpr > observedTprAvg × (1 + switch-threshold)        → OBSERVED_TPR   (default switch-threshold 0.15)
else                                                                 → busyTimeTpr
```

In words: trust the optimistic `busyTimeTpr` unless it claims more than roughly 15% more capacity than the conservative `OBSERVED_TPR` floor. The selection is value-based and applies to any vertex that has an `OBSERVED_TPR`, which in the current implementation means sources.

### What Drives the Switch

With `busy + backpressured + idle ≈ 1` and the same `R`:

```
busyTimeTpr / OBSERVED_TPR = (1 − backpressured) / busy = (busy + idle) / busy = 1 + idle/busy
```

So the switch (`busyTimeTpr > observed × (1 + threshold)`) is really `idle/busy > threshold`, driven by idle time relative to busy time, not by backpressure directly. But high backpressure shrinks `busy`, which amplifies `idle/busy`, so heavily backpressured sources fall back to `OBSERVED_TPR` far more readily. A vertex with zero idle stays on `busyTimeTpr` no matter how backpressured it is, because the two estimates coincide there.

{{< hint info >}}
Most vertices, most of the time, use `busyTimeTpr`. `OBSERVED_TPR` takes over for heavily backpressured sources with non-trivial idle, where busy time overstates capacity. And `TRUE_PROCESSING_RATE` is a windowed average, not an instantaneous rate, deliberately, for stability.
{{< /hint >}}

## Decide: From Ratio to Parallelism

For each non-excluded vertex with valid metrics (`TRUE_PROCESSING_RATE` not `NaN`, and at least one threshold not `NaN`), `JobVertexScaler` computes the new parallelism.

**1. Scale factor.** The ratio of required capacity (demand at the target utilization, plus catch-up) to actual capacity:

```
scaleFactor = ( CATCH_UP_DATA_RATE
              + (TARGET_DATA_RATE × restartTime) / job.autoscaler.catch-up.duration
              + TARGET_DATA_RATE / job.autoscaler.utilization.target )
              / TRUE_PROCESSING_RATE
```

Dividing by `utilization.target` adds headroom: a lower target scales to higher parallelism so the same throughput runs less saturated.

**2. Observed-scalability correction** (if `job.autoscaler.observed-scalability.enabled`): `scaleFactor = scaleFactor / α`. The coefficient `α ∈ [observed-scalability.coefficient-min, 1.0]` (default `[0.5, 1.0]`) captures sub-linear scaling and is fit per vertex from its own scaling history, assuming `R_i = β·α·P_i`:

```
α = Σ(P_i · R_i) / (β · Σ(P_i²))         (OLS), clamped to [coefficient-min, 1]
```

`α = 1` is perfect linear scaling, `α < 1` is sub-linear and inflates the required parallelism. It falls back to `1.0` with insufficient history, fewer than `observed-scalability.min-observations` samples (default 3).

**3. Cap the factor** to `[1 − scale-down.max-factor, 1 + scale-up.max-factor]` (default `[0.4, 100001]`).

**4. New parallelism.**

```
newParallelism = ceil(currentParallelism × scaleFactor)
                 clamped to [vertex.min-parallelism, min(vertex.max-parallelism, maxParallelism)]   (defaults 1, 200)
```

**Key-group and partition alignment** (sources and HASH-partitioned vertices). The clamped parallelism is aligned to the number of key groups or source partitions, `N`, by the mode selected through `job.autoscaler.scaling.parallelism-alignment.mode`. `ParallelismAligner` scans upward from the target, on a scale-up through `[target, upperAlignLimit]` and on a scale-down through the band between the target and the current parallelism, and returns the first accepted value: an exact divisor of `N` in every mode, and in the default `BALANCED` mode also any `p` that reduces the biggest per-subtask share (`N / p < N / target` in whole key groups or partitions). When nothing in the region is accepted, the computed target is used unchanged, alignment never blocks a scaling. The user-facing mode semantics and the search figure live under [Autoscaler → Executing Scaling Actions]({{< ref "docs/managing/autoscaler#executing-scaling-actions" >}}), custom modes plug in as described under [Custom Parallelism Alignment Modes]({{< ref "docs/deployment/plugins#custom-parallelism-alignment-modes" >}}), and only the deprecated legacy adjust modes keep the old blocking behavior with its `ScalingLimited` event.

**5. Expected processing rate.** Once the final `newParallelism` is known, after capping and alignment, the expected post-scale capacity is recorded for next-cycle effectiveness detection:

```
EXPECTED_PROCESSING_RATE = TRUE_PROCESSING_RATE × (newParallelism / currentParallelism)
```

It uses the applied ratio, computed after the constraints, not the raw pre-constraint `scaleFactor`. Otherwise a constraint that changed the applied parallelism (alignment, min, max) would make a perfectly effective scaling look ineffective.

**6. Direction guards.** A change is only emitted with the matching direction:

- **Scale up** requires `TRUE_PROCESSING_RATE < SCALE_UP_RATE_THRESHOLD`.
- **Scale down** requires `TRUE_PROCESSING_RATE > SCALE_DOWN_RATE_THRESHOLD`.

The safety mechanisms that wrap these guards are in the next section.

**The job-wide balanced gate.** A vertex is outside its band when `TRUE_PROCESSING_RATE` falls outside `[SCALE_DOWN_RATE_THRESHOLD, SCALE_UP_RATE_THRESHOLD]`. After all vertices are processed, if no vertex is outside its band, the cycle applies nothing, even if some vertices computed a different recommendation, and logs that all vertex processing rates are within target. This is the `balanced` path.

## Stay Stable: The Anti-Oscillation Toolkit

The autoscaler scales a live job, so its real enemy is thrashing. Four mechanisms exist for exactly this, and it helps to see them as one design.

- **Windowed averages** ([Sense](#sense-the-windowed-metric-history)) keep transient spikes out of every input.
- **The utilization band** ([Understand](#the-utilization-band)) means small drifts inside `[scale-down, scale-up]` trigger nothing.
- **Scale-up effectiveness detection.** Each scale-up records its `EXPECTED_PROCESSING_RATE`. The next cycle compares the actual gain to the expected gain:

  ```
  effectiveness = (TRUE_PROCESSING_RATE_now  − TRUE_PROCESSING_RATE_prev)
                / (EXPECTED_PROCESSING_RATE_prev − TRUE_PROCESSING_RATE_prev)
  ```

  If `effectiveness < job.autoscaler.scaling.effectiveness.threshold` (default `0.1`), the last scale-up delivered almost none of its promised gain. An event is always published, and the new scale-up is blocked only if `job.autoscaler.scaling.effectiveness.detection.enabled = true`. This is what stops the autoscaler from pouring parallelism into a vertex that does not actually scale, one bottlenecked on an external system for example, and it is precisely why `EXPECTED_PROCESSING_RATE` must be recorded from the applied parallelism.

- **Delayed scale-down.** When `job.autoscaler.scale-down.interval` is set (default 1h), a scale-down is recorded but not executed. When the interval elapses, the largest parallelism recommended during the window is applied. Releasing capacity slowly is safe, reclaiming it prematurely risks immediately needing it back. The delayed state is persisted in the state store and cleared after a scaling action.

## Execute and Apply

`ScalingExecutor` assembles the per-vertex decisions into a plan, runs the gates, and persists the outcome.

**Gates (any of these blocks the whole cycle):**

- **Memory pressure**, checked first. If `GC_PRESSURE` or `HEAP_MAX_USAGE_RATIO` exceeds the configured threshold, an event is published and no vertex is scaled. Both thresholds default to `1.0`, so the gate only takes effect once lowered.
- **Scaling disabled or excluded period**: `job.autoscaler.scaling.enabled = false`, or the current time falls within `job.autoscaler.excluded.periods` (cron-like windows), exits with an event.
- **Cluster resources**: blocked if `NUM_TASK_SLOTS_USED` is unavailable, or if the new total slots would exceed cluster capacity.
- **Resource quota**: per slot-sharing group, blocked if `TM_CPU × newTMCount > job.autoscaler.quota.cpu` or `TM_Memory × newTMCount > job.autoscaler.quota.memory`. Skipped when the change reduces TaskManagers, scale-down is always allowed.

**Persist.** Scaling history (the `ScalingSummary` per vertex per timestamp: current and new parallelism plus evaluated metrics), scaling tracking, parallelism overrides, and config overrides are written to the state store. The `scalings` counter is incremented on a change, `balanced` on a no-op, `errors` in the exception handler. The decisions also surface as Kubernetes events, catalogued under [Autoscaler Events]({{< ref "docs/operations/events#autoscaler-events" >}}).

**Apply.** The realizer writes the overrides into the spec, and the reconcile cycle applies them as described under [Integration with the Operator](#integration-with-the-operator): parallelism in place, memory via restart.

## Memory Tuning

Beyond parallelism, the autoscaler can right-size TaskManager memory from observed usage. The user-facing view of this feature is [Autotuning]({{< ref "docs/managing/autotuning" >}}), this section covers the budget computation. `MemoryTuning` parses the original user memory spec (`CommonProcessMemorySpec`), then budgets the pools in a fixed order, because each allocation eats into a shared `MemoryBudget` that caps the remainder:

```
initialBudget = TotalProcessMemory − FrameworkOffHeap − TaskOffHeap − JVMOverhead   (reserved, not tuned)

1. Network   : recomputed from topology + new parallelisms + buffer config; pinned via NETWORK_MEMORY_MIN = MAX
2. Metaspace : avg(METASPACE_MEMORY_USED) × (1 + memory.tuning.overhead)             (budgeted before heap)
3. Heap      : avg(HEAP_MEMORY_USED)      × (1 + memory.tuning.overhead)
4. Managed   : 0 if unused; all remaining budget if memory.tuning.maximize-managed-memory; else original
5. Re-scale heap for the parallelism change via MemoryScaling.applyMemoryScaling
              (factor = numTMsBefore / numTMsAfter: scaling up shrinks per-TM heap, scaling down grows it)
```

The result is a `ConfigChanges`: set `TOTAL_PROCESS_MEMORY`, `MANAGED_MEMORY_FRACTION`, `JVM_OVERHEAD_FRACTION`, `JVM_METASPACE`, pinned network, remove `TOTAL_FLINK_MEMORY`, `TASK_HEAP_MEMORY`, `MANAGED_MEMORY_SIZE`, and set `FRAMEWORK_HEAP_MEMORY = 0`. A recommendation event is published on every executed scaling action, memory tuning is computed only as part of one, so the recommendations stay visible even in dry-run mode. The change is only applied (the restart path) when `job.autoscaler.memory.tuning.enabled = true`, otherwise an empty config is returned.

## Quick Reference

### Capacity and Demand

| Quantity               | Formula                                                                                           | Scope                        |
|------------------------|---------------------------------------------------------------------------------------------------|------------------------------|
| `OBSERVED_TPR`         | `numRecordsInPerSecond / (1 − backPressuredTimeMsPerSecond/1000)`                                 | sources, while catching up   |
| `busyTimeTpr`          | `inputRateForTpr / (busyTimeAvg/1000)`, numerator and `busyTimeAvg` both follow `busy-time.aggregator` | all vertices            |
| `TRUE_PROCESSING_RATE` | `busyTimeTpr`, switching to `OBSERVED_TPR` when `busyTimeTpr > OBSERVED_TPR × (1 + switch-threshold)` | all vertices            |
| `TARGET_DATA_RATE`     | sources: `max(0, inputRate + lagRate)`, non-sources: `Σ edges (input × outputRatio)`              | all vertices                 |
| `CATCH_UP_DATA_RATE`   | `last_LAG / catch-up.duration` while in backlog, else `NaN` (propagated for non-sources)          | all vertices                 |

### Thresholds and Decision

| Quantity                    | Formula                                                                                                       |
|-----------------------------|----------------------------------------------------------------------------------------------------------------|
| `SCALE_UP_RATE_THRESHOLD`   | `CATCH_UP_DATA_RATE + TARGET_DATA_RATE / upperUtilization`                                                     |
| `SCALE_DOWN_RATE_THRESHOLD` | `CATCH_UP_DATA_RATE + (TARGET_DATA_RATE × restartTime)/catch-up.duration + TARGET_DATA_RATE / lowerUtilization` |
| `scaleFactor`               | `(CATCH_UP + (TARGET × restartTime)/catch-up.duration + TARGET / utilization.target) / TRUE_PROCESSING_RATE`, `÷ α` if enabled, then capped to `[1 − scale-down.max-factor, 1 + scale-up.max-factor]` |
| `newParallelism`            | `ceil(currentParallelism × scaleFactor)`, clamped to `[vertex.min, min(vertex.max, maxParallelism)]`, then partition-aligned |
| `EXPECTED_PROCESSING_RATE`  | `TRUE_PROCESSING_RATE × (newParallelism / currentParallelism)` (applied ratio)                                 |
| `α`                         | `Σ(P_i·R_i) / (β·Σ(P_i²))` ∈ `[coefficient-min, 1.0]`                                                          |
| `effectiveness`             | `(TPR_now − TPR_prev) / (EXPECTED_prev − TPR_prev)`, ineffective if `< effectiveness.threshold`                |

### Utilization Bounds

- `upperUtilization` = `1.0` in backlog, else `utilization.max` or `utilization.target + utilization.target.boundary` (default `0.7 + 0.3 = 1.0`).
- `lowerUtilization` = `0` in backlog (disables scale-down), else `utilization.min` or `utilization.target − utilization.target.boundary` (default `0.7 − 0.3 = 0.4`).

### Where a Change Lands

| Change      | Mechanism                                                                                              | Disruptive |
|-------------|--------------------------------------------------------------------------------------------------------|------------|
| Parallelism | `pipeline.jobvertex-parallelism-overrides` in the spec, `SCALE` diff, in-place rescale (`resource-requirements` REST or reactive) | No |
| Memory      | memory config + TaskManager resource in the spec, `UPGRADE` diff, restart (`last-state` or `savepoint`) | Yes        |

### Key Relations

```
SCALE_DOWN_RATE_THRESHOLD ≤ TRUE_PROCESSING_RATE ≤ SCALE_UP_RATE_THRESHOLD   → within band, no change
TRUE_PROCESSING_RATE < SCALE_UP_RATE_THRESHOLD                              → scale up (capacity short of demand)
TRUE_PROCESSING_RATE > SCALE_DOWN_RATE_THRESHOLD                            → scale down (excess capacity)
```

At least one vertex must leave its band for any scaling to proceed.

## Rules of Thumb and Worked Scenarios

- **`busyTimeTpr` for almost everything, `OBSERVED_TPR` for heavily backpressured sources.** The switch is driven by `idle/busy`, which backpressure amplifies.
- **Catch-up is additive**: required capacity is steady-state demand plus backlog catch-up, so a shorter `catch-up.duration` scales more aggressively, and scale-down is disabled entirely while catching up.
- **Keep `metrics.window` well above the reconcile interval**, or the window never holds two samples and the autoscaler silently never scales.
- **Alignment nudges, it does not pin**: the default `BALANCED` mode snaps to a divisor of the partition count when one is within reach but accepts mildly skewed values, and when nothing aligns, the computed target proceeds unchanged. Strict divisor-only alignment is `EVENLY_SPREAD`.
- **Effectiveness needs the applied ratio**: `EXPECTED_PROCESSING_RATE` is computed after alignment and clamping, so constraints do not masquerade as ineffective scalings.

Worked scenarios:

- **A steady CPU-bound operator** runs near 100% busy with little idle, so `busyTimeTpr ≈ OBSERVED_TPR` and it scales up cleanly on the busy-time estimate.
- **A non-source operator backpressured by a slow external system** never receives an `OBSERVED_TPR`, the floor exists only for sources, so it stays on `busyTimeTpr`. What protects it from over-scaling is effectiveness detection: capacity poured into an externally bottlenecked vertex delivers no gain, and the follow-up scale-up is blocked when detection is enabled.
- **A source recovering from a backlog** has lag above the threshold, so `OBSERVED_TPR` is computed and the catch-up term inflates demand for an aggressive scale-up, while scale-down stays disabled until the backlog clears.
- **A skewed source** (one hot subtask) is hard to size from aggregate metrics and is guided by partition alignment, so the autoscaler stays deliberately conservative.
