---
title: "Autoscaler"
weight: 4
type: docs
aliases:
- /custom-resource/autoscaler.html
- /docs/custom-resource/autoscaler/
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

The operator ships a built-in job autoscaler that keeps the parallelism of a running Flink job aligned with its actual load. It watches every enabled `FlinkDeployment` and `FlinkSessionJob`, measures how busy each job vertex really is, and adjusts parallelism in both directions: vertices that fall behind receive more subtasks, vertices that sit idle give capacity back. Because the adjustment happens per vertex rather than per job, a heterogeneous pipeline is sized precisely, every stage gets exactly the capacity its own load demands, and nothing more. The concepts behind these operations are covered under [Autoscaling]({{< ref "docs/concepts/autoscaling" >}}).

Once enabled, the autoscaler runs as a continuous loop alongside the reconciliation of the resource. Every pass moves through the same three phases: fresh metrics are collected from the running job, evaluated into a parallelism recommendation for each vertex, and the recommended change, if any, is executed on the cluster:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/autoscaler-loop.svg" alt="The autoscaler loop: collect, evaluate, execute" >}}

## Requirements

- **Streaming jobs**: the autoscaler scales streaming jobs, its whole model is built on continuously observed rates. Vertices that finish, such as bounded sources in a mixed pipeline, are automatically excluded from scaling. For batch workloads, parallelism is decided by Flink's own [Adaptive Batch Scheduler](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/adaptive_batch/) instead.
- **Flink version**: the autoscaler works with all [supported Flink versions]({{< ref "docs/deployment/compatibility#supported-flink-versions" >}}), for application-mode `FlinkDeployment` and `FlinkSessionJob` resources. In-place rescaling additionally requires the adaptive scheduler (`jobmanager.scheduler: adaptive`).
- **Sources**: scaling sources requires the [FLIP-27 Source API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface), which exposes the busy-time metric. Reserving catch-up capacity additionally requires the [standardized connector metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics) for backlog information. Most common connectors provide both, with Kafka sources being the most complete.

## Enabling the Autoscaler

The autoscaler is turned on per job, through the Flink configuration of the resource:

```yaml
flinkConfiguration:
  job.autoscaler.enabled: "true"
```

All `job.autoscaler.*` options can be set on the resource, or operator-wide as defaults. Changing them never restarts the job by itself: autoscaler options are applied in place, as described under [Spec Diffing]({{< ref "docs/custom-resource/overview#spec-diffing" >}}).

How scaling actions are executed depends on the job's scheduler. With the adaptive scheduler enabled (`jobmanager.scheduler: adaptive`), decisions are applied in place through Flink's [Externalized Declarative Resource Management](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/elastic_scaling/#externalized-declarative-resource-management), without restarting the job. Without it, every decision is rolled out as a full upgrade of the Flink job, in both the [Native and Standalone]({{< ref "docs/deployment/overview#operator-deployment-modes" >}}) operator deployment modes. This is not to be confused with Flink's [Reactive Mode](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/elastic_scaling/#reactive-mode), a separate elastic-scaling approach for standalone application clusters that is driven by the TaskManager replica count rather than by the autoscaler, described under [Standalone Mode]({{< ref "docs/deployment/overview#standalone-mode" >}}).

{{< hint info >}}
The autoscaler also supports a metrics-only mode where it collects and evaluates everything and reports the parallelism it would apply, without triggering any scaling. This is the recommended way to gain confidence in the module without any impact on the running application. To evaluate without acting, set `job.autoscaler.scaling.enabled: "false"`.
{{< /hint >}}

## Configuration

The options follow the three phases of the loop, and so do the sections below. The complete list, with every default, lives in the [autoscaler configuration reference]({{< ref "docs/deployment/configuration#autoscaler-configuration" >}}), and the full internal derivation behind each phase, every formula and branch, is covered under [Internals → Autoscaler]({{< ref "docs/internals/autoscaler" >}}).

### Collecting Metrics

The autoscaler works on the metrics the job itself exposes, queried over the Flink REST API. Container CPU and memory usage play no role. One sample is collected per reconciliation pass, every minute by default, and each sample arrives already aggregated: the metrics are queried across all subtasks of a vertex, with the minimum, maximum, and average computed upfront.

Collection follows a fixed rhythm, shown on the timeline below with the default intervals, five samples of stabilization, fifteen samples to fill the window, and an evaluation on every pass after that:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/autoscaler-collection-timeline.svg" alt="Stabilization, window filling, and the sliding evaluation window on a timeline" >}}

#### Stabilization

After every job start or rescale, the job needs time to settle before its metrics mean anything. For the duration of `job.autoscaler.stabilization.interval` (default 5 minutes) samples do not count toward the window, and no decisions are taken.

#### Window Filling

The samples that follow are averaged over the sliding window set by `job.autoscaler.metrics.window` (default 15 minutes). The first evaluation happens once the window is full, and from then on every pass re-evaluates the window as it slides forward by one sample, until a decision triggers a rescale and the cycle starts over. The window size is the main smoothing knob: a larger window makes decisions calmer but slower to react, a smaller one reacts faster but is more exposed to fluctuations. Values between 3 and 60 minutes work well in practice, depending on how spiky the load is.

{{< hint warning >}}
The relation between the reconciliation interval and the metrics window is essential: the window must retain at least two samples for scaling to be evaluated at all. A `job.autoscaler.metrics.window` smaller than `kubernetes.operator.reconcile.interval` is rejected by validation, since the window would be trimmed to a single sample on every pass and autoscaling would never be applied.
{{< /hint >}}

The samples survive operator restarts: they are persisted in the autoscaler ConfigMap under the `collectedMetrics` entry on every pass, stabilization included, samples taken while the job settles are stored like any other, they only never count toward the window. Once the window is full, every pass also trims the stored history down to the window, and when the job restarts, the history is cleared entirely and collection starts from scratch.

#### Busy Time and Observed Rates

For the busy-time metric, `job.autoscaler.metrics.busy-time.aggregator` (default `MAX`) selects which of the subtask aggregates counts for the vertex: with `MAX`, a vertex is considered as busy as its busiest subtask, which is robust against data skew, while `AVG` can produce more stable estimates when the load is known to be evenly distributed.

The busy time is what the capacity estimate is built on: the true processing rate of a vertex is its record rate divided by the share of time it spends busy, how much it processes per second of actual work. The estimate is only as good as the busy-time metric itself, and for sources under heavy load it tends to be at its least reliable, exactly when it matters most.

For sources there is a direct measurement to fall back on, driven by backpressure time instead of busy time: while a source works through a real backlog, its ingest rate divided by the share of time it is not backpressured is the capacity it demonstrably delivers, the observed true processing rate. Observation is gated by `job.autoscaler.observed-true-processing-rate.lag-threshold` (default 30 seconds), samples are taken only while the source lags behind by at least that much, and `job.autoscaler.observed-true-processing-rate.min-observations` samples (default `2`) are needed before the observed rate is trusted. The evaluation then switches a source from the busy-time estimate to the observed rate when the two deviate by more than `job.autoscaler.observed-true-processing-rate.switch-threshold` (default `0.15`, the busy-time estimate at least 15% higher during catch-up). Whichever estimate prevails is the vertex's true processing rate, its current capacity, the number the required capacity is later checked against and divided by:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/autoscaler-current-capacity.svg" alt="The two estimates of the true processing rate and the switch producing the current capacity" >}}

{{< hint info >}}
The current capacity is an average observed over the metrics window, not an instantaneous rate. This is deliberate: reacting to transient spikes or dips in throughput would lead to oscillating decisions, so by averaging over the window the autoscaler responds to sustained trends rather than momentary fluctuations.
{{< /hint >}}

#### Scaling Tracking

Besides the job metrics, collection also tracks the autoscaler's own actions: for every rescale, the start and the moment the job came back are recorded, capturing the actual restart durations. Whether these observations are used, and how, is decided during evaluation, where the restart time determines how much backlog a rescale is expected to accumulate, and with it the catch-up capacity.

These records are persisted in the autoscaler ConfigMap under the `scalingTracking` entry, next to the metric samples of `collectedMetrics`. What each entry holds and how to inspect it is covered under [State → Autoscaler State]({{< ref "docs/operations/state#autoscaler-state" >}}).

### Evaluating Scaling Decisions

The evaluation derives, per vertex, the capacity the vertex must provide, the required capacity that the execution phase then turns into a parallelism. The reasoning fits in one model, and each of its ingredients maps to one of the subsections below.

The utilization term, for the autoscaler, is not the momentary busy gauge shown in the Flink Web UI. It relates the incoming data rate to the capacity that should serve it: the required capacity is sized so that the normal load consumes exactly the target share of it, leaving the remainder as headroom. It is computed from the windowed and aggregated data rates, not from a momentary reading.

A decision starts from the capacity the vertex must provide, and three terms add up to it: the capacity to keep up with the normal incoming rate at the target utilization, the extra rate needed to drain any existing backlog within the configured catch-up window, and an allowance for the backlog that the rescale itself will create while the job restarts:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/autoscaler-capacity-model.svg" alt="The capacity model: the three capacity terms adding up to the required capacity" >}}

The whole model rests on the input rate, and that number is less obvious than it looks: it is not simply what a vertex currently reads. For a source, the evaluation uses the ingestion rate, what the source reads plus the rate at which its lag grows, so a source falling behind is credited with the load that actually arrives at the external system, not just the share it keeps up with. No other vertex gets its rate from its own measurements either. The evaluation walks the pipeline from the sources downstream, deriving each vertex's data rate from the vertices feeding it. Every edge carries an output ratio: how many records the upstream vertex sends down that edge for every record it receives. Multiplying the upstream's target rate by this ratio gives the data rate of the edge, and a vertex's own data rate is simply the sum across its incoming edges. The catch-up rate for the backlog travels the same way:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/autoscaler-evaluation-recursion.svg" alt="Data rates evaluated per edge, propagated from the sources downstream" >}}

The measured rates still matter, they calibrate the edge ratios, but what propagates is the target: size the sources for the true incoming load, and every downstream vertex is sized for the load it will face once the sources keep up. Because Flink exposes no metrics on edges, only per vertex, the measured per-edge rates behind the ratios are estimates, and the evaluation picks the most reliable derivation available, the cases the colors in the figure distinguish:
- A vertex with a single input can use its own measured input rate for the edge, the most reliable case.
- A vertex merging several inputs can still get one edge measured indirectly: when every other feeding vertex has a single output, their output rates are subtracted from the vertex's total input rate, and the remainder is the edge's rate.
- When neither holds, the estimate falls back to the upstream's total output rate, the least reliable case, since a total spread over several outgoing edges cannot be attributed to one of them.

{{< hint warning >}}
Accuracy degrades with fan-out: as soon as a vertex feeding a multi-input vertex has several outputs, for example side outputs or split streams, the data rates evaluated downstream of that point can over- or understate the real load. Judge scaling decisions in such pipelines with this in mind.
{{< /hint >}}

Every quantity of this model, the rates, the utilization, the thresholds, and the resulting recommendation, is computed per vertex as an evaluated metric, and the evaluated metrics are reported through the operator's metric system, described under [Scaling Metrics]({{< ref "docs/operations/metrics#scaling-metrics" >}}). What the scaling acts on and what can be observed from the outside are the same numbers.

#### Utilization Band

In order to provide stable job performance and some buffer for load fluctuations, the autoscaler works toward a configurable target utilization level for the job (`job.autoscaler.utilization.target`, default `0.7`).
A target of `0.6` means targeting 60% utilization for the job vertices.

In general, it is not recommended to set the target utilization close to the extremes: near 100%, performance usually degrades once capacity limits are reached in most real-world systems, while near 0% the job is heavily over-provisioned, reserving capacity that sits idle and defeating the cost benefits of scaling.

When scaling actions are triggered is controlled through the upper and lower utilization thresholds, `job.autoscaler.utilization.max` and `job.autoscaler.utilization.min` (defaulting to `target + 0.3` and `target - 0.3` respectively).
These define the utilization range within which the autoscaler will not take any scaling action, providing a buffer against load fluctuations.

For example, with a target utilization of `0.6`, setting `job.autoscaler.utilization.max: "0.8"` and `job.autoscaler.utilization.min: "0.4"` means:

- Scale-up is triggered when utilization exceeds `0.8`
- Scale-down is triggered when utilization drops below `0.4`
- No scaling action is taken while utilization remains between `0.4` and `0.8`

{{< hint warning >}}
The utilization band is at the core of triggering the scaling: whether the autoscaler acts at all, and in which direction, is decided solely by where the observed utilization sits relative to the band. Setting the target and thresholds poorly, or leaving them at defaults that do not match the workload, directly shapes how eagerly the job is scaled up or down.
{{< /hint >}}

#### Catch-Up Capacity

The catch-up terms are complementary to the utilization band: the band decides whether the steady-state capacity still fits the load, while catch-up adds the recovery on top, the extra rate needed to work off backlog. The existing-backlog term relies on the sources reporting the standardized `pendingRecords` metric, without it the backlog is treated as zero and only the restart allowance and the steady-state term remain.

Rescaling itself costs time: while the job restarts, backlog accumulates, and the new deployment must process it on top of the incoming load. The autoscaler reserves extra capacity for exactly that, sized from the expected restart time, detailed in the next subsection, and from the catch-up window, `job.autoscaler.catch-up.duration` (default 30 minutes), the time within which the accumulated backlog should be fully processed after a rescale. Lowering the duration reserves more spare capacity. Set it based on the actual delivery objective, for example 10, 30 or 60 minutes, or to `0` to disable backlog-based scaling entirely.

The backlog term pulls scale-up forward: it raises the required capacity, so a backlogged vertex is scaled earlier and further, and the shorter the catch-up window, the more aggressive the step. It also holds the other direction back: while an actual backlog is being drained, scale-down is suspended entirely, avoiding decisions that would create more lag. What counts as an actual backlog is decided by `job.autoscaler.backlog-processing.lag-threshold` (default 5 minutes): the suspension kicks in when the lag at any source is worth more than that much processing time.

#### Restart Time

The expected restart duration is the input of the restart allowance: it estimates for how long the job will be down during a rescale, and with it how much backlog will pile up before processing resumes. The longer the expected restart, the larger the allowance, and the more capacity every decision reserves.

By default the estimate is static, `job.autoscaler.restart.time` (default 5 minutes). With `job.autoscaler.restart.time-tracking.enabled: "true"` the autoscaler instead uses the maximum duration observed across the rescales recorded during collection, capped by `job.autoscaler.restart.time-tracking.limit` (default 15 minutes). The static estimate still applies until the first rescale has been observed. Tracking is worth enabling once a job has been through a few rescales: it replaces a guess with the job's own worst observed case. The estimate matters in both directions: an underestimated restart leaves the job lagging longer than the catch-up window promises, while an overestimated one reserves capacity that is never needed.

The restart allowance also brakes scale-down: it is part of the scale-down threshold, so capacity is only released when the smaller deployment could still absorb the backlog of a future rescale.

#### Custom Evaluators

The evaluated metrics are also the extension seam of this phase. A custom evaluator is invoked for every vertex at the end of each evaluation cycle, exactly before the evaluated metrics are reported, and the values it returns are merged on top of the internally evaluated ones, overriding existing values or adding new ones, so it shapes both what the execution acts on and what is exported.

Implementations of the `ScalingMetricsEvaluatorPlugin` interface are discovered as plugins and selected per job: list an instance name under `job.autoscaler.metrics.custom-evaluators`, point `job.autoscaler.metrics.custom-evaluator.<name>.class` at the implementation, and pass instance options under `job.autoscaler.metrics.custom-evaluator.<name>.<option>`. Only a single evaluator per job is honored for now: when several are configured, the first entry wins and the rest are ignored with a warning. Writing and packaging an evaluator is covered under [Custom Scaling Metric Evaluators]({{< ref "docs/deployment/plugins#custom-scaling-metric-evaluators" >}}).

### Executing Scaling Actions

Execution starts where evaluation ended, at the required capacity, and turns it into the parallelism it applies. Dividing the required capacity by the current capacity, the measured true processing rate, gives the scale factor. The factor then passes through a round of adjustments, the observed scalability coefficient reshapes it when learning from history is enabled and the maximum scale-up and scale-down factors cap it, before it is multiplied onto the current parallelism. The product is clamped to the configured bounds and, for keyBy vertices and partitioned sources, alignment snaps it to the key groups or partitions, yielding the recommended parallelism for the vertex. The recommendation still has to clear the blocking barrier of the scaling execution guards. The custom executors then shape it one final time, and only what survives is written to the job as its new parallelism:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/autoscaler-parallelism-pipeline.svg" alt="From the required capacity to the applied parallelism overrides" >}}

When a decision passes all checks, the autoscaler overrides the vertex parallelisms of the job, written as `pipeline.jobvertex-parallelism-overrides` into the job configuration and persisted under the `parallelismOverrides` entry of the autoscaler ConfigMap, so the current recommendation survives operator restarts and stays applied across job upgrades. The override lands in place when the cluster supports it, or through the regular upgrade cycle honoring the configured `upgradeMode`, as described under [Job Management]({{< ref "docs/managing/job-management#scaling-without-a-spec-change" >}}).

#### Observed Scalability

By default the scale factor assumes linear scaling: doubling the parallelism doubles the capacity. Real pipelines rarely scale perfectly, coordination and network overhead eat part of every step, and `job.autoscaler.observed-scalability.enabled` makes the autoscaler learn the actual behavior instead: a scalability coefficient is fitted over the vertex's own scaling history, the true processing rates observed at each past parallelism, and the scale factor is corrected by it. The fit needs at least `job.autoscaler.observed-scalability.min-observations` past decisions (default `3`), assuming linear scaling until then, and the coefficient is clamped between `job.autoscaler.observed-scalability.coefficient-min` (default `0.5`) and `1.0`, so a low estimate cannot turn into runaway scale-ups and super-linear scaling is never assumed.

The history itself, one entry per executed scaling, is retained per vertex under the `scalingHistory` entry of the autoscaler ConfigMap, trimmed by `job.autoscaler.history.max.count` (default `3`) and `job.autoscaler.history.max.age` (default 24 hours). Raising `min-observations` therefore only works together with a matching `history.max.count`, and both are bounded by what the state store can hold. The coefficient is not the history's only consumer: the effectiveness detection below judges the latest history entry, and the scaling tracking matches its restart durations against the recorded targets.

#### Parallelism Bounds

The computed parallelism is bounded by `job.autoscaler.vertex.min-parallelism` (default `1`) and `job.autoscaler.vertex.max-parallelism` (default `200`), and never exceeds the max parallelism configured for the vertex itself. For sources that report a partition count, Kafka and Pulsar do by default, the partition count acts as an additional upper bound, since scaling a source beyond its partitions would only add idle subtasks.

The max parallelism itself deserves care of its own: Flink's `pipeline.max-parallelism` (or a per-operator max parallelism) fixes the number of key groups of the keyed state, and with it the hard ceiling that no scaling can ever exceed. It is not a knob to adjust casually, changing it is incompatible with restoring the existing state, so it should be chosen generously upfront, with room to grow.

{{< hint info >}}
The autoscaler bounds are the safe way to play with the ceiling: `job.autoscaler.vertex.max-parallelism` and `job.autoscaler.vertex.min-parallelism` cap the parallelism dynamically and, like every autoscaler option, are applied in place without a restart, while the sensitive `pipeline.max-parallelism` stays untouched.
{{< /hint >}}

How far a single decision may move the parallelism is capped as well: `job.autoscaler.scale-down.max-factor` (default `0.6`) allows at most a 60% reduction in one step, so the new parallelism never drops below 40% of the current one, while `job.autoscaler.scale-up.max-factor` is effectively unbounded by default.

#### Parallelism Alignment

A computed parallelism can distribute data unevenly: the key groups of a keyBy (hash) vertex, or the partitions of a source, are shared across subtasks, and some parallelisms split them badly. Alignment adjusts the computed target to a nearby value that distributes better, controlled by `job.autoscaler.scaling.parallelism-alignment.mode`:

- `BALANCED` (default): aligns to the first parallelism in the scaling direction that reduces the per-subtask load, snapping to an exact divisor of the key groups or partitions when one is within reach. It balances even distribution against resource usage, tolerating mild skew to avoid over-provisioning.
- `EVENLY_SPREAD`: aligns strictly to a divisor of the key groups or source partitions, spreading the data evenly across subtasks.
- `OFF`: uses the computed target as-is.

All modes search the same way, upward from the computed target, on scale-up toward the upper alignment limit, on scale-down toward the current parallelism, and differ only in which parallelism they accept:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/custom-resource/autoscaler-alignment-modes.svg" alt="The search regions and accept conditions of the alignment modes" >}}

In the figure, `N` is the number a vertex aligns to, its key groups for a keyBy vertex or the partition count for a source, and `p` is the candidate parallelism the search tests. `EVENLY_SPREAD` accepts `p` only when it divides `N` without remainder, while `BALANCED` also accepts the first `p` that lowers the biggest per-subtask share relative to the computed target, which is what `N / p < N / target` expresses in whole key groups or partitions. The two limits cap the search on both ends: the upper align limit is the smallest of `N`, the vertex's max parallelism, and `job.autoscaler.vertex.max-parallelism`, so alignment never proposes more subtasks than there is data to distribute, and the lower align limit is the `job.autoscaler.vertex.min-parallelism` floor, which the computed target already respects through the [Parallelism Bounds](#parallelism-bounds) above.

Alignment never blocks a scale: when no aligned value exists in the searched region, the computed target is used unchanged. It applies to keyBy vertices and to sources reporting a partition count. Because divisors are what alignment snaps to, it pays off to set `pipeline.max-parallelism` to a number with many divisors, such as `120`, `180`, `240`, `360` or `720`.

{{< hint info >}}
The autoscaler currently collects source partition counts only for Kafka and Pulsar, so source alignment applies to those connectors alone, while other sources keep their computed target. Aligning keyBy (hash) vertices works regardless, as their key groups are always known.
{{< /hint >}}

Because both searches move upward, the built-in modes carry a structural bias toward more parallelism: a scale-up may be aligned further than the computed target, while a scale-down is softened back toward the current parallelism, aggressive up, conservative down. Custom alignment modes are the answer whenever this does not fit. They can implement a different trade-off, treat the parallelism ranges in any other way, or extend the alignment to vertices the built-ins do not recognize: between vertices, only keyBy (hash) partitioning is handled by default, so a pipeline that partitions data with a custom scheme can bring its own mode.

To plug one in, list the mode name in `job.autoscaler.scaling.parallelism-alignment.mode`, point `job.autoscaler.scaling.parallelism-alignment.mode.<name>.class` at the implementation, discovered as a plugin, and pass instance parameters under `job.autoscaler.scaling.parallelism-alignment.mode.<name>.<parameter>`. Writing and packaging a mode is covered under [Custom Parallelism Alignment Modes]({{< ref "docs/deployment/plugins#custom-parallelism-alignment-modes" >}}).

#### Scaling Effectiveness

Not every bottleneck scales. When a vertex is limited by an external system, additional subtasks add coordination but no throughput, and every further scale-up wastes resources without helping. Effectiveness detection catches this: after every scale-up, the autoscaler compares the processing-rate increase it expected with the increase that actually materialized, and when less than `job.autoscaler.scaling.effectiveness.threshold` (default `0.1`, a tenth of the expected gain) was accomplished, the scale-up is marked ineffective and an `IneffectiveScaling` event is emitted.

With `job.autoscaler.scaling.effectiveness.detection.enabled: "true"` (disabled by default) the autoscaler also acts on the detection, blocking further scale-ups for the affected vertex, scale-downs remain possible.

#### Scaling Execution Guards

Even a fully computed decision is not applied unconditionally. Before anything is executed it has to pass the guards below, which together form the blocking barrier in front of every scaling action: each guard is checked in turn, and a single failing check blocks the scaling entirely for the current pass. When this happens no vertex is rescaled, the job keeps running untouched, and the blocked decision is only reported, through events or the operator log, together with the reason. The barrier does not stop the loop itself: metrics keep being collected and evaluated, and the next pass recomputes the decision and faces the same checks again.

- `job.autoscaler.excluded.periods`: time periods during which no scaling is executed, expressed as Quartz cron or daily time-range expressions, for example `* * 9-11,14-16 * * ?` for business hours or `9:30:30-10:50:20 && * * * ? * 2,5`. Useful for keeping the job untouched during peaks or maintenance windows.
- `job.autoscaler.memory.gc-pressure.threshold` and `job.autoscaler.memory.heap-usage.threshold`: scaling is blocked while the observed garbage collection pressure or heap usage exceeds these limits, so a memory-strained job is not made worse by a restart. Both default to `1.0`, which disables the guard.
- Cluster capacity: scale-ups can be validated against what the cluster can actually hold. When `kubernetes.operator.cluster.resource-view.refresh-interval` is set to a positive duration (the check is disabled by default), the operator periodically reads the allocatable CPU and memory of the cluster nodes. Against that view it simulates placing the TaskManagers the rescaled job would need, estimated from the new parallelisms and `taskmanager.numberOfTaskSlots`. An action that would not fit is blocked, which above all prevents mass scale-ups after an outage from exhausting the cluster. The simulation is aware of the [Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler): when one is detected, the node view is extrapolated up to the maximum size of each autoscaled node group, so capacity that the Cluster Autoscaler can still provision counts as available. Independently of the view, an action is also held back while the JobManager has not reported its task slot usage yet, typically right after the job comes up.
- `job.autoscaler.quota.cpu` and `job.autoscaler.quota.memory`: hard resource quotas for the whole job, also accounted in task slots. The slots needed before and after the rescale are derived from the maximum vertex parallelism of each slot sharing group and converted into TaskManagers through `taskmanager.numberOfTaskSlots`, and the action is blocked when the resulting total TaskManager CPU or memory would exceed a quota. Only growth is checked, an action that does not add TaskManagers always passes, and a violation is additionally reported through a `ResourceQuotaReached` warning event.
- `job.autoscaler.vertex.exclude.ids`: vertex ids that are never scaled. This guard narrows the action instead of blocking it, the excluded vertices are left out of the decision while the remaining ones keep scaling normally.
- `job.autoscaler.scale-down.interval` (default 1 hour): scale-downs are delayed and merged within this window, so several consecutive load drops become one restart instead of many. This guard defers instead of blocking, and scale-ups are never delayed.

The scale-down interval does more than postpone the action. The first scale-down recommendation for a vertex only opens the window, nothing is applied yet, and every following pass records what it would have scaled the vertex down to. Only once recommendations have kept pointing down for the whole interval does the scale-down execute, and it applies the highest parallelism recommended within the window rather than the latest one, so a brief deep dip cannot drag the vertex to its lowest recorded target. Whenever a pass recommends the current parallelism or more in the meantime, the pending request is discarded, and any executed rescale clears all pending requests as well. These requests are persisted in the autoscaler ConfigMap under the `delayedScaleDown` entry, so an operator restart does not reset a window that is already running.

#### Scaling Autotuning

A scaling action can carry a memory change alongside the new parallelisms. This is autotuning, a feature of its own rather than a stage of the parallelism pipeline: with `job.autoscaler.memory.tuning.enabled: "true"` (disabled by default), every executed action recomputes the TaskManager memory configuration from the observed usage of the job, and the rescale rolls out both changes in the same restart. Within the execution it accompanies the resource guards: the tuning runs before them, so the cluster capacity and quota checks validate the TaskManager profile that would actually be deployed. The custom executors receive the tuned configuration as well. How the memory budget itself is derived is covered under [Autotuning]({{< ref "docs/managing/autotuning" >}}).

#### Custom Executors

The execution is extensible as well. Custom executors are invoked after the guards have passed and before the parallelism overrides are applied, and each one can modify or veto the computed decision: the plugins are chained, every plugin receives the output of the previous one, and returning an empty result vetoes the scaling entirely.

Implementations of the `ScalingExecutorPlugin` interface are discovered as plugins and selected per job: list an instance name under `job.autoscaler.scaling.custom-executors`, point `job.autoscaler.scaling.custom-executor.<name>.class` at the implementation, and pass instance parameters under `job.autoscaler.scaling.custom-executor.<name>.<parameter>`. Writing and packaging an executor is covered under [Custom Scaling Executors]({{< ref "docs/deployment/plugins#custom-scaling-executors" >}}).

{{< hint info >}}
Custom executors do not get the final word, the vertex exclusions do. At the very end of every pass, when the stored overrides are applied to the job, `job.autoscaler.vertex.exclude.ids` is consulted once more: for an excluded vertex, a `pipeline.jobvertex-parallelism-overrides` entry specified by the user takes precedence over the autoscaler's decision. Not even a custom executor can move an excluded vertex that the user has pinned this way.
{{< /hint >}}

## Standalone Autoscaler

The autoscaler also runs outside the operator, as a separate Java process, for Flink clusters that are not managed by Kubernetes: standalone clusters, YARN session or application clusters, and plain session clusters. For jobs managed by the operator, using the built-in integration described above is strongly recommended.

The standalone autoscaler monitors a single Flink cluster through its REST endpoint and applies decisions in place through the [rescale API](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/elastic_scaling/#externalized-declarative-resource-management), which requires the adaptive scheduler on the target cluster. Download the released `flink-autoscaler-standalone` jar from the [Maven repository](https://repo.maven.apache.org/maven2/org/apache/flink/flink-autoscaler-standalone/) and start it with:

```shell
java -cp flink-autoscaler-standalone-{{< version >}}.jar \
org.apache.flink.autoscaler.standalone.StandaloneAutoscalerEntrypoint \
--autoscaler.standalone.fetcher.flink-cluster.host localhost \
--autoscaler.standalone.fetcher.flink-cluster.port 8081
```

The host and port match the Flink Web UI of the target cluster. All `job.autoscaler.*` options apply, set either at the standalone process level or per job, and the process-specific options (fetcher, state store, event handler) are listed in the [standalone configuration reference]({{< ref "docs/deployment/configuration#autoscaler-standalone-configuration" >}}). Configuration is loaded from the `config.yaml` in the `FLINK_CONF_DIR` directory, with command-line parameters taking precedence.

By default the standalone process keeps its state in memory and logs its events. Both can be moved to a database by setting the state store and event handler type to `jdbc`, which survives process restarts. A JDBC driver is required, together with an initialized schema:

| Driver     | Group Id           | Artifact Id            | JAR                                                                          | Schema                                                                                                                                                |
|------------|--------------------|------------------------|------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| MySQL      | `mysql`            | `mysql-connector-java` | [Download](https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/) | [Table DDL](https://github.com/apache/flink-kubernetes-operator/blob/main/flink-autoscaler-plugin-jdbc/src/main/resources/schema/mysql_schema.sql)    |
| PostgreSQL | `org.postgresql`   | `postgresql`           | [Download](https://jdbc.postgresql.org/download/)                            | [Table DDL](https://github.com/apache/flink-kubernetes-operator/blob/main/flink-autoscaler-plugin-jdbc/src/main/resources/schema/postgres_schema.sql) |
| Derby      | `org.apache.derby` | `derby`                | [Download](http://db.apache.org/derby/derby_downloads.html)                  | [Table DDL](https://github.com/apache/flink-kubernetes-operator/blob/main/flink-autoscaler-plugin-jdbc/src/main/resources/schema/derby_schema.sql)    |

```shell
JDBC_DRIVER_JAR=./mysql-connector-java-8.0.30.jar
# export the password of the JDBC state store and event handler
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

## State

Everything the autoscaler learns is persisted next to the resource, in a dedicated ConfigMap for Kubernetes jobs: the rolling metric samples under `collectedMetrics`, past decisions under `scalingHistory`, and the applied parallelism under `parallelismOverrides`. What each entry holds, how to decode it, and how it is cleaned up is covered under [State → Autoscaler State]({{< ref "docs/operations/state#autoscaler-state" >}}). Changing `spec.job.autoscalerResetNonce` to a new non-null value resets this state entirely, metrics, history, and parallelism overrides, returning the job to its user-specified parallelism without toggling the autoscaler off and on.

## Metrics

The operator reports detailed vertex-level metrics about everything the autoscaler collects and evaluates: utilization, input and target rates, scaling thresholds, and the parallelism changes over time. They are published under the operator resource metric group as `[resource_prefix].AutoScaler.[jobVertexID].[ScalingMetric].Current/Average` and are available even in metrics-only mode. The metric system and its reporters are covered under [Scaling Metrics]({{< ref "docs/operations/metrics#scaling-metrics" >}}).

## Events

Scaling decisions, recommendations, and errors are reported as Kubernetes events on the resource, `ScalingReport` being the most important one. The autoscaler event types, their deduplication, and retention are covered under [Autoscaler Events]({{< ref "docs/operations/events#autoscaler-events" >}}).

## Limitations

All of the following are limitations of the autoscaler itself, not of Flink.

### Sources

- Partition counts are only discovered for Kafka and Pulsar: the partition-based upper bound and the source alignment do not apply to other connectors.
- Partitions added at runtime are only picked up after the source's own partition discovery has assigned them to readers.
- A Kafka source subscribed to several topics, by list or by pattern, reports a single summed partition count: bounds and alignment work on the sum, which does not reflect Kafka's per-topic partition assignment.
- The observed true processing rate exists only for sources: every other vertex relies solely on the busy-time estimate.
- Data skew at sources is not evened out: decisions are per vertex, so unevenly distributed data leaves some subtasks busier than the decision assumes.

### Blind Spots

- Flink exposes no per-edge metrics: the data rates of multi-input vertices fed by multi-output vertices are estimates, see [Evaluating Scaling Decisions](#evaluating-scaling-decisions).
- A slow or degraded TaskManager is indistinguishable from load: the response is a scale-up, not a replacement. Most relevant for long-lived session clusters.
- A throttled sink is not detected: its backpressure reads as load, and scale-ups cannot help. [Scaling Effectiveness](#scaling-effectiveness) partially mitigates this.

### Scaling Actions

- Session clusters are not autoscaled: the session `FlinkDeployment` itself carries no autoscaler, only the `FlinkSessionJob` resources running on it are scaled. In Native mode the cluster's TaskManagers follow the jobs' slot needs on their own, in Standalone mode the cluster keeps its declared replica count regardless of the jobs' scaling.
- Capacity limits at one vertex do not backpropagate: a vertex held back by exclusion, the parallelism bounds, or a blocked scale-up bottlenecks the job, while the remaining vertices stay sized for the full target rate and hold capacity the bottleneck will not let through.
- Aligning all vertices to one shared parallelism is not supported: alignment works per vertex. A [custom executor](#custom-executors) can enforce a uniform value, since it receives the decision for every vertex at once.
- Kubernetes `ResourceQuota` objects are not consulted by the resource guards: a namespace quota can still reject an approved scale-up.
- The cluster capacity check recognizes only the Kubernetes Cluster Autoscaler: node headroom that Karpenter or another provisioner could add is not extrapolated, so the check can block scale-ups the cluster would actually absorb.
