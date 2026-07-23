---
title: "Autoscaling"
weight: 5
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

# Autoscaling

The load of a streaming job is rarely constant: traffic follows daily peaks, seasonal patterns, campaigns, and backfills. A job with fixed parallelism leaves only two options, both bad. Provisioning for the peak wastes capacity, and money, whenever the peak is not there, while provisioning below it means backpressure and growing lag whenever it is. Retuning parallelism by hand does not keep up with this, and every manual retune is an operational event in itself.

The operator's autoscaler removes that trade-off by observing every managed job continuously and adjusting its parallelism automatically, in both directions. Its central concept is utilization: the share of its maximum processing capacity that a part of the job actually uses. The autoscaler holds the job at a configured target utilization, high enough to be efficient, low enough to absorb load fluctuations without immediately falling behind.

The figure below shows what happens when the target, here 60%, cannot be held. Load rises until processing hits capacity: utilization reaches 100%, the job processes less than what arrives, and the difference accumulates as backlog, the missing capacity between the input rate and what can be sustained. Scaling up closes that gap, the enlarged capacity absorbs the input rate again at the target utilization:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/concepts/autoscaling-scaling-up.svg" alt="Scaling up on load increase" >}}

The same logic works downward. When load falls, utilization sinks far below the target and the job pays for capacity that does nothing: the pipeline is perfectly healthy, just oversized for what now arrives. Scaling down releases the idle share and brings utilization back to the target, with the smaller deployment still sustaining the input rate comfortably:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/concepts/autoscaling-scaling-down.svg" alt="Scaling down on load decrease" >}}

Like everything the operator does, scaling rides the [reconciliation loop]({{< ref "docs/concepts/architecture#reconciliation-loop" >}}), and a scaling action is carried out as part of the managed [lifecycle]({{< ref "docs/concepts/lifecycle-management" >}}).

## Scaling Granularity

Parallelism is adjusted for individual job vertices, the chained operator groups that make up the pipeline, rather than for the job as a whole. A complex, heterogeneous pipeline therefore grows exactly where its bottleneck is and shrinks exactly where capacity sits idle, instead of multiplying every stage alike.

The example below plays the same scale-up out across a whole pipeline. Every vertex is saturated, and every vertex gets exactly what it needs: the source doubles to sustain the incoming rate, while the slower downstream vertices quadruple to keep up with what their upstream now emits. The utilization story from above repeats for each vertex individually, its input is matched at its own capacity. For simplicity the new capacities match the flowing rates exactly, a real decision adds headroom on top so that every vertex lands at the target utilization:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/concepts/autoscaling-vertex-granularity.svg" alt="Per-vertex scaling of a saturated pipeline" >}}

## Scaling Decisions

Decisions are driven by what the job itself reports: how many records enter and leave each vertex, how busy each vertex is, and how much backlog waits at the sources. Container CPU and memory play no direct role, which is what separates this from generic pod autoscaling. The autoscaler reasons about records and processing capacity, the terms in which a streaming job actually experiences load, and resource pressure shows up in those numbers anyway.

From the sources the autoscaler works its way downstream. Sources must sustain the rate at which data arrives, and every downstream vertex must sustain the combined output of the vertices feeding it. Out of these target rates it derives, for each vertex, the parallelism at which the vertex meets its target comfortably below full capacity, at the target utilization.

The approach follows the scaling model of [Kalavri et al.](https://www.usenix.org/system/files/osdi18-kalavri.pdf), built on the insight that a streaming pipeline's required capacity can be computed from observed rates in very few steps, instead of searched for by trial and error.

## Stability by Design

An autoscaler that chased every fluctuation would restart the job constantly and do more harm than good, so the design leans deliberately toward calm. Metrics are averaged over a time window, making decisions respond to sustained trends rather than spikes. Around the target utilization sits a tolerance band, and as long as a vertex stays inside the band, nothing happens at all. After every rescale a stabilization period gives the job time to settle before new measurements count. And capacity is released more cautiously than it is added, so a brief dip does not immediately take away headroom that the next peak will need.

## Rescaling Costs

Applying a scaling decision is an upgrade like any other, executed through the regular upgrade cycle, or in place without a restart when the cluster supports it, as described under [Job Management]({{< ref "docs/managing/job-management#scaling-without-a-spec-change" >}}). The pause has a price: while the job restarts, backlog accumulates. The autoscaler plans for exactly that, sizing the new capacity with enough extra headroom to catch the accumulated backlog up within a bounded time, so the job returns to processing fresh data instead of permanently lagging behind.

Scaling up is also never unbounded. Every vertex grows only up to a configurable maximum parallelism, within the limit the vertex itself was deployed with, and resource quotas can cap the job as a whole: a scale-up that would push total CPU or memory past the quota is not executed at all. Growth stops at a deliberate ceiling, not at an exhausted cluster.

## Building Trust

The autoscaler does not have to act to be useful. In metrics-only mode it evaluates everything and reports what it would do without touching the job, which is the recommended way to build confidence before enabling scaling actions. The evaluated per-vertex metrics, utilization, processing rates, and the applied parallelism, are exported alongside the operator's other metrics either way, and double as a detailed performance profile of the pipeline.

Enabling the autoscaler, choosing utilization targets, and the tuning knobs behind all of the above are covered under [Autoscaler]({{< ref "docs/managing/autoscaler" >}}).

## Autotuning

Parallelism is only half of right-sizing a job. The other half is memory, and it is notoriously the harder half to configure by hand: a TaskManager's memory is split across several pools (heap, managed, network, JVM overhead), and a poor split either crashes the job or quietly strands memory that is reserved but never used.

Autotuning automates this the same way scaling automates parallelism, from observation. Given only a maximum TaskManager size, declared exactly like regular memory settings, it watches the actual memory usage and garbage collection pressure, computes the network buffers the job topology actually needs, and right-sizes the pools accordingly, often shrinking the total container memory below its starting point. In the figure, the usage that filled only half of the originally granted memory fills 80% of the tuned size, and the difference is given back:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/concepts/autoscaling-autotuning.svg" alt="Autotuning right-sizing the TaskManager memory" >}}

The declared size is treated as a ceiling and is never exceeded. Tuned settings are applied together with scaling actions, so autotuning causes no restarts of its own, and like the autoscaler it can first run in a recommendation-only mode, reporting the memory settings it would apply. It applies to Kubernetes deployments and is covered under [Autotuning]({{< ref "docs/managing/autotuning" >}}).
