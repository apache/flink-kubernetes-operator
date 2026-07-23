---
title: "Zero-Downtime Upgrades"
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

# Zero-Downtime Upgrades

Every standard upgrade contains a stop. As described under [Lifecycle Management]({{< ref "docs/concepts/lifecycle-management#stateful-upgrades" >}}), an upgrade is a suspend followed by a restore with the latest spec, and between those two steps the job processes nothing. The upgrade modes shorten this gap but do not remove it: even the fastest stateful mode leaves a window in which no data flows. For most workloads this brief pause is perfectly acceptable. For pipelines with strict continuity requirements it is not.

Zero-downtime upgrades close the gap with the classic blue/green pattern, adapted to streaming. Instead of restarting the running job in place, the operator brings up a complete second deployment with the new spec next to the old one, and retires the old deployment only after the new one has proven itself. The pipeline is not stopped at any point:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/concepts/zero-downtime-upgrades.svg" alt="Zero-downtime upgrade timeline" >}}

A spec change does not touch the running deployment (v1). A savepoint is taken while the job keeps processing, nothing is suspended, and that savepoint becomes the handoff point. A second deployment (v2) is provisioned with the new spec, restores from the savepoint, and catches up while v1 continues to serve. Only once v2 has been observed stable, v1 is torn down, and v2 carries on alone. Seen from the outside, data keeps flowing through the entire transition.

The pattern name comes from the two deployment slots, blue and green, between which the active role alternates. The first upgrade hands over from blue to green, the next one from green back to blue, and the resource status reports every transition through the phases shown in the figure, from active through savepointing and transitioning back to active.

{{< hint warning >}}
Continuity has a price. During the transition two complete Flink clusters run side by side, so the environment needs the capacity for both. While both jobs run they process the same records, so the output is duplicated with at-least-once semantics. This holds even for jobs that are otherwise exactly-once: the two deployments process and commit independently of each other, and there is currently no coordination between them that would preserve exactly-once delivery across the transition. Downstream consumers must tolerate the duplicates, through idempotent writes or by deduplicating on read.
{{< /hint >}}

The transition is guarded in the other direction as well: if the new deployment does not become stable within a configurable grace period, the transition is aborted and the previous deployment keeps serving, with the failure reported on the resource.

Zero-downtime upgrades are currently an experimental capability, driven by a dedicated custom resource. Creating one, configuring the transition behavior, and the operational details are covered under [Blue/Green Deployments]({{< ref "docs/managing/bluegreen-deployments" >}}).
