---
title: "Lifecycle Management"
weight: 3
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

# Lifecycle Management

Lifecycle management is the operator's core duty. Once a Flink resource is declared, the operator runs its entire life: it deploys the job, watches it until it is stable, upgrades it on every spec change, heals it when it degrades, rolls a failed upgrade back, snapshots its state, and cleans everything up on deletion. Each of these is simply what the [reconciliation loop]({{< ref "docs/concepts/architecture#reconciliation-loop" >}}) does in a particular situation, there is no separate machinery behind any of them. Scaling rides the same loop and is covered separately under [Autoscaling]({{< ref "docs/concepts/autoscaling" >}}).

At its core, the managed lifecycle is one simple loop. A resource is created, upgraded whenever its spec changes, deployed, and observed until stable, where it rests until the next change. The first deployment and every later upgrade travel the same path, and deletion or a terminal failure can end the loop at any point:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/concepts/lifecycle-management-overview.svg" alt="Lifecycle management overview" >}}

Each stop in the figure is a conceptual grouping rather than a single state. The status a resource actually reports distinguishes many more, among them rollbacks, suspension, and the deletion phases, and the complete state machine behind this abstraction is documented under [Status and Lifecycle]({{< ref "docs/custom-resource/status-and-lifecycle" >}}).

## Deployment and Stabilization

The loop starts with a submitted spec. The operator validates it and drives the deployment end to end. For a Flink deployment it provisions the whole Flink cluster with all of its Kubernetes objects, laid out in the [Deployment Overview]({{< ref "docs/deployment/overview" >}}), and submits the job to it. For a session job it submits into an already running session cluster. No Flink client or manual step is involved, the declared resource is the only input.

A job that has just been submitted is running but not yet trusted. The operator keeps observing it, and only a deployment that stays healthy through the readiness window is marked stable. The distinction between running and stable is deliberate: it is what makes it possible to detect a failed upgrade and roll it back, as described under [Self-Healing and Rollback](#self-healing-and-rollback).

## Stateful Upgrades

Once stable, a resource rests until its spec changes, and the next pass through the loop is an upgrade. What makes upgrades delicate is state. A Flink job is stateful when its operators accumulate data that must survive a restart: aggregations, windows, source offsets. Flink persists this state in two kinds of snapshots, checkpoints taken automatically and owned by Flink, and savepoints taken deliberately and owned by the user. A stateful upgrade is a restart that carries this state across: the operator stops the job, deploys the new spec, and restores from a snapshot so that processing continues where it left off.

An upgrade is a suspend followed by a restore with the latest spec. The same two verbs are also available directly: a job can be suspended, stopped while keeping its state information, and later resumed from where it stopped, possibly with an updated spec.

How state is carried across the restart is decided by the upgrade mode:

<!-- The exported SVG embeds its own editable draw.io source: open it directly in draw.io to modify the figure. -->
{{< img src="/img/concepts/lifecycle-stateful-upgrades.svg" alt="Stateful upgrade modes" >}}

- **Stateless**: the job is cancelled and started again from empty state. Suitable for development and for jobs where state loss is acceptable, not recommended in production.
- **Last-state**: the fastest stateful option. The job is stopped while its HA metadata is kept, and the new job restores from the latest available checkpoint information. It works even when the job is unhealthy, and it requires checkpointing and high availability to be enabled.
- **Savepoint**: the safest option. A savepoint is taken as part of the suspend, and the new job restores from it. The savepoint doubles as a durable backup and fork point, at the price of requiring a healthy job and a longer upgrade. When high availability is enabled it can fall back to last-state behavior for unhealthy jobs.

For production, last-state favors speed and robustness while savepoint favors maximal safety and an explicit backup artifact. Every mode still stops the job briefly, and when even that pause is unacceptable, [Zero-Downtime Upgrades]({{< ref "docs/concepts/zero-downtime-upgrades" >}}) runs the new version next to the old one instead. The full comparison, requirements, and fallback rules are covered under [Job Management]({{< ref "docs/managing/job-management#upgrades" >}}).

## Self-Healing and Rollback

Beyond upgrades, the reconciliation loop also reacts to trouble on its own:

- an unhealthy cluster, one caught in restart storms or with stalled checkpoint progress, is restarted
- a JobManager deployment deleted by accident or by an external process is recreated, and the job recovers from its HA metadata
- a terminally failed job can be restarted from its latest checkpoint
- a failed upgrade is rolled back: every new spec starts as unstable (DEPLOYED), is marked STABLE once observed healthy, and if it does not stabilize within a readiness window the operator redeploys the last stable spec (experimental)

{{< hint info >}}
Self-healing leans on [high availability]({{< ref "docs/deployment/leader-election#flink-job-high-availability" >}}): restarting unhealthy clusters, recovering deleted deployments, and rolling back failed upgrades all require it. Restarting a failed job only needs a retained checkpoint to restore from.
{{< /hint >}}

## Snapshots

Upgrades and self-healing both lean on snapshots, and the two kinds play different roles. Checkpoints are Flink's own periodic safety net: taken automatically, owned and cleaned up by Flink, and used for failure recovery and last-state upgrades. Savepoints are deliberate: triggered by the user or by the operator, owned by the user, kept until explicitly disposed, and used for savepoint upgrades, backups, and forking a job into a new one.

Snapshots are triggered in three ways. Upgrade snapshots are taken automatically whenever the upgrade mode demands one, and they are required for correct operation: the operator records the resulting path and restores from it. Manual and periodic snapshots are optional extras for backup and forking, triggered on demand or on a schedule. They do not participate in upgrades, and automatic recovery always uses the latest checkpoint information instead.

Each snapshot is itself managed declaratively, through a dedicated custom resource that tracks its progress and records its final location. The operator also keeps track of the snapshot history and cleans old snapshots up lazily while the job runs, always retaining at least one completed snapshot per job.

Triggering, retention, and disposal are configured under [Snapshot Management]({{< ref "docs/managing/snapshot-management" >}}).

## Deletion and Terminal Failures

The loop has two exits, and they behave very differently.

Deleting the resource is the orderly one: the operator shuts the job down and removes everything it materialized, the complete object tree described in the [Deployment Overview]({{< ref "docs/deployment/overview" >}}). The tracked state information is removed with it, so a redeployment afterwards starts from empty state unless a starting snapshot is provided explicitly. The snapshot files themselves follow their owner: savepoints remain on storage unless disposal was requested, while checkpoints belong to Flink and may be removed by it at any time.

A terminal job failure ends the loop without removing anything: the resource stays in place with the failure recorded in its status, and its retained state information keeps recovery open, whether through the automatic restarts described under [Self-Healing and Rollback](#self-healing-and-rollback) or through a manual redeployment.

Day-to-day operation of this whole lifecycle, starting, upgrading, restarting, and deleting jobs, is covered under [Job Management]({{< ref "docs/managing/job-management" >}}).
