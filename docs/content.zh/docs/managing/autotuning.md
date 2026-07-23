---
title: "Autotuning"
weight: 5
type: docs
aliases:
- /custom-resource/autotuning.html
- /docs/custom-resource/autotuning/
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

# Autotuning

Autotuning right-sizes the memory of a running Flink job automatically. TaskManager memory is split across several pools, heap, network, managed, and JVM overhead, and sizing them by hand is notoriously difficult: a poor split either crashes the job or strands memory that is reserved but never used. Autotuning observes the job and adjusts the pools on its own, the reasoning behind it is described under [Autoscaling → Autotuning]({{< ref "docs/concepts/autoscaling#autotuning" >}}).

{{< hint info >}}
The container memory is adjusted on Kubernetes `FlinkDeployment` resources only, a `FlinkSessionJob` receives no memory tuning.
{{< /hint >}}

## Requirements

- **Autoscaler**: autotuning runs as part of the [Autoscaler]({{< ref "docs/managing/autoscaler" >}}) and requires `job.autoscaler.enabled: "true"` on the job, as described under [Enabling the Autoscaler]({{< ref "docs/managing/autoscaler#enabling-the-autoscaler" >}}).

## How It Works

The declared TaskManager memory acts as the ceiling: memory is configured exactly as usual, and the configured size is the maximum autotuning ever uses. It never grows a TaskManager beyond it, so pods stay safely inside their memory quotas and limits. Within that ceiling, autotuning observes the actual maximum memory usage on the TaskManagers, computes the exact number of network buffers the job topology requires, right-sizes the memory pools accordingly, and brings down the total container memory size. The tuned settings are persisted as part of the autoscaler state, surviving operator restarts, as described under [State → Autoscaler State]({{< ref "docs/operations/state#autoscaler-state" >}}).

The tuning rides the scaling cycle in both directions: the settings are applied with scaling actions, causing no restarts of their own, and new recommendations are likewise only computed when the autoscaler carries out one. A job that keeps running at its target utilization receives no fresh memory tuning until the next rescale.

Assigning memory generously at first is therefore the safe choice: unused memory is reclaimed automatically, while a ceiling set too low cannot be raised by autotuning.

## Enabling Autotuning

### Dry-Run Mode

With the autoscaler enabled, autotuning reports the memory settings it would apply, without changing anything. The recommendations are surfaced through Kubernetes events, catalogued under [Events → Autotuning Events]({{< ref "docs/operations/events#autotuning-events" >}}):

```yaml
job.autoscaler.memory.tuning.enabled: false
```

### Automatic Mode

Automatic memory tuning is enabled by setting:

```yaml
job.autoscaler.memory.tuning.enabled: true
```

## Configuration

### Maximizing Managed Memory

Instead of shrinking the container, the saved memory can be handed to the managed memory pool, beneficial for RocksDB workloads to maximize their in-memory performance:

```yaml
job.autoscaler.memory.tuning.maximize-managed-memory: true
```

### Scale-Down Memory Compensation

When a scale-down removes TaskManagers, the memory they carried disappears with them. With compensation enabled, autotuning grows the memory of the remaining TaskManagers in return, within the declared ceiling, keeping the total cluster memory steady until the usage after the rescale can be measured. Scale-ups do not shrink the per-TaskManager memory in turn. The compensation is on by default and can be turned off:

```yaml
job.autoscaler.memory.tuning.scale-down-compensation.enabled: false
```

### Setting the Memory Overhead

Autotuning reserves a constant share of overhead on top of the determined heap and metaspace sizes, as headroom for the memory to grow beyond the observed maximum. The default share is `0.2` and can be adjusted:

```yaml
job.autoscaler.memory.tuning.overhead: 0.5
```

## Limitations

- Tuning is computed only as part of scaling decisions: while a job holds its target utilization and no rescale happens, no new memory recommendations or adjustments are produced.
- Disabling the autoscaler clears the stored tuning overrides together with the rest of the autoscaler state, the job returns to its declared memory settings on the next upgrade.
- Container memory adjustment works only on Kubernetes `FlinkDeployment` resources, a `FlinkSessionJob` receives no memory tuning.
- Only TaskManager memory is tuned: JobManager memory and the CPU of both processes are left unchanged.
- The number of task slots is not optimized on its own, it only follows the parallelism decisions of the autoscaler.
- RocksDB managed memory is kept constant: Flink lacks the metrics to measure managed memory usage, so for RocksDB jobs autotuning sizes only the heap, metaspace, and network pools. With a heap-based state backend, managed memory is set to zero and saved entirely.
- The declared memory acts only as a ceiling, no lower bound exists: recommendations may shrink the container as far as the observed usage suggests, and guaranteed headroom below the declared size cannot be reserved.
