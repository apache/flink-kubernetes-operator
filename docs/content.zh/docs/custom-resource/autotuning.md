---
title: "Autotuning"
weight: 4
type: docs
aliases:
- /custom-resource/autotuning.html
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

# Flink Autotuning

Flink Autotuning aims at fully automating the configuration of Apache Flink.

One of the biggest challenges with deploying new Flink pipelines is to write an adequate Flink configuration. The most
important configuration values are:

- memory configuration (heap memory, network memory, managed memory, JVM off-heap, etc.)
- number of task slots

## Memory Autotuning

As a first step, we have tackled the memory configuration which, according to users, is the most frustrating part of
the configuration process. The most important aspect of the memory configuration is the right-sizing of the
various [Flink memory pools](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_tuning/).
These memory pools include: heap memory, network memory, managed memory, and JVM off-heap memory settings. Non-optimal
configuration of these pools can cause application crashes, or block large amounts of memory which remain unused.

### How It Works

With Flink Autoscaling and Flink Autotuning, all users need to do is set a max memory size for the TaskManagers, just
like they would normally configure TaskManager memory. Flink Autotuning then automatically adjusts the various memory
pools and brings down the total container memory size. It does that by observing the actual max memory usage on the
TaskMangers or by calculating the exact number of network buffers required for the job topology. The adjustments are
made together with Flink Autoscaling, so there is no extra downtime involved.

It is important to note that adjusting the container memory only works on Kubernetes and that the initially provided
memory settings represent the maximum amount of memory Flink Autotuning will use. You may want to be more conservative
than usual when initially assigning memory with Autotuning. We never go beyond the initial limits to ensure we can
safely create TaskManagers without running into pod memory quotas or limits.

### Getting Started

#### Dry-run Mode

As soon as Flink Autoscaling is enabled, Flink Autotuning will provide recommendations via events
(e.g. Kubernetes events):
```
# Autoscaling needs to be enabled
job.autoscaler.enabled: true
# Disable automatic memory tuning (only get recommendations)
job.autoscaler.memory.tuning.enabled: false
```

#### Automatic Mode

Automatic memory tuning via can be enabled by setting:

```
# Autoscaling needs to be enabled
job.autoscaler.enabled: true
# Turn on Autotuning and apply memory config changes
job.autoscaler.memory.tuning.enabled: true
```

### Advanced Options

#### Maximize Managed Memory

Enabling the following option allows to return all saved memory as managed memory. This is beneficial
when running with RocksDB to maximize its performance.

```
job.autoscaler.memory.tuning.maximize-managed-memory: true
```

#### Setting Memory Overhead

Memory Autotuning uses a constant amount of memory overhead for heap and metaspace to allow the memory to grow beyond
the determined maximum size. The default of 20% can be changed to 50% by setting:

```
job.autoscaler.memory.tuning.overhead: 0.5
```

## Future Work

### Task Slots Autotuning

The number of task slots are partially taken care by Flink Autoscaling which adjusts the task parallelism and hence
changes the total number of slots and the number of TaskManagers.

In future versions of Flink Autotuning, we will try to further optimize the number of task slots depending on the
number of tasks running inside a task slot.

### JobManager Memory Tuning

Currently, only TaskManager memory is adjusted.

### RocksDB Memory Tuning

Currently, if no managed memory is used, e.g. the heap-based state backend is used, managed memory will be set to
zero by Flink Autotuning which helps save a lot of memory. However, if managed memory is used, e.g. via RocksDB, the
configured managed memory will be kept constant because Flink currently lacks metrics to accurately measure the usage of
managed memory.

Nevertheless, users already benefit from the resource savings and optimizations for heap, metaspace, and
network memory. RocksDB users can solely focus their attention on configuring managed memory.

We already added an option to add all saved memory to the managed memory. This is beneficial when running with RocksDB
to maximize the in-memory performance.
