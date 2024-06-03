/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler.tuning;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.autoscaler.utils.ResourceCheckUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.config.memory.CommonProcessMemorySpec;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemory;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Map;

import static org.apache.flink.autoscaler.metrics.ScalingMetric.HEAP_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.MANAGED_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.METASPACE_MEMORY_USED;
import static org.apache.flink.autoscaler.topology.ShipStrategy.FORWARD;
import static org.apache.flink.autoscaler.topology.ShipStrategy.RESCALE;

/** Tunes the TaskManager memory. */
public class MemoryTuning {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryTuning.class);
    public static final ProcessMemoryUtils<TaskExecutorFlinkMemory> FLINK_MEMORY_UTILS =
            new ProcessMemoryUtils<>(getMemoryOptions(), new TaskExecutorFlinkMemoryUtils());

    private static final ConfigChanges EMPTY_CONFIG = new ConfigChanges();

    /**
     * Emits a Configuration which contains overrides for the current configuration. We are not
     * modifying the config directly, but we are emitting ConfigChanges which contain any overrides
     * or removals. This config is persisted separately and applied by the autoscaler. That way we
     * can clear any applied overrides if auto-tuning is disabled.
     */
    public static ConfigChanges tuneTaskManagerMemory(
            JobAutoScalerContext<?> context,
            EvaluatedMetrics evaluatedMetrics,
            JobTopology jobTopology,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            AutoScalerEventHandler eventHandler) {

        // Please note that this config is the original configuration created from the user spec.
        // It does not contain any already applied overrides.
        var config = new UnmodifiableConfiguration(context.getConfiguration());

        // Gather original memory configuration from the user spec
        CommonProcessMemorySpec<TaskExecutorFlinkMemory> memSpecs;
        try {
            memSpecs = FLINK_MEMORY_UTILS.memoryProcessSpecFromConfig(config);
        } catch (IllegalConfigurationException e) {
            LOG.warn("Current memory configuration is not valid. Aborting memory tuning.");
            return EMPTY_CONFIG;
        }

        MemorySize specHeapSize = memSpecs.getFlinkMemory().getJvmHeapMemorySize();
        MemorySize specManagedSize = memSpecs.getFlinkMemory().getManaged();
        MemorySize specNetworkSize = memSpecs.getFlinkMemory().getNetwork();
        MemorySize specMetaspaceSize = memSpecs.getJvmMetaspaceSize();
        LOG.info(
                "Spec memory - heap: {}, managed: {}, network: {}, meta: {}",
                specHeapSize.toHumanReadableString(),
                specManagedSize.toHumanReadableString(),
                specNetworkSize.toHumanReadableString(),
                specMetaspaceSize.toHumanReadableString());

        MemorySize maxMemoryBySpec = context.getTaskManagerMemory().orElse(MemorySize.ZERO);
        if (maxMemoryBySpec.compareTo(MemorySize.ZERO) <= 0) {
            LOG.warn("Spec TaskManager memory size could not be determined.");
            return EMPTY_CONFIG;
        }

        MemoryBudget memBudget = new MemoryBudget(maxMemoryBySpec.getBytes());
        // Budget the original spec's memory settings which we do not modify
        memBudget.budget(memSpecs.getFlinkMemory().getFrameworkOffHeap().getBytes());
        memBudget.budget(memSpecs.getFlinkMemory().getTaskOffHeap().getBytes());
        memBudget.budget(memSpecs.getJvmOverheadSize().getBytes());

        var globalMetrics = evaluatedMetrics.getGlobalMetrics();
        // The order matters in case the memory usage is higher than the maximum available memory.
        // Managed memory comes last because it can grow arbitrary for RocksDB jobs.
        MemorySize newNetworkSize =
                adjustNetworkMemory(
                        jobTopology,
                        ResourceCheckUtils.computeNewParallelisms(
                                scalingSummaries, evaluatedMetrics.getVertexMetrics()),
                        config,
                        memBudget);
        // Assign memory to the METASPACE before the HEAP to ensure all needed memory is provided
        // to the METASPACE
        MemorySize newMetaspaceSize =
                determineNewSize(getUsage(METASPACE_MEMORY_USED, globalMetrics), config, memBudget);
        MemorySize newHeapSize =
                determineNewSize(getUsage(HEAP_MEMORY_USED, globalMetrics), config, memBudget);
        MemorySize newManagedSize =
                adjustManagedMemory(
                        getUsage(MANAGED_MEMORY_USED, globalMetrics),
                        specManagedSize,
                        config,
                        memBudget);
        // Rescale heap according to scaling decision after distributing all memory pools
        newHeapSize =
                MemoryScaling.applyMemoryScaling(
                        newHeapSize, memBudget, context, scalingSummaries, evaluatedMetrics);
        LOG.info(
                "Optimized memory sizes: heap: {} managed: {}, network: {}, meta: {}",
                newHeapSize.toHumanReadableString(),
                newManagedSize.toHumanReadableString(),
                newNetworkSize.toHumanReadableString(),
                newMetaspaceSize.toHumanReadableString());

        // Diff can be negative (memory shrinks) or positive (memory grows)
        final long heapDiffBytes = newHeapSize.getBytes() - specHeapSize.getBytes();
        final long managedDiffBytes = newManagedSize.getBytes() - specManagedSize.getBytes();
        final long networkDiffBytes = newNetworkSize.getBytes() - specNetworkSize.getBytes();
        final long flinkMemoryDiffBytes = heapDiffBytes + managedDiffBytes + networkDiffBytes;

        // Update total memory according to memory diffs
        final MemorySize totalMemory =
                new MemorySize(maxMemoryBySpec.getBytes() - memBudget.getRemaining());
        if (totalMemory.compareTo(MemorySize.ZERO) <= 0) {
            LOG.warn("Invalid total memory configuration: {}", totalMemory);
            return EMPTY_CONFIG;
        }

        // Prepare the tuning config for new configuration values
        var tuningConfig = new ConfigChanges();
        // Adjust the total container memory
        tuningConfig.addOverride(TaskManagerOptions.TOTAL_PROCESS_MEMORY, totalMemory);
        // We do not set the framework/task heap memory because those are automatically derived from
        // setting the other mandatory memory options for managed memory, network, metaspace and jvm
        // overhead. However, we do precise accounting for heap memory above. In contrast to other
        // memory pools, there are no fractional variants for heap memory. Setting the absolute heap
        // memory options could cause invalid configuration states when users adapt the total amount
        // of memory. We also need to take care to remove any user-provided overrides for those.
        tuningConfig.addRemoval(TaskManagerOptions.TOTAL_FLINK_MEMORY);
        tuningConfig.addRemoval(TaskManagerOptions.TASK_HEAP_MEMORY);
        // Set default to zero because we already account for heap via task heap.
        tuningConfig.addOverride(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ZERO);

        MemorySize flinkMemorySize =
                new MemorySize(
                        memSpecs.getTotalFlinkMemorySize().getBytes() + flinkMemoryDiffBytes);

        // All memory options which can be configured via fractions need to be re-calculated.
        tuningConfig.addOverride(
                TaskManagerOptions.MANAGED_MEMORY_FRACTION,
                getFraction(newManagedSize, flinkMemorySize));
        tuningConfig.addRemoval(TaskManagerOptions.MANAGED_MEMORY_SIZE);

        tuningConfig.addOverride(TaskManagerOptions.NETWORK_MEMORY_MIN, newNetworkSize);
        tuningConfig.addOverride(TaskManagerOptions.NETWORK_MEMORY_MAX, newNetworkSize);

        tuningConfig.addOverride(
                TaskManagerOptions.JVM_OVERHEAD_FRACTION,
                getFraction(memSpecs.getJvmOverheadSize(), totalMemory));

        tuningConfig.addOverride(TaskManagerOptions.JVM_METASPACE, newMetaspaceSize);

        eventHandler.handleEvent(
                context,
                AutoScalerEventHandler.Type.Normal,
                "Configuration recommendation",
                String.format(
                        "Memory tuning recommends the following configuration (automatic tuning is %s):\n%s",
                        config.get(AutoScalerOptions.MEMORY_TUNING_ENABLED)
                                ? "enabled"
                                : "disabled",
                        formatConfig(tuningConfig)),
                "MemoryTuning",
                config.get(AutoScalerOptions.SCALING_EVENT_INTERVAL));

        if (!context.getConfiguration().get(AutoScalerOptions.MEMORY_TUNING_ENABLED)) {
            return EMPTY_CONFIG;
        }

        return tuningConfig;
    }

    private static MemorySize determineNewSize(
            MemorySize usage, Configuration config, MemoryBudget memoryBudget) {

        double overheadFactor = 1 + config.get(AutoScalerOptions.MEMORY_TUNING_OVERHEAD);
        long targetSizeBytes = (long) (usage.getBytes() * overheadFactor);

        // Upper limit is the available memory budget
        targetSizeBytes = memoryBudget.budget(targetSizeBytes);

        return new MemorySize(targetSizeBytes);
    }

    private static MemorySize adjustManagedMemory(
            MemorySize managedMemoryUsage,
            MemorySize managedMemoryConfigured,
            Configuration config,
            MemoryBudget memBudget) {
        // Managed memory usage can't accurately be measured yet.
        // It is either zero (no usage) or an opaque amount of memory (RocksDB).
        if (managedMemoryUsage.compareTo(MemorySize.ZERO) <= 0) {
            return MemorySize.ZERO;
        } else if (config.get(AutoScalerOptions.MEMORY_TUNING_MAXIMIZE_MANAGED_MEMORY)) {
            long maxManagedMemorySize = memBudget.budget(Long.MAX_VALUE);
            return new MemorySize(maxManagedMemorySize);
        } else {
            long managedMemorySize = memBudget.budget(managedMemoryConfigured.getBytes());
            return new MemorySize(managedMemorySize);
        }
    }

    /* Calculate the maximum amount of memory for a TaskManager required by all its subtask buffer pools.
     *
     * See https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/network_mem_tuning/#network-buffer-lifecycle
     */
    private static MemorySize adjustNetworkMemory(
            JobTopology jobTopology,
            Map<JobVertexID, Integer> updatedParallelisms,
            Configuration config,
            MemoryBudget memBudget) {

        final int buffersPerChannel =
                config.get(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
        final int floatingBuffers =
                config.get(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
        final long memorySegmentBytes =
                config.get(TaskManagerOptions.MEMORY_SEGMENT_SIZE).getBytes();

        long maxNetworkMemory = 0;
        for (VertexInfo vertexInfo : jobTopology.getVertexInfos().values()) {
            // Add max amount of memory for each input gate
            for (var inputEntry : vertexInfo.getInputs().entrySet()) {
                var inputVertexId = inputEntry.getKey();
                var shipStrategy = inputEntry.getValue();
                maxNetworkMemory +=
                        calculateNetworkSegmentNumber(
                                        updatedParallelisms.get(vertexInfo.getId()),
                                        updatedParallelisms.get(inputVertexId),
                                        shipStrategy,
                                        buffersPerChannel,
                                        floatingBuffers)
                                * memorySegmentBytes;
            }
            // Add max amount of memory for each output gate
            // Usually, there is just one output per task
            for (var outputEntry : vertexInfo.getOutputs().entrySet()) {
                var outputVertexId = outputEntry.getKey();
                var shipStrategy = outputEntry.getValue();
                maxNetworkMemory +=
                        calculateNetworkSegmentNumber(
                                        updatedParallelisms.get(vertexInfo.getId()),
                                        updatedParallelisms.get(outputVertexId),
                                        shipStrategy,
                                        buffersPerChannel,
                                        floatingBuffers)
                                * memorySegmentBytes;
            }
        }

        // Each task slot will potentially host all runtime subtasks if slot sharing enabled.
        // If slot sharing is disabled, this will use more memory than necessary, we better
        // overprovision slightly than failing with "Insufficient Network buffers".
        maxNetworkMemory *= config.get(TaskManagerOptions.NUM_TASK_SLOTS);

        return new MemorySize(memBudget.budget(maxNetworkMemory));
    }

    /**
     * Calculate how many network segment current vertex needs.
     *
     * @param currentVertexParallelism The parallelism of current vertex.
     * @param connectedVertexParallelism The parallelism of connected vertex.
     */
    @VisibleForTesting
    static int calculateNetworkSegmentNumber(
            int currentVertexParallelism,
            int connectedVertexParallelism,
            ShipStrategy shipStrategy,
            int buffersPerChannel,
            int floatingBuffers) {
        // TODO When the parallelism is changed via the rescale api, the FORWARD may be changed to
        // RESCALE. This logic may needs to be updated after FLINK-33123.
        if (currentVertexParallelism == connectedVertexParallelism
                && FORWARD.equals(shipStrategy)) {
            return buffersPerChannel + floatingBuffers;
        } else if (FORWARD.equals(shipStrategy) || RESCALE.equals(shipStrategy)) {
            final int channelCount =
                    (int) Math.ceil(connectedVertexParallelism / (double) currentVertexParallelism);
            return channelCount * buffersPerChannel + floatingBuffers;
        } else {
            return connectedVertexParallelism * buffersPerChannel + floatingBuffers;
        }
    }

    private static MemorySize getUsage(
            ScalingMetric scalingMetric, Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics) {
        MemorySize memoryUsed =
                new MemorySize((long) globalMetrics.get(scalingMetric).getAverage());
        LOG.debug("{}: {}", scalingMetric, memoryUsed);
        return memoryUsed;
    }

    public static MemorySize getTotalMemory(Configuration config, JobAutoScalerContext<?> ctx) {
        MemorySize overrideSize = config.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
        if (overrideSize != null) {
            return overrideSize;
        }
        return ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
    }

    private static ProcessMemoryOptions getMemoryOptions() {
        return new ProcessMemoryOptions(
                Arrays.asList(
                        TaskManagerOptions.TASK_HEAP_MEMORY,
                        TaskManagerOptions.MANAGED_MEMORY_SIZE),
                TaskManagerOptions.TOTAL_FLINK_MEMORY,
                TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                new JvmMetaspaceAndOverheadOptions(
                        TaskManagerOptions.JVM_METASPACE,
                        TaskManagerOptions.JVM_OVERHEAD_MIN,
                        TaskManagerOptions.JVM_OVERHEAD_MAX,
                        TaskManagerOptions.JVM_OVERHEAD_FRACTION));
    }

    private static float getFraction(MemorySize enumerator, MemorySize denominator) {
        // Round to three decimal places but make sure to round up values
        // like 0.0002 to 0.001 instead of 0.0
        return BigDecimal.valueOf(enumerator.getBytes() / (double) denominator.getBytes())
                .setScale(3, RoundingMode.CEILING)
                .floatValue();
    }

    /** Format config such that it can be directly used as a Flink configuration. */
    private static String formatConfig(ConfigChanges config) {
        var sb = new StringBuilder();
        for (Map.Entry<String, String> entry : config.getOverrides().entrySet()) {
            sb.append(entry.getKey())
                    .append(": ")
                    .append(entry.getValue())
                    .append(System.lineSeparator());
        }
        if (!config.getRemovals().isEmpty()) {
            sb.append("Remove the following config entries if present: [");
            boolean first = true;
            for (String toRemove : config.getRemovals()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(toRemove);
            }
            sb.append("]");
        }
        return sb.toString();
    }
}
