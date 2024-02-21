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

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.util.config.memory.CommonProcessMemorySpec;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemory;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

import static org.apache.flink.autoscaler.metrics.ScalingMetric.HEAP_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.MANAGED_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.METASPACE_MEMORY_USED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.NETWORK_MEMORY_USED;

/** Tunes the TaskManager memory. */
public class MemoryTuning {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryTuning.class);
    public static final ProcessMemoryUtils<TaskExecutorFlinkMemory> FLINK_MEMORY_UTILS =
            new ProcessMemoryUtils<>(getMemoryOptions(), new TaskExecutorFlinkMemoryUtils());

    private static final ConfigChanges EMPTY_CONFIG = new ConfigChanges();

    /**
     * Emits a Configuration which contains overrides for the current configuration. We are not
     * modifying the config directly, but we are emitting a new configuration which contains any
     * overrides. This config is persisted separately and applied by the autoscaler. That way we can
     * clear any applied overrides if auto-tuning is disabled.
     */
    public static ConfigChanges tuneTaskManagerHeapMemory(
            JobAutoScalerContext<?> context,
            EvaluatedMetrics evaluatedMetrics,
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
        // Add these current settings from the budget
        memBudget.budget(memSpecs.getFlinkMemory().getFrameworkOffHeap().getBytes());
        memBudget.budget(memSpecs.getFlinkMemory().getTaskOffHeap().getBytes());
        memBudget.budget(memSpecs.getJvmOverheadSize().getBytes());

        var globalMetrics = evaluatedMetrics.getGlobalMetrics();
        // The order matters in case the memory usage is higher than the maximum available memory.
        // Managed memory comes last because it can grow arbitrary for RocksDB jobs.
        MemorySize newHeapSize =
                determineNewSize(getUsage(HEAP_MEMORY_USED, globalMetrics), config, memBudget);
        MemorySize newMetaspaceSize =
                determineNewSize(getUsage(METASPACE_MEMORY_USED, globalMetrics), config, memBudget);
        MemorySize newNetworkSize =
                determineNewSize(getUsage(NETWORK_MEMORY_USED, globalMetrics), config, memBudget);
        MemorySize newManagedSize =
                adjustManagedMemory(
                        getUsage(MANAGED_MEMORY_USED, globalMetrics),
                        specManagedSize,
                        config,
                        memBudget);
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

        tuningConfig.addOverride(
                TaskManagerOptions.NETWORK_MEMORY_FRACTION,
                getFraction(newNetworkSize, flinkMemorySize));

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

        // Lower limit is the minimum configured memory size
        targetSizeBytes =
                Math.max(
                        config.get(AutoScalerOptions.MEMORY_TUNING_MIN).getBytes(),
                        targetSizeBytes);

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
            return managedMemoryConfigured;
        }
    }

    private static MemorySize getUsage(
            ScalingMetric scalingMetric, Map<ScalingMetric, EvaluatedScalingMetric> globalMetrics) {
        MemorySize heapUsed = new MemorySize((long) globalMetrics.get(scalingMetric).getAverage());
        LOG.debug("{}: {}", scalingMetric, heapUsed);
        return heapUsed;
    }

    public static MemorySize getTotalMemory(Configuration config, JobAutoScalerContext<?> ctx) {
        MemorySize overrideSize = config.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
        if (overrideSize != null) {
            return overrideSize;
        }
        return ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
    }

    private static MemorySize adjustTotalTmMemory(
            JobAutoScalerContext<?> ctx, long totalDiffBytes) {
        var specTaskManagerMemory = ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
        if (specTaskManagerMemory.compareTo(MemorySize.ZERO) <= 0) {
            LOG.warn("Spec TaskManager memory size could not be determined.");
            return MemorySize.ZERO;
        }

        long newTotalMemBytes = specTaskManagerMemory.getBytes() + totalDiffBytes;
        // TM container memory can never grow beyond the user-specified max memory
        newTotalMemBytes = Math.min(newTotalMemBytes, specTaskManagerMemory.getBytes());

        MemorySize totalMemory = new MemorySize(newTotalMemBytes);
        LOG.info("Setting new total TaskManager memory to {}", totalMemory);
        return totalMemory;
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
        // Round to three decimal places
        return (float)
                (Math.round(enumerator.getBytes() / (double) denominator.getBytes() * 1000)
                        / 1000.);
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
