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

package org.apache.flink.autoscaler.utils;

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemory;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/** Tunes the TaskManager memory. */
public class MemoryTuningUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryTuningUtils.class);
    public static final ProcessMemoryUtils<TaskExecutorFlinkMemory> FLINK_MEMORY_UTILS =
            new ProcessMemoryUtils<>(getMemoryOptions(), new TaskExecutorFlinkMemoryUtils());

    /**
     * Emits a Configuration which contains overrides for the current configuration. We are not
     * modifying the config directly, but we are emitting a new configuration which contains any
     * overrides. This config is persisted separately and applied by the autoscaler. That way we can
     * clear any applied overrides if auto-tuning is disabled.
     */
    public static Configuration tuneTaskManagerHeapMemory(
            JobAutoScalerContext<?> context, EvaluatedMetrics evaluatedMetrics) {

        if (!context.getConfiguration().get(AutoScalerOptions.MEMORY_TUNING_ENABLED)) {
            return new Configuration();
        }
        // Please note that this config is the original configuration created from the user spec.
        // It does not contain any already applied overrides.
        var config = new UnmodifiableConfiguration(context.getConfiguration());

        var globalMetrics = evaluatedMetrics.getGlobalMetrics();
        MemorySize avgHeapSize =
                new MemorySize(
                        (long) globalMetrics.get(ScalingMetric.HEAP_AVERAGE_SIZE).getAverage());
        LOG.info("Average TM used heap size: {}", avgHeapSize);

        // Gather original memory configuration from the user spec
        var memSpecs = FLINK_MEMORY_UTILS.memoryProcessSpecFromConfig(config);
        var maxHeapSize = memSpecs.getFlinkMemory().getJvmHeapMemorySize();
        LOG.info("Current configured heap size: {}", maxHeapSize);

        // Apply min/max heap size limits
        MemorySize newHeapSize =
                new MemorySize(
                        Math.min(
                                // Upper limit is the original max heap size in the spec
                                maxHeapSize.getBytes(),
                                Math.max(
                                        // Lower limit is the minimum configured heap size
                                        config.get(AutoScalerOptions.MEMORY_TUNING_MIN_HEAP)
                                                .getBytes(),
                                        avgHeapSize.getBytes())));
        LOG.info("New TM heap memory {}", newHeapSize.toHumanReadableString());

        // Diff can be negative (memory shrinks) or positive (memory grows)
        final long heapDiffBytes = newHeapSize.getBytes() - maxHeapSize.getBytes();

        final MemorySize totalMemory = adjustTotalTmMemory(context, heapDiffBytes);
        if (totalMemory.equals(MemorySize.ZERO)) {
            return config;
        }

        // Prepare the tuning config for new configuration values
        var tuningConfig = new Configuration();
        // Update total memory according to new heap size
        // Adjust the total container memory and the JVM heap size accordingly.
        tuningConfig.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, totalMemory);
        // Framework and Task heap memory configs add up together yield the max heap memory.
        // To simplify the calculation, set the framework heap memory to zero.
        tuningConfig.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ZERO);
        tuningConfig.set(TaskManagerOptions.TASK_HEAP_MEMORY, newHeapSize);

        // All memory options which can be configured via fractions need to be set to their
        // absolute values or, if there is no absolute setting, the fractions need to be
        // re-calculated.
        MemorySize managedMemory = memSpecs.getFlinkMemory().getManaged();
        if (shouldTransferHeapToManagedMemory(config, heapDiffBytes)) {
            // If RocksDB is configured, give back the heap memory as managed memory to RocksDB
            MemorySize newManagedMemory =
                    new MemorySize(managedMemory.getBytes() + Math.abs(heapDiffBytes));
            LOG.info(
                    "Increasing managed memory size from {} to {}",
                    managedMemory,
                    newManagedMemory);
            tuningConfig.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, newManagedMemory);
        } else {
            tuningConfig.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, managedMemory);
        }

        tuningConfig.set(
                TaskManagerOptions.NETWORK_MEMORY_FRACTION,
                getFraction(
                        memSpecs.getFlinkMemory().getNetwork(),
                        new MemorySize(
                                memSpecs.getTotalFlinkMemorySize().getBytes() + heapDiffBytes)));
        tuningConfig.set(
                TaskManagerOptions.JVM_OVERHEAD_FRACTION,
                getFraction(memSpecs.getJvmOverheadSize(), totalMemory));

        return tuningConfig;
    }

    private static boolean shouldTransferHeapToManagedMemory(
            Configuration config, long heapDiffBytes) {
        return config.get(AutoScalerOptions.MEMORY_TUNING_TRANSFER_HEAP_TO_MANAGED)
                && heapDiffBytes < 0
                && "rocksdb".equalsIgnoreCase(config.get(StateBackendOptions.STATE_BACKEND));
    }

    public static MemorySize getTotalMemory(Configuration config, JobAutoScalerContext<?> ctx) {
        MemorySize overrideSize = config.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
        if (overrideSize != null) {
            return overrideSize;
        }
        return ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
    }

    private static MemorySize adjustTotalTmMemory(JobAutoScalerContext<?> ctx, long heapDiffBytes) {

        var specTaskManagerMemory = ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
        if (specTaskManagerMemory.compareTo(MemorySize.ZERO) <= 0) {
            LOG.warn("Spec TaskManager memory could not be determined.");
            return MemorySize.ZERO;
        }

        if (shouldTransferHeapToManagedMemory(ctx.getConfiguration(), heapDiffBytes)) {
            // Total size does not change
            return specTaskManagerMemory;
        }

        var totalTaskManagerMemory = ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
        if (totalTaskManagerMemory.compareTo(MemorySize.ZERO) <= 0) {
            LOG.warn("Current TaskManager memory size could not be determined.");
            return MemorySize.ZERO;
        }

        long newTotalMemBytes = totalTaskManagerMemory.getBytes() + heapDiffBytes;
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
}
