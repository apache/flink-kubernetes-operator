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

import static org.apache.commons.lang3.math.Fraction.getFraction;

/** Tunes the TaskManager memory. */
public class MemoryTuningUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryTuningUtils.class);
    public static final ProcessMemoryUtils<TaskExecutorFlinkMemory> FLINK_MEMORY_UTILS =
            new ProcessMemoryUtils<>(getMemoryOptions(), new TaskExecutorFlinkMemoryUtils());
    static final String TOTAL_MEMORY_CONFIG_NAME =
            AutoScalerOptions.AUTOSCALER_CONF_PREFIX
                    + "memory.tuning.internal.desired-container-memory.bytes";

    public static Configuration tuneTaskManagerHeapMemory(
            JobAutoScalerContext<?> context, EvaluatedMetrics evaluatedMetrics) {

        var config = new UnmodifiableConfiguration(context.getConfiguration());
        if (!context.getConfiguration().get(AutoScalerOptions.MEMORY_TUNING_ENABLED)) {
            return config;
        }

        var globalMetrics = evaluatedMetrics.getGlobalMetrics();
        MemorySize avgHeapSize =
                new MemorySize(
                        (long) globalMetrics.get(ScalingMetric.HEAP_AVERAGE_SIZE).getAverage());
        LOG.info("Average TM heap size: {}", avgHeapSize);

        MemorySize newHeapSize = avgHeapSize;
        LOG.info("Adjusted TM heap size: {}", newHeapSize);

        // Apply limits
        newHeapSize =
                new MemorySize(
                        Math.max(
                                config.get(AutoScalerOptions.MEMORY_TUNING_MIN_HEAP).getBytes(),
                                newHeapSize.getBytes()));
        LOG.info("New TM heap memory {}", newHeapSize.toHumanReadableString());

        // Gather current memory configuration specs
        var memSpecs = FLINK_MEMORY_UTILS.memoryProcessSpecFromConfig(config);
        // Set the new heap memory size
        var tuningConfig = new Configuration();
        tuningConfig.set(TaskManagerOptions.TASK_HEAP_MEMORY, newHeapSize);

        long heapDiffBytes =
                newHeapSize.getBytes() - memSpecs.getFlinkMemory().getTaskHeap().getBytes();

        if (heapDiffBytes < 0
                && "rocksdb".equalsIgnoreCase(config.get(StateBackendOptions.STATE_BACKEND))) {
            // If RocksDB is configured, give back the heap memory as managed memory which RocksDB
            // uses
            MemorySize managedMemory = memSpecs.getFlinkMemory().getManaged();
            MemorySize newManagedMemory = new MemorySize(managedMemory.getBytes() + heapDiffBytes);
            LOG.info(
                    "Increasing managed memory size from {} to {}",
                    managedMemory,
                    newManagedMemory);
            config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, newManagedMemory);
        } else {
            // Adjust the JVM heap size and subsequently the total container memory.
            // All memory options which can be configured via fractions need to be set to their
            // absolute values or, if there is no absolute setting, the fractions need to be
            // re-calculated.
            tuningConfig.set(
                    TaskManagerOptions.MANAGED_MEMORY_SIZE, memSpecs.getFlinkMemory().getManaged());
            tuningConfig.set(
                    TaskManagerOptions.NETWORK_MEMORY_FRACTION,
                    getFraction(
                            memSpecs.getFlinkMemory().getNetwork(),
                            memSpecs.getTotalProcessMemorySize()));
            tuningConfig.set(
                    TaskManagerOptions.JVM_OVERHEAD_FRACTION,
                    getFraction(
                            memSpecs.getJvmHeapMemorySize(), memSpecs.getTotalProcessMemorySize()));

            var totalMemory = adjustTotalTmMemory(context, heapDiffBytes);
            if (totalMemory.equals(MemorySize.ZERO)) {
                return config;
            }

            tuningConfig.setLong(TOTAL_MEMORY_CONFIG_NAME, totalMemory.getBytes());
        }

        return tuningConfig;
    }

    public static MemorySize getTotalMemory(Configuration config, JobAutoScalerContext<?> ctx) {
        MemorySize overrideSize = new MemorySize(config.getLong(TOTAL_MEMORY_CONFIG_NAME, 0));
        if (overrideSize.compareTo(MemorySize.ZERO) > 0) {
            return overrideSize;
        }
        return ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
    }

    private static MemorySize adjustTotalTmMemory(JobAutoScalerContext<?> ctx, long diffBytes) {

        var totalTaskManagerMemory = ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
        if (totalTaskManagerMemory.compareTo(MemorySize.ZERO) <= 0) {
            LOG.warn("Total TaskManager memory size could not be determined.");
            return MemorySize.ZERO;
        }
        var specTaskManagerMemory = ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
        if (specTaskManagerMemory.compareTo(MemorySize.ZERO) <= 0) {
            LOG.warn("Spec TaskManager memory could not be determined.");
            return MemorySize.ZERO;
        }

        long newTotalMemBytes = specTaskManagerMemory.getBytes() + diffBytes;
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
