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
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/** Tunes the TaskManager memory. */
public class MemoryTuningUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryTuningUtils.class);

    public static Optional<MemorySize> tuneTaskManagerHeapMemory(
            JobAutoScalerContext<?> context,
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {

        var config = context.getConfiguration();
        if (!config.get(AutoScalerOptions.MEMORY_TUNING_ENABLED)) {
            return Optional.empty();
        }

        var globalMetrics = evaluatedMetrics.getGlobalMetrics();
        double avgHeapSize = globalMetrics.get(ScalingMetric.HEAP_AVERAGE_SIZE).getAverage();

        double numTaskSlotsUsed = globalMetrics.get(ScalingMetric.NUM_TASK_SLOTS_USED).getCurrent();
        int taskSlotsPerTm = config.get(TaskManagerOptions.NUM_TASK_SLOTS);
        int currentNumTMs = (int) Math.ceil(numTaskSlotsUsed / taskSlotsPerTm);

        double usedTotalHeapSize = currentNumTMs * avgHeapSize;
        LOG.info("Total used heap size: {}", new MemorySize((long) usedTotalHeapSize));
        usedTotalHeapSize *= computeDataChangeRate(evaluatedMetrics);
        LOG.info("Resized total heap size: {}", new MemorySize((long) usedTotalHeapSize));

        int numTaskSlotsAfterRescale =
                ResourceCheckUtils.estimateNumTaskSlotsAfterRescale(
                        evaluatedMetrics, scalingSummaries, numTaskSlotsUsed);
        int newNumTms = (int) Math.ceil(numTaskSlotsAfterRescale / (double) taskSlotsPerTm);
        LOG.info(
                "Estimating {} task slots in use after rescale, spread across {} TaskManagers",
                numTaskSlotsAfterRescale,
                newNumTms);

        MemorySize newHeapSize = new MemorySize((long) (usedTotalHeapSize / newNumTms));
        // TM container memory can never grow beyond the user-specified max
        Optional<MemorySize> maxMemory = context.getTaskManagerMemoryFromSpec();
        if (maxMemory.isEmpty()) {
            return Optional.empty();
        }
        // Apply limits
        newHeapSize =
                new MemorySize(
                        Math.min(
                                maxMemory.get().getBytes(),
                                Math.max(
                                        config.get(AutoScalerOptions.MEMORY_TUNING_MIN_HEAP)
                                                .getBytes(),
                                        newHeapSize.getBytes())));
        LOG.info("Calculated new TaskManager heap memory {}", newHeapSize.toHumanReadableString());

        return Optional.of(newHeapSize);
    }

    /**
     * Calculate the data change rate across the entire DAG. To add headroom, we use the
     * EXPECTED_PROCESSING_RATE which is the max processing capacity after scaleup. We use the
     * current known processing rate as the normalization factor.
     */
    private static double computeDataChangeRate(EvaluatedMetrics evaluatedMetrics) {
        double totalCurrentProcessingRate = 0;
        double targetedTotalProcessingRate = 0;
        for (Map<ScalingMetric, EvaluatedScalingMetric> entry :
                evaluatedMetrics.getVertexMetrics().values()) {
            totalCurrentProcessingRate +=
                    entry.get(ScalingMetric.CURRENT_PROCESSING_RATE).getAverage();
            targetedTotalProcessingRate +=
                    entry.get(ScalingMetric.EXPECTED_PROCESSING_RATE).getCurrent();
        }
        return targetedTotalProcessingRate / totalCurrentProcessingRate;
    }

    public static MemorySize adjustTotalTmMemory(
            JobAutoScalerContext<?> ctx,
            MemorySize newHeapSize,
            EvaluatedMetrics evaluatedMetrics) {
        var totalTaskManagerMemory = ctx.getTaskManagerMemory().orElse(MemorySize.ZERO);
        if (totalTaskManagerMemory.compareTo(MemorySize.ZERO) <= 0) {
            return MemorySize.ZERO;
        }
        if (newHeapSize.compareTo(MemorySize.ZERO) <= 0) {
            return totalTaskManagerMemory;
        }

        var currentMax =
                evaluatedMetrics.getGlobalMetrics().get(ScalingMetric.HEAP_MAX_SIZE).getCurrent();
        if (!Double.isFinite(currentMax)) {
            return totalTaskManagerMemory;
        }

        long newTotalMem =
                totalTaskManagerMemory.getBytes() - (long) currentMax + newHeapSize.getBytes();
        return new MemorySize(newTotalMem);
    }
}
