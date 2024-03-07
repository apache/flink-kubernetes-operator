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
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.utils.ResourceCheckUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Memory scaling ensures that memory is scaled alongside with the number of available TaskManagers.
 *
 * <p>When scaling down, TaskManagers are removed which can drastically limit the amount of
 * available memory. To mitigate this issue, we keep the total cluster memory constant, until we can
 * measure the actual needed memory usage.
 *
 * <p>When scaling up, i.e. adding more TaskManagers, we don't remove memory to ensure that we do
 * not run into memory-constrained scenarios. However, MemoryTuning will still be applied which can
 * result in a lower TaskManager memory baseline.
 */
public class MemoryScaling {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryScaling.class);

    /**
     * Scales the amount of memory per TaskManager proportionally to the number of TaskManagers
     * removed/added.
     */
    public static MemorySize applyMemoryScaling(
            MemorySize currentMemorySize,
            MemoryBudget memoryBudget,
            JobAutoScalerContext<?> context,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            EvaluatedMetrics evaluatedMetrics) {

        if (!context.getConfiguration().get(AutoScalerOptions.MEMORY_SCALING_ENABLED)) {
            return currentMemorySize;
        }

        double memScalingFactor =
                getMemoryScalingFactor(
                        evaluatedMetrics, scalingSummaries, context.getConfiguration());

        long additionalBytes =
                memoryBudget.budget(
                        (long) (memScalingFactor * currentMemorySize.getBytes())
                                - currentMemorySize.getBytes());

        MemorySize scaledTotalMemory =
                new MemorySize(currentMemorySize.getBytes() + additionalBytes);

        LOG.info(
                "Scaling factor: {}, Adjusting memory from {} to {}.",
                memScalingFactor,
                currentMemorySize,
                scaledTotalMemory);

        return scaledTotalMemory;
    }

    /**
     * Returns a factor for scaling the total amount of process memory when the number of
     * TaskManagers change.
     */
    private static double getMemoryScalingFactor(
            EvaluatedMetrics evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            Configuration config) {
        int numTaskSlotsUsed =
                (int)
                        evaluatedMetrics
                                .getGlobalMetrics()
                                .get(ScalingMetric.NUM_TASK_SLOTS_USED)
                                .getCurrent();
        int numTaskSlotsAfterRescale =
                ResourceCheckUtils.estimateNumTaskSlotsAfterRescale(
                        evaluatedMetrics.getVertexMetrics(), scalingSummaries, numTaskSlotsUsed);

        int numTaskSlotsPerTM = config.get(TaskManagerOptions.NUM_TASK_SLOTS);

        int numTMsBeforeRescale = (int) Math.ceil(numTaskSlotsUsed / (double) numTaskSlotsPerTM);
        int numTMsAfterRescale =
                (int) Math.ceil(numTaskSlotsAfterRescale / (double) numTaskSlotsPerTM);

        // Only add memory, don't remove any
        return Math.max(1, numTMsBeforeRescale / (double) numTMsAfterRescale);
    }
}
