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

package org.apache.flink.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.utils.ResourceCheckUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/** Applies the optional per-slot-sharing-group scale-factor policy. */
final class SlotSharingGroupScalingPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(SlotSharingGroupScalingPolicy.class);

    private SlotSharingGroupScalingPolicy() {}

    @VisibleForTesting
    static Map<JobVertexID, Double> computeMaxScaleFactorsPerSlotSharingGroup(
            Map<JobVertexID, Double> individualScaleFactors,
            JobTopology jobTopology,
            List<String> excludedVertexIds) {
        if (jobTopology == null) {
            return Map.of();
        }

        var eligibleVertices =
                individualScaleFactors.keySet().stream()
                        .filter(jobTopology.getVertexInfos()::containsKey)
                        .filter(v -> !jobTopology.getFinishedVertices().contains(v))
                        .filter(v -> !excludedVertexIds.contains(v.toHexString()))
                        .filter(v -> !Double.isNaN(individualScaleFactors.get(v)))
                        .toList();
        if (eligibleVertices.isEmpty()) {
            return Map.of();
        }

        var groupScaleFactors = new HashMap<JobVertexID, Double>();
        for (var slotSharingGroup : jobTopology.getSlotSharingGroupMapping().entrySet()) {
            var groupVertices =
                    eligibleVertices.stream()
                            .filter(slotSharingGroup.getValue()::contains)
                            .filter(individualScaleFactors::containsKey)
                            .toList();
            if (groupVertices.isEmpty()) {
                continue;
            }

            double groupScaleFactor =
                    groupVertices.stream()
                            .mapToDouble(individualScaleFactors::get)
                            .max()
                            .orElseThrow();
            LOG.debug(
                    "Using scale factor {} for eligible vertices {} in slot-sharing group {}",
                    groupScaleFactor,
                    groupVertices,
                    slotSharingGroup.getKey());
            groupVertices.forEach(v -> groupScaleFactors.put(v, groupScaleFactor));
        }
        return groupScaleFactors;
    }

    @VisibleForTesting
    static boolean scalingChangesTaskManagerCount(
            Configuration conf,
            EvaluatedMetrics evaluatedMetrics,
            JobTopology jobTopology,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {
        if (jobTopology == null || jobTopology.getSlotSharingGroupMapping().isEmpty()) {
            LOG.info(
                    "Skipping per-slot-sharing-group scaling because slot sharing group metrics are not ready");
            return false;
        }

        var globalMetrics = evaluatedMetrics.getGlobalMetrics();
        if (!globalMetrics.containsKey(ScalingMetric.NUM_TASK_SLOTS_USED)) {
            LOG.info(
                    "Skipping per-slot-sharing-group scaling because TaskManager metrics are not ready");
            return false;
        }

        int currentTaskSlots =
                (int) globalMetrics.get(ScalingMetric.NUM_TASK_SLOTS_USED).getCurrent();
        int currentJobTaskSlots =
                estimateTaskSlotsAfterRescale(
                        evaluatedMetrics.getVertexMetrics(), Map.of(), jobTopology);
        int taskSlotsAfterRescale =
                estimateTaskSlotsAfterRescale(
                        evaluatedMetrics.getVertexMetrics(), scalingSummaries, jobTopology);
        int clusterTaskSlotsAfterRescale =
                currentTaskSlots + taskSlotsAfterRescale - currentJobTaskSlots;
        if (clusterTaskSlotsAfterRescale < 0) {
            LOG.info(
                    "Skipping per-slot-sharing-group scaling because the estimated cluster slot count is invalid");
            return false;
        }
        int slotsPerTaskManager = conf.get(TaskManagerOptions.NUM_TASK_SLOTS);
        int currentTaskManagers = (int) Math.ceil(currentTaskSlots / (double) slotsPerTaskManager);
        int taskManagersAfterRescale =
                (int) Math.ceil(clusterTaskSlotsAfterRescale / (double) slotsPerTaskManager);

        if (currentTaskManagers == taskManagersAfterRescale) {
            LOG.info(
                    "Skipping per-slot-sharing-group scaling because the estimated TaskManager count remains {}",
                    currentTaskManagers);
            return false;
        }
        return true;
    }

    private static int estimateTaskSlotsAfterRescale(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            JobTopology jobTopology) {
        var newParallelisms =
                ResourceCheckUtils.computeNewParallelisms(scalingSummaries, vertexMetrics);
        var groupedVertices = new HashSet<JobVertexID>();
        int taskSlots = 0;

        for (var slotSharingGroup : jobTopology.getSlotSharingGroupMapping().values()) {
            int groupParallelism =
                    slotSharingGroup.stream()
                            .filter(newParallelisms::containsKey)
                            .mapToInt(newParallelisms::get)
                            .max()
                            .orElse(0);
            taskSlots += groupParallelism;
            groupedVertices.addAll(slotSharingGroup);
        }

        // Vertices without slot sharing metadata are treated as separate groups.
        taskSlots +=
                newParallelisms.entrySet().stream()
                        .filter(e -> !groupedVertices.contains(e.getKey()))
                        .mapToInt(Map.Entry::getValue)
                        .sum();
        return taskSlots;
    }
}
