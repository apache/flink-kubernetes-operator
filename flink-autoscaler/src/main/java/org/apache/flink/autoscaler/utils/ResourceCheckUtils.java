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

import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.Map;

/** Utils methods for resource checks. */
public class ResourceCheckUtils {

    public static int estimateNumTaskSlotsAfterRescale(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            int numTaskSlotsUsed) {

        Map<JobVertexID, Integer> newParallelisms =
                computeNewParallelisms(scalingSummaries, vertexMetrics);

        if (currentMaxParallelism(vertexMetrics) == numTaskSlotsUsed) {
            // Slot sharing is activated
            return newParallelisms.values().stream().reduce(0, Integer::max);
        } else {
            // Slot sharing is (partially) deactivated,
            // assuming no slot sharing in absence of additional metrics.
            return newParallelisms.values().stream().reduce(0, Integer::sum);
        }
    }

    public static Map<JobVertexID, Integer> computeNewParallelisms(
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics) {

        Map<JobVertexID, Integer> newParallelisms = new HashMap<>();

        for (Map.Entry<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> entry :
                vertexMetrics.entrySet()) {
            JobVertexID jobVertexID = entry.getKey();
            ScalingSummary scalingSummary = scalingSummaries.get(jobVertexID);
            if (scalingSummary != null) {
                newParallelisms.put(jobVertexID, scalingSummary.getNewParallelism());
            } else {
                newParallelisms.put(
                        jobVertexID,
                        (int) entry.getValue().get(ScalingMetric.PARALLELISM).getCurrent());
            }
        }

        return newParallelisms;
    }

    private static int currentMaxParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> vertexMetrics) {

        return vertexMetrics.values().stream()
                .map(map -> (int) map.get(ScalingMetric.PARALLELISM).getCurrent())
                .reduce(0, Integer::max);
    }
}
