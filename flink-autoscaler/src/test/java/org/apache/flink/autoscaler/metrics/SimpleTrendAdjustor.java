/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler.metrics;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * A simple implementation of the {@link CustomEvaluator} interface that adjusts scaling metrics
 * based on recent historical trends. This evaluator applies a weighted moving average to refine the
 * target data rate for source job vertices, enabling more responsive scaling decisions.
 */
public class SimpleTrendAdjustor implements CustomEvaluator {
    @Override
    public Map<ScalingMetric, EvaluatedScalingMetric> evaluateVertexMetrics(
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Context evaluationContext) {

        if (!evaluationContext.getTopology().isSource(vertex)) {
            return Collections.emptyMap();
        }

        var customEvaluatedMetrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();

        // Extract current target data rate
        EvaluatedScalingMetric targetDataRateMetric =
                evaluatedMetrics.get(ScalingMetric.TARGET_DATA_RATE);
        double currentTargetRate =
                (targetDataRateMetric != null) ? targetDataRateMetric.getAverage() : 0.0;

        // Compute historical trend adjustment
        double trendAdjustment =
                computeTrendAdjustment(vertex, evaluationContext.getMetricsHistory());

        // Apply a dynamic adjustment based on recent trends
        double adjustedTargetRate = currentTargetRate + trendAdjustment;

        // Store the updated metric
        customEvaluatedMetrics.put(
                ScalingMetric.TARGET_DATA_RATE, EvaluatedScalingMetric.avg(adjustedTargetRate));

        return customEvaluatedMetrics;
    }

    /**
     * Computes a trend-based adjustment using recent historical metrics. Uses a simple weighted
     * moving average over the last few recorded metrics.
     */
    private double computeTrendAdjustment(
            JobVertexID vertex, SortedMap<Instant, CollectedMetrics> metricsHistory) {
        if (metricsHistory.isEmpty()) {
            // Fallback: apply no increase if no history is available
            return 0.;
        }

        double totalWeight = 0.0;
        double weightedSum = 0.0;
        // Increasing weight for more recent data points
        int weight = 1;

        // Iterate over the last N entries (e.g., last 5 data points)
        int count = 0;
        for (var entry : metricsHistory.values()) {
            Double historicalRate =
                    entry.getVertexMetrics().get(vertex).get(ScalingMetric.TARGET_DATA_RATE);
            if (historicalRate != null) {
                weightedSum += historicalRate * weight;
                totalWeight += weight;
                weight++;
                count++;
            }
            if (count >= 5) { // Limit to last 5 points
                break;
            }
        }

        return (totalWeight > 0)
                ? (weightedSum / totalWeight)
                        - metricsHistory
                                .get(metricsHistory.lastKey())
                                .getVertexMetrics()
                                .get(vertex)
                                .get(ScalingMetric.TARGET_DATA_RATE)
                : 0.;
    }
}
