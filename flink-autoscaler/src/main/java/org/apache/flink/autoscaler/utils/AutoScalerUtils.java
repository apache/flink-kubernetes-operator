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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.metrics.ScalingMetric.CATCH_UP_DATA_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TARGET_DATA_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;

/** AutoScaler utilities. */
public class AutoScalerUtils {

    public static double getTargetProcessingCapacity(
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Configuration conf,
            double targetUtilization,
            boolean withRestart,
            Duration restartTime) {

        // Target = Lag Catchup Rate + Restart Catchup Rate + Processing at utilization
        // Target = LAG/CATCH_UP + INPUT_RATE*RESTART/CATCH_UP + INPUT_RATE/TARGET_UTIL

        double lagCatchupTargetRate = evaluatedMetrics.get(CATCH_UP_DATA_RATE).getCurrent();
        if (Double.isNaN(lagCatchupTargetRate)) {
            return Double.NaN;
        }

        double catchUpTargetSec = conf.get(AutoScalerOptions.CATCH_UP_DURATION).toSeconds();

        targetUtilization = Math.max(0., targetUtilization);
        targetUtilization = Math.min(1., targetUtilization);

        double avgInputTargetRate = evaluatedMetrics.get(TARGET_DATA_RATE).getAverage();
        if (Double.isNaN(avgInputTargetRate)) {
            return Double.NaN;
        }

        if (targetUtilization == 0) {
            return Double.POSITIVE_INFINITY;
        }

        double restartCatchupRate =
                !withRestart || catchUpTargetSec == 0
                        ? 0
                        : (avgInputTargetRate * restartTime.toSeconds()) / catchUpTargetSec;
        double inputTargetAtUtilization = avgInputTargetRate / targetUtilization;

        return Math.round(lagCatchupTargetRate + restartCatchupRate + inputTargetAtUtilization);
    }

    /**
     * Temporarily exclude vertex from scaling for this run. This does not update the
     * scalingRealizer.
     */
    public static boolean excludeVertexFromScaling(Configuration conf, JobVertexID jobVertexId) {
        return excludeVerticesFromScaling(conf, List.of(jobVertexId));
    }

    public static boolean excludeVerticesFromScaling(
            Configuration conf, Collection<JobVertexID> ids) {
        Set<String> excludedIds = new HashSet<>(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS));
        boolean anyAdded = false;
        for (JobVertexID id : ids) {
            String hexString = id.toHexString();
            anyAdded |= excludedIds.add(hexString);
        }
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, new ArrayList<>(excludedIds));
        return anyAdded;
    }

    /**
     * Computes the optimized linear scaling coefficient (α) by minimizing the least squares error.
     *
     * <p>This method estimates the scaling coefficient in a linear scaling model by fitting
     * observed processing rates and parallelism levels.
     *
     * <p>The computed coefficient is clamped within the specified lower and upper bounds to ensure
     * stability and prevent extreme scaling adjustments.
     *
     * @param parallelismLevels List of parallelism levels.
     * @param processingRates List of observed processing rates.
     * @param baselineProcessingRate Baseline processing rate.
     * @param upperBound Maximum allowable value for the scaling coefficient.
     * @param lowerBound Minimum allowable value for the scaling coefficient.
     * @return The optimized scaling coefficient (α), constrained within {@code [lowerBound,
     *     upperBound]}.
     */
    public static double optimizeLinearScalingCoefficient(
            List<Double> parallelismLevels,
            List<Double> processingRates,
            double baselineProcessingRate,
            double upperBound,
            double lowerBound) {

        double sum = 0.0;
        double squaredSum = 0.0;

        for (int i = 0; i < parallelismLevels.size(); i++) {
            double parallelism = parallelismLevels.get(i);
            double processingRate = processingRates.get(i);

            sum += parallelism * processingRate;
            squaredSum += parallelism * parallelism;
        }

        if (squaredSum == 0.0) {
            return 1.0; // Fallback to linear scaling if denominator is zero
        }

        double alpha = sum / (squaredSum * baselineProcessingRate);

        return Math.max(lowerBound, Math.min(upperBound, alpha));
    }

    /**
     * Computes the baseline processing rate from historical scaling data.
     *
     * <p>The baseline processing rate represents the **processing rate per unit of parallelism**.
     * It is determined using the smallest observed parallelism in the history.
     *
     * @param history A {@code SortedMap} where keys are timestamps ({@code Instant}), and values
     *     are {@code ScalingSummary} objects.
     * @return The computed baseline processing rate (processing rate per unit of parallelism).
     */
    public static double computeBaselineProcessingRate(SortedMap<Instant, ScalingSummary> history) {
        ScalingSummary latestSmallestParallelismSummary = null;

        for (Map.Entry<Instant, ScalingSummary> entry :
                ((NavigableMap<Instant, ScalingSummary>) history).descendingMap().entrySet()) {
            ScalingSummary summary = entry.getValue();
            double parallelism = summary.getCurrentParallelism();

            if (parallelism == 1) {
                return summary.getMetrics().get(TRUE_PROCESSING_RATE).getAverage();
            }

            if (latestSmallestParallelismSummary == null
                    || parallelism < latestSmallestParallelismSummary.getCurrentParallelism()) {
                latestSmallestParallelismSummary = entry.getValue();
            }
        }

        if (latestSmallestParallelismSummary == null) {
            return Double.NaN;
        }

        double parallelism = latestSmallestParallelismSummary.getCurrentParallelism();
        double processingRate =
                latestSmallestParallelismSummary
                        .getMetrics()
                        .get(TRUE_PROCESSING_RATE)
                        .getAverage();
        return processingRate / parallelism;
    }
}
