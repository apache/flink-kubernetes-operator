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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.MAX_SCALE_DOWN_FACTOR;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.SCALE_UP_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.VERTEX_MAX_PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.VERTEX_MIN_PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.MAX_PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;

/** Component responsible for computing vertex parallelism based on the scaling metrics. */
public class JobVertexScaler {

    private static final Logger LOG = LoggerFactory.getLogger(JobVertexScaler.class);

    private Clock clock = Clock.system(ZoneId.systemDefault());

    public int computeScaleTargetParallelism(
            Configuration conf,
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            SortedMap<Instant, ScalingSummary> history) {

        var currentParallelism = (int) evaluatedMetrics.get(PARALLELISM).getCurrent();
        double averageTrueProcessingRate = evaluatedMetrics.get(TRUE_PROCESSING_RATE).getAverage();

        if (Double.isNaN(averageTrueProcessingRate)) {
            LOG.info(
                    "True processing rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return currentParallelism;
        }

        double targetCapacity =
                AutoScalerUtils.getTargetProcessingCapacity(
                        evaluatedMetrics, conf, conf.get(TARGET_UTILIZATION), true);
        if (Double.isNaN(targetCapacity)) {
            LOG.info(
                    "Target data rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return currentParallelism;
        }

        LOG.info("Target processing capacity for {} is {}", vertex, targetCapacity);
        double scaleFactor = targetCapacity / averageTrueProcessingRate;
        double minScaleFactor = 1 - conf.get(MAX_SCALE_DOWN_FACTOR);
        if (scaleFactor < minScaleFactor) {
            LOG.info(
                    "Computed scale factor of {} for {} is capped by maximum scale down factor to {}",
                    scaleFactor,
                    vertex,
                    minScaleFactor);
            scaleFactor = minScaleFactor;
        }

        int newParallelism =
                scale(
                        currentParallelism,
                        (int) evaluatedMetrics.get(MAX_PARALLELISM).getCurrent(),
                        scaleFactor,
                        conf.getInteger(VERTEX_MIN_PARALLELISM),
                        conf.getInteger(VERTEX_MAX_PARALLELISM));

        if (!history.isEmpty()) {
            if (detectImmediateScaleDownAfterScaleUp(
                    conf, history, currentParallelism, newParallelism)) {
                LOG.info(
                        "Skipping immediate scale down after scale up for {} resetting target parallelism to {}",
                        vertex,
                        currentParallelism);
                newParallelism = currentParallelism;
            }

            // currentParallelism = 2 , newParallelism = 1, minimumProcRate = 1000 r/s
            // history
            // currentParallelism 1 => 3 -> empiricalProcRate = 800
            // empiricalProcRate + upperBoundary < minimumProcRate => don't scale
        }

        return newParallelism;
    }

    private boolean detectImmediateScaleDownAfterScaleUp(
            Configuration conf,
            SortedMap<Instant, ScalingSummary> history,
            int currentParallelism,
            int newParallelism) {
        var lastScalingTs = history.lastKey();
        var lastSummary = history.get(lastScalingTs);

        boolean isScaleDown = newParallelism < currentParallelism;
        boolean lastScaleUp = lastSummary.getNewParallelism() > lastSummary.getCurrentParallelism();

        var gracePeriod = conf.get(SCALE_UP_GRACE_PERIOD);

        boolean withinConfiguredTime =
                Duration.between(lastScalingTs, clock.instant()).minus(gracePeriod).isNegative();

        return isScaleDown && lastScaleUp && withinConfiguredTime;
    }

    @VisibleForTesting
    protected static int scale(
            int parallelism,
            int numKeyGroups,
            double scaleFactor,
            int minParallelism,
            int maxParallelism) {
        Preconditions.checkArgument(
                minParallelism <= maxParallelism,
                "The minimum parallelism must not be greater than the maximum parallelism.");
        if (minParallelism > numKeyGroups) {
            LOG.warn(
                    "Specified autoscaler minimum parallelism {} is greater than the operator max parallelism {}. The min parallelism will be set to the operator max parallelism.",
                    minParallelism,
                    numKeyGroups);
        }
        if (numKeyGroups < maxParallelism && maxParallelism != Integer.MAX_VALUE) {
            LOG.warn(
                    "Specified autoscaler maximum parallelism {} is greater than the operator max parallelism {}. This means the operator max parallelism can never be reached.",
                    maxParallelism,
                    numKeyGroups);
        }

        int newParallelism =
                // Prevent integer overflow when converting from double to integer.
                // We do not have to detect underflow because doubles cannot
                // underflow.
                (int) Math.min(Math.ceil(scaleFactor * parallelism), Integer.MAX_VALUE);

        // Cap parallelism at either number of key groups or parallelism limit
        final int upperBound = Math.min(numKeyGroups, maxParallelism);

        // Apply min/max parallelism
        newParallelism = Math.min(Math.max(minParallelism, newParallelism), upperBound);

        // Try to adjust the parallelism such that it divides the number of key groups without a
        // remainder => state is evenly spread across subtasks
        for (int p = newParallelism; p <= numKeyGroups / 2 && p <= upperBound; p++) {
            if (numKeyGroups % p == 0) {
                return p;
            }
        }

        // If key group adjustment fails, use originally computed parallelism
        return newParallelism;
    }

    @VisibleForTesting
    protected void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
    }
}
