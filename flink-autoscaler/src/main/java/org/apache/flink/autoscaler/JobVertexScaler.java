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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.handler.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.MAX_SCALE_DOWN_FACTOR;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.MAX_SCALE_UP_FACTOR;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALE_UP_GRACE_PERIOD;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.VERTEX_MAX_PARALLELISM;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.VERTEX_MIN_PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.EXPECTED_PROCESSING_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.MAX_PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;

/** Component responsible for computing vertex parallelism based on the scaling metrics. */
public class JobVertexScaler<KEY, INFO> {

    private static final Logger LOG = LoggerFactory.getLogger(JobVertexScaler.class);

    @VisibleForTesting
    public static final String INNEFFECTIVE_MESSAGE_FORMAT =
            "Skipping further scale up after ineffective previous scale up for %s";

    private Clock clock = Clock.system(ZoneId.systemDefault());

    private final AutoScalerEventHandler<KEY, INFO> eventHandler;

    public JobVertexScaler(AutoScalerEventHandler<KEY, INFO> eventHandler) {
        this.eventHandler = eventHandler;
    }

    public int computeScaleTargetParallelism(
            JobAutoScalerContext<KEY, INFO> context,
            Configuration conf,
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            SortedMap<Instant, ScalingSummary> history) {

        var currentParallelism = (int) evaluatedMetrics.get(PARALLELISM).getCurrent();
        double averageTrueProcessingRate = evaluatedMetrics.get(TRUE_PROCESSING_RATE).getAverage();
        if (Double.isNaN(averageTrueProcessingRate)) {
            LOG.warn(
                    "True processing rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return currentParallelism;
        }

        double targetCapacity =
                AutoScalerUtils.getTargetProcessingCapacity(
                        evaluatedMetrics, conf, conf.get(TARGET_UTILIZATION), true);
        if (Double.isNaN(targetCapacity)) {
            LOG.warn(
                    "Target data rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return currentParallelism;
        }

        LOG.debug("Target processing capacity for {} is {}", vertex, targetCapacity);
        double scaleFactor = targetCapacity / averageTrueProcessingRate;
        double minScaleFactor = 1 - conf.get(MAX_SCALE_DOWN_FACTOR);
        double maxScaleFactor = 1 + conf.get(MAX_SCALE_UP_FACTOR);
        if (scaleFactor < minScaleFactor) {
            LOG.debug(
                    "Computed scale factor of {} for {} is capped by maximum scale down factor to {}",
                    scaleFactor,
                    vertex,
                    minScaleFactor);
            scaleFactor = minScaleFactor;
        } else if (scaleFactor > maxScaleFactor) {
            LOG.debug(
                    "Computed scale factor of {} for {} is capped by maximum scale up factor to {}",
                    scaleFactor,
                    vertex,
                    maxScaleFactor);
            scaleFactor = maxScaleFactor;
        }

        // Cap target capacity according to the capped scale factor
        double cappedTargetCapacity = averageTrueProcessingRate * scaleFactor;
        LOG.debug("Capped target processing capacity for {} is {}", vertex, cappedTargetCapacity);

        int newParallelism =
                scale(
                        currentParallelism,
                        (int) evaluatedMetrics.get(MAX_PARALLELISM).getCurrent(),
                        scaleFactor,
                        Math.min(currentParallelism, conf.getInteger(VERTEX_MIN_PARALLELISM)),
                        Math.max(currentParallelism, conf.getInteger(VERTEX_MAX_PARALLELISM)));

        if (newParallelism == currentParallelism
                || blockScalingBasedOnPastActions(
                        context,
                        vertex,
                        conf,
                        evaluatedMetrics,
                        history,
                        currentParallelism,
                        newParallelism)) {
            return currentParallelism;
        }

        // We record our expectations for this scaling operation
        evaluatedMetrics.put(
                ScalingMetric.EXPECTED_PROCESSING_RATE,
                EvaluatedScalingMetric.of(cappedTargetCapacity));
        return newParallelism;
    }

    private boolean blockScalingBasedOnPastActions(
            JobAutoScalerContext<KEY, INFO> context,
            JobVertexID vertex,
            Configuration conf,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            SortedMap<Instant, ScalingSummary> history,
            int currentParallelism,
            int newParallelism) {

        // If we don't have past scaling actions for this vertex, there is nothing to do
        if (history.isEmpty()) {
            return false;
        }

        boolean scaledUp = currentParallelism < newParallelism;
        var lastScalingTs = history.lastKey();
        var lastSummary = history.get(lastScalingTs);

        if (currentParallelism == lastSummary.getNewParallelism() && lastSummary.isScaledUp()) {
            if (scaledUp) {
                return detectIneffectiveScaleUp(
                        context, vertex, conf, evaluatedMetrics, lastSummary);
            } else {
                return detectImmediateScaleDownAfterScaleUp(vertex, conf, lastScalingTs);
            }
        }
        return false;
    }

    private boolean detectImmediateScaleDownAfterScaleUp(
            JobVertexID vertex, Configuration conf, Instant lastScalingTs) {

        var gracePeriod = conf.get(SCALE_UP_GRACE_PERIOD);
        if (lastScalingTs.plus(gracePeriod).isAfter(clock.instant())) {
            LOG.info(
                    "Skipping immediate scale down after scale up within grace period for {}",
                    vertex);
            return true;
        } else {
            return false;
        }
    }

    private boolean detectIneffectiveScaleUp(
            JobAutoScalerContext<KEY, INFO> context,
            JobVertexID vertex,
            Configuration conf,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            ScalingSummary lastSummary) {

        double lastProcRate = lastSummary.getMetrics().get(TRUE_PROCESSING_RATE).getAverage();
        double lastExpectedProcRate =
                lastSummary.getMetrics().get(EXPECTED_PROCESSING_RATE).getCurrent();
        var currentProcRate = evaluatedMetrics.get(TRUE_PROCESSING_RATE).getAverage();

        // To judge the effectiveness of the scale up operation we compute how much of the expected
        // increase actually happened. For example if we expect a 100 increase in proc rate and only
        // got an increase of 10 we only accomplished 10% of the desired increase. If this number is
        // below the threshold, we mark the scaling ineffective.
        double expectedIncrease = lastExpectedProcRate - lastProcRate;
        double actualIncrease = currentProcRate - lastProcRate;

        boolean withinEffectiveThreshold =
                (actualIncrease / expectedIncrease)
                        >= conf.get(AutoScalerOptions.SCALING_EFFECTIVENESS_THRESHOLD);
        if (withinEffectiveThreshold) {
            return false;
        }

        var message = String.format(INNEFFECTIVE_MESSAGE_FORMAT, vertex);

        eventHandler.handlerScalingFailure(
                context, AutoScalerEventHandler.FailureReason.IneffectiveScaling, message);

        if (conf.get(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED)) {
            LOG.warn(
                    "Ineffective scaling detected for {}, expected increase {}, actual {}",
                    vertex,
                    expectedIncrease,
                    actualIncrease);
            return true;
        } else {
            return false;
        }
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
            LOG.debug(
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
