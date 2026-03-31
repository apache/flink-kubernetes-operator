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
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.JobVertexScaler.KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.MAX_SCALE_DOWN_FACTOR;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.MAX_SCALE_UP_FACTOR;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.OBSERVED_SCALABILITY_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.OBSERVED_SCALABILITY_MIN_OBSERVATIONS;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALE_DOWN_INTERVAL;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_EVENT_INTERVAL;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.UTILIZATION_TARGET;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.VERTEX_MAX_PARALLELISM;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.VERTEX_MIN_PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.EXPECTED_PROCESSING_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.MAX_PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.NUM_SOURCE_PARTITIONS;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.SCALE_DOWN_RATE_THRESHOLD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.SCALE_UP_RATE_THRESHOLD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;
import static org.apache.flink.autoscaler.topology.ShipStrategy.HASH;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Component responsible for computing vertex parallelism based on the scaling metrics. */
public class JobVertexScaler<KEY, Context extends JobAutoScalerContext<KEY>> {

    private static final Logger LOG = LoggerFactory.getLogger(JobVertexScaler.class);

    @VisibleForTesting protected static final String INEFFECTIVE_SCALING = "IneffectiveScaling";

    @VisibleForTesting
    protected static final String INEFFECTIVE_MESSAGE_FORMAT =
            "Ineffective scaling detected for %s (expected increase: %s, actual increase %s). Blocking of ineffective scaling decisions is %s";

    @VisibleForTesting protected static final String SCALING_LIMITED = "ScalingLimited";

    @VisibleForTesting
    protected static final String SCALE_LIMITED_MESSAGE_FORMAT =
            "Scaling limited detected for %s (expected parallelism: %s, actual parallelism %s). "
                    + "Scaling limited due to numKeyGroupsOrPartitions : %s，"
                    + "upperBoundForAlignment(maxParallelism or parallelismUpperLimit): %s, parallelismLowerLimit: %s.";

    private Clock clock = Clock.system(ZoneId.systemDefault());

    private final AutoScalerEventHandler<KEY, Context> autoScalerEventHandler;

    public JobVertexScaler(AutoScalerEventHandler<KEY, Context> autoScalerEventHandler) {
        this.autoScalerEventHandler = autoScalerEventHandler;
    }

    /** The rescaling will be triggered if any vertex's {@link ParallelismChange} is changed. */
    @Getter
    public static class ParallelismChange {

        private static final ParallelismChange NO_CHANGE = new ParallelismChange(-1, false);

        private final int newParallelism;

        private final boolean outsideUtilizationBound;

        private ParallelismChange(int newParallelism, boolean outsideUtilizationBound) {
            this.newParallelism = newParallelism;
            this.outsideUtilizationBound = outsideUtilizationBound;
        }

        public boolean isNoChange() {
            return this == NO_CHANGE;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ParallelismChange that = (ParallelismChange) o;
            return newParallelism == that.newParallelism
                    && outsideUtilizationBound == that.outsideUtilizationBound;
        }

        @Override
        public int hashCode() {
            return Objects.hash(newParallelism, outsideUtilizationBound);
        }

        @Override
        public String toString() {
            return isNoChange()
                    ? "NoParallelismChange"
                    : "ParallelismChange{newParallelism="
                            + newParallelism
                            + ", outsideUtilizationBound="
                            + outsideUtilizationBound
                            + "}";
        }

        public static ParallelismChange build(int newParallelism, boolean outsideUtilizationBound) {
            checkArgument(newParallelism > 0, "The parallelism should be greater than 0.");
            return new ParallelismChange(newParallelism, outsideUtilizationBound);
        }

        public static ParallelismChange noChange() {
            return NO_CHANGE;
        }
    }

    public ParallelismChange computeScaleTargetParallelism(
            Context context,
            JobVertexID vertex,
            Collection<ShipStrategy> inputShipStrategies,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            SortedMap<Instant, ScalingSummary> history,
            Duration restartTime,
            DelayedScaleDown delayedScaleDown) {
        var conf = context.getConfiguration();
        var currentParallelism = (int) evaluatedMetrics.get(PARALLELISM).getCurrent();
        double averageTrueProcessingRate = evaluatedMetrics.get(TRUE_PROCESSING_RATE).getAverage();
        if (Double.isNaN(averageTrueProcessingRate)) {
            LOG.warn(
                    "True processing rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return ParallelismChange.noChange();
        }

        double targetCapacity =
                AutoScalerUtils.getTargetProcessingCapacity(
                        evaluatedMetrics, conf, conf.get(UTILIZATION_TARGET), true, restartTime);
        if (Double.isNaN(targetCapacity)) {
            LOG.warn(
                    "Target data rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return ParallelismChange.noChange();
        }

        LOG.debug("Target processing capacity for {} is {}", vertex, targetCapacity);
        double scaleFactor = targetCapacity / averageTrueProcessingRate;
        if (conf.get(OBSERVED_SCALABILITY_ENABLED)) {

            double scalingCoefficient =
                    JobVertexScaler.calculateObservedScalingCoefficient(history, conf);

            scaleFactor = scaleFactor / scalingCoefficient;
        }
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
                        vertex,
                        currentParallelism,
                        inputShipStrategies,
                        (int) evaluatedMetrics.get(NUM_SOURCE_PARTITIONS).getCurrent(),
                        (int) evaluatedMetrics.get(MAX_PARALLELISM).getCurrent(),
                        scaleFactor,
                        Math.min(currentParallelism, conf.get(VERTEX_MIN_PARALLELISM)),
                        Math.max(currentParallelism, conf.get(VERTEX_MAX_PARALLELISM)),
                        autoScalerEventHandler,
                        context);

        if (newParallelism == currentParallelism) {
            // Clear delayed scale down request if the new parallelism is equal to
            // currentParallelism.
            delayedScaleDown.clearVertex(vertex);
            return ParallelismChange.noChange();
        }

        // We record our expectations for this scaling operation
        evaluatedMetrics.put(
                ScalingMetric.EXPECTED_PROCESSING_RATE,
                EvaluatedScalingMetric.of(cappedTargetCapacity));

        return detectBlockScaling(
                context,
                vertex,
                conf,
                evaluatedMetrics,
                history,
                currentParallelism,
                newParallelism,
                delayedScaleDown);
    }

    /**
     * Calculates the scaling coefficient based on historical scaling data.
     *
     * <p>The scaling coefficient is computed using the least squares approach. If there are not
     * enough observations, or if the computed coefficient is invalid, a default value of {@code
     * 1.0} is returned, assuming linear scaling.
     *
     * @param history A {@code SortedMap} of {@code Instant} timestamps to {@code ScalingSummary}
     * @param conf Deployment configuration.
     * @return The computed scaling coefficient.
     */
    @VisibleForTesting
    protected static double calculateObservedScalingCoefficient(
            SortedMap<Instant, ScalingSummary> history, Configuration conf) {
        /*
         * The scaling coefficient is computed using the least squares approach
         * to fit a linear model:
         *
         *      R_i = β * P_i * α
         *
         * where:
         * - R_i = observed processing rate
         * - P_i = parallelism
         * - β   = baseline processing rate
         * - α   = scaling coefficient to optimize
         *
         * The optimization minimizes the **sum of squared errors**:
         *
         *      Loss = ∑ (R_i - β * α * P_i)^2
         *
         * Differentiating w.r.t. α and solving for α:
         *
         *      α = ∑ (P_i * R_i) / (∑ (P_i^2) * β)
         *
         * We keep the system conservative for higher returns scenario by clamping computed α to an upper bound of 1.0.
         */

        var minObservations = conf.get(OBSERVED_SCALABILITY_MIN_OBSERVATIONS);

        // not enough data to compute scaling coefficient; we assume linear scaling.
        if (history.isEmpty() || history.size() < minObservations) {
            return 1.0;
        }

        var baselineProcessingRate = AutoScalerUtils.computeBaselineProcessingRate(history);

        if (Double.isNaN(baselineProcessingRate)) {
            return 1.0;
        }

        List<Double> parallelismList = new ArrayList<>();
        List<Double> processingRateList = new ArrayList<>();

        for (Map.Entry<Instant, ScalingSummary> entry : history.entrySet()) {
            ScalingSummary summary = entry.getValue();
            double parallelism = summary.getCurrentParallelism();
            double processingRate = summary.getMetrics().get(TRUE_PROCESSING_RATE).getAverage();

            if (Double.isNaN(processingRate)) {
                LOG.warn(
                        "True processing rate is not available in scaling history. Cannot compute scaling coefficient.");
                return 1.0;
            }

            parallelismList.add(parallelism);
            processingRateList.add(processingRate);
        }

        double lowerBound = conf.get(AutoScalerOptions.OBSERVED_SCALABILITY_COEFFICIENT_MIN);

        var coefficient =
                AutoScalerUtils.optimizeLinearScalingCoefficient(
                        parallelismList, processingRateList, baselineProcessingRate, 1, lowerBound);

        return BigDecimal.valueOf(coefficient).setScale(2, RoundingMode.CEILING).doubleValue();
    }

    private ParallelismChange detectBlockScaling(
            Context context,
            JobVertexID vertex,
            Configuration conf,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            SortedMap<Instant, ScalingSummary> history,
            int currentParallelism,
            int newParallelism,
            DelayedScaleDown delayedScaleDown) {
        checkArgument(
                currentParallelism != newParallelism,
                "The newParallelism is equal to currentParallelism, no scaling is needed. This is probably a bug.");

        var outsideUtilizationBound = outsideUtilizationBound(vertex, evaluatedMetrics);

        var scaledUp = currentParallelism < newParallelism;

        if (scaledUp) {
            // Clear delayed scale down request if the new parallelism is greater than
            // currentParallelism.
            delayedScaleDown.clearVertex(vertex);

            // If we don't have past scaling actions for this vertex, don't block scale up.
            if (history.isEmpty()) {
                return ParallelismChange.build(newParallelism, outsideUtilizationBound);
            }

            var lastSummary = history.get(history.lastKey());
            if (currentParallelism == lastSummary.getNewParallelism()
                    && lastSummary.isScaledUp()
                    && detectIneffectiveScaleUp(
                            context, vertex, conf, evaluatedMetrics, lastSummary)) {
                // Block scale up when last rescale is ineffective.
                return ParallelismChange.noChange();
            }

            return ParallelismChange.build(newParallelism, outsideUtilizationBound);
        } else {
            return applyScaleDownInterval(
                    delayedScaleDown, vertex, conf, newParallelism, outsideUtilizationBound);
        }
    }

    private static boolean outsideUtilizationBound(
            JobVertexID vertex, Map<ScalingMetric, EvaluatedScalingMetric> metrics) {
        double trueProcessingRate = metrics.get(TRUE_PROCESSING_RATE).getAverage();
        double scaleUpRateThreshold = metrics.get(SCALE_UP_RATE_THRESHOLD).getCurrent();
        double scaleDownRateThreshold = metrics.get(SCALE_DOWN_RATE_THRESHOLD).getCurrent();

        if (trueProcessingRate < scaleUpRateThreshold
                || trueProcessingRate > scaleDownRateThreshold) {
            LOG.debug(
                    "Vertex {} processing rate {} is outside ({}, {})",
                    vertex,
                    trueProcessingRate,
                    scaleUpRateThreshold,
                    scaleDownRateThreshold);
            return true;
        } else {
            LOG.debug(
                    "Vertex {} processing rate {} is within target ({}, {})",
                    vertex,
                    trueProcessingRate,
                    scaleUpRateThreshold,
                    scaleDownRateThreshold);
        }
        return false;
    }

    private ParallelismChange applyScaleDownInterval(
            DelayedScaleDown delayedScaleDown,
            JobVertexID vertex,
            Configuration conf,
            int newParallelism,
            boolean outsideUtilizationBound) {
        var scaleDownInterval = conf.get(SCALE_DOWN_INTERVAL);
        if (scaleDownInterval.toMillis() <= 0) {
            // The scale down interval is disable, so don't block scaling.
            return ParallelismChange.build(newParallelism, outsideUtilizationBound);
        }

        var now = clock.instant();
        var windowStartTime = now.minus(scaleDownInterval);
        var delayedScaleDownInfo =
                delayedScaleDown.triggerScaleDown(
                        vertex, now, newParallelism, outsideUtilizationBound);

        // Never scale down within scale down interval
        if (windowStartTime.isBefore(delayedScaleDownInfo.getFirstTriggerTime())) {
            if (now.equals(delayedScaleDownInfo.getFirstTriggerTime())) {
                LOG.info("The scale down of {} is delayed by {}.", vertex, scaleDownInterval);
            } else {
                LOG.debug(
                        "Try to skip immediate scale down within scale-down interval for {}",
                        vertex);
            }
            return ParallelismChange.noChange();
        } else {
            // Using the maximum parallelism within the scale down interval window instead of the
            // latest parallelism when scaling down
            var maxRecommendedParallelism =
                    delayedScaleDownInfo.getMaxRecommendedParallelism(windowStartTime);
            return ParallelismChange.build(
                    maxRecommendedParallelism.getParallelism(),
                    maxRecommendedParallelism.isOutsideUtilizationBound());
        }
    }

    private boolean detectIneffectiveScaleUp(
            Context context,
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

        boolean blockIneffectiveScalings =
                conf.get(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED);

        var message =
                String.format(
                        INEFFECTIVE_MESSAGE_FORMAT,
                        vertex,
                        expectedIncrease,
                        actualIncrease,
                        blockIneffectiveScalings ? "enabled" : "disabled");

        autoScalerEventHandler.handleEvent(
                context,
                AutoScalerEventHandler.Type.Normal,
                INEFFECTIVE_SCALING,
                message,
                "ineffective" + vertex + expectedIncrease,
                conf.get(SCALING_EVENT_INTERVAL));

        return blockIneffectiveScalings;
    }

    /**
     * Computes the new parallelism for a vertex. In general, {@code newParallelism =
     * currentParallelism * scaleFactor}, clamped to {@code [parallelismLowerLimit,
     * min(parallelismUpperLimit, maxParallelism)]}.
     *
     * <p>For source vertices (with known partition counts) and keyed vertices (with HASH inputs),
     * the parallelism is further adjusted to align with the number of key groups or source
     * partitions, according to the configured {@link KeyGroupOrPartitionsAdjustMode}.
     *
     * <p>The alignment uses a two-phase algorithm:
     *
     * <ol>
     *   <li><b>Phase 1 (upward search):</b> searches from the computed {@code newParallelism}
     *       upward for divisor-aligned values. For scale-down, the search is capped at {@code
     *       currentParallelism} to prevent the alignment from crossing into the scale-up direction.
     *       If the nearest divisor is {@code currentParallelism} itself, it is returned
     *       (effectively blocking the scale-down).
     *   <li><b>Phase 2 (downward fallback):</b> if Phase 1 finds no match, searches downward from
     *       {@code newParallelism} for the boundary where per-subtask load increases. A
     *       direction-safety guard ensures the result never inverts the intended scaling direction
     *       (scale-up results stay above {@code currentParallelism}, scale-down results stay
     *       below).
     * </ol>
     */
    @VisibleForTesting
    protected static <KEY, Context extends JobAutoScalerContext<KEY>> int scale(
            JobVertexID vertex,
            int currentParallelism,
            Collection<ShipStrategy> inputShipStrategies,
            int numSourcePartitions,
            int maxParallelism,
            double scaleFactor,
            int parallelismLowerLimit,
            int parallelismUpperLimit,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            Context context) {
        checkArgument(
                parallelismLowerLimit <= parallelismUpperLimit,
                "The parallelism lower limitation must not be greater than the parallelism upper limitation.");
        if (parallelismLowerLimit > maxParallelism) {
            LOG.warn(
                    "Specified autoscaler minimum parallelism {} is greater than the operator max parallelism {}. The min parallelism will be set to the operator max parallelism.",
                    parallelismLowerLimit,
                    maxParallelism);
        }
        if (maxParallelism < parallelismUpperLimit && parallelismUpperLimit != Integer.MAX_VALUE) {
            LOG.debug(
                    "Specified autoscaler maximum parallelism {} is greater than the operator max parallelism {}. This means the operator max parallelism can never be reached.",
                    parallelismUpperLimit,
                    maxParallelism);
        }

        int newParallelism =
                // Prevent integer overflow when converting from double to integer.
                // We do not have to detect underflow because doubles cannot
                // underflow.
                (int) Math.min(Math.ceil(scaleFactor * currentParallelism), Integer.MAX_VALUE);

        // Cap parallelism at either maxParallelism(number of key groups or source partitions) or
        // parallelism upper limit
        final int upperBound = Math.min(maxParallelism, parallelismUpperLimit);

        // Apply min/max parallelism
        newParallelism = Math.min(Math.max(parallelismLowerLimit, newParallelism), upperBound);

        var adjustByMaxParallelismOrPartitions =
                numSourcePartitions > 0 || inputShipStrategies.contains(HASH);
        if (!adjustByMaxParallelismOrPartitions) {
            return newParallelism;
        }

        var numKeyGroupsOrPartitions =
                numSourcePartitions <= 0 ? maxParallelism : numSourcePartitions;
        var upperBoundForAlignment = Math.min(numKeyGroupsOrPartitions, upperBound);

        KeyGroupOrPartitionsAdjustMode mode =
                context.getConfiguration().get(SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE);

        if (newParallelism == currentParallelism) {
            return newParallelism;
        }

        boolean isScaleUp = newParallelism > currentParallelism;

        // Phase 1: Upward search from newParallelism for divisor or approximate match.
        // For scale-down, cap search at currentParallelism to prevent the alignment
        // from crossing into the scale-up direction (direction-safety fix).
        // Returning currentParallelism itself is allowed — it effectively blocks the
        // scale-down since no aligned value was found strictly below it.
        int phase1UpperBound =
                isScaleUp
                        ? upperBoundForAlignment
                        : Math.min(currentParallelism, upperBoundForAlignment);

        for (int p = newParallelism; p <= phase1UpperBound; p++) {
            if (numKeyGroupsOrPartitions % p == 0
                    ||
                    // When Mode is MAXIMIZE_UTILISATION , Try to find the smallest parallelism
                    // that can satisfy the current consumption rate.
                    (mode == MAXIMIZE_UTILISATION
                            && numKeyGroupsOrPartitions / p
                                    < numKeyGroupsOrPartitions / newParallelism)) {
                return p;
            }
        }

        // When adjusting the parallelism after rounding up cannot
        // find the parallelism to meet requirements.
        // Try to find the smallest parallelism that can satisfy the current consumption rate.
        int p = newParallelism;
        for (; p > 0; p--) {
            if (numKeyGroupsOrPartitions / p > numKeyGroupsOrPartitions / newParallelism) {
                if (numKeyGroupsOrPartitions % p != 0) {
                    p++;
                }
                break;
            }
        }
        p = Math.max(p, parallelismLowerLimit);

        // Direction-safety guard: prevent the alignment from inverting the scaling direction.
        if ((isScaleUp && p <= currentParallelism) || (!isScaleUp && p >= currentParallelism)) {
            LOG.warn(
                    "Scaling of {} blocked: alignment could not find a value that preserves the "
                            + "{} direction from current parallelism {} with mode {}. "
                            + "Keeping current parallelism.",
                    vertex,
                    isScaleUp ? "scale-up" : "scale-down",
                    currentParallelism,
                    mode);
            emitScalingLimitedEvent(
                    vertex,
                    newParallelism,
                    currentParallelism,
                    numKeyGroupsOrPartitions,
                    upperBound,
                    parallelismLowerLimit,
                    eventHandler,
                    context);
            return currentParallelism;
        }

        if (p != newParallelism) {
            emitScalingLimitedEvent(
                    vertex,
                    newParallelism,
                    p,
                    numKeyGroupsOrPartitions,
                    upperBound,
                    parallelismLowerLimit,
                    eventHandler,
                    context);
        }
        return p;
    }

    /**
     * Emits a {@link #SCALING_LIMITED} warning event when the alignment logic had to deviate from
     * the computed target parallelism, or when no aligned parallelism could be found and the
     * scaling operation was blocked.
     *
     * @param expectedParallelism the originally computed target parallelism
     * @param actualParallelism the parallelism that will actually be applied
     */
    private static <KEY, Context extends JobAutoScalerContext<KEY>> void emitScalingLimitedEvent(
            JobVertexID vertex,
            int expectedParallelism,
            int actualParallelism,
            int numKeyGroupsOrPartitions,
            int upperBound,
            int parallelismLowerLimit,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            Context context) {
        var message =
                String.format(
                        SCALE_LIMITED_MESSAGE_FORMAT,
                        vertex,
                        expectedParallelism,
                        actualParallelism,
                        numKeyGroupsOrPartitions,
                        upperBound,
                        parallelismLowerLimit);
        eventHandler.handleEvent(
                context,
                AutoScalerEventHandler.Type.Warning,
                SCALING_LIMITED,
                message,
                SCALING_LIMITED + vertex + expectedParallelism,
                context.getConfiguration().get(SCALING_EVENT_INTERVAL));
    }

    @VisibleForTesting
    protected void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
    }

    /** The mode of the key group or parallelism adjustment. */
    public enum KeyGroupOrPartitionsAdjustMode implements DescribedEnum {
        EVENLY_SPREAD(
                "This mode ensures that the parallelism adjustment attempts to evenly distribute data across subtasks"
                        + ". It is particularly effective for source vertices that are aware of partition counts or vertices after "
                        + "'keyBy' operation. The goal is to have the number of key groups or partitions be divisible by the set parallelism, ensuring even data distribution and reducing data skew."),

        MAXIMIZE_UTILISATION(
                "This model is to maximize resource utilization. In this mode, an attempt is made to set"
                        + " the parallelism that meets the current consumption rate requirements. It is not enforced "
                        + "that the number of key groups or partitions is divisible by the parallelism."),
        ;

        private final InlineElement description;

        KeyGroupOrPartitionsAdjustMode(String description) {
            this.description = text(description);
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
