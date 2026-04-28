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
import static org.apache.flink.autoscaler.JobVertexScaler.KeyGroupOrPartitionsAdjustMode.allowsOutsideRange;
import static org.apache.flink.autoscaler.JobVertexScaler.KeyGroupOrPartitionsAdjustMode.searchesWithinRange;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.MAX_SCALE_DOWN_FACTOR;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.MAX_SCALE_UP_FACTOR;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.OBSERVED_SCALABILITY_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.OBSERVED_SCALABILITY_MIN_OBSERVATIONS;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALE_DOWN_INTERVAL;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_EVENT_INTERVAL;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_FALLBACK;
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

        private final int newParallelism;

        private final int currentParallelism;

        private final boolean outsideUtilizationBound;

        private ParallelismChange(
                int newParallelism, int currentParallelism, boolean outsideUtilizationBound) {
            this.newParallelism = newParallelism;
            this.currentParallelism = currentParallelism;
            this.outsideUtilizationBound = outsideUtilizationBound;
        }

        public boolean isNoChange() {
            return newParallelism == currentParallelism;
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
            if (this.isNoChange() && that.isNoChange()) {
                // When no scaling happens, outsideUtilizationBound is irrelevant.
                return currentParallelism == that.currentParallelism;
            }
            return newParallelism == that.newParallelism
                    && currentParallelism == that.currentParallelism
                    && outsideUtilizationBound == that.outsideUtilizationBound;
        }

        @Override
        public int hashCode() {
            if (isNoChange()) {
                // When no scaling happens, outsideUtilizationBound is irrelevant.
                return Objects.hash(currentParallelism);
            }
            return Objects.hash(newParallelism, currentParallelism, outsideUtilizationBound);
        }

        @Override
        public String toString() {
            return isNoChange()
                    ? "NoParallelismChange{currentParallelism=" + currentParallelism + "}"
                    : "ParallelismChange{newParallelism="
                            + newParallelism
                            + ", currentParallelism="
                            + currentParallelism
                            + ", outsideUtilizationBound="
                            + outsideUtilizationBound
                            + "}";
        }

        public static ParallelismChange build(
                int newParallelism, int currentParallelism, boolean outsideUtilizationBound) {
            checkArgument(newParallelism > 0, "The parallelism should be greater than 0.");
            checkArgument(currentParallelism > 0, "The parallelism should be greater than 0.");
            return new ParallelismChange(
                    newParallelism, currentParallelism, outsideUtilizationBound);
        }

        public static ParallelismChange noChange(int currentParallelism) {
            checkArgument(currentParallelism > 0, "The parallelism should be greater than 0.");
            return new ParallelismChange(currentParallelism, currentParallelism, false);
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
            return ParallelismChange.noChange(currentParallelism);
        }

        double targetCapacity =
                AutoScalerUtils.getTargetProcessingCapacity(
                        evaluatedMetrics, conf, conf.get(UTILIZATION_TARGET), true, restartTime);
        if (Double.isNaN(targetCapacity)) {
            LOG.warn(
                    "Target data rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return ParallelismChange.noChange(currentParallelism);
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
            return ParallelismChange.noChange(currentParallelism);
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
                return ParallelismChange.build(
                        newParallelism, currentParallelism, outsideUtilizationBound);
            }

            var lastSummary = history.get(history.lastKey());
            if (currentParallelism == lastSummary.getNewParallelism()
                    && lastSummary.isScaledUp()
                    && detectIneffectiveScaleUp(
                            context, vertex, conf, evaluatedMetrics, lastSummary)) {
                // Block scale up when last rescale is ineffective.
                return ParallelismChange.noChange(currentParallelism);
            }

            return ParallelismChange.build(
                    newParallelism, currentParallelism, outsideUtilizationBound);
        } else {
            return applyScaleDownInterval(
                    delayedScaleDown,
                    vertex,
                    conf,
                    newParallelism,
                    currentParallelism,
                    outsideUtilizationBound);
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
            int currentParallelism,
            boolean outsideUtilizationBound) {
        var scaleDownInterval = conf.get(SCALE_DOWN_INTERVAL);
        if (scaleDownInterval.toMillis() <= 0) {
            // The scale down interval is disable, so don't block scaling.
            return ParallelismChange.build(
                    newParallelism, currentParallelism, outsideUtilizationBound);
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
            return ParallelismChange.noChange(currentParallelism);
        } else {
            // Using the maximum parallelism within the scale down interval window instead of the
            // latest parallelism when scaling down
            var maxRecommendedParallelism =
                    delayedScaleDownInfo.getMaxRecommendedParallelism(windowStartTime);
            return ParallelismChange.build(
                    maxRecommendedParallelism.getParallelism(),
                    currentParallelism,
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
     * <p>When alignment is required and the computed parallelism differs from the current
     * parallelism, the method delegates to {@link #scaleUp} or {@link #scaleDown} based on the
     * scaling direction. These methods enforce direction-safety: scale-up adjustments never produce
     * a result below {@code currentParallelism}, and scale-down adjustments never produce a result
     * above {@code currentParallelism}. This prevents the alignment logic from cancelling or
     * inverting the intended scaling direction.
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

        var mode = context.getConfiguration().get(SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE);
        var fallback =
                KeyGroupOrPartitionsAdjustFallback.resolve(
                        mode,
                        context.getConfiguration()
                                .get(SCALING_KEY_GROUP_PARTITIONS_ADJUST_FALLBACK));

        if (newParallelism > currentParallelism) {
            return scaleUp(
                    vertex,
                    currentParallelism,
                    newParallelism,
                    numKeyGroupsOrPartitions,
                    upperBoundForAlignment,
                    upperBound,
                    parallelismLowerLimit,
                    mode,
                    fallback,
                    eventHandler,
                    context);
        } else if (newParallelism < currentParallelism) {
            return scaleDown(
                    vertex,
                    currentParallelism,
                    newParallelism,
                    numKeyGroupsOrPartitions,
                    upperBoundForAlignment,
                    upperBound,
                    parallelismLowerLimit,
                    mode,
                    fallback,
                    eventHandler,
                    context);
        }

        return newParallelism;
    }

    /**
     * Adjusts parallelism for a scale-up operation using a sequential mode-then-fallback strategy,
     * ensuring the result always stays strictly above {@code currentParallelism} to preserve the
     * scaling direction.
     *
     * <p>First, the primary {@code mode}'s complete alignment algorithm is executed via {@link
     * #applyScaleUpMode}. If the mode cannot find an aligned value (returns {@code
     * currentParallelism}), the configured {@code fallback} is resolved to a mode via {@link
     * KeyGroupOrPartitionsAdjustFallback#toMode()} and executed as a second pass. If both fail, a
     * {@link #SCALING_LIMITED} event is emitted and {@code currentParallelism} is returned,
     * effectively blocking the scale-up.
     */
    private static <KEY, Context extends JobAutoScalerContext<KEY>> int scaleUp(
            JobVertexID vertex,
            int currentParallelism,
            int newParallelism,
            int numKeyGroupsOrPartitions,
            int upperBoundForAlignment,
            int upperBound,
            int parallelismLowerLimit,
            KeyGroupOrPartitionsAdjustMode mode,
            KeyGroupOrPartitionsAdjustFallback fallback,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            Context context) {

        int result =
                applyScaleUpMode(
                        vertex,
                        currentParallelism,
                        newParallelism,
                        numKeyGroupsOrPartitions,
                        upperBoundForAlignment,
                        upperBound,
                        parallelismLowerLimit,
                        mode,
                        eventHandler,
                        context);

        if (result == currentParallelism && fallback != KeyGroupOrPartitionsAdjustFallback.NONE) {
            var fallbackMode = fallback.toMode();
            if (fallbackMode != null) {
                result =
                        applyScaleUpMode(
                                vertex,
                                currentParallelism,
                                newParallelism,
                                numKeyGroupsOrPartitions,
                                upperBoundForAlignment,
                                upperBound,
                                parallelismLowerLimit,
                                fallbackMode,
                                eventHandler,
                                context);
            }
        }

        if (result == currentParallelism) {
            boolean compliant = isAligned(currentParallelism, numKeyGroupsOrPartitions, mode);
            if (!compliant && fallback.toMode() != null) {
                compliant =
                        isAligned(currentParallelism, numKeyGroupsOrPartitions, fallback.toMode());
            }
            if (compliant) {
                LOG.warn(
                        "Scale-up of {} blocked: current parallelism {} is aligned with mode {} "
                                + "(fallback {}), but no aligned value found above it in ({}, {}].",
                        vertex,
                        currentParallelism,
                        mode,
                        fallback,
                        currentParallelism,
                        upperBoundForAlignment);
            } else {
                LOG.warn(
                        "Scale-up of {} blocked: current parallelism {} does NOT comply with "
                                + "mode {} or fallback {}. No aligned value found in ({}, {}]. "
                                + "Returning current parallelism as last resort to preserve "
                                + "scaling direction.",
                        vertex,
                        currentParallelism,
                        mode,
                        fallback,
                        currentParallelism,
                        upperBoundForAlignment);
            }
            emitScalingLimitedEvent(
                    vertex,
                    newParallelism,
                    currentParallelism,
                    numKeyGroupsOrPartitions,
                    upperBound,
                    parallelismLowerLimit,
                    eventHandler,
                    context);
        }

        return result;
    }

    /**
     * Adjusts parallelism for a scale-down operation using a sequential mode-then-fallback
     * strategy, ensuring the result always stays strictly below {@code currentParallelism} to
     * preserve the scaling direction.
     *
     * <p>First, the primary {@code mode}'s complete alignment algorithm is executed via {@link
     * #applyScaleDownMode}. If the mode cannot find an aligned value (returns {@code
     * currentParallelism}), the configured {@code fallback} is resolved to a mode via {@link
     * KeyGroupOrPartitionsAdjustFallback#toMode()} and executed as a second pass. If both fail, a
     * {@link #SCALING_LIMITED} event is emitted and {@code currentParallelism} is returned,
     * effectively blocking the scale-down.
     */
    private static <KEY, Context extends JobAutoScalerContext<KEY>> int scaleDown(
            JobVertexID vertex,
            int currentParallelism,
            int newParallelism,
            int numKeyGroupsOrPartitions,
            int upperBoundForAlignment,
            int upperBound,
            int parallelismLowerLimit,
            KeyGroupOrPartitionsAdjustMode mode,
            KeyGroupOrPartitionsAdjustFallback fallback,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            Context context) {

        int result =
                applyScaleDownMode(
                        vertex,
                        currentParallelism,
                        newParallelism,
                        numKeyGroupsOrPartitions,
                        upperBoundForAlignment,
                        upperBound,
                        parallelismLowerLimit,
                        mode,
                        eventHandler,
                        context);

        if (result == currentParallelism && fallback != KeyGroupOrPartitionsAdjustFallback.NONE) {
            var fallbackMode = fallback.toMode();
            if (fallbackMode != null) {
                result =
                        applyScaleDownMode(
                                vertex,
                                currentParallelism,
                                newParallelism,
                                numKeyGroupsOrPartitions,
                                upperBoundForAlignment,
                                upperBound,
                                parallelismLowerLimit,
                                fallbackMode,
                                eventHandler,
                                context);
            }
        }

        if (result == currentParallelism) {
            boolean compliant = isAligned(currentParallelism, numKeyGroupsOrPartitions, mode);
            if (!compliant && fallback.toMode() != null) {
                compliant =
                        isAligned(currentParallelism, numKeyGroupsOrPartitions, fallback.toMode());
            }
            if (compliant) {
                LOG.warn(
                        "Scale-down of {} blocked: current parallelism {} is aligned with mode {} "
                                + "(fallback {}), but no aligned value found below it in [{}, {}).",
                        vertex,
                        currentParallelism,
                        mode,
                        fallback,
                        parallelismLowerLimit,
                        currentParallelism);
            } else {
                LOG.warn(
                        "Scale-down of {} blocked: current parallelism {} does NOT comply with "
                                + "mode {} or fallback {}. No aligned value found in [{}, {}). "
                                + "Returning current parallelism as last resort to preserve "
                                + "scaling direction.",
                        vertex,
                        currentParallelism,
                        mode,
                        fallback,
                        parallelismLowerLimit,
                        currentParallelism);
            }
            emitScalingLimitedEvent(
                    vertex,
                    newParallelism,
                    currentParallelism,
                    numKeyGroupsOrPartitions,
                    upperBound,
                    parallelismLowerLimit,
                    eventHandler,
                    context);
        }

        return result;
    }

    /**
     * Runs a single mode's complete alignment algorithm for a scale-up operation.
     *
     * <p>Search strategy (phases are tried in order; first match wins):
     *
     * <ol>
     *   <li><b>Phase 1 (within range, divisor search):</b> Search from {@code newParallelism}
     *       downward to {@code currentParallelism + 1} for the largest divisor of {@code
     *       numKeyGroupsOrPartitions}. Only active for modes where {@link
     *       KeyGroupOrPartitionsAdjustMode#searchesWithinRange} returns {@code true} (i.e., {@link
     *       KeyGroupOrPartitionsAdjustMode#EVENLY_SPREAD EVENLY_SPREAD} and {@link
     *       KeyGroupOrPartitionsAdjustMode#OPTIMIZE_RESOURCE_UTILIZATION
     *       OPTIMIZE_RESOURCE_UTILIZATION}).
     *   <li><b>Phase 2 + Phase 3 (outside-range search):</b> Delegated to {@link
     *       #applyOutsideRangeSearch}. Only active for modes where {@link
     *       KeyGroupOrPartitionsAdjustMode#allowsOutsideRange} returns {@code true}. Phase 2
     *       searches upward to {@code upperBoundForAlignment}; Phase 3 is a relaxed downward
     *       fallback with a direction guard ensuring the result stays above {@code
     *       currentParallelism}.
     * </ol>
     *
     * @return the aligned parallelism, or {@code currentParallelism} if no aligned value was found
     *     (used as a sentinel by the caller to trigger fallback or emit a blocking event)
     */
    private static <KEY, Context extends JobAutoScalerContext<KEY>> int applyScaleUpMode(
            JobVertexID vertex,
            int currentParallelism,
            int newParallelism,
            int numKeyGroupsOrPartitions,
            int upperBoundForAlignment,
            int upperBound,
            int parallelismLowerLimit,
            KeyGroupOrPartitionsAdjustMode mode,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            Context context) {

        // Phase 1: Search within [currentParallelism+1, newParallelism] downward from target.
        // Active for EVENLY_SPREAD and OPTIMIZE_RESOURCE_UTILIZATION.
        // Skipped for MAXIMIZE_UTILISATION (whose N/p < N/new condition only triggers
        // above newParallelism) and ADAPTIVE_UPWARD_SPREAD (which delegates directly to
        // the upward Phase 2 search).
        if (searchesWithinRange(mode)) {
            for (int p = newParallelism; p > currentParallelism; p--) {
                if (isAligned(p, numKeyGroupsOrPartitions, mode)) {
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
            }
        }

        // Phase 2 + Phase 3: Outside-range upward search with relaxed downward fallback.
        // Direction guard ensures scale-up results stay above currentParallelism.
        if (allowsOutsideRange(mode)) {
            return applyOutsideRangeSearch(
                    vertex,
                    currentParallelism,
                    newParallelism,
                    numKeyGroupsOrPartitions,
                    upperBoundForAlignment,
                    upperBound,
                    parallelismLowerLimit,
                    mode,
                    true,
                    eventHandler,
                    context);
        }

        // No aligned value found for this mode
        return currentParallelism;
    }

    /**
     * Runs Phase 2 (upward outside-range search) and Phase 3 (relaxed downward fallback) of the
     * alignment algorithm. Shared by both scale-up and scale-down operations.
     *
     * <p><b>Phase 2 (upward search):</b> Searches from {@code newParallelism} upward to {@code
     * phase2UpperBound} for exact divisors ({@code N % p == 0}), or, in {@link
     * KeyGroupOrPartitionsAdjustMode#MAXIMIZE_UTILISATION MAXIMIZE_UTILISATION} mode, values where
     * per-subtask load decreases ({@code N/p < N/newParallelism}).
     *
     * <p><b>Phase 3 (relaxed downward fallback):</b> Searches downward from {@code newParallelism}
     * for the boundary where per-subtask load increases ({@code N/p > N/newParallelism}), then
     * snaps up to the nearest divisor-aligned value. For scale-up operations, a direction guard
     * ensures the result stays above {@code currentParallelism}.
     *
     * @param phase2UpperBound upper bound for Phase 2 search ({@code upperBoundForAlignment} for
     *     scale-up, {@code min(currentParallelism, upperBoundForAlignment)} for scale-down)
     * @param isScaleUp {@code true} for scale-up (enables direction guard in Phase 3), {@code
     *     false} for scale-down
     * @return the aligned parallelism, or {@code currentParallelism} if no valid value was found
     *     (sentinel for the caller to trigger fallback or emit a blocking event)
     */
    private static <KEY, Context extends JobAutoScalerContext<KEY>> int applyOutsideRangeSearch(
            JobVertexID vertex,
            int currentParallelism,
            int newParallelism,
            int numKeyGroupsOrPartitions,
            int phase2UpperBound,
            int upperBound,
            int parallelismLowerLimit,
            KeyGroupOrPartitionsAdjustMode mode,
            boolean isScaleUp,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            Context context) {

        // Phase 2: Search upward from newParallelism to phase2UpperBound.
        // Checks for exact divisors (N % p == 0). Additionally, for MAXIMIZE_UTILISATION,
        // accepts values where per-subtask load decreases (N/p < N/newParallelism).
        for (int p = newParallelism; p <= phase2UpperBound; p++) {
            if (numKeyGroupsOrPartitions % p == 0
                    || (mode == MAXIMIZE_UTILISATION
                            && numKeyGroupsOrPartitions / p
                                    < numKeyGroupsOrPartitions / newParallelism)) {
                return p;
            }
        }

        // Phase 3: Relaxed downward fallback.
        // Finds the boundary where per-subtask load increases (N/p > N/newParallelism),
        // then snaps up to the nearest divisor-aligned value.
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

        // Direction guard: for scale-up, the result must stay above currentParallelism
        if (isScaleUp && p <= currentParallelism) {
            return currentParallelism;
        }

        emitScalingLimitedEvent(
                vertex,
                newParallelism,
                p,
                numKeyGroupsOrPartitions,
                upperBound,
                parallelismLowerLimit,
                eventHandler,
                context);
        return p;
    }

    /**
     * Runs a single mode's complete alignment algorithm for a scale-down operation.
     *
     * <p>Search strategy (phases are tried in order; first match wins):
     *
     * <ol>
     *   <li><b>Phase 1 (within range, divisor search):</b> Search from {@code newParallelism}
     *       upward to {@code currentParallelism - 1} for the smallest divisor of {@code
     *       numKeyGroupsOrPartitions}. Only active for modes where {@link
     *       KeyGroupOrPartitionsAdjustMode#searchesWithinRange} returns {@code true}.
     *   <li><b>Phase 2 + Phase 3 (outside-range search):</b> Delegated to {@link
     *       #applyOutsideRangeSearch}. Only active for modes where {@link
     *       KeyGroupOrPartitionsAdjustMode#allowsOutsideRange} returns {@code true}. Phase 2
     *       searches upward, capped at {@code min(currentParallelism, upperBoundForAlignment)} to
     *       prevent crossing the current parallelism; Phase 3 is a relaxed downward fallback that
     *       is inherently direction-safe since results never exceed {@code newParallelism}, which
     *       is already below {@code currentParallelism}.
     * </ol>
     *
     * @return the aligned parallelism, or {@code currentParallelism} if no aligned value was found
     *     (used as a sentinel by the caller to trigger fallback or emit a blocking event)
     */
    private static <KEY, Context extends JobAutoScalerContext<KEY>> int applyScaleDownMode(
            JobVertexID vertex,
            int currentParallelism,
            int newParallelism,
            int numKeyGroupsOrPartitions,
            int upperBoundForAlignment,
            int upperBound,
            int parallelismLowerLimit,
            KeyGroupOrPartitionsAdjustMode mode,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            Context context) {

        // Phase 1: Search within [newParallelism, currentParallelism-1] upward from target.
        // Active for EVENLY_SPREAD and OPTIMIZE_RESOURCE_UTILIZATION.
        // Skipped for MAXIMIZE_UTILISATION and ADAPTIVE_UPWARD_SPREAD which delegate
        // directly to the upward Phase 2 search.
        if (searchesWithinRange(mode)) {
            for (int p = newParallelism; p < currentParallelism; p++) {
                if (isAligned(p, numKeyGroupsOrPartitions, mode)) {
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
            }
        }

        // Phase 2 + Phase 3: Outside-range upward search (capped at currentParallelism to
        // prevent crossing) with relaxed downward fallback. Inherently direction-safe
        // since results are always <= newParallelism < currentParallelism.
        if (allowsOutsideRange(mode)) {
            return applyOutsideRangeSearch(
                    vertex,
                    currentParallelism,
                    newParallelism,
                    numKeyGroupsOrPartitions,
                    Math.min(currentParallelism, upperBoundForAlignment),
                    upperBound,
                    parallelismLowerLimit,
                    mode,
                    false,
                    eventHandler,
                    context);
        }

        // No aligned value found for this mode
        return currentParallelism;
    }

    /**
     * Checks whether a given parallelism value is considered "aligned" with the number of key
     * groups or partitions, based on the configured {@link KeyGroupOrPartitionsAdjustMode}.
     *
     * <p>This method is only used for <b>within-range</b> searches (Phase 1), where candidates lie
     * between {@code currentParallelism} and {@code newParallelism}. Outside-range alignment (Phase
     * 2 + Phase 3) is handled directly inside {@link #applyOutsideRangeSearch}.
     *
     * <p>All modes accept a perfect divisor ({@code numKeyGroupsOrPartitions % parallelism == 0})
     * as the universal alignment condition. Beyond that, behavior differs per mode:
     *
     * <ul>
     *   <li>{@link KeyGroupOrPartitionsAdjustMode#EVENLY_SPREAD EVENLY_SPREAD} - strict divisor
     *       only. No relaxation beyond the universal check.
     *   <li>{@link KeyGroupOrPartitionsAdjustMode#MAXIMIZE_UTILISATION MAXIMIZE_UTILISATION} -
     *       within range, only divisors are accepted. The relaxed per-subtask load condition
     *       ({@code N/p < N/newParallelism}) is applied in {@link #applyOutsideRangeSearch}.
     *   <li>{@link KeyGroupOrPartitionsAdjustMode#OPTIMIZE_RESOURCE_UTILIZATION
     *       OPTIMIZE_RESOURCE_UTILIZATION} - accepts any parallelism where every subtask has at
     *       least one item to process ({@code parallelism <= numKeyGroupsOrPartitions}), regardless
     *       of even distribution.
     *   <li>{@link KeyGroupOrPartitionsAdjustMode#ADAPTIVE_UPWARD_SPREAD ADAPTIVE_UPWARD_SPREAD} -
     *       strict divisor only (same as {@code EVENLY_SPREAD}). The relaxed
     *       MAXIMIZE_UTILISATION-style fallback is handled in {@link #applyOutsideRangeSearch}.
     * </ul>
     *
     * @param parallelism the candidate parallelism value to check
     * @param numKeyGroupsOrPartitions the number of key groups or source partitions
     * @param mode the configured alignment mode
     * @return {@code true} if the candidate parallelism is considered aligned
     */
    private static boolean isAligned(
            int parallelism, int numKeyGroupsOrPartitions, KeyGroupOrPartitionsAdjustMode mode) {

        if (numKeyGroupsOrPartitions % parallelism == 0) {
            return true;
        }

        return switch (mode) {
            case EVENLY_SPREAD, MAXIMIZE_UTILISATION, ADAPTIVE_UPWARD_SPREAD ->
            false;
            case OPTIMIZE_RESOURCE_UTILIZATION ->
            parallelism <= numKeyGroupsOrPartitions;
        };
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
                SCALING_LIMITED + vertex,
                context.getConfiguration().get(SCALING_EVENT_INTERVAL));
    }

    @VisibleForTesting
    protected void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
    }

    /** The mode of the key group or parallelism adjustment. */
    public enum KeyGroupOrPartitionsAdjustMode implements DescribedEnum {
        EVENLY_SPREAD(
                "Conservative mode that only accepts parallelism values that evenly divide the "
                        + "number of key groups or partitions (N % p == 0). Searches within the "
                        + "direction-safe range between currentParallelism and newParallelism. "
                        + "If no divisor is found, scaling is blocked to avoid uneven data "
                        + "distribution."),

        ADAPTIVE_UPWARD_SPREAD(
                "Default mode. Combines the divisor alignment of EVENLY_SPREAD with the "
                        + "relaxed fallback of MAXIMIZE_UTILISATION. Searches above the computed "
                        + "target for a divisor of key groups or partitions, falling back to the "
                        + "nearest value that reduces per-subtask load. Direction-safety guards "
                        + "prevent cancelling or inverting the scaling direction."),

        MAXIMIZE_UTILISATION(
                "Searches upward from the computed target parallelism for the smallest value "
                        + "where either (a) it evenly divides the number of key groups or "
                        + "partitions, or (b) the per-subtask load decreases compared to the "
                        + "target (N/p < N/newParallelism). This accepts slightly uneven "
                        + "distribution in exchange for using fewer resources than a strict "
                        + "divisor alignment."),

        OPTIMIZE_RESOURCE_UTILIZATION(
                "Accepts any parallelism where every subtask has at least one key group or "
                        + "partition to process (p <= N). Does not enforce even distribution. "
                        + "Searches within the direction-safe range between currentParallelism "
                        + "and newParallelism. This is the most resource-efficient mode, using "
                        + "exactly the parallelism the autoscaler computes without alignment "
                        + "overhead."),
        ;

        private final InlineElement description;

        KeyGroupOrPartitionsAdjustMode(String description) {
            this.description = text(description);
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }

        public static boolean searchesWithinRange(KeyGroupOrPartitionsAdjustMode mode) {
            return mode != MAXIMIZE_UTILISATION && mode != ADAPTIVE_UPWARD_SPREAD;
        }

        public static boolean allowsOutsideRange(KeyGroupOrPartitionsAdjustMode mode) {
            return mode == MAXIMIZE_UTILISATION || mode == ADAPTIVE_UPWARD_SPREAD;
        }
    }

    /**
     * Controls the fallback strategy when the primary {@link KeyGroupOrPartitionsAdjustMode} cannot
     * find an aligned parallelism value.
     *
     * <p>When set to {@link #DEFAULT}, only {@link
     * KeyGroupOrPartitionsAdjustMode#ADAPTIVE_UPWARD_SPREAD} has a built-in fallback ({@link
     * #MAXIMIZE_UTILISATION}). All other modes resolve to {@link #NONE}, blocking scaling when no
     * aligned value is found.
     */
    public enum KeyGroupOrPartitionsAdjustFallback implements DescribedEnum {
        DEFAULT(
                "Uses the mode's built-in fallback. Only ADAPTIVE_UPWARD_SPREAD has a "
                        + "built-in fallback (MAXIMIZE_UTILISATION). All other modes resolve "
                        + "to NONE, blocking scaling when no aligned value is found."),

        NONE(
                "Blocks scaling when no aligned parallelism is found. Returns "
                        + "currentParallelism, effectively cancelling the scaling operation."),

        ADAPTIVE_UPWARD_SPREAD(
                "Delegates to the full ADAPTIVE_UPWARD_SPREAD strategy: searches upward "
                        + "from the target for a divisor, then falls back to a relaxed search "
                        + "for the nearest value that reduces per-subtask load."),

        MAXIMIZE_UTILISATION(
                "Falls back to a relaxed search that finds the smallest parallelism "
                        + "where per-subtask load decreases compared to the target "
                        + "(N/p < N/newParallelism). Direction-safety guards still apply."),

        OPTIMIZE_RESOURCE_UTILIZATION(
                "Accepts the computed target parallelism directly as long as every "
                        + "subtask has at least one item to process (p <= N)."),
        ;

        private final InlineElement description;

        KeyGroupOrPartitionsAdjustFallback(String description) {
            this.description = text(description);
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }

        /**
         * Resolves the effective fallback. Only {@link
         * KeyGroupOrPartitionsAdjustMode#ADAPTIVE_UPWARD_SPREAD} has a built-in fallback ({@link
         * #MAXIMIZE_UTILISATION}). All other modes resolve to {@link #NONE}.
         */
        public static KeyGroupOrPartitionsAdjustFallback resolve(
                KeyGroupOrPartitionsAdjustMode mode, KeyGroupOrPartitionsAdjustFallback fallback) {
            if (fallback != DEFAULT) {
                return fallback;
            }
            if (mode == KeyGroupOrPartitionsAdjustMode.ADAPTIVE_UPWARD_SPREAD) {
                return MAXIMIZE_UTILISATION;
            }
            return NONE;
        }

        /**
         * Maps this fallback to the equivalent {@link KeyGroupOrPartitionsAdjustMode} so it can be
         * executed as a full second-pass search via {@code applyScaleUpMode} / {@code
         * applyScaleDownMode}. Returns {@code null} for {@link #NONE} and {@link #DEFAULT} which do
         * not map to a mode.
         */
        public KeyGroupOrPartitionsAdjustMode toMode() {
            return switch (this) {
                case ADAPTIVE_UPWARD_SPREAD -> KeyGroupOrPartitionsAdjustMode
                        .ADAPTIVE_UPWARD_SPREAD;
                case MAXIMIZE_UTILISATION -> KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION;
                case OPTIMIZE_RESOURCE_UTILIZATION -> KeyGroupOrPartitionsAdjustMode
                        .OPTIMIZE_RESOURCE_UTILIZATION;
                case NONE, DEFAULT -> null;
            };
        }
    }
}
