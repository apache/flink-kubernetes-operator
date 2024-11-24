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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.JobVertexScaler.KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.MAX_SCALE_DOWN_FACTOR;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.MAX_SCALE_UP_FACTOR;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALE_DOWN_INTERVAL;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_EVENT_INTERVAL;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.VERTEX_MAX_PARALLELISM;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.VERTEX_MIN_PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.EXPECTED_PROCESSING_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.MAX_PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.NUM_SOURCE_PARTITIONS;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.PARALLELISM;
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

        private static final ParallelismChange NO_CHANGE = new ParallelismChange(-1);

        private final int newParallelism;

        private ParallelismChange(int newParallelism) {
            this.newParallelism = newParallelism;
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
            return newParallelism == that.newParallelism;
        }

        @Override
        public int hashCode() {
            return Objects.hash(newParallelism);
        }

        @Override
        public String toString() {
            return isNoChange()
                    ? "NoParallelismChange"
                    : "ParallelismChange{newParallelism=" + newParallelism + '}';
        }

        public static ParallelismChange build(int newParallelism) {
            checkArgument(newParallelism > 0, "The parallelism should be greater than 0.");
            return new ParallelismChange(newParallelism);
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
                        evaluatedMetrics, conf, conf.get(TARGET_UTILIZATION), true, restartTime);
        if (Double.isNaN(targetCapacity)) {
            LOG.warn(
                    "Target data rate is not available for {}, cannot compute new parallelism",
                    vertex);
            return ParallelismChange.noChange();
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
                        vertex,
                        currentParallelism,
                        inputShipStrategies,
                        (int) evaluatedMetrics.get(NUM_SOURCE_PARTITIONS).getCurrent(),
                        (int) evaluatedMetrics.get(MAX_PARALLELISM).getCurrent(),
                        scaleFactor,
                        Math.min(currentParallelism, conf.getInteger(VERTEX_MIN_PARALLELISM)),
                        Math.max(currentParallelism, conf.getInteger(VERTEX_MAX_PARALLELISM)),
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

        var scaledUp = currentParallelism < newParallelism;

        if (scaledUp) {
            // Clear delayed scale down request if the new parallelism is greater than
            // currentParallelism.
            delayedScaleDown.clearVertex(vertex);

            // If we don't have past scaling actions for this vertex, don't block scale up.
            if (history.isEmpty()) {
                return ParallelismChange.build(newParallelism);
            }

            var lastSummary = history.get(history.lastKey());
            if (currentParallelism == lastSummary.getNewParallelism()
                    && lastSummary.isScaledUp()
                    && detectIneffectiveScaleUp(
                            context, vertex, conf, evaluatedMetrics, lastSummary)) {
                // Block scale up when last rescale is ineffective.
                return ParallelismChange.noChange();
            }

            return ParallelismChange.build(newParallelism);
        } else {
            return applyScaleDownInterval(delayedScaleDown, vertex, conf, newParallelism);
        }
    }

    private ParallelismChange applyScaleDownInterval(
            DelayedScaleDown delayedScaleDown,
            JobVertexID vertex,
            Configuration conf,
            int newParallelism) {
        var scaleDownInterval = conf.get(SCALE_DOWN_INTERVAL);
        if (scaleDownInterval.toMillis() <= 0) {
            // The scale down interval is disable, so don't block scaling.
            return ParallelismChange.build(newParallelism);
        }

        var now = clock.instant();
        var delayedScaleDownInfo = delayedScaleDown.triggerScaleDown(vertex, now, newParallelism);

        // Never scale down within scale down interval
        if (now.isBefore(delayedScaleDownInfo.getFirstTriggerTime().plus(scaleDownInterval))) {
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
            return ParallelismChange.build(delayedScaleDownInfo.getMaxRecommendedParallelism());
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
     * Computing the newParallelism. In general, newParallelism = currentParallelism * scaleFactor.
     * But we limit newParallelism between parallelismLowerLimit and min(parallelismUpperLimit,
     * maxParallelism).
     *
     * <p>Also, in order to ensure the data is evenly spread across subtasks, we try to adjust the
     * parallelism for source and keyed vertex such that it divides the maxParallelism without a
     * remainder.
     *
     * <p>This method also attempts to adjust the parallelism to ensure it aligns well with the
     * number of source partitions if a vertex has a known source partition count.
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

        // When the shuffle type of vertex inputs contains keyBy or vertex is a source,
        // we try to adjust the parallelism such that it divides
        // the numKeyGroupsOrPartitions without a remainder => data is evenly spread across subtasks
        for (int p = newParallelism; p <= upperBoundForAlignment; p++) {
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
        var message =
                String.format(
                        SCALE_LIMITED_MESSAGE_FORMAT,
                        vertex,
                        newParallelism,
                        p,
                        numKeyGroupsOrPartitions,
                        upperBound,
                        parallelismLowerLimit);
        eventHandler.handleEvent(
                context,
                AutoScalerEventHandler.Type.Warning,
                SCALING_LIMITED,
                message,
                SCALING_LIMITED + vertex + newParallelism,
                context.getConfiguration().get(SCALING_EVENT_INTERVAL));
        return p;
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
