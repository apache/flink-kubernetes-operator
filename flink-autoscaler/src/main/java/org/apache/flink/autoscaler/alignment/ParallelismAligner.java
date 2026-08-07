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

package org.apache.flink.autoscaler.alignment;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.ALIGNMENT_MODE;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_EVENT_INTERVAL;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE;

/**
 * Aligns a computed target parallelism to the number of key groups or source partitions. Holds the
 * custom alignment modes discovered as plugins, resolves the configured mode (built-in by name,
 * custom by class, or the deprecated legacy mode), builds the {@link
 * ParallelismAlignmentMode.Context}, and emits the {@code SCALING_LIMITED} event used by the legacy
 * modes. The per-direction region search, the legacy relaxed/blocking fallbacks, and the derived
 * alignment quantities are stateless and exposed as static helpers for the {@link
 * ParallelismAlignmentMode} implementations. This keeps all the alignment logic out of {@link
 * org.apache.flink.autoscaler.JobVertexScaler}, which only computes the clamped target and
 * delegates here.
 */
public final class ParallelismAligner {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelismAligner.class);

    @VisibleForTesting public static final String SCALING_LIMITED = "ScalingLimited";

    @VisibleForTesting
    public static final String SCALE_LIMITED_MESSAGE_FORMAT =
            "Scaling limited detected for %s (expected parallelism: %s, actual parallelism %s). "
                    + "Scaling limited due to numKeyGroupsOrPartitions : %s，"
                    + "upperBoundForAlignment(maxParallelism or parallelismUpperLimit): %s, parallelismLowerLimit: %s.";

    /** Emits a {@code SCALING_LIMITED} event. Used only by the deprecated blocking modes. */
    @FunctionalInterface
    public interface ScalingLimitedEmitter {
        void emit(int expectedParallelism, int actualParallelism);
    }

    /** Custom modes discovered as plugins; built-ins and the legacy modes are resolved by name. */
    private final Collection<ParallelismAlignmentMode> discoveredModes;

    public ParallelismAligner(Collection<ParallelismAlignmentMode> discoveredModes) {
        this.discoveredModes = discoveredModes;
    }

    /**
     * Resolves the configured alignment mode, builds the {@link ParallelismAlignmentMode.Context}
     * for the vertex, and applies the mode to the clamped {@code newParallelism}.
     */
    @SuppressWarnings("deprecation")
    public <KEY, Context extends JobAutoScalerContext<KEY>> int align(
            JobVertexID vertex,
            int currentParallelism,
            int newParallelism,
            int numSourcePartitions,
            int maxParallelism,
            int parallelismLowerLimit,
            int parallelismUpperLimit,
            Collection<ShipStrategy> inputShipStrategies,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            Context context) {
        Configuration conf = context.getConfiguration();
        String modeName = conf.get(ALIGNMENT_MODE);

        ParallelismAlignmentMode.Context<KEY> alignmentContext =
                new ParallelismAlignmentMode.Context<>(
                        context,
                        AutoScalerOptions.customAlignmentModeConfiguration(conf, modeName),
                        vertex,
                        currentParallelism,
                        newParallelism,
                        numSourcePartitions,
                        maxParallelism,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        inputShipStrategies,
                        evaluatedMetrics);

        ParallelismAlignmentMode mode = resolve(conf);
        if (!mode.isApplicable(alignmentContext)) {
            return alignmentContext.getNewParallelism();
        }
        if (mode instanceof KeyGroupOrPartitionsAdjustMode) {
            ScalingLimitedEmitter emitter =
                    (expected, actual) ->
                            emitScalingLimitedEvent(
                                    vertex,
                                    expected,
                                    actual,
                                    numKeyGroupsOrPartitions(alignmentContext),
                                    Math.min(maxParallelism, parallelismUpperLimit),
                                    parallelismLowerLimit,
                                    eventHandler,
                                    context);
            return ((KeyGroupOrPartitionsAdjustMode) mode)
                    .alignParallelism(alignmentContext, emitter);
        }
        return mode.alignParallelism(alignmentContext);
    }

    /**
     * Resolves the effective alignment mode from configuration. The {@code
     * scaling.parallelism-alignment.mode} key is read first: a value matching a {@link
     * BuiltInAlignmentMode} name selects that built-in, otherwise it is treated as a custom mode
     * {@code <name>} whose class ({@code scaling.parallelism-alignment.mode.<name>.class}) is
     * matched against the discovered plugins. If that key is unset, the deprecated {@code
     * scaling.key-group.partitions.adjust.mode} key is honored with its original blocking behavior.
     * If neither is set, {@link BuiltInAlignmentMode#BALANCED} applies.
     */
    @VisibleForTesting
    @SuppressWarnings("deprecation")
    public ParallelismAlignmentMode resolve(Configuration conf) {
        Optional<String> selected = conf.getOptional(ALIGNMENT_MODE);
        if (selected.isPresent()) {
            return resolveByName(selected.get(), conf);
        }

        Optional<KeyGroupOrPartitionsAdjustMode> legacy =
                conf.getOptional(SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE);
        if (legacy.isPresent()) {
            return legacy.get();
        }

        return BuiltInAlignmentMode.BALANCED;
    }

    private ParallelismAlignmentMode resolveByName(String name, Configuration conf) {
        for (BuiltInAlignmentMode builtIn : BuiltInAlignmentMode.values()) {
            if (builtIn.name().equals(name)) {
                return builtIn;
            }
        }

        String className = conf.get(AutoScalerOptions.customAlignmentModeClassOption(name));
        if (className == null) {
            LOG.warn(
                    "Alignment mode '{}' is not a built-in and has no '{}' configured. Falling back "
                            + "to {}.",
                    name,
                    AutoScalerOptions.customAlignmentModeClassKey(name),
                    BuiltInAlignmentMode.BALANCED);
            return BuiltInAlignmentMode.BALANCED;
        }

        for (ParallelismAlignmentMode mode : discoveredModes) {
            if (mode.getClass().getName().equals(className)) {
                return mode;
            }
        }

        LOG.warn(
                "Custom alignment mode '{}' (class {}) was not found among the discovered plugins. "
                        + "Falling back to {}.",
                name,
                className,
                BuiltInAlignmentMode.BALANCED);
        return BuiltInAlignmentMode.BALANCED;
    }

    /** Whether the computed target is above the current parallelism. */
    public static boolean isScaleUp(ParallelismAlignmentMode.Context<?> ctx) {
        return ctx.getNewParallelism() > ctx.getCurrentParallelism();
    }

    /** {@code N}: the number of key groups or source partitions to align to. */
    public static int numKeyGroupsOrPartitions(ParallelismAlignmentMode.Context<?> ctx) {
        return ctx.getNumSourcePartitions() <= 0
                ? ctx.getMaxParallelism()
                : ctx.getNumSourcePartitions();
    }

    /** The alignment cap: {@code min(N, min(maxParallelism, parallelismUpperLimit))}. */
    public static int upperBoundForAlignment(ParallelismAlignmentMode.Context<?> ctx) {
        return Math.min(
                numKeyGroupsOrPartitions(ctx),
                Math.min(ctx.getMaxParallelism(), ctx.getParallelismUpperLimit()));
    }

    /**
     * Scans the alignment region for the scaling direction and returns the first accepted
     * parallelism, or {@code 0} when none is found.
     *
     * <p>For a scale-up the region is above the target, {@code [target, upperAlignLimit]}. For a
     * scale-down it is the within-range band between the target and the current parallelism, {@code
     * [target, currentParallelism]} (capped so the result never crosses into a scale-up). Both are
     * scanned upward from the target.
     *
     * @param acceptLoadReducing when {@code true}, also accept a parallelism that reduces
     *     per-subtask load (not only an exact divisor)
     * @return the first accepted parallelism, or {@code 0} when none is found in the region
     */
    public static int firstAlignedInRegion(
            ParallelismAlignmentMode.Context<?> ctx, boolean acceptLoadReducing) {
        int n = numKeyGroupsOrPartitions(ctx);
        int target = ctx.getNewParallelism();
        int regionEnd =
                isScaleUp(ctx)
                        ? upperBoundForAlignment(ctx)
                        : Math.min(ctx.getCurrentParallelism(), upperBoundForAlignment(ctx));

        for (int p = target; p <= regionEnd; p++) {
            if (n % p == 0 || (acceptLoadReducing && n / p < n / target)) {
                return p;
            }
        }
        return 0;
    }

    /**
     * The relaxed downward boundary fallback used by the deprecated legacy modes when {@link
     * #firstAlignedInRegion} finds nothing. Searches downward from the target for the boundary
     * where per-subtask load increases, snapping up to the nearest divisor, then clamps to the
     * parallelism lower limit.
     */
    public static int relaxedDownwardFallback(ParallelismAlignmentMode.Context<?> ctx) {
        int n = numKeyGroupsOrPartitions(ctx);
        int target = ctx.getNewParallelism();

        int p = target;
        for (; p > 0; p--) {
            if (n / p > n / target) {
                if (n % p != 0) {
                    p++;
                }
                break;
            }
        }
        return Math.max(p, ctx.getParallelismLowerLimit());
    }

    /**
     * Blocking fallback used by the deprecated legacy modes. If {@code candidate} would invert the
     * scaling direction, emits a {@code SCALING_LIMITED} event and returns the current parallelism
     * (blocking the scale). Otherwise it returns {@code candidate}, emitting the same event when
     * the aligned value deviates from the computed target. Matches the historical behavior.
     */
    public static int applyBlockingFallback(
            ParallelismAlignmentMode.Context<?> ctx, int candidate, ScalingLimitedEmitter emitter) {
        if (invertsDirection(ctx, candidate)) {
            emitter.emit(ctx.getNewParallelism(), ctx.getCurrentParallelism());
            return ctx.getCurrentParallelism();
        }
        if (candidate != ctx.getNewParallelism()) {
            emitter.emit(ctx.getNewParallelism(), candidate);
        }
        return candidate;
    }

    private static boolean invertsDirection(
            ParallelismAlignmentMode.Context<?> ctx, int candidate) {
        return (isScaleUp(ctx) && candidate <= ctx.getCurrentParallelism())
                || (!isScaleUp(ctx) && candidate >= ctx.getCurrentParallelism());
    }

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
}
