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

import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * The legacy key group / partitions parallelism adjustment modes, kept for backward compatibility.
 *
 * <p>Unlike {@link BuiltInAlignmentMode}, these preserve the original <b>blocking</b> behavior:
 * when no aligned value can preserve the scaling direction they return the current parallelism and
 * emit a {@code SCALING_LIMITED} event, and they also emit when the aligned value deviates from the
 * computed target. New configurations should use {@code scaling.parallelism-alignment.mode}.
 *
 * @deprecated Superseded by {@link BuiltInAlignmentMode} (config key {@code
 *     scaling.parallelism-alignment.mode}). Retained only so the deprecated {@code
 *     scaling.key-group.partitions.adjust.mode} key still parses and behaves exactly as before.
 */
@Deprecated
public enum KeyGroupOrPartitionsAdjustMode implements ParallelismAlignmentMode, DescribedEnum {
    EVENLY_SPREAD(
            "This mode ensures that the parallelism adjustment attempts to evenly distribute data across subtasks"
                    + ". It is particularly effective for source vertices that are aware of partition counts or vertices after "
                    + "'keyBy' operation. The goal is to have the number of key groups or partitions be divisible by the set parallelism, ensuring even data distribution and reducing data skew.",
            false),

    MAXIMIZE_UTILISATION(
            "This model is to maximize resource utilization. In this mode, an attempt is made to set"
                    + " the parallelism that meets the current consumption rate requirements. It is not enforced "
                    + "that the number of key groups or partitions is divisible by the parallelism.",
            true),
    ;

    private final InlineElement description;

    private final boolean maximize;

    KeyGroupOrPartitionsAdjustMode(String description, boolean maximize) {
        this.description = text(description);
        this.maximize = maximize;
    }

    @Override
    public int alignParallelism(Context ctx) {
        // Called directly (without an emitter) the SCALING_LIMITED event is suppressed; the
        // autoscaler path goes through the emitter overload below via ParallelismAligner.
        return alignParallelism(ctx, (expected, actual) -> {});
    }

    int alignParallelism(Context ctx, ParallelismAligner.ScalingLimitedEmitter emitter) {
        int aligned = ParallelismAligner.firstAlignedInRegion(ctx, maximize);
        if (aligned > 0) {
            return aligned;
        }
        return ParallelismAligner.applyBlockingFallback(
                ctx, ParallelismAligner.relaxedDownwardFallback(ctx), emitter);
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }
}
