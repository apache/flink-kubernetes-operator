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
 * The built-in {@link ParallelismAlignmentMode}s selectable through the {@code
 * scaling.parallelism-alignment.mode} option. They align a computed target parallelism to the
 * number of key groups (keyBy) or source partitions to reduce data skew, searching one of the
 * regions illustrated on {@link ParallelismAlignmentMode}.
 *
 * <p>{@code BALANCED} and {@code EVENLY_SPREAD} scan a single region for the scaling direction (the
 * above-target region for a scale-up, the within-range region between the target and the current
 * parallelism for a scale-down) and differ only in what they accept. When no aligned value is found
 * in that region they use the computed target unchanged rather than blocking the scale. All three
 * apply only to source / keyBy (hash) vertices, via the default {@link
 * ParallelismAlignmentMode#isApplicable(Context)}.
 */
public enum BuiltInAlignmentMode implements ParallelismAlignmentMode, DescribedEnum {
    BALANCED(
            "Default. Aligns to the first parallelism in the search region that reduces per-subtask "
                    + "load, snapping to an exact divisor of the key groups or source partitions "
                    + "when one is within reach. Balances even data distribution against resource "
                    + "usage, tolerating mild skew to avoid over-provisioning.") {
        @Override
        public int alignParallelism(Context<?> ctx) {
            return alignOrKeepTarget(ctx, true);
        }
    },

    EVENLY_SPREAD(
            "Aligns to a parallelism that evenly divides the number of key groups or source "
                    + "partitions, spreading data evenly across subtasks to reduce skew.") {
        @Override
        public int alignParallelism(Context<?> ctx) {
            return alignOrKeepTarget(ctx, false);
        }
    },

    OFF("Disables alignment. The autoscaler's computed target parallelism is used as-is.") {
        @Override
        public int alignParallelism(Context<?> ctx) {
            return ctx.getNewParallelism();
        }
    };

    /**
     * Scans the direction's region for an aligned value, and keeps the computed target when none is
     * found (never blocking the scale).
     */
    private static int alignOrKeepTarget(Context<?> ctx, boolean acceptLoadReducing) {
        int aligned = ParallelismAligner.firstAlignedInRegion(ctx, acceptLoadReducing);
        return aligned > 0 ? aligned : ctx.getNewParallelism();
    }

    private final InlineElement description;

    BuiltInAlignmentMode(String description) {
        this.description = text(description);
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }
}
