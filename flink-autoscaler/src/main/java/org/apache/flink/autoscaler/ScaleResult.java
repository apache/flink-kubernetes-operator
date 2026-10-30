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

import org.apache.flink.annotation.Internal;

import lombok.Getter;

/**
 * The outcome of a single scaling evaluation cycle.
 *
 * <p>{@link ScalingExecutor#execute} previously returned a {@code boolean}, which collapsed every
 * reason for not scaling into the single {@code autoscaler.balanced} counter. A job reporting
 * {@code balanced=100, scalings=0} could be perfectly healthy or completely blocked, and the metric
 * could not distinguish the two. Each constant here names one distinct outcome so that the counter
 * can be tagged with the reason.
 *
 * <p>The tag is the value reported on the {@code reason} metric variable. Tags are lower case and
 * stable: they are part of the metric surface, so renaming one breaks existing dashboards.
 */
@Internal
public enum ScaleResult {

    /** A parallelism change was computed and applied. */
    SCALED("scaled"),

    /** No vertex needed a parallelism change. The job runs at its target parallelism. */
    BALANCED("balanced"),

    /** Scaling is turned off through {@code job.autoscaler.scaling.enabled}. */
    BLOCKED_BY_CONFIG("config_disabled"),

    /** The current time falls inside {@code job.autoscaler.excluded.periods}. */
    BLOCKED_BY_EXCLUDED_PERIOD("excluded_period"),

    /** GC pressure or heap usage is above the configured limit. */
    BLOCKED_BY_MEMORY("memory_pressure"),

    /** The change would exceed the configured CPU or memory quota. */
    BLOCKED_BY_QUOTA("resource_quota"),

    /** The cluster cannot schedule the TaskManagers that the change needs. */
    BLOCKED_BY_CLUSTER_RESOURCES("cluster_resources"),

    /** A custom {@link ScalingExecutorPlugin} vetoed the change. */
    BLOCKED_BY_CUSTOM_EXECUTOR("custom_executor_veto"),

    /** The previous scale up did not improve the processing rate enough. */
    BLOCKED_BY_INEFFECTIVE("ineffective_scaling"),

    /** The scale down interval has not elapsed yet. */
    BLOCKED_BY_COOLDOWN("cooldown"),

    /** The true processing rate or the target rate is not available yet. */
    BLOCKED_BY_DATA_UNAVAILABLE("data_unavailable");

    @Getter private final String tag;

    ScaleResult(String tag) {
        this.tag = tag;
    }

    /** Returns true if this cycle applied a parallelism change. */
    public boolean isScaled() {
        return this == SCALED;
    }
}
