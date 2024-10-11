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

package org.apache.flink.autoscaler.metrics;

import lombok.Getter;

import java.util.Set;

/**
 * Supported scaling metrics. These represent high level metrics computed from Flink job metrics
 * that are used for scaling decisions in the autoscaler module.
 */
public enum ScalingMetric {

    /** Subtask load (busy time ratio 0 (idle) to 1 (fully utilized)). */
    LOAD(true),

    /** Processing rate at full capacity (records/sec). */
    TRUE_PROCESSING_RATE(true),

    /** Observed true processing rate for sources. */
    OBSERVED_TPR(true),

    /** Target processing rate of operators as derived from source inputs (records/sec). */
    TARGET_DATA_RATE(true),

    /** Target processing rate of operators as derived from backlog (records/sec). */
    CATCH_UP_DATA_RATE(false),

    /** Total number of pending records. */
    LAG(false),

    /** Job vertex parallelism. */
    PARALLELISM(false),

    /** Recommended job vertex parallelism. */
    RECOMMENDED_PARALLELISM(false),

    /** Job vertex max parallelism. */
    MAX_PARALLELISM(false),

    /** Source vertex partition count. */
    NUM_SOURCE_PARTITIONS(false),

    /** Upper boundary of the target data rate range. */
    SCALE_UP_RATE_THRESHOLD(false),

    /** Lower boundary of the target data rate range. */
    SCALE_DOWN_RATE_THRESHOLD(false),

    /** Expected true processing rate after scale up. */
    EXPECTED_PROCESSING_RATE(false),

    NUM_RECORDS_IN(false),

    NUM_RECORDS_OUT(false),

    ACCUMULATED_BUSY_TIME(false),

    /**
     * Maximum GC pressure across taskmanagers. Percentage of time spent garbage collecting between
     * 0 (no time in GC) and 1 (100% time in GC).
     */
    GC_PRESSURE(false),

    /** Measured max used heap size in bytes. */
    HEAP_MEMORY_USED(true),

    /** Measured max managed memory size in bytes. */
    MANAGED_MEMORY_USED(true),

    /** Measured max metaspace memory size in bytes. */
    METASPACE_MEMORY_USED(true),

    /** Percentage of max heap used (between 0 and 1). */
    HEAP_MAX_USAGE_RATIO(true),

    NUM_TASK_SLOTS_USED(false);

    @Getter private final boolean calculateAverage;

    /** List of {@link ScalingMetric}s to be reported as per vertex Flink metrics. */
    public static final Set<ScalingMetric> REPORTED_VERTEX_METRICS =
            Set.of(
                    LOAD,
                    TRUE_PROCESSING_RATE,
                    TARGET_DATA_RATE,
                    CATCH_UP_DATA_RATE,
                    LAG,
                    PARALLELISM,
                    RECOMMENDED_PARALLELISM,
                    MAX_PARALLELISM,
                    NUM_SOURCE_PARTITIONS,
                    SCALE_UP_RATE_THRESHOLD,
                    SCALE_DOWN_RATE_THRESHOLD,
                    EXPECTED_PROCESSING_RATE);

    ScalingMetric(boolean calculateAverage) {
        this.calculateAverage = calculateAverage;
    }
}
