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

package org.apache.flink.kubernetes.operator.autoscaler.metrics;

/**
 * Supported scaling metrics. These represent high level metrics computed from Flink job metrics
 * that are used for scaling decisions in the autoscaler module.
 */
public enum ScalingMetric {

    /** Subtask load (busy time ratio 0 (idle) to 1 (fully utilized)). */
    LOAD(true),

    /** Processing rate at full capacity (records/sec). */
    TRUE_PROCESSING_RATE(true),

    /** Current processing rate. */
    CURRENT_PROCESSING_RATE(true),

    /**
     * Incoming data rate to the source, e.g. rate of records written to the Kafka topic
     * (records/sec).
     */
    SOURCE_DATA_RATE(true),

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
    /** Upper boundary of the target data rate range. */
    SCALE_UP_RATE_THRESHOLD(false),

    /** Lower boundary of the target data rate range. */
    SCALE_DOWN_RATE_THRESHOLD(false),

    NUM_RECORDS_OUT_PER_SECOND(true),

    /** Expected true processing rate after scale up. */
    EXPECTED_PROCESSING_RATE(false);

    private final boolean calculateAverage;

    ScalingMetric(boolean calculateAverage) {
        this.calculateAverage = calculateAverage;
    }

    public boolean isCalculateAverage() {
        return calculateAverage;
    }
}
