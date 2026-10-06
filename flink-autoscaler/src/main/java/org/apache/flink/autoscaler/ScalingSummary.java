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

import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Set;

/** Scaling summary returned by the {@link ScalingMetricEvaluator}. */
@Data
@NoArgsConstructor
public class ScalingSummary {

    private int currentParallelism;

    private int newParallelism;

    private Map<ScalingMetric, EvaluatedScalingMetric> metrics;

    /**
     * Why the autoscaler changed the parallelism of this vertex, used to tag the {@code
     * autoscaler.scalings} counter.
     *
     * <p>Not serialized. The scaling history in the state store keeps the shape it had before this
     * field existed, so an operator can roll back without a state migration. The reason is only
     * needed inside the cycle that produces it.
     */
    @JsonIgnore private Set<ScaleReason> scaleReasons = Set.of();

    public ScalingSummary(
            int currentParallelism,
            int newParallelism,
            Map<ScalingMetric, EvaluatedScalingMetric> metrics) {
        this(currentParallelism, newParallelism, metrics, Set.of());
    }

    public ScalingSummary(
            int currentParallelism,
            int newParallelism,
            Map<ScalingMetric, EvaluatedScalingMetric> metrics,
            Set<ScaleReason> scaleReasons) {
        if (currentParallelism == newParallelism) {
            throw new IllegalArgumentException(
                    "Current parallelism should not be equal to newParallelism during scaling.");
        }
        this.currentParallelism = currentParallelism;
        this.newParallelism = newParallelism;
        this.metrics = metrics;
        this.scaleReasons = scaleReasons;
    }

    @JsonIgnore
    public boolean isScaledUp() {
        return newParallelism > currentParallelism;
    }
}
