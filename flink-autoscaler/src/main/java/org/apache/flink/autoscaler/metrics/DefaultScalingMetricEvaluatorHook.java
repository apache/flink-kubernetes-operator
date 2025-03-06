/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;

/**
 * Default implementation of {@link ScalingMetricEvaluatorHook}.
 *
 * <p>This hook provides a basic evaluation of scaling metrics. If the metric is {@link
 * ScalingMetric#TARGET_DATA_RATE}, it returns a fixed average value. Otherwise, it returns the
 * existing evaluated metric.
 */
public class DefaultScalingMetricEvaluatorHook implements ScalingMetricEvaluatorHook {
    /**
     * Evaluates the given scaling metric and provides a new {@link EvaluatedScalingMetric}.
     *
     * <p>If the scaling metric is {@link ScalingMetric#TARGET_DATA_RATE}, this implementation
     * returns a constant average value of 100000.0. For all other metrics, it returns the
     * previously evaluated metric unchanged.
     *
     * @param scalingMetric The scaling metric being evaluated.
     * @param evaluatedScalingMetric The currently evaluated scaling metric by autoscaler.
     * @param evaluatorHookContext Context containing additional metadata about the evaluation.
     * @param vertex The job vertex ID associated with the metric, or {@code null} if not
     *     applicable.
     * @return The evaluated scaling metric from the hook.
     */
    @Override
    public EvaluatedScalingMetric evaluate(
            ScalingMetric scalingMetric,
            EvaluatedScalingMetric evaluatedScalingMetric,
            ScalingMetricEvaluatorHookContext evaluatorHookContext,
            @Nullable JobVertexID vertex) {
        if (scalingMetric.equals(ScalingMetric.TARGET_DATA_RATE)) {
            return EvaluatedScalingMetric.avg(100000.0);
        }
        return evaluatedScalingMetric;
    }
}
