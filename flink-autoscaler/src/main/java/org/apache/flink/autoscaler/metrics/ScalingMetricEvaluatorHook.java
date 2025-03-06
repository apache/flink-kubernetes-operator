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

import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;

/**
 * A hook interface for evaluating scaling metrics in the Flink Autoscaler. Implementations of this
 * interface can modify or override the default scaling metric evaluation logic.
 *
 * <p>Hooks implementing this interface are loaded as plugins.
 */
public interface ScalingMetricEvaluatorHook extends Plugin {

    /**
     * Evaluates the given scaling metric and provides a new {@link EvaluatedScalingMetric}.
     *
     * @param scalingMetric The scaling metric being evaluated.
     * @param evaluatedScalingMetric The currently evaluated scaling metric by autoscaler.
     * @param evaluatorHookContext Context containing additional metadata about the evaluation.
     * @param vertex The job vertex ID associated with the metric, or {@code null} if not
     *     applicable.
     * @return The evaluated scaling metric from the hook.
     */
    EvaluatedScalingMetric evaluate(
            ScalingMetric scalingMetric,
            EvaluatedScalingMetric evaluatedScalingMetric,
            ScalingMetricEvaluatorHookContext evaluatorHookContext,
            @Nullable JobVertexID vertex);
}
