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

import org.apache.flink.autoscaler.topology.JobTopology;

import lombok.Getter;

/**
 * Context class providing metadata and configuration details for a {@link
 * ScalingMetricEvaluatorHook}.
 *
 * <p>This class encapsulates information about the Flink job topology, the hook's name, its
 * configuration, and the implementing class. It is passed to scaling metric evaluators to provide
 * context for their decision-making process.
 *
 * @see ScalingMetricEvaluatorHook
 */
@Getter
public class ScalingMetricEvaluatorHookContext {

    /** The job topology representing the structure of the Flink job. */
    private final JobTopology topology;

    /** The name of the evaluator hook. */
    private final String evaluatorHookName;

    /** The configuration associated with the evaluator hook. */
    private final ScalingMetricEvaluatorHookConfig evaluatorHookConfig;

    /** The fully qualified class name of the evaluator hook implementation. */
    private final String evaluatorHookClass;

    /**
     * Constructs a new {@code ScalingMetricEvaluatorHookContext}.
     *
     * @param topology The job topology representing the structure of the Flink job.
     * @param evaluatorHookClass The fully qualified class name of the evaluator hook
     *     implementation.
     * @param evaluatorHookName The name of the evaluator hook.
     * @param evaluatorHookConfig The configuration for the evaluator hook.
     */
    public ScalingMetricEvaluatorHookContext(
            JobTopology topology,
            String evaluatorHookClass,
            String evaluatorHookName,
            ScalingMetricEvaluatorHookConfig evaluatorHookConfig) {
        this.topology = topology;
        this.evaluatorHookClass = evaluatorHookClass;
        this.evaluatorHookName = evaluatorHookName;
        this.evaluatorHookConfig = evaluatorHookConfig;
    }
}
