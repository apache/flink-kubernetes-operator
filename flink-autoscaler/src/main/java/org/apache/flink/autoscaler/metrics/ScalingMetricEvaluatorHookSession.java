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

import lombok.Getter;

/**
 * Represents a session for a {@link ScalingMetricEvaluatorHook}.
 *
 * <p>This class encapsulates an evaluator hook instance along with its associated context,
 * providing a structured way to manage scaling metric evaluations within a session.
 */
@Getter
public class ScalingMetricEvaluatorHookSession {

    /** The scaling metric evaluator hook instance. */
    private final ScalingMetricEvaluatorHook evaluatorHook;

    /** The context associated with the evaluator hook. */
    private final ScalingMetricEvaluatorHookContext evaluatorHookContext;

    /**
     * Constructs a new session for the given evaluator hook and its context.
     *
     * @param evaluatorHook the scaling metric evaluator hook.
     * @param evaluatorHookContext the context providing metadata for evaluation.
     */
    public ScalingMetricEvaluatorHookSession(
            ScalingMetricEvaluatorHook evaluatorHook,
            ScalingMetricEvaluatorHookContext evaluatorHookContext) {
        this.evaluatorHook = evaluatorHook;
        this.evaluatorHookContext = evaluatorHookContext;
    }
}
