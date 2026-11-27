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

import java.util.Map;

/**
 * A test {@link ScalingMetricsEvaluatorPlugin} with priority 50 that always throws. It sits between
 * the priority-0 and priority-100 test evaluators so tests can observe that a failing evaluator is
 * isolated and the rest of the chain still runs.
 */
public class TestThrowingEvaluator implements ScalingMetricsEvaluatorPlugin {

    @Override
    public int priority() {
        return 50;
    }

    @Override
    public Map<ScalingMetric, EvaluatedScalingMetric> evaluateVertexMetrics(
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Context<?> evaluationContext) {
        throw new RuntimeException("Custom evaluator failure");
    }
}
