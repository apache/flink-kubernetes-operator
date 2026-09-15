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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A test {@link ScalingMetricsEvaluatorPlugin} with priority 100. For source vertices it reads the
 * {@code TARGET_DATA_RATE} as it stands after the earlier evaluators in the chain have run and
 * increments it by one, so tests can observe both the priority ordering and that each evaluator
 * sees the accumulated output of the previous ones.
 */
public class TestChainingEvaluator implements ScalingMetricsEvaluatorPlugin {

    @Override
    public int priority() {
        return 100;
    }

    @Override
    public Map<ScalingMetric, EvaluatedScalingMetric> evaluateVertexMetrics(
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Context<?> evaluationContext) {
        if (!evaluationContext.getJobTopology().isSource(vertex)) {
            return Collections.emptyMap();
        }
        var current = evaluatedMetrics.get(ScalingMetric.TARGET_DATA_RATE);
        if (current == null) {
            return Collections.emptyMap();
        }
        var overrides = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        overrides.put(
                ScalingMetric.TARGET_DATA_RATE,
                EvaluatedScalingMetric.avg(current.getAverage() + 1));
        return overrides;
    }
}
