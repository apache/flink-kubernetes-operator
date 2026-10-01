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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.Getter;

import java.util.Map;

/**
 * A pluggable plugin that allows users to provide custom scaling-metric evaluation logic on top of
 * the metrics evaluated internally by the autoscaler. Implementations are invoked once per job
 * vertex during each evaluation cycle, and the metrics they return are merged on top of the
 * internally evaluated metrics, allowing users to override or augment specific {@link
 * ScalingMetric} values.
 *
 * <p>Multiple evaluators can be registered for a single job and are composed into an ordered chain
 * by ascending {@link #priority()} (lower runs first, matching {@code ScalingExecutorPlugin}). Each
 * evaluator is applied on top of the metrics already overridden by the earlier ones, so on a
 * conflicting {@link ScalingMetric} the later (higher-priority-value) evaluator wins. Evaluators
 * with equal priority have no guaranteed relative ordering.
 *
 * <p>Implementations must be stateless. Instances are selected by class FQN, so registering the
 * same class under several instance names reuses a single object for all of them, invoking it once
 * per instance and per vertex in every cycle. Per-instance settings must therefore come from {@code
 * Context#getConfiguration()}, which is scoped to the instance being invoked. A value cached in a
 * field would instead be shared by every instance.
 *
 * <p>This was introduced as part of <a
 * href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-514%3A+Custom+Evaluator+plugin+for+Flink+Autoscaler">FLIP-514:
 * Custom Evaluator plugin for Flink Autoscaler</a> and is complementary to <a
 * href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-575%3A+Scaling+Executor+Plugin+SPI+for+Flink+Autoscaler">FLIP-575:
 * Scaling Executor Plugin SPI for Flink Autoscaler</a> which provides extensibility at the scaling
 * decision execution layer.
 *
 * <p>Implementations are discovered via Java's {@link java.util.ServiceLoader} mechanism. To
 * register a custom metric evaluator, add the fully qualified class name of the implementation to
 * {@code META-INF/services/org.apache.flink.autoscaler.metrics.ScalingMetricsEvaluatorPlugin}.
 */
@Experimental
public interface ScalingMetricsEvaluatorPlugin {

    /**
     * Evaluates scaling metrics for a given job vertex based on the internally evaluated metrics
     * and context.
     *
     * @param vertex The {@link JobVertexID} identifying the vertex whose metrics are being
     *     evaluated.
     * @param evaluatedMetrics An un-modifiable view of current vertex internally evaluated metrics.
     * @param evaluationContext The evaluation context providing job-related configurations and
     *     historical metrics.
     * @return A map of evaluated scaling metrics for the vertex which would get merged with
     *     internally evaluated metrics.
     * @throws UnsupportedOperationException if an attempt is made to modify the {@code
     *     evaluatedMetrics}, the context's configuration, its metric history or its evaluated
     *     vertex metrics.
     */
    Map<ScalingMetric, EvaluatedScalingMetric> evaluateVertexMetrics(
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Context<?> evaluationContext);

    /**
     * Returns the priority of this evaluator in the chain. Evaluators with lower priority values
     * are applied first, and later evaluators see the metrics as already overridden by the earlier
     * ones. The default priority is 0. Evaluators with equal priority have no guaranteed relative
     * ordering.
     *
     * @return the priority value; lower values are applied first.
     */
    default int priority() {
        return 0;
    }

    /**
     * The custom metric evaluator context. It {@code extends} {@link JobAutoScalerContext}, sharing
     * its {@link org.apache.flink.autoscaler.JobAutoScalerContext.ScalingCycleState} and inherited
     * cycle accessors (topology, metric history, restart time). Its {@code getConfiguration()}
     * returns the effective evaluator configuration (the job configuration with this evaluator's
     * {@code job.autoscaler.metrics.custom-evaluator.<name>.} overrides merged on top), enriched
     * with the in-progress evaluated vertex metrics ({@code getEvaluatedVertexMetrics()}) and the
     * backlog flag ({@code isProcessingBacklog()}).
     */
    @Getter
    class Context<KEY> extends JobAutoScalerContext<KEY> {

        /**
         * An un-modifiable view of evaluated metrics for previously evaluated vertices. Note:
         * evaluation of a vertex for scaling metrics happens topologically.
         */
        private final Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>
                evaluatedVertexMetrics;

        /** Indicates whether the job is processing backlog. */
        private final boolean processingBacklog;

        public Context(
                JobAutoScalerContext<KEY> autoScalerContext,
                Configuration overrides,
                Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedVertexMetrics,
                boolean processingBacklog) {
            super(autoScalerContext, overrides);
            this.evaluatedVertexMetrics = evaluatedVertexMetrics;
            this.processingBacklog = processingBacklog;
        }
    }
}
