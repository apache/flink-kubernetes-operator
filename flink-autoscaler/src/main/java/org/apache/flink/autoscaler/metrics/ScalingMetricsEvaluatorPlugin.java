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
 * <p>Only one custom metric evaluator per pipeline is supported for now. If multiple instances are
 * configured, the autoscaler logs a warning and falls back to the first entry, ignoring the rest.
 * Registering multiple implementations via {@code META-INF/services} is fine as they form a
 * registry that different jobs can select from by class FQN, but a single job cannot chain or
 * compose more than one evaluator.
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
