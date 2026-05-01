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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.SortedMap;

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
 * {@code META-INF/services/org.apache.flink.autoscaler.metrics.FlinkAutoscalerEvaluator}.
 */
public interface FlinkAutoscalerEvaluator {

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
     *     evaluatedMetrics}, {@code Context.jobConf}, {@code Context.metricsHistory}, {@code
     *     Context.evaluatedVertexMetrics}.
     */
    Map<ScalingMetric, EvaluatedScalingMetric> evaluateVertexMetrics(
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Context evaluationContext);

    /**
     * Context providing relevant job and metric information to assist in custom metric evaluation.
     */
    class Context {
        private final Configuration jobConf;
        private final SortedMap<Instant, CollectedMetrics> metricsHistory;
        private final Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>
                evaluatedVertexMetrics;
        private final JobTopology topology;
        private final boolean processingBacklog;
        private final Duration restartTime;
        private final Configuration customEvaluatorConf;

        /**
         * Constructs a new {@link Context} instance.
         *
         * @param jobConf An un-modifiable view of job's configuration.
         * @param metricsHistory An un-modifiable view of historical record of collected metrics,
         *     ordered by timestamp.
         * @param evaluatedVertexMetrics This map contains an un-modifiable view of evaluated
         *     metrics for previously evaluated vertex. Note: evaluation of Vertex for scaling
         *     metrics happens topologically.
         * @param topology The job topology representing the structure of the Flink job.
         * @param processingBacklog Indicates whether the job is processing backlog.
         * @param restartTime Maximum restart time based on scaling records.
         * @param customEvaluatorConf The configuration associated with the custom metric evaluator.
         */
        public Context(
                Configuration jobConf,
                SortedMap<Instant, CollectedMetrics> metricsHistory,
                Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedVertexMetrics,
                JobTopology topology,
                boolean processingBacklog,
                Duration restartTime,
                Configuration customEvaluatorConf) {
            this.jobConf = jobConf;
            this.metricsHistory = metricsHistory;
            this.evaluatedVertexMetrics = evaluatedVertexMetrics;
            this.topology = topology;
            this.processingBacklog = processingBacklog;
            this.restartTime = restartTime;
            this.customEvaluatorConf = customEvaluatorConf;
        }

        public Configuration getJobConf() {
            return jobConf;
        }

        public SortedMap<Instant, CollectedMetrics> getMetricsHistory() {
            return metricsHistory;
        }

        public Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>
                getEvaluatedVertexMetrics() {
            return evaluatedVertexMetrics;
        }

        public JobTopology getTopology() {
            return topology;
        }

        public boolean isProcessingBacklog() {
            return processingBacklog;
        }

        public Duration getRestartTime() {
            return restartTime;
        }

        public Configuration getCustomEvaluatorConf() {
            return customEvaluatorConf;
        }
    }
}
