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
import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.Getter;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.SortedMap;

/**
 * Interface for custom evaluators that allow custom scaling metric evaluations. Implementations of
 * this interface can provide custom logic to evaluate vertex metrics and merge them with internally
 * evaluated metrics.
 */
public interface CustomEvaluator extends Plugin {

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
    @Getter
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
         * @param customEvaluatorConf The configuration associated with the custom evaluator.
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
    }
}
