/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregateTaskManagerMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedTaskManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsAggregationParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A test {@link ScalingExecutorPlugin} implementation that demonstrates a CPU-aware, source-aligned
 * scaling strategy:
 *
 * <ol>
 *   <li>Allow scaling only when the average TaskManager JVM CPU usage (queried via REST API from
 *       {@code Status.JVM.CPU.Load}) is above a configurable threshold (default 0.8, i.e. 80%).
 *   <li>Align all non-source vertex parallelism to match the source vertex's new parallelism.
 *   <li>If the source vertex's parallelism did not increase, veto the scaling entirely.
 * </ol>
 *
 * <p>This plugin is intended for testing and demonstration purposes. In production, deploy it as a
 * plugin JAR with a {@code META-INF/services/org.apache.flink.autoscaler.ScalingExecutorPlugin}
 * file.
 */
public class TestScalingExecutorPlugin<KEY, Context extends JobAutoScalerContext<KEY>>
        implements ScalingExecutorPlugin<KEY, Context> {

    private static final Logger LOG = LoggerFactory.getLogger(TestScalingExecutorPlugin.class);

    /** The Flink TM metric name for JVM process CPU usage (0.0 to 1.0). */
    private static final String TM_CPU_LOAD_METRIC = "Status.JVM.CPU.Load";

    /** Default CPU load threshold. 0.8 = 80%. */
    private static final double DEFAULT_CPU_THRESHOLD = 0.8;

    private final double cpuThreshold;

    public TestScalingExecutorPlugin() {
        this(DEFAULT_CPU_THRESHOLD);
    }

    public TestScalingExecutorPlugin(double cpuThreshold) {
        this.cpuThreshold = cpuThreshold;
    }

    @Override
    public Map<JobVertexID, ScalingSummary> apply(
            ScalingExecutorPlugin.Context<KEY, Context> context,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {

        // Step 1: Query the avg JVM CPU load across all TaskManagers via REST API.
        double avgCpuLoad;
        try {
            avgCpuLoad = queryAverageTmCpuLoad(context.getAutoScalerContext());
        } catch (Exception e) {
            LOG.warn(
                    "Failed to query TaskManager CPU load via REST API. Vetoing scaling as a"
                            + " safety measure.",
                    e);
            return Collections.emptyMap();
        }

        if (avgCpuLoad < cpuThreshold) {
            LOG.info(
                    "Average TM CPU load ({}) is below threshold ({}). Vetoing scaling.",
                    avgCpuLoad,
                    cpuThreshold);
            return Collections.emptyMap();
        }
        LOG.info(
                "Average TM CPU load ({}) is above threshold ({}). Proceeding with scaling.",
                avgCpuLoad,
                cpuThreshold);

        // Step 2: Find the source vertex and its new parallelism.
        JobVertexID sourceVertex = null;
        ScalingSummary sourceSummary = null;
        for (Map.Entry<JobVertexID, ScalingSummary> entry : scalingSummaries.entrySet()) {
            if (context.getJobTopology().isSource(entry.getKey())) {
                sourceVertex = entry.getKey();
                sourceSummary = entry.getValue();
                break;
            }
        }

        // Step 3: If source is not in scaling summaries or didn't scale up, veto.
        if (sourceSummary == null) {
            LOG.info("No source vertex found in scaling summaries. Vetoing scaling.");
            return Collections.emptyMap();
        }
        if (!sourceSummary.isScaledUp()) {
            LOG.info(
                    "Source vertex {} parallelism did not increase (current={}, new={}). "
                            + "Vetoing scaling.",
                    sourceVertex,
                    sourceSummary.getCurrentParallelism(),
                    sourceSummary.getNewParallelism());
            return Collections.emptyMap();
        }
        int sourceNewParallelism = sourceSummary.getNewParallelism();
        LOG.info(
                "Source vertex {} scaled up to parallelism {}. "
                        + "Aligning all other vertices to source parallelism.",
                sourceVertex,
                sourceNewParallelism);

        // Step 4: Align all non-source vertices to the source's new parallelism.
        var alignedSummaries = new HashMap<JobVertexID, ScalingSummary>();
        for (Map.Entry<JobVertexID, ScalingSummary> entry : scalingSummaries.entrySet()) {
            var vertexId = entry.getKey();
            var summary = entry.getValue();

            if (vertexId.equals(sourceVertex)) {
                // Keep source as-is
                alignedSummaries.put(vertexId, summary);
            } else {
                int currentParallelism = summary.getCurrentParallelism();
                if (currentParallelism == sourceNewParallelism) {
                    // Already at source parallelism, skip this vertex
                    LOG.info(
                            "Vertex {} already at source parallelism {}. Skipping.",
                            vertexId,
                            sourceNewParallelism);
                    continue;
                }
                // Create a new ScalingSummary aligned to source parallelism
                alignedSummaries.put(
                        vertexId,
                        new ScalingSummary(
                                currentParallelism, sourceNewParallelism, summary.getMetrics()));
                LOG.info(
                        "Aligned vertex {} parallelism from {} -> {} (source parallelism).",
                        vertexId,
                        summary.getNewParallelism(),
                        sourceNewParallelism);
            }
        }

        if (alignedSummaries.isEmpty()) {
            LOG.info("No vertices require scaling after alignment. Vetoing scaling.");
            return Collections.emptyMap();
        }

        return alignedSummaries;
    }

    /**
     * Queries the average JVM CPU load across all TaskManagers using the Flink REST API's
     * aggregated TM metrics endpoint ({@code /taskmanagers/metrics?get=Status.JVM.CPU.Load}).
     *
     * <p>The metric {@code Status.JVM.CPU.Load} reports the JVM process CPU usage as a value
     * between 0.0 (no CPU) and 1.0 (100% CPU).
     *
     * @return the average CPU load across all TaskManagers, or 0.0 if not available.
     */
    private double queryAverageTmCpuLoad(Context context) throws Exception {
        try (var restClient = context.getRestClusterClient()) {
            var parameters = new AggregateTaskManagerMetricsParameters();
            var queryParamIt = parameters.getQueryParameters().iterator();

            // Set the metric filter to Status.JVM.CPU.Load
            MetricsFilterParameter filterParameter = (MetricsFilterParameter) queryParamIt.next();
            filterParameter.resolve(List.of(TM_CPU_LOAD_METRIC));

            // Request AVG aggregation
            MetricsAggregationParameter aggregationParameter =
                    (MetricsAggregationParameter) queryParamIt.next();
            aggregationParameter.resolve(List.of(MetricsAggregationParameter.AggregationMode.AVG));

            AggregatedMetricsResponseBody response =
                    restClient
                            .sendRequest(
                                    AggregatedTaskManagerMetricsHeaders.getInstance(),
                                    parameters,
                                    EmptyRequestBody.getInstance())
                            .get();

            return response.getMetrics().stream()
                    .filter(m -> TM_CPU_LOAD_METRIC.equals(m.getId()))
                    .findFirst()
                    .map(m -> m.getAvg() != null ? m.getAvg() : 0.0)
                    .orElse(0.0);
        }
    }
}
