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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedTaskManagerMetricsHeaders;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.autoscaler.topology.ShipStrategy.HASH;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TestScalingExecutorPlugin}. */
class TestScalingExecutorPluginTest {

    private Configuration conf;
    private JobVertexID source;
    private JobVertexID sink;
    private JobTopology jobTopology;

    @BeforeEach
    void setup() {
        conf = new Configuration();
        source = new JobVertexID();
        sink = new JobVertexID();
        jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 10, 1000, false, null),
                        new VertexInfo(sink, Map.of(source, HASH), 10, 1000, false, null));
    }

    @Test
    void testCpuThresholdBoundary() {
        var context = createContextWithCpuLoad(0.9);
        var plugin = new TestScalingExecutorPlugin<JobID, JobAutoScalerContext<JobID>>(0.9);

        var summaries = createSummaries(source, 10, 20, sink, 10, 20);

        var result =
                plugin.filterScalingDecisions(
                        context, conf, createMetrics(), jobTopology, summaries);

        assertThat(result).isPresent();
    }

    @Test
    void testDefaultThreshold() {
        // Uses default threshold (0.8), CPU 0.7 -> below
        var context = createContextWithCpuLoad(0.7);
        var plugin = new TestScalingExecutorPlugin<JobID, JobAutoScalerContext<JobID>>();

        var summaries = createSummaries(source, 10, 20, sink, 10, 15);

        var result =
                plugin.filterScalingDecisions(
                        context, conf, createMetrics(), jobTopology, summaries);

        assertThat(result).isEmpty();
    }

    @Test
    void testVetoWhenRestClientFails() {
        // REST client that throws an exception
        var context = createContextWithFailingRestClient();
        var plugin = new TestScalingExecutorPlugin<JobID, JobAutoScalerContext<JobID>>(0.5);

        var summaries = createSummaries(source, 10, 20, sink, 10, 15);

        var result =
                plugin.filterScalingDecisions(
                        context, conf, createMetrics(), jobTopology, summaries);

        // Should veto as safety measure when REST query fails
        assertThat(result).isEmpty();
    }

    @Test
    void testVetoWhenAvgCpuBelowThreshold() {
        // CPU 0.5 < threshold 0.8
        var context = createContextWithCpuLoad(0.5);
        var plugin = new TestScalingExecutorPlugin<JobID, JobAutoScalerContext<JobID>>(0.8);

        var summaries = createSummaries(source, 10, 20, sink, 10, 20);

        var result =
                plugin.filterScalingDecisions(
                        context, conf, createMetrics(), jobTopology, summaries);

        assertThat(result).isEmpty();
    }

    @Test
    void testAllowWhenAvgCpuAboveThreshold() {
        // CPU 0.9 > threshold 0.8; source scales up
        var context = createContextWithCpuLoad(0.9);
        var plugin = new TestScalingExecutorPlugin<JobID, JobAutoScalerContext<JobID>>(0.8);

        var summaries = createSummaries(source, 10, 20, sink, 10, 15);

        var result =
                plugin.filterScalingDecisions(
                        context, conf, createMetrics(), jobTopology, summaries);

        assertThat(result).isPresent();
        // Sink should be aligned to source's new parallelism (20)
        assertThat(result.get().get(source).getNewParallelism()).isEqualTo(20);
        assertThat(result.get().get(sink).getNewParallelism()).isEqualTo(20);
    }

    @Test
    void testVetoWhenSourceNotScalingUp() {
        // CPU is high but source scales DOWN (20 -> 10)
        var context = createContextWithCpuLoad(0.9);
        var plugin = new TestScalingExecutorPlugin<JobID, JobAutoScalerContext<JobID>>(0.5);

        var summaries = createSummaries(source, 20, 10, sink, 10, 20);

        var result =
                plugin.filterScalingDecisions(
                        context, conf, createMetrics(), jobTopology, summaries);

        // Source didn't increase -> veto
        assertThat(result).isEmpty();
    }

    @Test
    void testVetoWhenSourceNotInSummaries() {
        // CPU is high but source is not in summaries
        var context = createContextWithCpuLoad(0.9);
        var plugin = new TestScalingExecutorPlugin<JobID, JobAutoScalerContext<JobID>>(0.5);

        var summaries = new HashMap<JobVertexID, ScalingSummary>();
        summaries.put(sink, new ScalingSummary(10, 20, createVertexMetrics()));

        var result =
                plugin.filterScalingDecisions(
                        context, conf, createMetrics(), jobTopology, summaries);

        // No source in summaries -> veto
        assertThat(result).isEmpty();
    }

    @Test
    void testAlignSinkToSourceParallelism() {
        // Source: 10 -> 30, Sink: 10 -> 15 (autoscaler computed differently)
        var context = createContextWithCpuLoad(0.9);
        var plugin = new TestScalingExecutorPlugin<JobID, JobAutoScalerContext<JobID>>(0.5);

        var summaries = createSummaries(source, 10, 30, sink, 10, 15);

        var result =
                plugin.filterScalingDecisions(
                        context, conf, createMetrics(), jobTopology, summaries);

        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(2);
        assertThat(result.get().get(source).getNewParallelism()).isEqualTo(30);
        // Sink aligned to source's new parallelism
        assertThat(result.get().get(sink).getNewParallelism()).isEqualTo(30);
    }

    @Test
    void testSkipVertexAlreadyAtSourceParallelism() {
        // Source: 10 -> 20, Sink: current=20, computed new=25
        var context = createContextWithCpuLoad(0.9);
        var plugin = new TestScalingExecutorPlugin<JobID, JobAutoScalerContext<JobID>>(0.5);

        var summaries = createSummaries(source, 10, 20, sink, 20, 25);

        var result =
                plugin.filterScalingDecisions(
                        context, conf, createMetrics(), jobTopology, summaries);

        assertThat(result).isPresent();
        // Sink's current parallelism (20) == source's new parallelism (20) -> sink skipped
        assertThat(result.get()).hasSize(1);
        assertThat(result.get()).containsKey(source);
    }

    /**
     * Creates a {@link JobAutoScalerContext} with a mocked REST client that returns the given CPU
     * load as the avg value for {@code Status.JVM.CPU.Load}.
     */
    private JobAutoScalerContext<JobID> createContextWithCpuLoad(double cpuLoad) {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup metricGroup = new GenericMetricGroup(registry, null, "test");
        JobID jobID = new JobID();
        return new JobAutoScalerContext<>(
                jobID,
                jobID,
                JobStatus.RUNNING,
                conf,
                metricGroup,
                () -> createRestClientWithCpuMetric(cpuLoad));
    }

    /** Creates a context whose REST client throws on getRestClusterClient(). */
    private JobAutoScalerContext<JobID> createContextWithFailingRestClient() {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup metricGroup = new GenericMetricGroup(registry, null, "test");
        JobID jobID = new JobID();
        return new JobAutoScalerContext<>(
                jobID,
                jobID,
                JobStatus.RUNNING,
                conf,
                metricGroup,
                () -> {
                    throw new RuntimeException("REST client unavailable");
                });
    }

    /**
     * Creates a {@link RestClusterClient} that returns an aggregated TM metrics response with the
     * given CPU load average for the {@code Status.JVM.CPU.Load} metric.
     */
    @SuppressWarnings("unchecked")
    private RestClusterClient<String> createRestClientWithCpuMetric(double cpuLoad) {
        try {
            return new RestClusterClient<>(
                    conf, "test-cluster", (c, e) -> new StandaloneClientHAServices("localhost")) {
                @Override
                public <
                                M extends MessageHeaders<R, P, U>,
                                U extends MessageParameters,
                                R extends RequestBody,
                                P extends ResponseBody>
                        CompletableFuture<P> sendRequest(
                                M messageHeaders, U messageParameters, R request) {
                    if (messageHeaders instanceof AggregatedTaskManagerMetricsHeaders) {
                        var cpuMetric =
                                new AggregatedMetric(
                                        "Status.JVM.CPU.Load",
                                        cpuLoad, // min
                                        cpuLoad, // max
                                        cpuLoad, // avg
                                        cpuLoad, // sum
                                        null);
                        return (CompletableFuture<P>)
                                CompletableFuture.completedFuture(
                                        new AggregatedMetricsResponseBody(List.of(cpuMetric)));
                    }
                    return (CompletableFuture<P>)
                            CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<ScalingMetric, EvaluatedScalingMetric> createVertexMetrics() {
        var metrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        metrics.put(ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(10));
        metrics.put(ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(1000));
        return metrics;
    }

    private EvaluatedMetrics createMetrics() {
        var vertexMetrics = new HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>();
        vertexMetrics.put(source, createVertexMetrics());
        vertexMetrics.put(sink, createVertexMetrics());
        return new EvaluatedMetrics(vertexMetrics, Map.of());
    }

    private Map<JobVertexID, ScalingSummary> createSummaries(
            JobVertexID sourceId,
            int sourceCurrentP,
            int sourceNewP,
            JobVertexID sinkId,
            int sinkCurrentP,
            int sinkNewP) {
        var metrics = createVertexMetrics();
        var summaries = new HashMap<JobVertexID, ScalingSummary>();
        summaries.put(sourceId, new ScalingSummary(sourceCurrentP, sourceNewP, metrics));
        summaries.put(sinkId, new ScalingSummary(sinkCurrentP, sinkNewP, metrics));
        return summaries;
    }
}
