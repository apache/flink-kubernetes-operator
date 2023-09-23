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
import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link ScalingMetricCollector}. */
public class ScalingMetricCollectorTest {

    private JobAutoScalerContext<JobID> context;

    @BeforeEach
    public void setup() {
        context = createDefaultJobAutoScalerContext();
    }

    @Test
    public void testJobTopologyParsingFromJobDetails() throws Exception {
        String s =
                "{\"jid\":\"8b6cdb9a1db8876d3dd803d5e6108ae3\",\"name\":\"State machine job\",\"isStoppable\":false,\"state\":\"RUNNING\",\"start-time\":1686216314565,\"end-time\":-1,\"duration\":25867,\"maxParallelism\":-1,\"now\":1686216340432,\"timestamps\":{\"INITIALIZING\":1686216314565,\"RECONCILING\":0,\"CANCELLING\":0,\"FAILING\":0,\"CREATED\":1686216314697,\"SUSPENDED\":0,\"RUNNING\":1686216314900,\"FAILED\":0,\"CANCELED\":0,\"FINISHED\":0,\"RESTARTING\":0},\"vertices\":[{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"name\":\"Source: Custom Source\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1686216324513,\"end-time\":-1,\"duration\":15919,\"tasks\":{\"DEPLOYING\":0,\"RUNNING\":2,\"FINISHED\":0,\"CANCELING\":0,\"SCHEDULED\":0,\"RECONCILING\":0,\"INITIALIZING\":0,\"CREATED\":0,\"FAILED\":0,\"CANCELED\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":true,\"write-bytes\":297883,\"write-bytes-complete\":true,\"read-records\":0,\"read-records-complete\":true,\"write-records\":22972,\"write-records-complete\":true,\"accumulated-backpressured-time\":0,\"accumulated-idle-time\":0,\"accumulated-busy-time\":\"NaN\"}},{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"name\":\"Flat Map -> Sink: Print to Std. Out\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1686216324570,\"end-time\":-1,\"duration\":15862,\"tasks\":{\"DEPLOYING\":0,\"RUNNING\":2,\"FINISHED\":0,\"CANCELING\":0,\"SCHEDULED\":0,\"RECONCILING\":0,\"INITIALIZING\":0,\"CREATED\":0,\"FAILED\":0,\"CANCELED\":0},\"metrics\":{\"read-bytes\":321025,\"read-bytes-complete\":true,\"write-bytes\":0,\"write-bytes-complete\":true,\"read-records\":22957,\"read-records-complete\":true,\"write-records\":0,\"write-records-complete\":true,\"accumulated-backpressured-time\":0,\"accumulated-idle-time\":28998,\"accumulated-busy-time\":0.0}}],\"status-counts\":{\"DEPLOYING\":0,\"RUNNING\":2,\"FINISHED\":0,\"CANCELING\":0,\"SCHEDULED\":0,\"RECONCILING\":0,\"INITIALIZING\":0,\"CREATED\":0,\"FAILED\":0,\"CANCELED\":0},\"plan\":{\"jid\":\"8b6cdb9a1db8876d3dd803d5e6108ae3\",\"name\":\"State machine job\",\"type\":\"STREAMING\",\"nodes\":[{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Flat Map<br/>+- Sink: Print to Std. Out<br/>\",\"inputs\":[{\"num\":0,\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"ship_strategy\":\"HASH\",\"exchange\":\"pipelined_bounded\"}],\"optimizer_properties\":{}},{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Source: Custom Source<br/>\",\"optimizer_properties\":{}}]}}";
        JobDetailsInfo jobDetailsInfo = new ObjectMapper().readValue(s, JobDetailsInfo.class);

        var metricsCollector = new RestApiMetricsCollector<>();
        metricsCollector.getJobTopology(jobDetailsInfo);
    }

    @Test
    public void testJobUpdateTsLogic() {
        var details =
                new JobDetailsInfo(
                        new JobID(),
                        "",
                        false,
                        org.apache.flink.api.common.JobStatus.RUNNING,
                        0,
                        0,
                        0,
                        0,
                        0,
                        Map.of(JobStatus.RUNNING, 3L, JobStatus.CREATED, 2L),
                        List.of(),
                        Map.of(),
                        new JobPlanInfo.RawJson(""));
        var metricsCollector = new RestApiMetricsCollector<>();
        assertEquals(Instant.ofEpochMilli(3L), metricsCollector.getJobUpdateTs(details));
    }

    @Test
    public void testQueryNamesOnTopologyChange() {
        var metricNameQueryCounter = new AtomicInteger(0);
        RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>> collector =
                new RestApiMetricsCollector<>() {
                    @Override
                    protected Map<String, FlinkMetric> getFilteredVertexMetricNames(
                            RestClusterClient<?> rc, JobID id, JobVertexID jvi, JobTopology t) {
                        metricNameQueryCounter.incrementAndGet();
                        return Map.of();
                    }
                };

        var t1 = new JobTopology(new VertexInfo(new JobVertexID(), Collections.emptySet(), 1, 1));
        var t2 = new JobTopology(new VertexInfo(new JobVertexID(), Collections.emptySet(), 1, 1));

        collector.queryFilteredMetricNames(context, t1);
        assertEquals(1, metricNameQueryCounter.get());
        collector.queryFilteredMetricNames(context, t1);
        collector.queryFilteredMetricNames(context, t1);
        assertEquals(1, metricNameQueryCounter.get());
        collector.queryFilteredMetricNames(context, t2);
        assertEquals(2, metricNameQueryCounter.get());
        collector.queryFilteredMetricNames(context, t2);
        collector.queryFilteredMetricNames(context, t2);
        assertEquals(2, metricNameQueryCounter.get());
    }

    @Test
    public void testRequiredMetrics() {
        List<AggregatedMetric> metricList = new ArrayList<>();
        RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>> testCollector =
                new RestApiMetricsCollector<>() {
                    @Override
                    protected Collection<AggregatedMetric> queryAggregatedMetricNames(
                            RestClusterClient<?> restClient, JobID jobID, JobVertexID jobVertexID) {
                        return metricList;
                    }
                };

        var source = new JobVertexID();
        var sink = new JobVertexID();
        var topology =
                new JobTopology(
                        new VertexInfo(source, Set.of(), 1, 1),
                        new VertexInfo(sink, Set.of(source), 1, 1));

        testRequiredMetrics(
                metricList, getSourceRequiredMetrics(), testCollector, source, topology);
        testRequiredMetrics(metricList, getRequiredMetrics(), testCollector, sink, topology);
    }

    private void testRequiredMetrics(
            List<AggregatedMetric> metricList,
            List<AggregatedMetric> requiredMetrics,
            RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>> testCollector,
            JobVertexID vertex,
            JobTopology topology) {
        for (var m : requiredMetrics) {
            metricList.clear();
            metricList.addAll(requiredMetrics);
            metricList.removeIf(a -> a.getId().equals(m.getId()));
            try {
                testCollector.getFilteredVertexMetricNames(null, new JobID(), vertex, topology);
                fail(m.getId());
            } catch (Exception e) {
                assertTrue(e.getMessage().startsWith("Could not find required metric "));
            }
        }
    }

    private List<AggregatedMetric> getSourceRequiredMetrics() {
        return List.of(
                new AggregatedMetric("busyTimeMsPerSecond"),
                new AggregatedMetric("numRecordsOutPerSecond"),
                new AggregatedMetric("Source__XXX.numRecordsInPerSecond"));
    }

    private List<AggregatedMetric> getRequiredMetrics() {
        return List.of(
                new AggregatedMetric("busyTimeMsPerSecond"),
                new AggregatedMetric("numRecordsInPerSecond"));
    }
}
