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
import org.apache.flink.autoscaler.metrics.MetricNotFoundException;
import org.apache.flink.autoscaler.topology.IOMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.topology.ShipStrategy.HASH;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                "{\n"
                        + "  \"jid\": \"bb8f15efbb37f2ce519f55cdc0e049bf\",\n"
                        + "  \"name\": \"State machine job\",\n"
                        + "  \"isStoppable\": false,\n"
                        + "  \"state\": \"RUNNING\",\n"
                        + "  \"start-time\": 1707893512027,\n"
                        + "  \"end-time\": -1,\n"
                        + "  \"duration\": 214716,\n"
                        + "  \"maxParallelism\": -1,\n"
                        + "  \"now\": 1707893726743,\n"
                        + "  \"timestamps\": {\n"
                        + "    \"SUSPENDED\": 0,\n"
                        + "    \"CREATED\": 1707893512139,\n"
                        + "    \"FAILING\": 0,\n"
                        + "    \"FAILED\": 0,\n"
                        + "    \"INITIALIZING\": 1707893512027,\n"
                        + "    \"RECONCILING\": 0,\n"
                        + "    \"RUNNING\": 1707893512217,\n"
                        + "    \"RESTARTING\": 0,\n"
                        + "    \"CANCELLING\": 0,\n"
                        + "    \"FINISHED\": 0,\n"
                        + "    \"CANCELED\": 0\n"
                        + "  },\n"
                        + "  \"vertices\": [\n"
                        + "    {\n"
                        + "      \"id\": \"bc764cd8ddf7a0cff126f51c16239658\",\n"
                        + "      \"name\": \"Source: Custom Source\",\n"
                        + "      \"maxParallelism\": 128,\n"
                        + "      \"parallelism\": 2,\n"
                        + "      \"status\": \"FINISHED\",\n"
                        + "      \"start-time\": 1707893517277,\n"
                        + "      \"end-time\": -1,\n"
                        + "      \"duration\": 209466,\n"
                        + "      \"tasks\": {\n"
                        + "        \"DEPLOYING\": 0,\n"
                        + "        \"INITIALIZING\": 0,\n"
                        + "        \"SCHEDULED\": 0,\n"
                        + "        \"CANCELING\": 0,\n"
                        + "        \"CANCELED\": 0,\n"
                        + "        \"RECONCILING\": 0,\n"
                        + "        \"RUNNING\": 2,\n"
                        + "        \"FAILED\": 0,\n"
                        + "        \"CREATED\": 0,\n"
                        + "        \"FINISHED\": 0\n"
                        + "      },\n"
                        + "      \"metrics\": {\n"
                        + "        \"read-bytes\": 0,\n"
                        + "        \"read-bytes-complete\": true,\n"
                        + "        \"write-bytes\": 4036982,\n"
                        + "        \"write-bytes-complete\": true,\n"
                        + "        \"read-records\": 0,\n"
                        + "        \"read-records-complete\": true,\n"
                        + "        \"write-records\": 291629,\n"
                        + "        \"write-records-complete\": true,\n"
                        + "        \"accumulated-backpressured-time\": 0,\n"
                        + "        \"accumulated-idle-time\": 0,\n"
                        + "        \"accumulated-busy-time\": \"NaN\"\n"
                        + "      }\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"id\": \"20ba6b65f97481d5570070de90e4e791\",\n"
                        + "      \"name\": \"Flat Map -> Sink: Print to Std. Out\",\n"
                        + "      \"maxParallelism\": 128,\n"
                        + "      \"parallelism\": 2,\n"
                        + "      \"status\": \"RUNNING\",\n"
                        + "      \"start-time\": 1707893517280,\n"
                        + "      \"end-time\": -1,\n"
                        + "      \"duration\": 209463,\n"
                        + "      \"tasks\": {\n"
                        + "        \"DEPLOYING\": 0,\n"
                        + "        \"INITIALIZING\": 0,\n"
                        + "        \"SCHEDULED\": 0,\n"
                        + "        \"CANCELING\": 0,\n"
                        + "        \"CANCELED\": 0,\n"
                        + "        \"RECONCILING\": 0,\n"
                        + "        \"RUNNING\": 2,\n"
                        + "        \"FAILED\": 0,\n"
                        + "        \"CREATED\": 0,\n"
                        + "        \"FINISHED\": 0\n"
                        + "      },\n"
                        + "      \"metrics\": {\n"
                        + "        \"read-bytes\": 4078629,\n"
                        + "        \"read-bytes-complete\": true,\n"
                        + "        \"write-bytes\": 0,\n"
                        + "        \"write-bytes-complete\": true,\n"
                        + "        \"read-records\": 291532,\n"
                        + "        \"read-records-complete\": true,\n"
                        + "        \"write-records\": 1,\n"
                        + "        \"write-records-complete\": true,\n"
                        + "        \"accumulated-backpressured-time\": 0,\n"
                        + "        \"accumulated-idle-time\": 407702,\n"
                        + "        \"accumulated-busy-time\": 2\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ],\n"
                        + "  \"status-counts\": {\n"
                        + "    \"DEPLOYING\": 0,\n"
                        + "    \"INITIALIZING\": 0,\n"
                        + "    \"SCHEDULED\": 0,\n"
                        + "    \"CANCELING\": 0,\n"
                        + "    \"CANCELED\": 0,\n"
                        + "    \"RECONCILING\": 0,\n"
                        + "    \"RUNNING\": 2,\n"
                        + "    \"FAILED\": 0,\n"
                        + "    \"CREATED\": 0,\n"
                        + "    \"FINISHED\": 0\n"
                        + "  },\n"
                        + "  \"plan\": {\n"
                        + "    \"jid\": \"bb8f15efbb37f2ce519f55cdc0e049bf\",\n"
                        + "    \"name\": \"State machine job\",\n"
                        + "    \"type\": \"STREAMING\",\n"
                        + "    \"nodes\": [\n"
                        + "      {\n"
                        + "        \"id\": \"20ba6b65f97481d5570070de90e4e791\",\n"
                        + "        \"parallelism\": 2,\n"
                        + "        \"operator\": \"\",\n"
                        + "        \"operator_strategy\": \"\",\n"
                        + "        \"description\": \"Flat Map<br/>+- Sink: Print to Std. Out<br/>\",\n"
                        + "        \"inputs\": [\n"
                        + "          {\n"
                        + "            \"num\": 0,\n"
                        + "            \"id\": \"bc764cd8ddf7a0cff126f51c16239658\",\n"
                        + "            \"ship_strategy\": \"HASH\",\n"
                        + "            \"exchange\": \"pipelined_bounded\"\n"
                        + "          }\n"
                        + "        ],\n"
                        + "        \"optimizer_properties\": {}\n"
                        + "      },\n"
                        + "      {\n"
                        + "        \"id\": \"bc764cd8ddf7a0cff126f51c16239658\",\n"
                        + "        \"parallelism\": 2,\n"
                        + "        \"operator\": \"\",\n"
                        + "        \"operator_strategy\": \"\",\n"
                        + "        \"description\": \"Source: Custom Source<br/>\",\n"
                        + "        \"optimizer_properties\": {}\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }\n"
                        + "}";
        JobDetailsInfo jobDetailsInfo = new ObjectMapper().readValue(s, JobDetailsInfo.class);

        var metricsCollector = new RestApiMetricsCollector<>();

        var sourceId = JobVertexID.fromHexString("bc764cd8ddf7a0cff126f51c16239658");
        var sinkId = JobVertexID.fromHexString("20ba6b65f97481d5570070de90e4e791");

        var source = new VertexInfo(sourceId, Map.of(), 2, 128, true, IOMetrics.FINISHED_METRICS);
        var sink =
                new VertexInfo(
                        sinkId, Map.of(sourceId, HASH), 2, 128, false, new IOMetrics(291532, 1, 2));

        assertEquals(
                new JobTopology(source, sink), metricsCollector.getJobTopology(jobDetailsInfo));
    }

    @Test
    public void testJobTopologyParsingFromJobDetailsWithSlotSharingGroup() throws Exception {
        String s =
                "{\"jid\":\"a1b1b53c7c71e7199aa8c43bc703fe7f\",\"name\":\"basic-example\",\"isStoppable\":false,\"state\":\"RUNNING\",\"start-time\":1697114719143,\"end-time\":-1,\"duration\":60731,\"maxParallelism\":-1,\"now\":1697114779874,\"timestamps\":{\"CANCELLING\":0,\"INITIALIZING\":1697114719143,\"RUNNING\":1697114719743,\"CANCELED\":0,\"FINISHED\":0,\"FAILED\":0,\"RESTARTING\":0,\"FAILING\":0,\"CREATED\":1697114719343,\"SUSPENDED\":0,\"RECONCILING\":0},\"vertices\":[{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"slotSharingGroupId\":\"a9c52ec4c7200ab4bd141cbae8022105\",\"name\":\"Source: Events Generator Source\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1697114724603,\"end-time\":-1,\"duration\":55271,\"tasks\":{\"FAILED\":0,\"CANCELED\":0,\"SCHEDULED\":0,\"FINISHED\":0,\"CREATED\":0,\"DEPLOYING\":0,\"CANCELING\":0,\"RECONCILING\":0,\"INITIALIZING\":0,\"RUNNING\":2},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":true,\"write-bytes\":1985978,\"write-bytes-complete\":true,\"read-records\":0,\"read-records-complete\":true,\"write-records\":92037,\"write-records-complete\":true,\"accumulated-backpressured-time\":0,\"accumulated-idle-time\":78319,\"accumulated-busy-time\":13347.0}},{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"slotSharingGroupId\":\"a9c52ec4c7200ab4bd141cbae8022105\",\"name\":\"Flat Map -> Sink: Print to Std. Out\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1697114724639,\"end-time\":-1,\"duration\":55235,\"tasks\":{\"FAILED\":0,\"CANCELED\":0,\"SCHEDULED\":0,\"FINISHED\":0,\"CREATED\":0,\"DEPLOYING\":0,\"CANCELING\":0,\"RECONCILING\":0,\"INITIALIZING\":0,\"RUNNING\":2},\"metrics\":{\"read-bytes\":2019044,\"read-bytes-complete\":true,\"write-bytes\":0,\"write-bytes-complete\":true,\"read-records\":91881,\"read-records-complete\":true,\"write-records\":0,\"write-records-complete\":true,\"accumulated-backpressured-time\":0,\"accumulated-idle-time\":91352,\"accumulated-busy-time\":273.0}}],\"status-counts\":{\"FAILED\":0,\"CANCELED\":0,\"SCHEDULED\":0,\"FINISHED\":0,\"CREATED\":0,\"DEPLOYING\":0,\"CANCELING\":0,\"RECONCILING\":0,\"INITIALIZING\":0,\"RUNNING\":2},\"plan\":{\"jid\":\"a1b1b53c7c71e7199aa8c43bc703fe7f\",\"name\":\"basic-example\",\"type\":\"STREAMING\",\"nodes\":[{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Flat Map<br/>+- Sink: Print to Std. Out<br/>\",\"operator_metadata\":[{},{}],\"inputs\":[{\"num\":0,\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"ship_strategy\":\"HASH\",\"exchange\":\"pipelined_bounded\"}],\"optimizer_properties\":{}},{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Source: Events Generator Source<br/>\",\"operator_metadata\":[{}],\"optimizer_properties\":{}}]}}\n";
        JobDetailsInfo jobDetailsInfo = new ObjectMapper().readValue(s, JobDetailsInfo.class);

        var metricsCollector = new RestApiMetricsCollector();
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
        assertEquals(Instant.ofEpochMilli(3L), metricsCollector.getJobRunningTs(details));
    }

    @Test
    public void testQueryNamesOnTopologyChange() {
        var metricNameQueryCounter = new HashMap<JobVertexID, Integer>();
        var collector =
                new RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>>() {
                    @Override
                    protected Map<String, FlinkMetric> getFilteredVertexMetricNames(
                            RestClusterClient<?> rc, JobID id, JobVertexID jvi, JobTopology t) {
                        metricNameQueryCounter.compute(jvi, (j, c) -> c + 1);
                        return Map.of();
                    }
                };

        var source = new JobVertexID();
        var source2 = new JobVertexID();
        var sink = new JobVertexID();
        metricNameQueryCounter.put(source, 0);
        metricNameQueryCounter.put(source2, 0);
        metricNameQueryCounter.put(sink, 0);

        var t1 =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 1, 1),
                        new VertexInfo(sink, Map.of(source, REBALANCE), 1, 1));

        var t2 =
                new JobTopology(
                        new VertexInfo(source2, Map.of(), 1, 1),
                        new VertexInfo(sink, Map.of(source2, REBALANCE), 1, 1));

        collector.queryFilteredMetricNames(context, t1);
        assertEquals(1, metricNameQueryCounter.get(source));
        assertEquals(0, metricNameQueryCounter.get(source2));
        assertEquals(1, metricNameQueryCounter.get(sink));

        collector.queryFilteredMetricNames(context, t1);
        collector.queryFilteredMetricNames(context, t1);
        // Make sure source metrics are refreshed
        assertEquals(3, metricNameQueryCounter.get(source));
        assertEquals(0, metricNameQueryCounter.get(source2));
        assertEquals(1, metricNameQueryCounter.get(sink));

        // Topology change
        collector.queryFilteredMetricNames(context, t2);
        assertEquals(3, metricNameQueryCounter.get(source));
        assertEquals(1, metricNameQueryCounter.get(source2));
        assertEquals(2, metricNameQueryCounter.get(sink));

        collector.queryFilteredMetricNames(context, t2);
        assertEquals(3, metricNameQueryCounter.get(source));
        assertEquals(2, metricNameQueryCounter.get(source2));
        assertEquals(2, metricNameQueryCounter.get(sink));

        // Mark source finished, should not be queried again
        t2 =
                new JobTopology(
                        new VertexInfo(source2, Map.of(), 1, 1, true, null),
                        new VertexInfo(sink, Map.of(source2, REBALANCE), 1, 1));
        collector.queryFilteredMetricNames(context, t2);
        assertEquals(3, metricNameQueryCounter.get(source));
        assertEquals(2, metricNameQueryCounter.get(source2));
        assertEquals(2, metricNameQueryCounter.get(sink));
    }

    @Test
    public void testRequiredMetrics() {
        List<String> metricList = new ArrayList<>();
        RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>> testCollector =
                new RestApiMetricsCollector<>() {
                    @Override
                    protected Collection<String> queryAggregatedMetricNames(
                            RestClusterClient<?> restClient, JobID jobID, JobVertexID jobVertexID) {
                        return metricList;
                    }
                };

        var source = new JobVertexID();
        var sink = new JobVertexID();
        var topology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 1, 1),
                        new VertexInfo(sink, Map.of(source, REBALANCE), 1, 1));

        testRequiredMetrics(
                metricList, getSourceRequiredMetrics(), testCollector, source, topology);
        testRequiredMetrics(metricList, getRequiredMetrics(), testCollector, sink, topology);
    }

    @Test
    public void testQueryMetricNames() throws Exception {
        var testCollector = new RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>>();
        var response = new ArrayList<AggregatedMetric>();
        var client =
                new RestClusterClient<>(
                        new Configuration(),
                        "test-cluster",
                        (c, e) -> new StandaloneClientHAServices("localhost")) {
                    @Override
                    public <
                                    M extends MessageHeaders<R, P, U>,
                                    U extends MessageParameters,
                                    R extends RequestBody,
                                    P extends ResponseBody>
                            CompletableFuture<P> sendRequest(M headers, U parameters, R request) {
                        if (headers instanceof AggregatedSubtaskMetricsHeaders) {
                            return (CompletableFuture<P>)
                                    CompletableFuture.completedFuture(
                                            new AggregatedMetricsResponseBody(response));
                        }
                        throw new UnsupportedOperationException();
                    }
                };

        response.add(new AggregatedMetric("a"));
        response.add(new AggregatedMetric("b"));

        assertEquals(
                Set.of("a", "b"),
                testCollector.queryAggregatedMetricNames(client, new JobID(), new JobVertexID()));
    }

    private void testRequiredMetrics(
            List<String> metricList,
            List<String> requiredMetrics,
            RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>> testCollector,
            JobVertexID vertex,
            JobTopology topology) {
        for (var m : requiredMetrics) {
            metricList.clear();
            metricList.addAll(requiredMetrics);
            metricList.remove(m);
            try {
                testCollector.getFilteredVertexMetricNames(null, new JobID(), vertex, topology);
                fail(m);
            } catch (Exception e) {
                assertTrue(e.getMessage().startsWith("Could not find required metric "));
            }
        }
    }

    private List<String> getSourceRequiredMetrics() {
        return List.of(
                "busyTimeMsPerSecond",
                "numRecordsOutPerSecond",
                "Source__XXX.numRecordsInPerSecond");
    }

    private List<String> getRequiredMetrics() {
        return List.of("busyTimeMsPerSecond");
    }

    @Test
    public void testThrowsMetricNotFoundException() {
        var source = new JobVertexID();
        var sink = new JobVertexID();
        var topology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 1, 1),
                        new VertexInfo(sink, Map.of(source, REBALANCE), 1, 1));

        var metricCollector = new TestingMetricsCollector<>(topology);

        assertThrows(
                MetricNotFoundException.class,
                () ->
                        metricCollector.getFilteredVertexMetricNames(
                                null, new JobID(), source, topology));
    }
}
