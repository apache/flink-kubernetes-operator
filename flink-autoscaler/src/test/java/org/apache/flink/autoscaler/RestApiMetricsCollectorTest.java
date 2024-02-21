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
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregateTaskManagerMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedTaskManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link RestApiMetricsCollector}. */
public class RestApiMetricsCollectorTest {

    private static final String GC_METRIC_NAME = "Status.JVM.GarbageCollector.All.TimeMsPerSecond";
    private static final String HEAP_MAX_NAME = "Status.JVM.Memory.Heap.Max";
    private static final String HEAP_USED_NAME = "Status.JVM.Memory.Heap.Used";
    private static final String MANAGED_MEMORY_NAME = "Status.Flink.Memory.Managed.Used";
    private static final String METASPACE_MEMORY_NAME = "Status.JVM.Memory.Metaspace.Used";

    @Test
    public void testAggregateMultiplePendingRecordsMetricsPerSource() throws Exception {
        var collector = new RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>>();

        JobVertexID jobVertexID = new JobVertexID();
        var flinkMetrics =
                Map.of(
                        "a.pendingRecords", FlinkMetric.PENDING_RECORDS,
                        "b.pendingRecords", FlinkMetric.PENDING_RECORDS);
        var metrics = Map.of(jobVertexID, flinkMetrics);

        var aggregatedMetricsResponse =
                List.of(
                        new AggregatedMetric(
                                "a.pendingRecords", Double.NaN, Double.NaN, Double.NaN, 100.),
                        new AggregatedMetric(
                                "b.pendingRecords", Double.NaN, Double.NaN, Double.NaN, 100.),
                        new AggregatedMetric(
                                "c.unrelated", Double.NaN, Double.NaN, Double.NaN, 100.));

        var conf = new Configuration();
        var restClusterClient =
                new RestClusterClient<>(
                        conf,
                        "test-cluster",
                        (c, e) -> new StandaloneClientHAServices("localhost")) {
                    @Override
                    public <
                                    M extends MessageHeaders<R, P, U>,
                                    U extends MessageParameters,
                                    R extends RequestBody,
                                    P extends ResponseBody>
                            CompletableFuture<P> sendRequest(
                                    M messageHeaders, U messageParameters, R request) {
                        if (messageHeaders instanceof AggregatedSubtaskMetricsHeaders) {
                            return (CompletableFuture<P>)
                                    CompletableFuture.completedFuture(
                                            new AggregatedMetricsResponseBody(
                                                    aggregatedMetricsResponse));
                        }
                        return (CompletableFuture<P>)
                                CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
                    }
                };

        JobID jobID = new JobID();
        var context =
                new JobAutoScalerContext<>(
                        jobID,
                        jobID,
                        JobStatus.RUNNING,
                        conf,
                        new UnregisteredMetricsGroup(),
                        () -> restClusterClient);

        var jobVertexIDMapMap = collector.queryAllAggregatedMetrics(context, metrics);

        assertEquals(1, jobVertexIDMapMap.size());
        Map<FlinkMetric, AggregatedMetric> vertexMetrics = jobVertexIDMapMap.get(jobVertexID);
        Assertions.assertNotNull(vertexMetrics);
        AggregatedMetric pendingRecordsMetric = vertexMetrics.get(FlinkMetric.PENDING_RECORDS);
        Assertions.assertNotNull(pendingRecordsMetric);
        assertEquals(pendingRecordsMetric.getSum(), 200);
    }

    @Test
    @Timeout(60)
    public void testJmMetricCollection() throws Exception {
        try (MiniCluster miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(3)
                                .build())) {
            miniCluster.start();
            var client =
                    new RestClusterClient<>(
                            new Configuration(),
                            "cluster",
                            (c, e) ->
                                    new StandaloneClientHAServices(
                                            miniCluster.getRestAddress().get().toString()));
            do {
                var collector = new RestApiMetricsCollector<>();
                Map<FlinkMetric, Metric> flinkMetricMetricMap =
                        collector.queryJmMetrics(
                                client,
                                Map.of(
                                        "taskSlotsTotal", FlinkMetric.NUM_TASK_SLOTS_TOTAL,
                                        "taskSlotsAvailable",
                                                FlinkMetric.NUM_TASK_SLOTS_AVAILABLE));
                try {
                    assertEquals(
                            "3",
                            flinkMetricMetricMap.get(FlinkMetric.NUM_TASK_SLOTS_TOTAL).getValue());
                    assertEquals(
                            "3",
                            flinkMetricMetricMap
                                    .get(FlinkMetric.NUM_TASK_SLOTS_AVAILABLE)
                                    .getValue());
                    break;
                } catch (NullPointerException e) {
                    // Metrics might not be available yet (timeout above will eventually kill this
                    // test)
                    Thread.sleep(100);
                }
            } while (true);
        }
    }

    @Test
    public void testTmMetricCollection() throws Exception {

        var metricValues = new HashMap<String, AggregatedMetric>();

        var conf = new Configuration();
        var client =
                new RestClusterClient<>(
                        conf,
                        "test-cluster",
                        (c, e) -> new StandaloneClientHAServices("localhost")) {
                    @Override
                    public <
                                    M extends MessageHeaders<R, P, U>,
                                    U extends MessageParameters,
                                    R extends RequestBody,
                                    P extends ResponseBody>
                            CompletableFuture<P> sendRequest(M headers, U parameters, R request) {
                        if (headers instanceof AggregatedTaskManagerMetricsHeaders) {
                            var p = (AggregateTaskManagerMetricsParameters) parameters;
                            var filterParam =
                                    (MetricsFilterParameter)
                                            p.getQueryParameters().iterator().next();

                            if (filterParam.getValue() == null
                                    || filterParam.getValue().isEmpty()) {
                                fail("Metric names should not be queried");
                            } else {
                                var names = filterParam.getValue();
                                List<AggregatedMetric> out;
                                if (names.stream().allMatch(metricValues::containsKey)) {
                                    out =
                                            filterParam.getValue().stream()
                                                    .map(metricValues::get)
                                                    .collect(Collectors.toList());
                                } else {
                                    out = List.of();
                                }
                                return (CompletableFuture<P>)
                                        CompletableFuture.completedFuture(
                                                new AggregatedMetricsResponseBody(out));
                            }
                        }
                        throw new UnsupportedOperationException();
                    }
                };
        var jobID = new JobID();
        var context =
                new JobAutoScalerContext<>(
                        jobID,
                        jobID,
                        JobStatus.RUNNING,
                        conf,
                        new UnregisteredMetricsGroup(),
                        () -> client);
        var collector = new RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>>();

        assertThrows(RuntimeException.class, () -> collector.queryTmMetrics(context));

        // Test only heap metrics available
        var heapMax = new AggregatedMetric(HEAP_MAX_NAME, null, 100., null, null);
        var heapUsed = new AggregatedMetric(HEAP_USED_NAME, null, 50., null, null);
        var managedUsed = new AggregatedMetric(MANAGED_MEMORY_NAME, null, 42., null, null);
        var metaspaceUsed = new AggregatedMetric(METASPACE_MEMORY_NAME, null, 11., null, null);
        metricValues.put(HEAP_MAX_NAME, heapMax);
        metricValues.put(HEAP_USED_NAME, heapUsed);
        metricValues.put(MANAGED_MEMORY_NAME, managedUsed);
        metricValues.put(METASPACE_MEMORY_NAME, metaspaceUsed);

        assertMetricsEquals(
                Map.of(
                        FlinkMetric.HEAP_MEMORY_MAX,
                        heapMax,
                        FlinkMetric.HEAP_MEMORY_USED,
                        heapUsed,
                        FlinkMetric.MANAGED_MEMORY_USED,
                        managedUsed,
                        FlinkMetric.METASPACE_MEMORY_USED,
                        metaspaceUsed),
                collector.queryTmMetrics(context));
        collector.cleanup(context.getJobKey());

        // Test all metrics available
        var gcTime = new AggregatedMetric(GC_METRIC_NAME, null, 150., null, null);
        metricValues.put(GC_METRIC_NAME, gcTime);

        assertMetricsEquals(
                Map.of(
                        FlinkMetric.HEAP_MEMORY_MAX,
                        heapMax,
                        FlinkMetric.HEAP_MEMORY_USED,
                        heapUsed,
                        FlinkMetric.MANAGED_MEMORY_USED,
                        managedUsed,
                        FlinkMetric.METASPACE_MEMORY_USED,
                        metaspaceUsed,
                        FlinkMetric.TOTAL_GC_TIME_PER_SEC,
                        gcTime),
                collector.queryTmMetrics(context));

        // Make sure we don't query the names again
        collector.queryTmMetrics(context);
        collector.queryTmMetrics(context);
    }

    private static void assertMetricsEquals(
            Map<FlinkMetric, AggregatedMetric> expected,
            Map<FlinkMetric, AggregatedMetric> actual) {
        assertEquals(expected.keySet(), actual.keySet());
        expected.forEach(
                (k, v) -> {
                    var a = actual.get(k);
                    assertEquals(v.getId(), a.getId(), k.name());
                    assertEquals(v.getMin(), a.getMin(), k.name());
                    assertEquals(v.getMax(), a.getMax(), k.name());
                    assertEquals(v.getAvg(), a.getAvg(), k.name());
                    assertEquals(v.getSum(), a.getSum(), k.name());
                });
    }
}
