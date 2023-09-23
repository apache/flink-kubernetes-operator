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
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Tests for {@link RestApiMetricsCollector}. */
public class RestApiMetricsCollectorTest {

    @Test
    public void testAggregateMultiplePendingRecordsMetricsPerSource() throws Exception {
        RestApiMetricsCollector<JobID, JobAutoScalerContext<JobID>> collector =
                new RestApiMetricsCollector<>();

        JobVertexID jobVertexID = new JobVertexID();
        Map<String, FlinkMetric> flinkMetrics =
                Map.of(
                        "a.pendingRecords", FlinkMetric.PENDING_RECORDS,
                        "b.pendingRecords", FlinkMetric.PENDING_RECORDS);
        Map<JobVertexID, Map<String, FlinkMetric>> metrics = Map.of(jobVertexID, flinkMetrics);

        List<AggregatedMetric> aggregatedMetricsResponse =
                List.of(
                        new AggregatedMetric(
                                "a.pendingRecords", Double.NaN, Double.NaN, Double.NaN, 100.),
                        new AggregatedMetric(
                                "b.pendingRecords", Double.NaN, Double.NaN, Double.NaN, 100.),
                        new AggregatedMetric(
                                "c.unrelated", Double.NaN, Double.NaN, Double.NaN, 100.));

        Configuration conf = new Configuration();
        RestClusterClient<String> restClusterClient =
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
        JobAutoScalerContext<JobID> context =
                new JobAutoScalerContext<>(
                        jobID,
                        jobID,
                        JobStatus.RUNNING,
                        conf,
                        new UnregisteredMetricsGroup(),
                        () -> restClusterClient);

        Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> jobVertexIDMapMap =
                collector.queryAllAggregatedMetrics(context, metrics);

        Assertions.assertEquals(1, jobVertexIDMapMap.size());
        Map<FlinkMetric, AggregatedMetric> vertexMetrics = jobVertexIDMapMap.get(jobVertexID);
        Assertions.assertNotNull(vertexMetrics);
        AggregatedMetric pendingRecordsMetric = vertexMetrics.get(FlinkMetric.PENDING_RECORDS);
        Assertions.assertNotNull(pendingRecordsMetric);
        Assertions.assertEquals(pendingRecordsMetric.getSum(), 200);
    }
}
