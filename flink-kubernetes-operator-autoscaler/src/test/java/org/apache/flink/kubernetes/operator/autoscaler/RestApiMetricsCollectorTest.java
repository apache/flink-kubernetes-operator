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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.FlinkMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/** Tests for RestApiMetrisCollector. */
public class RestApiMetricsCollectorTest {

    @Test
    public void testAggregateMultiplePendingRecordsMetricsPerSource() {
        var collector = new RestApiMetricsCollector();

        JobVertexID jobVertexID = new JobVertexID();
        Map<String, FlinkMetric> flinkMetrics =
                Map.of(
                        "a.pendingRecords", FlinkMetric.PENDING_RECORDS,
                        "b.pendingRecords", FlinkMetric.PENDING_RECORDS);
        Map<JobVertexID, Map<String, FlinkMetric>> metrics = Map.of(jobVertexID, flinkMetrics);

        FlinkDeployment cr = new FlinkDeployment();
        cr.getStatus().getJobStatus().setJobId(new JobID().toHexString());

        TestingFlinkService flinkService = new TestingFlinkService();
        flinkService.setAggregatedMetricsResponse(
                List.of(
                        new AggregatedMetric(
                                "a.pendingRecords", Double.NaN, Double.NaN, Double.NaN, 100.),
                        new AggregatedMetric(
                                "b.pendingRecords", Double.NaN, Double.NaN, Double.NaN, 100.),
                        new AggregatedMetric(
                                "c.unrelated", Double.NaN, Double.NaN, Double.NaN, 100.)));

        Configuration conf = new Configuration();
        conf.set(KubernetesConfigOptions.CLUSTER_ID, "id");

        Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> jobVertexIDMapMap =
                collector.queryAllAggregatedMetrics(cr, flinkService, conf, metrics);

        System.out.println(jobVertexIDMapMap);

        Assertions.assertEquals(1, jobVertexIDMapMap.size());
        Map<FlinkMetric, AggregatedMetric> vertexMetrics = jobVertexIDMapMap.get(jobVertexID);
        Assertions.assertNotNull(vertexMetrics);
        AggregatedMetric pendingRecordsMetric = vertexMetrics.get(FlinkMetric.PENDING_RECORDS);
        Assertions.assertNotNull(pendingRecordsMetric);
        Assertions.assertEquals(pendingRecordsMetric.getSum(), 200);
    }
}
