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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.jupiter.api.Test;

import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.METRIC_GROUP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link FlinkDeploymentMetrics tests. */
public class FlinkDeploymentMetricsTest {

    @Test
    public void testMetrics() {
        MetricListener metricListener = new MetricListener();
        FlinkDeploymentMetrics metrics =
                new FlinkDeploymentMetrics(metricListener.getMetricGroup());

        assertTrue(metricListener.getGauge(METRIC_GROUP_NAME, "Count").isPresent());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            assertTrue(
                    metricListener
                            .getGauge(METRIC_GROUP_NAME, status.toString(), "Count")
                            .isPresent());
        }

        assertEquals(0, metricListener.getGauge(METRIC_GROUP_NAME, "Count").get().getValue());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            assertEquals(
                    0,
                    metricListener
                            .getGauge(METRIC_GROUP_NAME, status.toString(), "Count")
                            .get()
                            .getValue());
        }

        FlinkDeployment flinkDeployment = TestUtils.buildApplicationCluster();

        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            flinkDeployment.getStatus().setJobManagerDeploymentStatus(status);
            metrics.onUpdate(flinkDeployment);
            assertEquals(
                    1,
                    metricListener
                            .getGauge(METRIC_GROUP_NAME, status.toString(), "Count")
                            .get()
                            .getValue());
            assertEquals(1, metricListener.getGauge(METRIC_GROUP_NAME, "Count").get().getValue());
        }

        metrics.onRemove(flinkDeployment);
        assertEquals(0, metricListener.getGauge(METRIC_GROUP_NAME, "Count").get().getValue());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            assertEquals(
                    0,
                    metricListener
                            .getGauge(METRIC_GROUP_NAME, status.toString(), "Count")
                            .get()
                            .getValue());
        }
    }
}
