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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.COUNTER_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.STATUS_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link FlinkDeploymentMetrics tests. */
public class FlinkDeploymentMetricsTest {

    private final Configuration configuration = new Configuration();
    private TestingMetricListener listener;
    private MetricManager<FlinkDeployment> metricManager;

    @BeforeEach
    public void init() {
        listener = new TestingMetricListener(configuration);
        metricManager =
                MetricManager.createFlinkDeploymentMetricManager(
                        new FlinkConfigManager(configuration), listener.getMetricGroup());
    }

    @Test
    public void testMetricsSameNamespace() {
        var namespace = TestUtils.TEST_NAMESPACE;
        var deployment1 = TestUtils.buildApplicationCluster("deployment1", namespace);
        var deployment2 = TestUtils.buildApplicationCluster("deployment2", namespace);

        var counterId =
                listener.getNamespaceMetricId(FlinkDeployment.class, namespace, COUNTER_NAME);
        assertTrue(listener.getGauge(counterId).isEmpty());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            var statusId =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            assertTrue(listener.getGauge(statusId).isEmpty());
        }

        metricManager.onUpdate(deployment1);
        metricManager.onUpdate(deployment2);
        assertEquals(2, listener.getGauge(counterId).get().getValue());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            deployment1.getStatus().setJobManagerDeploymentStatus(status);
            deployment2.getStatus().setJobManagerDeploymentStatus(status);
            metricManager.onUpdate(deployment1);
            metricManager.onUpdate(deployment2);

            var statusId =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            assertEquals(2, listener.getGauge(statusId).get().getValue());
        }

        metricManager.onRemove(deployment1);
        metricManager.onRemove(deployment2);
        assertEquals(0, listener.getGauge(counterId).get().getValue());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            var statusId =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            assertEquals(0, listener.getGauge(statusId).get().getValue());
        }
    }

    @Test
    public void testMetricsMultiNamespace() {
        var namespace1 = "ns1";
        var namespace2 = "ns2";
        var deployment1 = TestUtils.buildApplicationCluster("deployment", namespace1);
        var deployment2 = TestUtils.buildApplicationCluster("deployment", namespace2);

        var counterId1 =
                listener.getNamespaceMetricId(FlinkDeployment.class, namespace1, COUNTER_NAME);
        var counterId2 =
                listener.getNamespaceMetricId(FlinkDeployment.class, namespace2, COUNTER_NAME);
        assertTrue(listener.getGauge(counterId1).isEmpty());
        assertTrue(listener.getGauge(counterId2).isEmpty());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            var statusId1 =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace1,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            var statusId2 =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace2,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            assertTrue(listener.getGauge(statusId1).isEmpty());
            assertTrue(listener.getGauge(statusId2).isEmpty());
        }

        metricManager.onUpdate(deployment1);
        metricManager.onUpdate(deployment2);
        assertEquals(1, listener.getGauge(counterId1).get().getValue());
        assertEquals(1, listener.getGauge(counterId2).get().getValue());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            deployment1.getStatus().setJobManagerDeploymentStatus(status);
            deployment2.getStatus().setJobManagerDeploymentStatus(status);
            metricManager.onUpdate(deployment1);
            metricManager.onUpdate(deployment2);
            var statusId1 =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace1,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            var statusId2 =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace2,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            assertEquals(1, listener.getGauge(statusId1).get().getValue());
            assertEquals(1, listener.getGauge(statusId2).get().getValue());
        }

        metricManager.onRemove(deployment1);
        metricManager.onRemove(deployment2);

        assertEquals(0, listener.getGauge(counterId1).get().getValue());
        assertEquals(0, listener.getGauge(counterId2).get().getValue());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            deployment1.getStatus().setJobManagerDeploymentStatus(status);
            deployment2.getStatus().setJobManagerDeploymentStatus(status);
            var statusId1 =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace1,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            var statusId2 =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace2,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            assertEquals(0, listener.getGauge(statusId1).get().getValue());
            assertEquals(0, listener.getGauge(statusId2).get().getValue());
        }
    }

    @Test
    public void testMetricsDisabled() {

        var conf = new Configuration();
        conf.set(OPERATOR_RESOURCE_METRICS_ENABLED, false);
        var listener = new TestingMetricListener(conf);
        var metricManager =
                MetricManager.createFlinkDeploymentMetricManager(
                        new FlinkConfigManager(conf), listener.getMetricGroup());

        var namespace = TestUtils.TEST_NAMESPACE;
        var deployment = TestUtils.buildApplicationCluster("deployment", namespace);

        var counterId =
                listener.getNamespaceMetricId(FlinkDeployment.class, namespace, COUNTER_NAME);
        metricManager.onUpdate(deployment);
        assertTrue(listener.getGauge(counterId).isEmpty());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            var statusId =
                    listener.getNamespaceMetricId(
                            FlinkDeployment.class,
                            namespace,
                            STATUS_GROUP_NAME,
                            status.name(),
                            COUNTER_NAME);
            assertTrue(listener.getGauge(statusId).isEmpty());
        }
    }
}
