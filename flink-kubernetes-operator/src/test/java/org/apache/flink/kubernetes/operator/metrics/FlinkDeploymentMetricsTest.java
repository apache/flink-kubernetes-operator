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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.COUNTER_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.CPU_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.FLINK_MINOR_VERSION_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.FLINK_VERSION_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.MEMORY_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.RESOURCE_USAGE_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.STATUS_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @link FlinkDeploymentMetrics tests.
 */
public class FlinkDeploymentMetricsTest {

    private final Configuration configuration = new Configuration();
    private TestingMetricListener listener;
    private MetricManager<FlinkDeployment> metricManager;

    @BeforeEach
    public void init() {
        listener = new TestingMetricListener(configuration);
        metricManager =
                MetricManager.createFlinkDeploymentMetricManager(
                        configuration, listener.getMetricGroup());
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
        var stateCounter1 =
                listener.getNamespaceMetricId(
                        FlinkDeployment.class,
                        namespace1,
                        "Lifecycle",
                        "State",
                        ResourceLifecycleState.CREATED.name(),
                        "Count");
        var stateCounter2 =
                listener.getNamespaceMetricId(
                        FlinkDeployment.class,
                        namespace1,
                        "Lifecycle",
                        "State",
                        ResourceLifecycleState.CREATED.name(),
                        "Count");
        assertTrue(listener.getGauge(stateCounter1).isEmpty());
        assertTrue(listener.getGauge(stateCounter2).isEmpty());
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
        assertEquals(1L, listener.getGauge(stateCounter1).get().getValue());
        assertEquals(1L, listener.getGauge(stateCounter2).get().getValue());
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
        assertEquals(0L, listener.getGauge(stateCounter1).get().getValue());
        assertEquals(0L, listener.getGauge(stateCounter2).get().getValue());
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
    public void testFlinkVersionMetrics() {
        Map<String, String> ns1Values = new HashMap<>();
        ns1Values.put("deployment1", "    ");
        ns1Values.put("deployment2", "1.14.0");
        ns1Values.put("deployment3", "1.14.0");
        ns1Values.put("deployment4", "1.15.1");
        ns1Values.put("deployment5", "1.15.1");
        ns1Values.put("deployment6", "1.16.0");
        ns1Values.put("deployment7", "1.17.1");
        ns1Values.put("deployment8", "1.14.1");
        ns1Values.put("deployment9", "test");

        Map<String, String> ns2Values = new HashMap<>();
        ns2Values.put("deployment1", "");
        ns2Values.put("deployment2", "1.14");
        ns2Values.put("deployment3", "1.15");
        ns2Values.put("deployment4", "1.15");
        ns2Values.put("deployment5", "1.16");
        ns2Values.put("deployment6", "1.17");
        ns2Values.put("deployment7", "1.14");
        ns2Values.put("deployment8", null);
        ns2Values.put("deployment9", "1.15");

        var namespaceVersions = Map.of("ns1", ns1Values, "ns2", ns2Values);
        var expected =
                Map.of(
                        "ns1",
                                Map.of(
                                        "UNKNOWN", 1, "1.14.0", 2, "1.14.1", 1, "1.15.1", 2,
                                        "1.16.0", 1, "1.17.1", 1, "test", 1),
                        "ns2", Map.of("UNKNOWN", 2, "1.14", 2, "1.15", 3, "1.16", 1, "1.17", 1));
        updateFlinkVersionsAndAssert(FLINK_VERSION_GROUP_NAME, namespaceVersions, expected);

        var expectedMinors =
                Map.of(
                        "ns1", Map.of("MALFORMED", 2, "1.14", 3, "1.15", 2, "1.16", 1, "1.17", 1),
                        "ns2", Map.of("MALFORMED", 2, "1.14", 2, "1.15", 3, "1.16", 1, "1.17", 1));
        updateFlinkVersionsAndAssert(
                FLINK_MINOR_VERSION_GROUP_NAME, namespaceVersions, expectedMinors);

        // Remove invalid version and insert 1.14.1
        namespaceVersions.get("ns1").put("deployment1", "1.14.1");
        expected =
                Map.of(
                        "ns1",
                                Map.of(
                                        "1.14.0", 2, "1.14.1", 2, "1.15.1", 2, "1.16.0", 1,
                                        "1.17.1", 1, "test", 1),
                        "ns2", Map.of("UNKNOWN", 2, "1.14", 2, "1.15", 3, "1.16", 1, "1.17", 1));
        updateFlinkVersionsAndAssert(FLINK_VERSION_GROUP_NAME, namespaceVersions, expected);

        expectedMinors =
                Map.of(
                        "ns1", Map.of("MALFORMED", 1, "1.14", 4, "1.15", 2, "1.16", 1, "1.17", 1),
                        "ns2", Map.of("MALFORMED", 2, "1.14", 2, "1.15", 3, "1.16", 1, "1.17", 1));
        updateFlinkVersionsAndAssert(
                FLINK_MINOR_VERSION_GROUP_NAME, namespaceVersions, expectedMinors);
    }

    private void updateFlinkVersionsAndAssert(
            String metricGroup,
            Map<String, Map<String, String>> namespaceVersions,
            Map<String, Map<String, Integer>> expected) {
        for (var namespaceEntry : namespaceVersions.entrySet()) {
            var namespaceName = namespaceEntry.getKey();
            for (var versionEntry : namespaceEntry.getValue().entrySet()) {
                var deploymentName = versionEntry.getKey();
                var version = versionEntry.getValue();
                var deployment = TestUtils.buildApplicationCluster(deploymentName, namespaceName);

                deployment
                        .getStatus()
                        .getClusterInfo()
                        .put(DashboardConfiguration.FIELD_NAME_FLINK_VERSION, version);

                metricManager.onUpdate(deployment);
            }
        }

        for (var namespaceEntry : expected.entrySet()) {
            var namespaceName = namespaceEntry.getKey();

            for (var versionEntry : namespaceEntry.getValue().entrySet()) {
                var version = versionEntry.getKey();
                var expectedCount = versionEntry.getValue();

                var versionGroupId =
                        listener.getNamespaceMetricId(
                                FlinkDeployment.class,
                                namespaceName,
                                metricGroup,
                                version,
                                COUNTER_NAME);

                assertThat(listener.getGauge(versionGroupId))
                        .map(Gauge::getValue)
                        .hasValue(expectedCount);
            }
        }
    }

    @Test
    public void testResourceMetrics() {
        var namespace1 = "ns1";
        var namespace2 = "ns2";
        var deployment1 = TestUtils.buildApplicationCluster("deployment1", namespace1);
        var deployment2 = TestUtils.buildApplicationCluster("deployment2", namespace1);
        var deployment3 = TestUtils.buildApplicationCluster("deployment3", namespace2);

        deployment1
                .getStatus()
                .getClusterInfo()
                .putAll(
                        Map.of(
                                AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "5",
                                AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY, "1024"));

        deployment2
                .getStatus()
                .getClusterInfo()
                .putAll(
                        Map.of(
                                AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "10",
                                AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY, "2048"));

        deployment3
                .getStatus()
                .getClusterInfo()
                .putAll(
                        Map.of(
                                AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "13",
                                AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY, "4096"));

        var cpuGroupId1 =
                listener.getNamespaceMetricId(
                        FlinkDeployment.class, namespace1, RESOURCE_USAGE_GROUP_NAME, CPU_NAME);
        var memoryGroupId1 =
                listener.getNamespaceMetricId(
                        FlinkDeployment.class, namespace1, RESOURCE_USAGE_GROUP_NAME, MEMORY_NAME);
        var cpuGroupId2 =
                listener.getNamespaceMetricId(
                        FlinkDeployment.class, namespace2, RESOURCE_USAGE_GROUP_NAME, CPU_NAME);
        var memoryGroupId2 =
                listener.getNamespaceMetricId(
                        FlinkDeployment.class, namespace2, RESOURCE_USAGE_GROUP_NAME, MEMORY_NAME);

        assertTrue(listener.getGauge(cpuGroupId1).isEmpty());
        assertTrue(listener.getGauge(memoryGroupId1).isEmpty());
        assertTrue(listener.getGauge(cpuGroupId2).isEmpty());
        assertTrue(listener.getGauge(memoryGroupId2).isEmpty());

        metricManager.onUpdate(deployment1);
        metricManager.onUpdate(deployment2);
        metricManager.onUpdate(deployment3);

        assertEquals(15D, listener.getGauge(cpuGroupId1).get().getValue());
        assertEquals(3072L, listener.getGauge(memoryGroupId1).get().getValue());
        assertEquals(13D, listener.getGauge(cpuGroupId2).get().getValue());
        assertEquals(4096L, listener.getGauge(memoryGroupId2).get().getValue());
    }

    @Test
    public void testResourceMetricsWithInvalidInput() {
        var namespace = "ns";
        var deployment = TestUtils.buildApplicationCluster("deployment", namespace);

        var cpuGroupId =
                listener.getNamespaceMetricId(
                        FlinkDeployment.class, namespace, RESOURCE_USAGE_GROUP_NAME, CPU_NAME);
        var memoryGroupId =
                listener.getNamespaceMetricId(
                        FlinkDeployment.class, namespace, RESOURCE_USAGE_GROUP_NAME, MEMORY_NAME);

        metricManager.onUpdate(deployment);

        assertTrue(listener.getGauge(cpuGroupId).isPresent());
        assertTrue(listener.getGauge(memoryGroupId).isPresent());
        assertEquals(0D, listener.getGauge(cpuGroupId).get().getValue());
        assertEquals(0L, listener.getGauge(memoryGroupId).get().getValue());

        deployment
                .getStatus()
                .getClusterInfo()
                .putAll(
                        Map.of(
                                AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "5",
                                AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY,
                                        "9223372036854775808"));
        metricManager.onUpdate(deployment);

        assertEquals(5D, listener.getGauge(cpuGroupId).get().getValue());
        assertEquals(0L, listener.getGauge(memoryGroupId).get().getValue());

        deployment
                .getStatus()
                .getClusterInfo()
                .putAll(
                        Map.of(
                                AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "null",
                                AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY,
                                        "9223372036854775808"));
        metricManager.onUpdate(deployment);

        assertEquals(0D, listener.getGauge(cpuGroupId).get().getValue());
        assertEquals(0L, listener.getGauge(memoryGroupId).get().getValue());

        deployment
                .getStatus()
                .getClusterInfo()
                .putAll(
                        Map.of(
                                AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "",
                                AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY, "invalid"));
        metricManager.onUpdate(deployment);

        assertEquals(0D, listener.getGauge(cpuGroupId).get().getValue());
        assertEquals(0L, listener.getGauge(memoryGroupId).get().getValue());

        deployment
                .getStatus()
                .getClusterInfo()
                .put(AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "Infinity");
        deployment
                .getStatus()
                .getClusterInfo()
                .remove(AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY);
        metricManager.onUpdate(deployment);

        assertEquals(0D, listener.getGauge(cpuGroupId).get().getValue());
        assertEquals(0L, listener.getGauge(memoryGroupId).get().getValue());
    }

    @Test
    public void testMetricsDisabled() {

        var conf = new Configuration();
        conf.set(OPERATOR_RESOURCE_METRICS_ENABLED, false);
        var listener = new TestingMetricListener(conf);
        var metricManager =
                MetricManager.createFlinkDeploymentMetricManager(conf, listener.getMetricGroup());

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
