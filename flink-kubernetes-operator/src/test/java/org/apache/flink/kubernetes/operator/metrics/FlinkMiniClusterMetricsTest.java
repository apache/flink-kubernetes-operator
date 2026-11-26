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
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.flink.kubernetes.operator.metrics.AbstractFlinkClusterMetrics.COUNTER_NAME;
import static org.apache.flink.kubernetes.operator.metrics.AbstractFlinkClusterMetrics.CPU_NAME;
import static org.apache.flink.kubernetes.operator.metrics.AbstractFlinkClusterMetrics.FLINK_MINOR_VERSION_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.AbstractFlinkClusterMetrics.FLINK_VERSION_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.AbstractFlinkClusterMetrics.MALFORMED_MINOR_VERSION;
import static org.apache.flink.kubernetes.operator.metrics.AbstractFlinkClusterMetrics.MEMORY_NAME;
import static org.apache.flink.kubernetes.operator.metrics.AbstractFlinkClusterMetrics.RESOURCE_USAGE_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.AbstractFlinkClusterMetrics.STATE_SIZE;
import static org.apache.flink.kubernetes.operator.metrics.AbstractFlinkClusterMetrics.UNKNOWN_VERSION;
import static org.apache.flink.kubernetes.operator.metrics.FlinkMiniClusterMetrics.STATUS_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkMiniClusterMetrics}. */
class FlinkMiniClusterMetricsTest {

    private final Configuration configuration = new Configuration();
    private TestingMetricListener listener;
    private MetricManager<FlinkMiniCluster> metricManager;

    @BeforeEach
    void init() {
        listener = new TestingMetricListener(configuration);
        metricManager =
                MetricManager.createFlinkMiniClusterMetricManager(
                        configuration, listener.getMetricGroup());
    }

    @Test
    void testCountsByPodDeploymentStatus() {
        var namespace = TestUtils.TEST_NAMESPACE;
        var miniCluster1 = miniCluster("minicluster1", namespace);
        var miniCluster2 = miniCluster("minicluster2", namespace);

        var counterId = namespaceMetricId(namespace, COUNTER_NAME);
        assertThat(listener.getGauge(counterId)).isEmpty();
        for (var status : JobManagerDeploymentStatus.values()) {
            assertThat(listener.getGauge(statusMetricId(namespace, status))).isEmpty();
        }

        metricManager.onUpdate(miniCluster1);
        metricManager.onUpdate(miniCluster2);
        assertThat(gaugeValue(counterId)).isEqualTo(2);
        assertThat(gaugeValue(statusMetricId(namespace, JobManagerDeploymentStatus.MISSING)))
                .isEqualTo(2);

        for (var status : JobManagerDeploymentStatus.values()) {
            miniCluster1.getStatus().setPodDeploymentStatus(status);
            miniCluster2.getStatus().setPodDeploymentStatus(status);
            metricManager.onUpdate(miniCluster1);
            metricManager.onUpdate(miniCluster2);

            for (var other : JobManagerDeploymentStatus.values()) {
                assertThat(gaugeValue(statusMetricId(namespace, other)))
                        .isEqualTo(other == status ? 2 : 0);
            }
            assertThat(gaugeValue(counterId)).isEqualTo(2);
        }

        metricManager.onRemove(miniCluster1);
        metricManager.onRemove(miniCluster2);
        assertThat(gaugeValue(counterId)).isEqualTo(0);
        for (var status : JobManagerDeploymentStatus.values()) {
            assertThat(gaugeValue(statusMetricId(namespace, status))).isEqualTo(0);
        }
    }

    @Test
    void testCountsPerNamespace() {
        var miniCluster1 = miniCluster("minicluster", "ns1");
        var miniCluster2 = miniCluster("minicluster", "ns2");
        var miniCluster3 = miniCluster("other", "ns2");

        metricManager.onUpdate(miniCluster1);
        metricManager.onUpdate(miniCluster2);
        metricManager.onUpdate(miniCluster3);

        assertThat(gaugeValue(namespaceMetricId("ns1", COUNTER_NAME))).isEqualTo(1);
        assertThat(gaugeValue(namespaceMetricId("ns2", COUNTER_NAME))).isEqualTo(2);
        assertThat(
                        gaugeValue(
                                namespaceMetricId(
                                        "ns2",
                                        "Lifecycle",
                                        "State",
                                        ResourceLifecycleState.CREATED.name(),
                                        "Count")))
                .isEqualTo(2L);

        metricManager.onRemove(miniCluster2);
        assertThat(gaugeValue(namespaceMetricId("ns1", COUNTER_NAME))).isEqualTo(1);
        assertThat(gaugeValue(namespaceMetricId("ns2", COUNTER_NAME))).isEqualTo(1);
    }

    @Test
    void testMetricsAreSeparateFromFlinkDeployments() {
        var deploymentMetricManager =
                MetricManager.createFlinkDeploymentMetricManager(
                        configuration, listener.getMetricGroup());
        var namespace = TestUtils.TEST_NAMESPACE;

        metricManager.onUpdate(miniCluster("same-name", namespace));
        deploymentMetricManager.onUpdate(TestUtils.buildApplicationCluster("same-name", namespace));
        deploymentMetricManager.onUpdate(TestUtils.buildApplicationCluster("other", namespace));

        assertThat(gaugeValue(namespaceMetricId(namespace, COUNTER_NAME))).isEqualTo(1);
        assertThat(
                        listener.getGauge(
                                        listener.getNamespaceMetricId(
                                                FlinkDeployment.class, namespace, COUNTER_NAME))
                                .map(Gauge::getValue))
                .hasValue(2);
        // The MiniCluster status group is named for the pod, not the JobManager
        assertThat(
                        listener.getGauge(
                                listener.getNamespaceMetricId(
                                        FlinkMiniCluster.class,
                                        namespace,
                                        FlinkDeploymentMetrics.STATUS_GROUP_NAME,
                                        JobManagerDeploymentStatus.MISSING.name(),
                                        COUNTER_NAME)))
                .isEmpty();
    }

    @Test
    void testFlinkVersionMetrics() {
        var namespace = TestUtils.TEST_NAMESPACE;
        var versions =
                Map.of(
                        "minicluster1", "2.4.0",
                        "minicluster2", "2.4.0",
                        "minicluster3", "2.4.1",
                        "minicluster4", "2.5.0",
                        "minicluster5", "",
                        "minicluster6", "invalid");
        versions.forEach(
                (name, version) -> {
                    var miniCluster = miniCluster(name, namespace);
                    miniCluster
                            .getStatus()
                            .getClusterInfo()
                            .put(DashboardConfiguration.FIELD_NAME_FLINK_VERSION, version);
                    metricManager.onUpdate(miniCluster);
                });

        assertThat(gaugeValue(versionMetricId(namespace, FLINK_VERSION_GROUP_NAME, "2.4.0")))
                .isEqualTo(2);
        assertThat(gaugeValue(versionMetricId(namespace, FLINK_VERSION_GROUP_NAME, "2.4.1")))
                .isEqualTo(1);
        assertThat(gaugeValue(versionMetricId(namespace, FLINK_VERSION_GROUP_NAME, "2.5.0")))
                .isEqualTo(1);
        assertThat(
                        gaugeValue(
                                versionMetricId(
                                        namespace, FLINK_VERSION_GROUP_NAME, UNKNOWN_VERSION)))
                .isEqualTo(1);
        assertThat(gaugeValue(versionMetricId(namespace, FLINK_VERSION_GROUP_NAME, "invalid")))
                .isEqualTo(1);

        assertThat(gaugeValue(versionMetricId(namespace, FLINK_MINOR_VERSION_GROUP_NAME, "2.4")))
                .isEqualTo(3);
        assertThat(gaugeValue(versionMetricId(namespace, FLINK_MINOR_VERSION_GROUP_NAME, "2.5")))
                .isEqualTo(1);
        // Both the unknown and the invalid version have no minor version
        assertThat(
                        gaugeValue(
                                versionMetricId(
                                        namespace,
                                        FLINK_MINOR_VERSION_GROUP_NAME,
                                        MALFORMED_MINOR_VERSION)))
                .isEqualTo(2);

        // A MiniCluster moving to a new version is counted once, under its new version
        var upgraded = miniCluster("minicluster1", namespace);
        upgraded.getStatus()
                .getClusterInfo()
                .put(DashboardConfiguration.FIELD_NAME_FLINK_VERSION, "2.5.0");
        metricManager.onUpdate(upgraded);
        assertThat(gaugeValue(versionMetricId(namespace, FLINK_VERSION_GROUP_NAME, "2.4.0")))
                .isEqualTo(1);
        assertThat(gaugeValue(versionMetricId(namespace, FLINK_VERSION_GROUP_NAME, "2.5.0")))
                .isEqualTo(2);
    }

    @Test
    void testResourceMetrics() {
        var miniCluster1 = miniCluster("minicluster1", "ns1");
        var miniCluster2 = miniCluster("minicluster2", "ns1");
        var miniCluster3 = miniCluster("minicluster3", "ns2");
        setResourceUsage(miniCluster1, "0.5", "536870912", "1024");
        setResourceUsage(miniCluster2, "1", "1073741824", "2048");
        setResourceUsage(miniCluster3, "2", "2147483648", "0");

        metricManager.onUpdate(miniCluster1);
        metricManager.onUpdate(miniCluster2);
        metricManager.onUpdate(miniCluster3);

        assertThat(gaugeValue(resourceMetricId("ns1", CPU_NAME))).isEqualTo(1.5D);
        assertThat(gaugeValue(resourceMetricId("ns1", MEMORY_NAME))).isEqualTo(1610612736L);
        assertThat(gaugeValue(resourceMetricId("ns1", STATE_SIZE))).isEqualTo(3072L);
        assertThat(gaugeValue(resourceMetricId("ns2", CPU_NAME))).isEqualTo(2D);
        assertThat(gaugeValue(resourceMetricId("ns2", MEMORY_NAME))).isEqualTo(2147483648L);
        assertThat(gaugeValue(resourceMetricId("ns2", STATE_SIZE))).isEqualTo(0L);

        metricManager.onRemove(miniCluster1);
        assertThat(gaugeValue(resourceMetricId("ns1", CPU_NAME))).isEqualTo(1D);
        assertThat(gaugeValue(resourceMetricId("ns1", MEMORY_NAME))).isEqualTo(1073741824L);
        assertThat(gaugeValue(resourceMetricId("ns1", STATE_SIZE))).isEqualTo(2048L);
    }

    @Test
    void testResourceMetricsWithInvalidInput() {
        var miniCluster = miniCluster("minicluster", "ns");
        metricManager.onUpdate(miniCluster);
        assertThat(gaugeValue(resourceMetricId("ns", CPU_NAME))).isEqualTo(0D);
        assertThat(gaugeValue(resourceMetricId("ns", MEMORY_NAME))).isEqualTo(0L);

        setResourceUsage(miniCluster, "Infinity", "invalid", "-1");
        metricManager.onUpdate(miniCluster);
        assertThat(gaugeValue(resourceMetricId("ns", CPU_NAME))).isEqualTo(0D);
        assertThat(gaugeValue(resourceMetricId("ns", MEMORY_NAME))).isEqualTo(0L);
        assertThat(gaugeValue(resourceMetricId("ns", STATE_SIZE))).isEqualTo(0L);
    }

    @Test
    void testMetricsDisabled() {
        var conf = new Configuration();
        conf.set(OPERATOR_RESOURCE_METRICS_ENABLED, false);
        var disabledListener = new TestingMetricListener(conf);
        var disabledMetricManager =
                MetricManager.createFlinkMiniClusterMetricManager(
                        conf, disabledListener.getMetricGroup());

        disabledMetricManager.onUpdate(miniCluster("minicluster", TestUtils.TEST_NAMESPACE));

        assertThat(disabledMetricManager.getRegisteredMetrics()).isEmpty();
        assertThat(
                        disabledListener.getGauge(
                                disabledListener.getNamespaceMetricId(
                                        FlinkMiniCluster.class,
                                        TestUtils.TEST_NAMESPACE,
                                        COUNTER_NAME)))
                .isEmpty();
    }

    @Test
    void testLifecycleMetricsDisabled() {
        var conf = new Configuration();
        conf.set(OPERATOR_LIFECYCLE_METRICS_ENABLED, false);
        var metricManagerWithoutLifecycle =
                MetricManager.createFlinkMiniClusterMetricManager(
                        conf, new TestingMetricListener(conf).getMetricGroup());

        assertThat(metricManagerWithoutLifecycle.getRegisteredMetrics())
                .singleElement()
                .isInstanceOf(FlinkMiniClusterMetrics.class);
    }

    private static FlinkMiniCluster miniCluster(String name, String namespace) {
        return TestUtils.buildMiniCluster(name, namespace, FlinkVersion.v2_4, JobState.RUNNING);
    }

    private static void setResourceUsage(
            FlinkMiniCluster miniCluster, String cpu, String memory, String stateSize) {
        miniCluster
                .getStatus()
                .getClusterInfo()
                .putAll(
                        Map.of(
                                AbstractFlinkService.FIELD_NAME_TOTAL_CPU, cpu,
                                AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY, memory,
                                AbstractFlinkService.FIELD_NAME_STATE_SIZE, stateSize));
    }

    private String namespaceMetricId(String namespace, String... names) {
        return listener.getNamespaceMetricId(FlinkMiniCluster.class, namespace, names);
    }

    private String statusMetricId(String namespace, JobManagerDeploymentStatus status) {
        return namespaceMetricId(namespace, STATUS_GROUP_NAME, status.name(), COUNTER_NAME);
    }

    private String versionMetricId(String namespace, String group, String version) {
        return namespaceMetricId(namespace, group, version, COUNTER_NAME);
    }

    private String resourceMetricId(String namespace, String name) {
        return namespaceMetricId(namespace, RESOURCE_USAGE_GROUP_NAME, name);
    }

    private Object gaugeValue(String metricId) {
        return listener.getGauge(metricId).map(Gauge::getValue).orElseThrow();
    }
}
