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

package org.apache.flink.kubernetes.operator.metrics.lifecycle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.metrics.CustomResourceMetrics;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.metrics.TestingMetricListener;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.LIFECYCLE_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.STATE_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TIME_SECONDS_NAME;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_BLUE_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_GREEN_TO_BLUE;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_INITIAL_DEPLOYMENT;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link BlueGreenLifecycleMetrics}. */
public class BlueGreenLifecycleMetricsTest {

    private static final String TEST_NAMESPACE = "test-namespace";
    private static final String[] TRANSITIONS = {
        TRANSITION_INITIAL_DEPLOYMENT, TRANSITION_BLUE_TO_GREEN, TRANSITION_GREEN_TO_BLUE
    };

    private final Configuration configuration = new Configuration();
    private TestingMetricListener listener;
    private MetricManager<FlinkBlueGreenDeployment> metricManager;

    @BeforeEach
    public void init() {
        listener = new TestingMetricListener(configuration);
        metricManager =
                MetricManager.createFlinkBlueGreenDeploymentMetricManager(
                        configuration, listener.getMetricGroup());
    }

    @Test
    public void testNamespaceHistogramMetricsExist() {
        var deployment = buildBlueGreenDeployment("test-deployment", TEST_NAMESPACE);
        metricManager.onUpdate(deployment);

        for (String transition : TRANSITIONS) {
            assertTrue(
                    listener.getHistogram(
                                    getNamespaceHistogramId(
                                            TEST_NAMESPACE, TRANSITION_GROUP_NAME, transition))
                            .isPresent(),
                    "Transition histogram should exist for: " + transition);
        }

        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            assertTrue(
                    listener.getHistogram(
                                    getNamespaceHistogramId(
                                            TEST_NAMESPACE, STATE_GROUP_NAME, state.name()))
                            .isPresent(),
                    "State time histogram should exist for: " + state);
        }
    }

    @Test
    public void testSystemLevelHistogramsExist() {
        var deployment = buildBlueGreenDeployment("test-deployment", TEST_NAMESPACE);
        metricManager.onUpdate(deployment);

        for (String transition : TRANSITIONS) {
            assertTrue(
                    listener.getHistogram(
                                    getSystemLevelHistogramId(
                                            listener, TRANSITION_GROUP_NAME, transition))
                            .isPresent(),
                    "System-level transition histogram should exist for: " + transition);
        }

        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            assertTrue(
                    listener.getHistogram(
                                    getSystemLevelHistogramId(
                                            listener, STATE_GROUP_NAME, state.name()))
                            .isPresent(),
                    "System-level state time histogram should exist for: " + state);
        }
    }

    @Test
    public void testMultiNamespaceHistogramIsolation() {
        var namespace1 = "ns1";
        var namespace2 = "ns2";
        var ns1HistoId =
                getNamespaceHistogramId(
                        namespace1, TRANSITION_GROUP_NAME, TRANSITION_INITIAL_DEPLOYMENT);

        var dep1 = buildBlueGreenDeployment("dep1", namespace1);
        metricManager.onUpdate(dep1);
        var ns1Histo = listener.getHistogram(ns1HistoId).get();

        var dep2 = buildBlueGreenDeployment("dep2", namespace1);
        metricManager.onUpdate(dep2);
        assertTrue(
                ns1Histo == listener.getHistogram(ns1HistoId).get(),
                "Deployments in same namespace should share histogram instance");

        var dep3 = buildBlueGreenDeployment("dep3", namespace2);
        metricManager.onUpdate(dep3);
        var ns2HistoId =
                getNamespaceHistogramId(
                        namespace2, TRANSITION_GROUP_NAME, TRANSITION_INITIAL_DEPLOYMENT);
        assertTrue(
                ns1Histo != listener.getHistogram(ns2HistoId).get(),
                "Different namespaces should have different histogram instances");
    }

    @Test
    public void testLifecycleMetricsDisabled() {
        var conf = new Configuration();
        conf.set(OPERATOR_LIFECYCLE_METRICS_ENABLED, false);
        var disabledMetricManager =
                MetricManager.createFlinkBlueGreenDeploymentMetricManager(
                        conf, new TestingMetricListener(conf).getMetricGroup());

        assertNull(
                getBlueGreenLifecycleMetrics(disabledMetricManager),
                "BlueGreenLifecycleMetrics should not be registered when disabled");
    }

    private FlinkBlueGreenDeployment buildBlueGreenDeployment(String name, String namespace) {
        var deployment = new FlinkBlueGreenDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withUid(UUID.randomUUID().toString())
                        .withCreationTimestamp(Instant.now().toString())
                        .build());

        var flinkDeploymentSpec =
                FlinkDeploymentSpec.builder()
                        .flinkConfiguration(new ConfigObjectNode())
                        .job(JobSpec.builder().upgradeMode(UpgradeMode.STATELESS).build())
                        .build();

        var bgDeploymentSpec =
                new FlinkBlueGreenDeploymentSpec(
                        new HashMap<>(),
                        null,
                        FlinkDeploymentTemplateSpec.builder().spec(flinkDeploymentSpec).build());

        deployment.setSpec(bgDeploymentSpec);

        var status = new FlinkBlueGreenDeploymentStatus();
        status.setBlueGreenState(INITIALIZING_BLUE);
        deployment.setStatus(status);

        return deployment;
    }

    private String getNamespaceHistogramId(String namespace, String groupName, String metricName) {
        return listener.getNamespaceMetricId(
                FlinkBlueGreenDeployment.class,
                namespace,
                LIFECYCLE_GROUP_NAME,
                groupName,
                metricName,
                TIME_SECONDS_NAME);
    }

    private String getSystemLevelHistogramId(
            TestingMetricListener metricListener, String groupName, String metricName) {
        return metricListener.getMetricId(
                String.format(
                        "%s.%s.%s.%s.%s",
                        FlinkBlueGreenDeployment.class.getSimpleName(),
                        LIFECYCLE_GROUP_NAME,
                        groupName,
                        metricName,
                        TIME_SECONDS_NAME));
    }

    private static BlueGreenLifecycleMetrics getBlueGreenLifecycleMetrics(
            MetricManager<FlinkBlueGreenDeployment> metricManager) {
        for (CustomResourceMetrics<?> metrics : metricManager.getRegisteredMetrics()) {
            if (metrics instanceof BlueGreenLifecycleMetrics) {
                return (BlueGreenLifecycleMetrics) metrics;
            }
        }
        return null;
    }
}
