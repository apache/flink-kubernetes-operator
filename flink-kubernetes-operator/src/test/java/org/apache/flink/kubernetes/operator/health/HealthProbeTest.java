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

package org.apache.flink.kubernetes.operator.health;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.FlinkOperator;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.util.NetUtils;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RuntimeInfo;
import io.javaoperatorsdk.operator.health.InformerHealthIndicator;
import io.javaoperatorsdk.operator.health.InformerWrappingEventSourceHealthIndicator;
import io.javaoperatorsdk.operator.health.Status;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @link Health probe unit tests
 */
@EnableKubernetesMockClient(crud = true)
public class HealthProbeTest {
    KubernetesClient client;

    @BeforeAll
    public static void setAutoTryKubeConfig() {
        System.setProperty(Config.KUBERNETES_AUTH_TRYKUBECONFIG_SYSTEM_PROPERTY, "false");
    }

    @Test
    public void testHealthProbeEndpoint() throws Exception {
        try (var port = NetUtils.getAvailablePort()) {
            var conf = new Configuration();
            conf.set(KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_PORT, port.getPort());

            FlinkOperator operator =
                    new FlinkOperator(conf) {
                        @Override
                        protected Operator createOperator() {
                            return new Operator(
                                    overrider -> overrider.withKubernetesClient(client));
                        }
                    };
            try {
                operator.run();
                assertTrue(callHealthEndpoint(conf));
                assertNotNull(HealthProbe.INSTANCE.getRuntimeInfo());
            } finally {
                operator.stop();
                assertFalse(callHealthEndpoint(conf));
            }
        }
    }

    @Test
    public void testHealthProbeInfomers() {
        var isRunning = new AtomicBoolean(false);
        var unhealthyEventSources =
                new HashMap<String, Map<String, InformerWrappingEventSourceHealthIndicator>>();
        var runtimeInfo =
                new RuntimeInfo(new Operator(overrider -> overrider.withKubernetesClient(client))) {
                    @Override
                    public boolean isStarted() {
                        return isRunning.get();
                    }

                    @Override
                    public Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>>
                            unhealthyInformerWrappingEventSourceHealthIndicator() {
                        return unhealthyEventSources;
                    }
                };

        // Test if a new event source becomes unhealthy
        HealthProbe.INSTANCE.setRuntimeInfo(runtimeInfo);

        assertFalse(HealthProbe.INSTANCE.isHealthy());
        isRunning.set(true);
        assertTrue(HealthProbe.INSTANCE.isHealthy());
        unhealthyEventSources.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        unhealthyEventSources.clear();
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        // Test if we detect when there is an unhealthy at start
        unhealthyEventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY))));
        HealthProbe.INSTANCE.setRuntimeInfo(runtimeInfo);
        assertTrue(HealthProbe.INSTANCE.isHealthy());
        unhealthyEventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i3", Status.UNHEALTHY))));
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        unhealthyEventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i3", Status.HEALTHY))));
        assertTrue(HealthProbe.INSTANCE.isHealthy());
        unhealthyEventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.UNHEALTHY))));
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        unhealthyEventSources.clear();
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        // All informers unhealthy at start:
        unhealthyEventSources.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        HealthProbe.INSTANCE.setRuntimeInfo(runtimeInfo);
        assertFalse(HealthProbe.INSTANCE.isHealthy());
    }

    @Test
    public void testHealthProbeCanary() {
        var runtimeInfo =
                new RuntimeInfo(new Operator(overrider -> overrider.withKubernetesClient(client))) {
                    @Override
                    public boolean isStarted() {
                        return true;
                    }

                    @Override
                    public Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>>
                            unhealthyInformerWrappingEventSourceHealthIndicator() {
                        return Collections.emptyMap();
                    }
                };
        HealthProbe.INSTANCE.setRuntimeInfo(runtimeInfo);

        var canaryManager =
                new CanaryResourceManager<FlinkDeployment>(
                        new FlinkConfigManager(new Configuration()));
        HealthProbe.INSTANCE.registerCanaryResourceManager(canaryManager);

        // No canary resources
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        var canary = TestUtils.createCanaryDeployment();
        canaryManager.handleCanaryResourceReconciliation(ReconciliationUtils.clone(canary), client);

        // Canary resource healthy before health check
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        // No reconciliation before health check
        canaryManager.checkHealth(ResourceID.fromResource(canary), client);
        assertFalse(HealthProbe.INSTANCE.isHealthy());

        // Healthy again
        canary.getMetadata().setGeneration(2L);
        canaryManager.handleCanaryResourceReconciliation(ReconciliationUtils.clone(canary), client);
        canaryManager.checkHealth(ResourceID.fromResource(canary), client);
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        canary.getMetadata().setGeneration(3L);
        canaryManager.handleCanaryResourceReconciliation(ReconciliationUtils.clone(canary), client);
        canaryManager.checkHealth(ResourceID.fromResource(canary), client);
        assertTrue(HealthProbe.INSTANCE.isHealthy());
    }

    private boolean callHealthEndpoint(Configuration conf) throws Exception {
        URL u =
                new URL(
                        "http://localhost:"
                                + conf.get(
                                        KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_PORT)
                                + "/");
        HttpURLConnection connection = (HttpURLConnection) u.openConnection();
        connection.setConnectTimeout(100000);
        connection.connect();
        return connection.getResponseCode() == OK.code();
    }

    private static InformerWrappingEventSourceHealthIndicator informerHealthIndicator(
            Map<String, Status> informerStatuses) {
        Map<String, InformerHealthIndicator> informers = new HashMap<>();
        informerStatuses.forEach(
                (n, s) ->
                        informers.put(
                                n,
                                new InformerHealthIndicator() {
                                    @Override
                                    public boolean hasSynced() {
                                        return false;
                                    }

                                    @Override
                                    public boolean isWatching() {
                                        return false;
                                    }

                                    @Override
                                    public boolean isRunning() {
                                        return false;
                                    }

                                    @Override
                                    public Status getStatus() {
                                        return s;
                                    }

                                    @Override
                                    public String getTargetNamespace() {
                                        return null;
                                    }
                                }));

        return () -> informers;
    }
}
