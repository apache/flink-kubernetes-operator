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
import org.apache.flink.util.NetUtils;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RegisteredController;
import io.javaoperatorsdk.operator.RuntimeInfo;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.health.ControllerHealthInfo;
import io.javaoperatorsdk.operator.health.InformerHealthIndicator;
import io.javaoperatorsdk.operator.health.InformerWrappingEventSourceHealthIndicator;
import io.javaoperatorsdk.operator.health.Status;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @link Health probe unit tests
 */
@SuppressWarnings("rawtypes")
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
        var eventSources =
                new HashMap<String, Map<String, InformerWrappingEventSourceHealthIndicator>>();
        var runtimeInfo = runtimeInfo(isRunning, eventSources);

        // Test if a new event source becomes unhealthy
        HealthProbe.INSTANCE.setRuntimeInfo(runtimeInfo);

        assertFalse(HealthProbe.INSTANCE.isHealthy());
        isRunning.set(true);
        assertTrue(HealthProbe.INSTANCE.isHealthy());
        eventSources.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        eventSources.clear();
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        // Test if we detect when there is an unhealthy at start
        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY))));
        HealthProbe.INSTANCE.setRuntimeInfo(runtimeInfo);
        assertTrue(HealthProbe.INSTANCE.isHealthy());
        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i3", Status.UNHEALTHY))));
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i3", Status.HEALTHY))));
        assertTrue(HealthProbe.INSTANCE.isHealthy());
        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.UNHEALTHY))));
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        eventSources.clear();
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        // All informers unhealthy at start:
        eventSources.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        HealthProbe.INSTANCE.setRuntimeInfo(runtimeInfo);
        assertFalse(HealthProbe.INSTANCE.isHealthy());
    }

    @Test
    public void testHealthProbeInformersUnhealthyEventSourceAtStart() {
        var isRunning = new AtomicBoolean(true);
        var eventSources =
                new HashMap<String, Map<String, InformerWrappingEventSourceHealthIndicator>>();
        var runtimeInfo = runtimeInfo(isRunning, eventSources);

        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(Map.of("i1", Status.HEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i2", Status.UNHEALTHY))));
        eventSources.put(
                "c2",
                Map.of(
                        "e3",
                        informerHealthIndicator(Map.of("i3", Status.HEALTHY)),
                        "e4",
                        informerHealthIndicator(Map.of("i4", Status.UNHEALTHY))));
        HealthProbe.INSTANCE.setRuntimeInfo(runtimeInfo);
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        eventSources.put(
                "c2",
                Map.of(
                        "e3",
                        informerHealthIndicator(Map.of("i3", Status.UNHEALTHY)),
                        "e4",
                        informerHealthIndicator(Map.of("i4", Status.UNHEALTHY))));
        assertFalse(HealthProbe.INSTANCE.isHealthy());

        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(Map.of("i1", Status.UNHEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i2", Status.UNHEALTHY))));
        assertFalse(HealthProbe.INSTANCE.isHealthy());
    }

    @Test
    public void testHealthProbeCanary() {
        HealthProbe.INSTANCE.setRuntimeInfo(runtimeInfo(new AtomicBoolean(true), Map.of()));

        var canaryManager =
                new CanaryResourceManager<FlinkDeployment>(
                        new FlinkConfigManager(new Configuration()));
        HealthProbe.INSTANCE.registerCanaryResourceManager(canaryManager);

        // No canary resources
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        var canary = client.resource(TestUtils.createCanaryDeployment()).create();
        var canaryId = ResourceID.fromResource(canary);
        canaryManager.handleCanaryResourceReconciliation(client.resource(canary).get(), client);

        // Canary resource healthy before health check
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        // No reconciliation before health check
        canaryManager.checkHealth(canaryId, client);
        assertFalse(HealthProbe.INSTANCE.isHealthy());

        // Healthy again
        canaryManager.handleCanaryResourceReconciliation(client.resource(canary).get(), client);
        canaryManager.checkHealth(canaryId, client);
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        canaryManager.handleCanaryResourceReconciliation(client.resource(canary).get(), client);
        canaryManager.checkHealth(canaryId, client);
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

    private RuntimeInfo runtimeInfo(
            AtomicBoolean isRunning,
            Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>> eventSources) {
        return new RuntimeInfo(new Operator(overrider -> overrider.withKubernetesClient(client))) {
            @Override
            public boolean isStarted() {
                return isRunning.get();
            }

            @Override
            @SuppressWarnings("rawtypes")
            public Set<RegisteredController> getRegisteredControllers() {
                Set<RegisteredController> controllers = new HashSet<>();
                eventSources.forEach(
                        (name, controllerEventSources) ->
                                controllers.add(
                                        registeredController(name, controllerEventSources)));
                return controllers;
            }

            @Override
            public Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>>
                    unhealthyInformerWrappingEventSourceHealthIndicator() {
                var unhealthy =
                        new HashMap<
                                String, Map<String, InformerWrappingEventSourceHealthIndicator>>();
                eventSources.forEach(
                        (name, controllerEventSources) ->
                                unhealthy.put(
                                        name,
                                        controllerEventSources.entrySet().stream()
                                                .filter(
                                                        e ->
                                                                e.getValue().getStatus()
                                                                        == Status.UNHEALTHY)
                                                .collect(
                                                        Collectors.toMap(
                                                                Map.Entry::getKey,
                                                                Map.Entry::getValue))));
                return unhealthy;
            }
        };
    }

    private static RegisteredController<FlinkDeployment> registeredController(
            String name, Map<String, InformerWrappingEventSourceHealthIndicator> eventSources) {
        return new RegisteredController<>() {
            @Override
            public ControllerConfiguration<FlinkDeployment> getConfiguration() {
                return controllerConfiguration(name);
            }

            @Override
            public ControllerHealthInfo getControllerHealthInfo() {
                return new ControllerHealthInfo(null) {
                    @Override
                    public Map<String, InformerWrappingEventSourceHealthIndicator>
                            informerEventSourceHealthIndicators() {
                        return eventSources;
                    }
                };
            }

            @Override
            public void changeNamespaces(Set<String> namespaces) {}
        };
    }

    @SuppressWarnings("unchecked")
    private static ControllerConfiguration<FlinkDeployment> controllerConfiguration(String name) {
        return (ControllerConfiguration<FlinkDeployment>)
                Proxy.newProxyInstance(
                        ControllerConfiguration.class.getClassLoader(),
                        new Class<?>[] {ControllerConfiguration.class},
                        (proxy, method, args) -> {
                            if ("getName".equals(method.getName())) {
                                return name;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
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
