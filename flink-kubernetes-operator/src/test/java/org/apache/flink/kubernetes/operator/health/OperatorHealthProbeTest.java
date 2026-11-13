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

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RegisteredController;
import io.javaoperatorsdk.operator.RuntimeInfo;
import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.config.dependent.DependentResourceSpec;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.health.ControllerHealthInfo;
import io.javaoperatorsdk.operator.health.InformerHealthIndicator;
import io.javaoperatorsdk.operator.health.InformerWrappingEventSourceHealthIndicator;
import io.javaoperatorsdk.operator.health.Status;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Operator health probe tests. */
@SuppressWarnings("rawtypes")
@EnableKubernetesMockClient(crud = true)
public class OperatorHealthProbeTest {

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
            var josdkOperator = new AtomicReference<Operator>();
            FlinkOperator operator =
                    new FlinkOperator(conf) {
                        @Override
                        protected Operator createOperator() {
                            josdkOperator.set(
                                    new Operator(
                                            overrider -> overrider.withKubernetesClient(client)));
                            return josdkOperator.get();
                        }
                    };
            try {
                operator.run();
                assertTrue(callHealthEndpoint(conf));

                // The resource versions of the real informers are readable and do not move on the
                // mock server, so a probe over the same runtime goes stale once the timeout passes
                var clock = new TestClock(Instant.now());
                var probe =
                        new OperatorHealthProbe(
                                josdkOperator.get().getRuntimeInfo(),
                                List.of(),
                                Duration.ofMinutes(1),
                                clock);
                assertTrue(probe.isHealthy());
                clock.advance(Duration.ofMinutes(2));
                assertFalse(probe.isHealthy());
            } finally {
                operator.stop();
                assertThrows(ConnectException.class, () -> callHealthEndpoint(conf));
            }
        }
    }

    @Test
    public void testHealthProbeInformers() {
        var isRunning = new AtomicBoolean(false);
        var eventSources =
                new HashMap<String, Map<String, InformerWrappingEventSourceHealthIndicator>>();
        var runtimeInfo = runtimeInfo(isRunning, eventSources);

        // Test if a new event source becomes unhealthy
        var probe = probe(runtimeInfo, List.of());
        assertFalse(probe.isHealthy());
        isRunning.set(true);
        assertTrue(probe.isHealthy());
        eventSources.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        assertFalse(probe.isHealthy());
        eventSources.clear();
        assertTrue(probe.isHealthy());

        // Test if we detect when there is an unhealthy at start
        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY))));
        probe = probe(runtimeInfo, List.of());
        assertTrue(probe.isHealthy());
        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i3", Status.UNHEALTHY))));
        assertFalse(probe.isHealthy());
        assertFalse(probe.isHealthy());
        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i3", Status.HEALTHY))));
        assertTrue(probe.isHealthy());
        eventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.UNHEALTHY))));
        assertFalse(probe.isHealthy());
        eventSources.clear();
        assertTrue(probe.isHealthy());

        // All informers unhealthy at start:
        eventSources.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        probe = probe(runtimeInfo, List.of());
        assertFalse(probe.isHealthy());
    }

    @Test
    public void testHealthProbeInformerStaleness() {
        var eventSource = informerHealthIndicator(Map.of("i1", Status.HEALTHY));
        var informer = eventSource.informerHealthIndicators().get("i1");
        var eventSources = new HashMap<>(Map.of("c1", Map.of("e1", eventSource)));
        var runtimeInfo = runtimeInfo(new AtomicBoolean(true), eventSources);
        var resourceVersions = new HashMap<>(Map.of(informer, "1"));
        var clock = new TestClock(Instant.ofEpochSecond(0));
        var probe =
                new OperatorHealthProbe(runtimeInfo, List.of(), Duration.ofMinutes(1), clock) {
                    @Override
                    String resourceVersion(InformerHealthIndicator informer) {
                        return resourceVersions.get(informer);
                    }
                };
        assertTrue(probe.isHealthy());

        // Nothing arrives for longer than the timeout
        clock.advance(Duration.ofSeconds(61));
        assertFalse(probe.isHealthy());
        assertFalse(probe.isHealthy());

        // An event or bookmark moves the resource version
        resourceVersions.put(informer, "2");
        assertTrue(probe.isHealthy());
        clock.advance(Duration.ofSeconds(61));
        assertFalse(probe.isHealthy());

        // An informer that goes away is forgotten and starts fresh when it comes back
        eventSources.clear();
        assertTrue(probe.isHealthy());
        eventSources.put("c1", Map.of("e1", eventSource));
        assertTrue(probe.isHealthy());
        clock.advance(Duration.ofSeconds(61));
        assertFalse(probe.isHealthy());

        // A zero timeout disables the check
        var disabledProbe =
                new OperatorHealthProbe(runtimeInfo, List.of(), Duration.ZERO, clock) {
                    @Override
                    String resourceVersion(InformerHealthIndicator informer) {
                        return resourceVersions.get(informer);
                    }
                };
        clock.advance(Duration.ofDays(1));
        assertTrue(disabledProbe.isHealthy());
    }

    @Test
    public void testHealthProbeCanary() {
        var canaryManager =
                new CanaryResourceManager<FlinkDeployment>(
                        new FlinkConfigManager(new Configuration()));
        var probe = probe(runtimeInfo(new AtomicBoolean(true), Map.of()), List.of(canaryManager));

        // No canary resources
        assertTrue(probe.isHealthy());

        var canary = client.resource(TestUtils.createCanaryDeployment()).create();
        var canaryId = ResourceID.fromResource(canary);
        canaryManager.handleCanaryResourceReconciliation(client.resource(canary).get(), client);

        // Canary resource healthy before health check
        assertTrue(probe.isHealthy());

        // No reconciliation before health check
        canaryManager.checkHealth(canaryId, client);
        assertFalse(probe.isHealthy());

        // Healthy again
        canaryManager.handleCanaryResourceReconciliation(client.resource(canary).get(), client);
        canaryManager.checkHealth(canaryId, client);
        assertTrue(probe.isHealthy());

        canaryManager.handleCanaryResourceReconciliation(client.resource(canary).get(), client);
        canaryManager.checkHealth(canaryId, client);
        assertTrue(probe.isHealthy());
    }

    @Test
    public void testHealthHandlerResponses() {
        var runtimeInfo = runtimeInfo(new AtomicBoolean(true), Map.of());
        assertEquals("OK", handle(probe(runtimeInfo, List.of()), OK.code()));

        var failingProbe =
                new OperatorHealthProbe(runtimeInfo, List.of(), Duration.ZERO, Clock.systemUTC()) {
                    @Override
                    boolean isHealthy() {
                        throw new IllegalStateException("probe failure");
                    }
                };
        assertEquals("ERROR", handle(failingProbe, INTERNAL_SERVER_ERROR.code()));
    }

    private static OperatorHealthProbe probe(
            RuntimeInfo runtimeInfo, List<CanaryResourceManager<?>> canaryResourceManagers) {
        return new OperatorHealthProbe(
                runtimeInfo, canaryResourceManagers, Duration.ZERO, Clock.systemUTC());
    }

    private static String handle(OperatorHealthProbe probe, int expectedStatus) {
        var channel = new EmbeddedChannel(new OperatorHealthHandler(probe));
        channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
        FullHttpResponse response = channel.readOutbound();
        try {
            assertEquals(expectedStatus, response.status().code());
            return response.content().toString(StandardCharsets.UTF_8);
        } finally {
            response.release();
            channel.finishAndReleaseAll();
        }
    }

    private RuntimeInfo runtimeInfo(
            AtomicBoolean started,
            Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>> eventSources) {
        return new RuntimeInfo(new Operator(overrider -> overrider.withKubernetesClient(client))) {
            @Override
            public boolean isStarted() {
                return started.get();
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

    private static ControllerConfiguration<FlinkDeployment> controllerConfiguration(String name) {
        return new ControllerConfiguration<>() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public String getAssociatedReconcilerClassName() {
                return null;
            }

            @Override
            public ConfigurationService getConfigurationService() {
                return null;
            }

            @Override
            public <C> C getConfigurationFor(DependentResourceSpec<?, FlinkDeployment, C> spec) {
                return null;
            }

            @Override
            public InformerConfiguration<FlinkDeployment> getInformerConfig() {
                return null;
            }
        };
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

    @Test
    public void testCanaryResourceManagerCloseStopsExecutor() {
        try (var configManager = new FlinkConfigManager(new Configuration())) {
            var canaryManager = new CanaryResourceManager<FlinkDeployment>(configManager);
            assertFalse(canaryManager.executorService.isShutdown());

            canaryManager.close();
            assertTrue(canaryManager.executorService.isShutdown());
        }
    }
}
