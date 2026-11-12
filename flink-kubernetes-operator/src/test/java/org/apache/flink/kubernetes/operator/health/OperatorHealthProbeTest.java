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
import io.javaoperatorsdk.operator.RuntimeInfo;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Operator health probe tests. */
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
            } finally {
                operator.stop();
                assertThrows(ConnectException.class, () -> callHealthEndpoint(conf));
            }
        }
    }

    @Test
    public void testHealthProbeInformers() {
        var isRunning = new AtomicBoolean(false);
        var unhealthyEventSources =
                new HashMap<String, Map<String, InformerWrappingEventSourceHealthIndicator>>();
        var runtimeInfo = runtimeInfo(isRunning, unhealthyEventSources);

        // Test if a new event source becomes unhealthy
        var probe = new OperatorHealthProbe(runtimeInfo, List.of());
        assertFalse(probe.isHealthy());
        isRunning.set(true);
        assertTrue(probe.isHealthy());
        unhealthyEventSources.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        assertFalse(probe.isHealthy());
        unhealthyEventSources.clear();
        assertTrue(probe.isHealthy());

        // Test if we detect when there is an unhealthy at start
        unhealthyEventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY))));
        probe = new OperatorHealthProbe(runtimeInfo, List.of());
        assertTrue(probe.isHealthy());
        unhealthyEventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i3", Status.UNHEALTHY))));
        assertFalse(probe.isHealthy());
        assertFalse(probe.isHealthy());
        unhealthyEventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.HEALTHY)),
                        "e2",
                        informerHealthIndicator(Map.of("i3", Status.HEALTHY))));
        assertTrue(probe.isHealthy());
        unhealthyEventSources.put(
                "c1",
                Map.of(
                        "e1",
                        informerHealthIndicator(
                                Map.of("i1", Status.UNHEALTHY, "i2", Status.UNHEALTHY))));
        assertFalse(probe.isHealthy());
        unhealthyEventSources.clear();
        assertTrue(probe.isHealthy());

        // All informers unhealthy at start:
        unhealthyEventSources.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        probe = new OperatorHealthProbe(runtimeInfo, List.of());
        assertFalse(probe.isHealthy());
    }

    @Test
    public void testHealthProbeCanary() {
        var canaryManager =
                new CanaryResourceManager<FlinkDeployment>(
                        new FlinkConfigManager(new Configuration()));
        var probe =
                new OperatorHealthProbe(
                        runtimeInfo(new AtomicBoolean(true), Map.of()), List.of(canaryManager));

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
        assertEquals("OK", handle(new OperatorHealthProbe(runtimeInfo, List.of()), OK.code()));

        var failingProbe =
                new OperatorHealthProbe(runtimeInfo, List.of()) {
                    @Override
                    boolean isHealthy() {
                        throw new IllegalStateException("probe failure");
                    }
                };
        assertEquals("ERROR", handle(failingProbe, INTERNAL_SERVER_ERROR.code()));
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
            Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>>
                    unhealthyEventSources) {
        return new RuntimeInfo(new Operator(overrider -> overrider.withKubernetesClient(client))) {
            @Override
            public boolean isStarted() {
                return started.get();
            }

            @Override
            public Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>>
                    unhealthyInformerWrappingEventSourceHealthIndicator() {
                return unhealthyEventSources;
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
