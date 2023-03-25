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

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RuntimeInfo;
import io.javaoperatorsdk.operator.api.config.ConfigurationServiceProvider;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link Health probe unit tests */
@EnableKubernetesMockClient(crud = true)
public class HealthProbeTest {
    KubernetesClient client;

    @Test
    public void testHealthProbeEndpoint() throws Exception {
        try (var port = NetUtils.getAvailablePort()) {
            var conf = new Configuration();
            conf.set(KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_PORT, port.getPort());

            FlinkOperator operator =
                    new FlinkOperator(conf) {
                        @Override
                        protected Operator createOperator() {
                            ConfigurationServiceProvider.reset();
                            return new Operator(client);
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
    public void testHealthProbe() {
        var isRunning = new AtomicBoolean(false);
        var isHealthy = new AtomicBoolean(false);

        HealthProbe.INSTANCE.setRuntimeInfo(
                new RuntimeInfo(new Operator(client)) {
                    @Override
                    public boolean isStarted() {
                        return isRunning.get();
                    }

                    @Override
                    public boolean allEventSourcesAreHealthy() {
                        return isHealthy.get();
                    }
                });

        assertFalse(HealthProbe.INSTANCE.isHealthy());
        isRunning.set(true);
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        isHealthy.set(true);
        assertTrue(HealthProbe.INSTANCE.isHealthy());
        isRunning.set(false);
        assertFalse(HealthProbe.INSTANCE.isHealthy());
        isRunning.set(true);

        var canaryManager =
                new CanaryResourceManager<FlinkDeployment>(
                        new FlinkConfigManager(new Configuration()), client);
        HealthProbe.INSTANCE.registerCanaryResourceManager(canaryManager);

        // No canary resources
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        var canary = TestUtils.createCanaryDeployment();
        canaryManager.handleCanaryResourceReconciliation(ReconciliationUtils.clone(canary));

        // Canary resource healthy before health check
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        // No reconciliation before health check
        canaryManager.checkHealth(ResourceID.fromResource(canary));
        assertFalse(HealthProbe.INSTANCE.isHealthy());

        // Healthy again
        canary.getMetadata().setGeneration(2L);
        canaryManager.handleCanaryResourceReconciliation(ReconciliationUtils.clone(canary));
        canaryManager.checkHealth(ResourceID.fromResource(canary));
        assertTrue(HealthProbe.INSTANCE.isHealthy());

        canary.getMetadata().setGeneration(3L);
        canaryManager.handleCanaryResourceReconciliation(ReconciliationUtils.clone(canary));
        canaryManager.checkHealth(ResourceID.fromResource(canary));
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
}
