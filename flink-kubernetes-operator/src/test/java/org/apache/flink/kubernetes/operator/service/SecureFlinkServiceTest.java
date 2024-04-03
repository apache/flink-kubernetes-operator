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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @link FlinkService unit tests
 */
@EnableKubernetesMockClient(crud = true)
public class SecureFlinkServiceTest {
    KubernetesClient client;
    private final Configuration configuration = new Configuration();

    private final FlinkResourceEventCollector flinkResourceEventCollector =
            new FlinkResourceEventCollector();
    private final FlinkStateSnapshotEventCollector flinkStateSnapshotEventCollector =
            new FlinkStateSnapshotEventCollector();

    private EventRecorder eventRecorder;
    private FlinkOperatorConfiguration operatorConfig;
    private ExecutorService executorService;

    @BeforeEach
    public void setup() {
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TestUtils.TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);
        configuration.set(FLINK_VERSION, FlinkVersion.v1_18);
        eventRecorder =
                new EventRecorder(flinkResourceEventCollector, flinkStateSnapshotEventCollector);
        operatorConfig = FlinkOperatorConfiguration.fromConfiguration(configuration);
        executorService = Executors.newDirectExecutorService();
    }

    @Test
    public void testGetClusterClientWithoutCerts() throws Exception {
        Configuration deployConfig = createOperatorConfig();
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        ConfigurationException thrown =
                assertThrows(
                        ConfigurationException.class,
                        () -> {
                            flinkService.getClusterClient(deployConfig);
                        });
        assertEquals("Failed to initialize SSLContext for the REST client", thrown.getMessage());
    }

    @Test
    public void testGetRestClientWithoutCerts() throws Exception {
        Configuration deployConfig = createOperatorConfig();
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        ConfigurationException thrown =
                assertThrows(
                        ConfigurationException.class,
                        () -> {
                            flinkService.getRestClient(deployConfig);
                        });
        assertEquals("Failed to initialize SSLContext for the REST client", thrown.getMessage());
    }

    @Test
    public void testSubmitApplicationClusterWithoutCerts() throws Exception {
        Configuration deployConfig = createOperatorConfig();
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        var testService = new TestingFlinkService(flinkService);
        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () -> {
                            final FlinkDeployment deployment = TestUtils.buildApplicationCluster();
                            testService.submitApplicationCluster(
                                    deployment.getSpec().getJob(), deployConfig, false);
                        });
        assertInstanceOf(ClusterRetrieveException.class, thrown.getCause());
    }

    @Test
    public void testSubmitSessionClusterWithoutCerts() throws Exception {
        Configuration deployConfig = createOperatorConfig();
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        var testService = new TestingFlinkService(flinkService);
        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () -> {
                            testService.submitSessionCluster(deployConfig);
                        });
        assertInstanceOf(ClusterRetrieveException.class, thrown.getCause());
    }

    @Test
    public void testGetClusterClientWithCerts() throws Exception {
        Map<String, String> originalEnv = System.getenv();
        Configuration deployConfig = createOperatorConfig();
        Map<String, String> systemEnv = new HashMap<>(originalEnv);
        // Set the env var to define the certficates
        systemEnv.put(EnvUtils.ENV_OPERATOR_TRUSTSTORE_PATH, getAbsolutePath("/keystore.jks"));
        systemEnv.put(EnvUtils.ENV_OPERATOR_KEYSTORE_PASSWORD, "password1234");
        TestUtils.setEnv(systemEnv);
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);

        try {
            flinkService.getClusterClient(deployConfig);
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }

    @Test
    public void testAuthenticatedClusterClientWithNoKeystore() throws Exception {
        Map<String, String> originalEnv = System.getenv();
        Configuration deployConfig = createOperatorConfig();
        deployConfig.set(SecurityOptions.SSL_REST_AUTHENTICATION_ENABLED, true);
        Map<String, String> systemEnv = new HashMap<>(originalEnv);
        // Set the env var to define the certficates
        systemEnv.put(EnvUtils.ENV_OPERATOR_TRUSTSTORE_PATH, getAbsolutePath("/truststore.jks"));
        systemEnv.put(EnvUtils.ENV_OPERATOR_KEYSTORE_PASSWORD, "password1234");
        TestUtils.setEnv(systemEnv);
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);

        try {
            ConfigurationException thrown =
                    assertThrows(
                            ConfigurationException.class,
                            () -> {
                                flinkService.getClusterClient(deployConfig);
                            });
            assertEquals(
                    "Failed to initialize SSLContext for the REST client", thrown.getMessage());
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }

    @Test
    public void testAuthenticatedClusterClientWithKeystore() throws Exception {
        Map<String, String> originalEnv = System.getenv();
        Configuration deployConfig = createOperatorConfig();
        deployConfig.set(SecurityOptions.SSL_REST_AUTHENTICATION_ENABLED, true);
        Map<String, String> systemEnv = new HashMap<>(originalEnv);
        // Set the env var to define the certficates
        systemEnv.put(EnvUtils.ENV_OPERATOR_TRUSTSTORE_PATH, getAbsolutePath("/truststore.jks"));
        systemEnv.put(EnvUtils.ENV_OPERATOR_KEYSTORE_PATH, getAbsolutePath("/keystore.jks"));
        systemEnv.put(EnvUtils.ENV_OPERATOR_KEYSTORE_PASSWORD, "password1234");
        TestUtils.setEnv(systemEnv);
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);

        try {
            flinkService.getClusterClient(deployConfig);
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }

    @Test
    public void testGetSecureRestClientWithCerts() throws Exception {
        Map<String, String> originalEnv = System.getenv();
        Configuration deployConfig = createOperatorConfig();
        Map<String, String> systemEnv = new HashMap<>(originalEnv);
        // Set the env var to define the certficates
        systemEnv.put(EnvUtils.ENV_OPERATOR_TRUSTSTORE_PATH, getAbsolutePath("/truststore.jks"));
        systemEnv.put(EnvUtils.ENV_OPERATOR_KEYSTORE_PASSWORD, "password1234");
        TestUtils.setEnv(systemEnv);
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);

        try {
            flinkService.getRestClient(deployConfig);
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }

    @Test
    public void testSubmitApplicationClusterWithCerts() throws Exception {
        Map<String, String> originalEnv = System.getenv();
        Configuration deployConfig = createOperatorConfig();
        Map<String, String> systemEnv = new HashMap<>(originalEnv);
        // Set the env var to define the certficates
        systemEnv.put(EnvUtils.ENV_OPERATOR_TRUSTSTORE_PATH, getAbsolutePath("/truststore.jks"));
        systemEnv.put(EnvUtils.ENV_OPERATOR_KEYSTORE_PASSWORD, "password1234");
        TestUtils.setEnv(systemEnv);
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        var testService = new TestingFlinkService(flinkService);

        try {
            final FlinkDeployment deployment = TestUtils.buildApplicationCluster();
            testService.submitApplicationCluster(
                    deployment.getSpec().getJob(), deployConfig, false);
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }

    @Test
    public void testSubmitSessionClusterWithCerts() throws Exception {
        Map<String, String> originalEnv = System.getenv();
        Configuration deployConfig = createOperatorConfig();
        Map<String, String> systemEnv = new HashMap<>(originalEnv);
        // Set the env var to define the certficates
        systemEnv.put(EnvUtils.ENV_OPERATOR_TRUSTSTORE_PATH, getAbsolutePath("/truststore.jks"));
        systemEnv.put(EnvUtils.ENV_OPERATOR_KEYSTORE_PASSWORD, "password1234");
        TestUtils.setEnv(systemEnv);
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        var testService = new TestingFlinkService(flinkService);

        try {
            testService.submitSessionCluster(deployConfig);
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }

    private String getAbsolutePath(String path) throws URISyntaxException {
        return SecureFlinkServiceTest.class.getResource(path).toURI().getPath();
    }

    private Configuration createOperatorConfig() {
        Configuration deployConfig = new Configuration(configuration);
        deployConfig.setString(OPERATOR_HEALTH_PROBE_PORT.key(), "80");
        deployConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        deployConfig.setString(SecurityOptions.SSL_REST_KEYSTORE, "/etc/certs/keystore.jks");
        deployConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE, "/etc/certs/truststore.jks");
        return deployConfig;
    }

    class TestingFlinkService extends NativeFlinkService {
        private Configuration runtimeConfig;

        public TestingFlinkService(NativeFlinkService nativeFlinkService) {
            super(
                    nativeFlinkService.kubernetesClient,
                    nativeFlinkService.artifactManager,
                    nativeFlinkService.executorService,
                    nativeFlinkService.operatorConfig,
                    eventRecorder);
        }

        @Override
        public void submitApplicationCluster(
                JobSpec jobSpec, Configuration conf, boolean requireHaMetadata) throws Exception {
            try {
                getClusterClient(conf);
            } catch (ConfigurationException e) {
                throw new RuntimeException(
                        new ClusterRetrieveException("Could not create the RestClusterClient.", e));
            }
        }

        @Override
        public void submitSessionCluster(Configuration conf) throws Exception {
            try {
                getClusterClient(conf);
            } catch (ConfigurationException e) {
                throw new RuntimeException(
                        new ClusterRetrieveException("Could not create the RestClusterClient.", e));
            }
        }
    }
}
