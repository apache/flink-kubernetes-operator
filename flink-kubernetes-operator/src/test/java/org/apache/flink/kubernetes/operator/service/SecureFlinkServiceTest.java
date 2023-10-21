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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_PORT;
import static org.apache.flink.kubernetes.operator.service.AbstractFlinkService.CERT_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link FlinkService unit tests */
@EnableKubernetesMockClient(crud = true)
public class SecureFlinkServiceTest {
    public static final String DUMMY_KEYSTORE_CONTENTS = "Dummy Keystore Contents";
    public static final String DUMMY_TRUSTSTORE_CONTENTS = "Dummy Truststore Contents";
    KubernetesClient client;
    private final Configuration configuration = new Configuration();
    private final FlinkConfigManager configManager = new FlinkConfigManager(configuration);

    private final EventCollector eventCollector = new EventCollector();

    private EventRecorder eventRecorder;
    private FlinkOperatorConfiguration operatorConfig;
    private ExecutorService executorService;
    private Path certLocation;
    private Path keyStoreFile;
    private Path trustStoreFile;
    private String certificateSecret = "certsecret";
    private Base64.Encoder encoder = Base64.getEncoder();

    @BeforeEach
    public void setup() {
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TestUtils.TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);
        configuration.set(FLINK_VERSION, FlinkVersion.v1_15);
        eventRecorder = new EventRecorder(eventCollector);
        operatorConfig = FlinkOperatorConfiguration.fromConfiguration(configuration);
        executorService = Executors.newDirectExecutorService();
        certLocation = Paths.get(System.getProperty("user.dir"), "temp");
        keyStoreFile = Paths.get(certLocation.toString(), "keystore.jks");
        trustStoreFile = Paths.get(certLocation.toString(), "truststore.jks");
        createCertSecret();
    }

    @Test
    public void testDeleteSecureClusterDeployment() throws IOException {
        var deployment = TestUtils.buildApplicationCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        Path operatorCertDir =
                Paths.get(CERT_DIR, TestUtils.TEST_NAMESPACE, TestUtils.TEST_DEPLOYMENT_NAME);
        Files.createDirectories(operatorCertDir);
        Path keyPath = Paths.get(operatorCertDir.toString(), "keystore.jks");
        Files.createFile(keyPath);
        Path trustPath = Paths.get(operatorCertDir.toString(), "truststore.jks");
        Files.createFile(trustPath);
        var conf = createOperatorConfig();

        var dep =
                new DeploymentBuilder()
                        .withNewMetadata()
                        .withName(TestUtils.TEST_DEPLOYMENT_NAME)
                        .withNamespace(TestUtils.TEST_NAMESPACE)
                        .endMetadata()
                        .withNewSpec()
                        .endSpec()
                        .build();
        client.resource(dep).create();

        assertTrue(Files.exists(keyPath));
        assertTrue(Files.exists(trustPath));

        assertNotNull(
                client.apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .withName(TestUtils.TEST_DEPLOYMENT_NAME)
                        .get());

        flinkService.deleteClusterDeployment(
                deployment.getMetadata(), deployment.getStatus(), conf, false);
        assertNull(
                client.apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .withName(TestUtils.TEST_DEPLOYMENT_NAME)
                        .get());
        assertFalse(Files.exists(keyPath));
        assertFalse(Files.exists(trustPath));
        Files.deleteIfExists(keyPath.getParent());
    }

    @Test
    public void testGetSecureClusterClient() throws Exception {
        Configuration deployConfig = createOperatorConfig();
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        try {
            flinkService.getClusterClient(deployConfig);
        } catch (ConfigurationException e) {
            assertEquals("Failed to initialize SSLContext for the REST client", e.getMessage());
        }
        Path operatorCertDir =
                Paths.get(CERT_DIR, TestUtils.TEST_NAMESPACE, TestUtils.TEST_DEPLOYMENT_NAME);
        Path operatorKey = Paths.get(operatorCertDir.toString(), "keystore.jks");
        assertTrue(Files.exists(operatorKey));
        assertEquals(DUMMY_KEYSTORE_CONTENTS, Files.readString(operatorKey));
        Path operatorTrust = Paths.get(operatorCertDir.toString(), "truststore.jks");
        assertTrue(Files.exists(operatorTrust));
        assertEquals(DUMMY_TRUSTSTORE_CONTENTS, Files.readString(operatorTrust));
        FileUtils.deleteDirectory(certLocation.toFile());
        FileUtils.deleteDirectory(operatorCertDir.toFile());
    }

    @Test
    public void testInvalidCertLocationGetClusterClient() throws Exception {
        Configuration deployConfig = createOperatorConfig();
        deployConfig.setString(
                SecurityOptions.SSL_REST_KEYSTORE,
                Paths.get(certLocation.toString(), "missingKey.crt").toString());
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        try {
            flinkService.getClusterClient(deployConfig);
        } catch (RuntimeException e) {
            assertEquals("No data found for missingKey.crt in secret certsecret", e.getMessage());
        } finally {
            FileUtils.deleteDirectory(certLocation.toFile());
        }
    }

    @Test
    public void testMissingSecretGetClusterClient() throws Exception {
        Configuration deployConfig = createOperatorConfig();
        client.secrets().inNamespace(TestUtils.TEST_NAMESPACE).withName(certificateSecret).delete();
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder);
        try {
            flinkService.getClusterClient(deployConfig);
        } catch (RuntimeException e) {
            assertEquals(
                    "Secret certsecret in namespace flink-operator-test does not exist",
                    e.getMessage());
        } finally {
            FileUtils.deleteDirectory(certLocation.toFile());
        }
    }

    private Configuration createOperatorConfig() {
        Configuration deployConfig = new Configuration(configuration);
        deployConfig.setString(OPERATOR_HEALTH_PROBE_PORT.key(), "80");
        deployConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        deployConfig.setString(SecurityOptions.SSL_REST_KEYSTORE, keyStoreFile.toString());
        deployConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE, trustStoreFile.toString());
        deployConfig.setString(
                KubernetesConfigOptions.KUBERNETES_SECRETS.key(),
                certificateSecret + ":" + certLocation.toString());
        return deployConfig;
    }

    private void createCertSecret() {
        Secret certsecret =
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(certificateSecret)
                        .endMetadata()
                        .addToData(
                                "keystore.jks",
                                encoder.encodeToString(DUMMY_KEYSTORE_CONTENTS.getBytes()))
                        .addToData(
                                "truststore.jks",
                                encoder.encodeToString(DUMMY_TRUSTSTORE_CONTENTS.getBytes()))
                        .build();
        client.secrets().inNamespace(TestUtils.TEST_NAMESPACE).resource(certsecret).create();
    }
}
