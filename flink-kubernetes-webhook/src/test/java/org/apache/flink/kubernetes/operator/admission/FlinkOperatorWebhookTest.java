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

package org.apache.flink.kubernetes.operator.admission;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.admission.informer.InformerManager;
import org.apache.flink.kubernetes.operator.admission.mutator.FlinkMutator;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.mutator.DefaultFlinkMutator;
import org.apache.flink.kubernetes.operator.mutator.TestMutator;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.kubernetes.operator.validation.DefaultValidator;
import org.apache.flink.kubernetes.operator.validation.TestValidator;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.GroupVersionKind;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.javaoperatorsdk.operator.api.reconciler.Constants.DEFAULT_NAMESPACES_SET;
import static io.javaoperatorsdk.webhook.admission.Operation.CREATE;
import static org.apache.flink.kubernetes.operator.admission.AdmissionHandler.MUTATOR_REQUEST_PATH;
import static org.apache.flink.kubernetes.operator.admission.AdmissionHandler.VALIDATE_REQUEST_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkOperatorWebhook}. */
@EnableKubernetesMockClient(crud = true)
class FlinkOperatorWebhookTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String TEST_PLUGINS = "test-plugins";
    private static final String PLUGINS_JAR = TEST_PLUGINS + "-test-jar.jar";

    private KubernetesClient kubernetesClient;
    private Channel serverChannel;
    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private int port;

    @BeforeEach
    void setup() throws Exception {
        var informerManager = new InformerManager(kubernetesClient);
        informerManager.setNamespaces(DEFAULT_NAMESPACES_SET);
        var validator = new FlinkValidator(Set.of(), informerManager);
        var mutator = new FlinkMutator(Set.of(new DefaultFlinkMutator()), informerManager);
        var admissionHandler = new AdmissionHandler(validator, mutator);

        var initializer = FlinkOperatorWebhook.createChannelInitializer(admissionHandler);
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        serverChannel =
                new ServerBootstrap()
                        .group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(initializer)
                        .bind(0)
                        .sync()
                        .channel();
        port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @AfterEach
    void teardown() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    @Test
    void validateEndpointAcceptsFlinkDeployment() throws Exception {
        var review = createAdmissionReview(createDeployment());

        var response = sendRequest(VALIDATE_REQUEST_PATH, review);

        assertEquals(200, response.statusCode());
        var responseReview = mapper.readValue(response.body(), AdmissionReview.class);
        assertTrue(responseReview.getResponse().getAllowed());
    }

    @Test
    void mutateEndpointAddsTargetSessionLabelToSessionJob() throws Exception {
        var review = createAdmissionReview(createSessionJob());

        var response = sendRequest(MUTATOR_REQUEST_PATH, review);

        assertEquals(200, response.statusCode());
        var responseReview = mapper.readValue(response.body(), AdmissionReview.class);
        assertTrue(responseReview.getResponse().getAllowed());
        var patch = new String(Base64.getDecoder().decode(responseReview.getResponse().getPatch()));
        assertTrue(
                patch.contains(CrdConstants.LABEL_TARGET_SESSION),
                "Patch should contain the target-session label");
    }

    @Test
    void mutateEndpointProducesEmptyPatchForNoOpDeployment() throws Exception {
        var review = createAdmissionReview(createDeployment());

        var response = sendRequest(MUTATOR_REQUEST_PATH, review);

        assertEquals(200, response.statusCode());
        var responseReview = mapper.readValue(response.body(), AdmissionReview.class);
        assertTrue(responseReview.getResponse().getAllowed());
        var patch = new String(Base64.getDecoder().decode(responseReview.getResponse().getPatch()));
        assertEquals("[]", patch, "No-op mutation should produce an empty JSON patch");
    }

    @Test
    void illegalPathReturnsInternalServerError() throws Exception {
        var review = createAdmissionReview(createDeployment());

        var response = sendRequest("/illegal-path", review);

        assertEquals(500, response.statusCode());
    }

    @Test
    void malformedBodyReturnsInternalServerError() throws IOException, InterruptedException {
        var request =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + VALIDATE_REQUEST_PATH))
                        .POST(HttpRequest.BodyPublishers.ofString("not-json"))
                        .build();

        var response =
                HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(500, response.statusCode());
    }

    @Test
    void serverDoesNotUseSslWhenNoKeystoreConfigured() throws Exception {
        var review = createAdmissionReview(createDeployment());

        var response = sendRequest(VALIDATE_REQUEST_PATH, review);

        assertEquals(200, response.statusCode(), "Plain HTTP should work without SSL configured");
    }

    @Test
    void webhookDiscoversCustomValidatorFromPlugins(@TempDir Path temporaryFolder)
            throws Exception {
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put(
                    ConfigConstants.ENV_FLINK_PLUGINS_DIR, getTestPluginsRootDir(temporaryFolder));
            setEnv(systemEnv);

            var informerManager = new InformerManager(kubernetesClient);
            informerManager.setNamespaces(DEFAULT_NAMESPACES_SET);
            var configManager = new FlinkConfigManager(new Configuration());

            var webhook = new FlinkOperatorWebhook(informerManager, configManager);

            assertEquals(
                    new HashSet<>(
                            Arrays.asList(
                                    DefaultValidator.class.getName(),
                                    TestValidator.class.getName())),
                    webhook.validators.stream()
                            .map(v -> v.getClass().getName())
                            .collect(Collectors.toSet()),
                    "Should discover both DefaultValidator and TestValidator from plugins");
        } finally {
            setEnv(originalEnv);
        }
    }

    @Test
    void webhookDiscoversCustomMutatorFromPlugins(@TempDir Path temporaryFolder) throws Exception {
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put(
                    ConfigConstants.ENV_FLINK_PLUGINS_DIR, getTestPluginsRootDir(temporaryFolder));
            setEnv(systemEnv);

            var informerManager = new InformerManager(kubernetesClient);
            informerManager.setNamespaces(DEFAULT_NAMESPACES_SET);
            var configManager = new FlinkConfigManager(new Configuration());

            var webhook = new FlinkOperatorWebhook(informerManager, configManager);

            assertEquals(
                    new HashSet<>(
                            Arrays.asList(
                                    DefaultFlinkMutator.class.getName(),
                                    TestMutator.class.getName())),
                    webhook.mutators.stream()
                            .map(m -> m.getClass().getName())
                            .collect(Collectors.toSet()),
                    "Should discover both DefaultFlinkMutator and TestMutator from plugins");
        } finally {
            setEnv(originalEnv);
        }
    }

    @Test
    void createChannelInitializerWithSslContext(@TempDir Path temporaryFolder) throws Exception {
        Path keystorePath = createTestKeystore(temporaryFolder);
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put(EnvUtils.ENV_WEBHOOK_KEYSTORE_FILE, keystorePath.toString());
            systemEnv.put(EnvUtils.ENV_WEBHOOK_KEYSTORE_TYPE, "PKCS12");
            systemEnv.put(EnvUtils.ENV_WEBHOOK_KEYSTORE_PASSWORD, "testpass");
            setEnv(systemEnv);

            var informerManager = new InformerManager(kubernetesClient);
            informerManager.setNamespaces(DEFAULT_NAMESPACES_SET);
            var validator = new FlinkValidator(Set.of(), informerManager);
            var mutator = new FlinkMutator(Set.of(new DefaultFlinkMutator()), informerManager);
            var admissionHandler = new AdmissionHandler(validator, mutator);

            var initializer = FlinkOperatorWebhook.createChannelInitializer(admissionHandler);
            assertNotNull(initializer, "Channel initializer with SSL should not be null");

            // Start a server with SSL and verify HTTPS works
            NioEventLoopGroup sslBossGroup = new NioEventLoopGroup(1);
            NioEventLoopGroup sslWorkerGroup = new NioEventLoopGroup();
            try {
                Channel sslChannel =
                        new ServerBootstrap()
                                .group(sslBossGroup, sslWorkerGroup)
                                .channel(NioServerSocketChannel.class)
                                .childHandler(initializer)
                                .bind(0)
                                .sync()
                                .channel();
                int sslPort = ((InetSocketAddress) sslChannel.localAddress()).getPort();

                var review = createAdmissionReview(createDeployment());
                var body = mapper.writeValueAsString(review);
                var request =
                        HttpRequest.newBuilder()
                                .uri(
                                        URI.create(
                                                "https://localhost:"
                                                        + sslPort
                                                        + VALIDATE_REQUEST_PATH))
                                .POST(HttpRequest.BodyPublishers.ofString(body))
                                .header("Content-Type", "application/json")
                                .build();

                var trustAllContext = createTrustAllSslContext();
                var response =
                        HttpClient.newBuilder()
                                .sslContext(trustAllContext)
                                .build()
                                .send(request, HttpResponse.BodyHandlers.ofString());

                assertEquals(
                        200,
                        response.statusCode(),
                        "HTTPS request should succeed with SSL enabled");

                sslChannel.close();
            } finally {
                sslBossGroup.shutdownGracefully();
                sslWorkerGroup.shutdownGracefully();
            }
        } finally {
            setEnv(originalEnv);
        }
    }

    @Test
    void createChannelInitializerWithSslContextMissingKeystoreType(@TempDir Path temporaryFolder)
            throws Exception {
        Path keystorePath = createTestKeystore(temporaryFolder);
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put(EnvUtils.ENV_WEBHOOK_KEYSTORE_FILE, keystorePath.toString());
            // Missing KEYSTORE_TYPE and KEYSTORE_PASSWORD
            systemEnv.remove(EnvUtils.ENV_WEBHOOK_KEYSTORE_TYPE);
            systemEnv.remove(EnvUtils.ENV_WEBHOOK_KEYSTORE_PASSWORD);
            setEnv(systemEnv);

            var informerManager = new InformerManager(kubernetesClient);
            informerManager.setNamespaces(DEFAULT_NAMESPACES_SET);
            var validator = new FlinkValidator(Set.of(), informerManager);
            var mutator = new FlinkMutator(Set.of(new DefaultFlinkMutator()), informerManager);
            var admissionHandler = new AdmissionHandler(validator, mutator);

            assertThrows(
                    java.util.NoSuchElementException.class,
                    () -> FlinkOperatorWebhook.createChannelInitializer(admissionHandler),
                    "Should throw when keystore type env var is missing");
        } finally {
            setEnv(originalEnv);
        }
    }

    private static Path createTestKeystore(Path dir) throws Exception {
        Path keystorePath = dir.resolve("test-keystore.p12");
        ProcessBuilder pb =
                new ProcessBuilder(
                        "keytool",
                        "-genkeypair",
                        "-alias",
                        "test",
                        "-keyalg",
                        "RSA",
                        "-keysize",
                        "2048",
                        "-storetype",
                        "PKCS12",
                        "-keystore",
                        keystorePath.toString(),
                        "-storepass",
                        "testpass",
                        "-dname",
                        "CN=localhost",
                        "-validity",
                        "365");
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();
        assertEquals(0, exitCode, "keytool should succeed");
        assertTrue(keystorePath.toFile().exists(), "Keystore file should be created");
        return keystorePath;
    }

    private static SSLContext createTrustAllSslContext() throws Exception {
        TrustManager[] trustAll =
                new TrustManager[] {
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }

                        public void checkClientTrusted(X509Certificate[] certs, String authType) {}

                        public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                    }
                };
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(null, trustAll, new java.security.SecureRandom());
        return sc;
    }

    private static String getTestPluginsRootDir(Path temporaryFolder) throws IOException {
        File testPluginFolder = new File(temporaryFolder.toFile(), TEST_PLUGINS);
        assertTrue(testPluginFolder.mkdirs());
        File testPluginJar = new File("target", PLUGINS_JAR);
        assertTrue(
                testPluginJar.exists(),
                "Test plugin jar not found at "
                        + testPluginJar.getAbsolutePath()
                        + ". Run 'mvn process-test-classes' first.");
        Files.copy(testPluginJar.toPath(), Paths.get(testPluginFolder.toString(), PLUGINS_JAR));
        return temporaryFolder.toAbsolutePath().toString();
    }

    @SuppressWarnings({"unchecked", "JavaReflectionMemberAccess"})
    private static void setEnv(Map<String, String> newEnv) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> clazz = env.getClass();
            Field field = clazz.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> map = (Map<String, String>) field.get(env);
            map.clear();
            map.putAll(newEnv);
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            try {
                Field theCaseInsensitiveEnvironmentField =
                        processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
                theCaseInsensitiveEnvironmentField.setAccessible(true);
                Map<String, String> ciEnv =
                        (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
                ciEnv.clear();
                ciEnv.putAll(newEnv);
            } catch (NoSuchFieldException ignored) {
            }
        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }
    }

    private HttpResponse<String> sendRequest(String path, AdmissionReview review)
            throws IOException, InterruptedException {
        var body = mapper.writeValueAsString(review);
        var request =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + path))
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .header("Content-Type", "application/json")
                        .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private AdmissionReview createAdmissionReview(Object resource) {
        var admissionRequest = new AdmissionRequest();
        admissionRequest.setOperation(CREATE.name());
        admissionRequest.setObject(resource);
        if (resource instanceof FlinkDeployment fd) {
            admissionRequest.setKind(
                    new GroupVersionKind(fd.getGroup(), fd.getVersion(), fd.getKind()));
        } else if (resource instanceof FlinkSessionJob sj) {
            admissionRequest.setKind(
                    new GroupVersionKind(sj.getGroup(), sj.getVersion(), sj.getKind()));
        }
        var review = new AdmissionReview();
        review.setRequest(admissionRequest);
        return review;
    }

    private FlinkDeployment createDeployment() {
        var deployment = new FlinkDeployment();
        var meta = new ObjectMeta();
        meta.setName("test-deployment");
        meta.setNamespace("default");
        deployment.setMetadata(meta);
        deployment.setSpec(new FlinkDeploymentSpec());
        return deployment;
    }

    private FlinkSessionJob createSessionJob() {
        var sessionJob = new FlinkSessionJob();
        var meta = new ObjectMeta();
        meta.setName("test-job");
        meta.setNamespace("default");
        sessionJob.setMetadata(meta);
        sessionJob.setSpec(
                FlinkSessionJobSpec.builder()
                        .job(JobSpec.builder().jarURI("http://test-job.jar").build())
                        .deploymentName("test-deployment")
                        .build());
        return sessionJob;
    }
}
