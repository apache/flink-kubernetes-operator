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

package org.apache.flink.kubernetes.operator.artifact;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.util.Preconditions;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** Test for {@link ArtifactManager}. */
public class ArtifactManagerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactManagerTest.class);
    @TempDir Path tempDir;
    private ArtifactManager artifactManager;

    @BeforeEach
    public void setup() {
        // The test server binds to loopback, so the operator policy must permit http + loopback.
        artifactManager = artifactManagerWithPolicy(List.of("http"), false);
    }

    private ArtifactManager artifactManagerWithPolicy(
            List<String> allowedSchemes, boolean disallowRestrictedHosts) {
        Configuration configuration = new Configuration();
        configuration.setString(
                KubernetesOperatorConfigOptions.OPERATOR_USER_ARTIFACTS_BASE_DIR,
                tempDir.toAbsolutePath().toString());
        configuration.set(KubernetesOperatorConfigOptions.JAR_URI_ALLOWED_SCHEMES, allowedSchemes);
        configuration.set(
                KubernetesOperatorConfigOptions.JAR_URI_DISALLOW_RESTRICTED_HOSTS,
                disallowRestrictedHosts);
        return new ArtifactManager(new FlinkConfigManager(configuration));
    }

    @Test
    public void testGenerateJarDir() {
        var sessionJob = TestUtils.buildSessionJob();
        String baseDir =
                artifactManager.generateJarDir(sessionJob.getMetadata(), sessionJob.getSpec());
        String expected =
                tempDir.toString()
                        + File.separator
                        + TestUtils.TEST_NAMESPACE
                        + File.separator
                        + TestUtils.TEST_DEPLOYMENT_NAME
                        + File.separator
                        + TestUtils.TEST_SESSION_JOB_NAME;
        Assertions.assertEquals(expected, baseDir);
    }

    @Test
    public void testFilesystemFetch() throws Exception {
        var sourceFile = mockTheJarFile();
        File file =
                artifactManager.fetch(
                        String.format("file://%s", sourceFile.getAbsolutePath()),
                        new Configuration(),
                        tempDir.toString());
        Assertions.assertTrue(file.exists());
        Assertions.assertEquals(tempDir.toString(), file.getParentFile().toString());
    }

    @Test
    public void testHttpFetch() throws Exception {
        HttpServer httpServer = null;
        try {
            httpServer = startHttpServer();
            var sourceFile = mockTheJarFile();
            httpServer.createContext("/download/file.jar", new DownloadFileHttpHandler(sourceFile));

            var file =
                    artifactManager.fetch(
                            String.format(
                                    "http://127.0.0.1:%d/download/file.jar?some=params",
                                    httpServer.getAddress().getPort()),
                            new Configuration()
                                    .set(
                                            KubernetesOperatorConfigOptions
                                                    .JAR_ARTIFACT_HTTP_HEADER,
                                            Map.of("k1", "v1")),
                            tempDir.toString());
            Assertions.assertTrue(file.exists());
            Assertions.assertEquals(tempDir.toString(), file.getParent());
            Assertions.assertEquals("file.jar", file.getName());
        } finally {
            if (httpServer != null) {
                httpServer.stop(0);
            }
        }
    }

    @Test
    public void testHttpFetchFollowsRedirectToAllowedTarget() throws Exception {
        HttpServer httpServer = null;
        try {
            httpServer = startHttpServer();
            var port = httpServer.getAddress().getPort();
            var sourceFile = mockTheJarFile();
            httpServer.createContext("/download/file.jar", new DownloadFileHttpHandler(sourceFile));
            httpServer.createContext(
                    "/myjob.jar",
                    new RedirectHttpHandler(
                            String.format("http://127.0.0.1:%d/download/file.jar", port)));

            var file =
                    artifactManager.fetch(
                            String.format("http://127.0.0.1:%d/myjob.jar", port),
                            new Configuration(),
                            tempDir.toString());
            Assertions.assertTrue(file.exists());
            // Content comes from the redirect target, but the name from the original jarURI.
            Assertions.assertEquals("myjob.jar", file.getName());
            Assertions.assertEquals(sourceFile.length(), file.length());
        } finally {
            if (httpServer != null) {
                httpServer.stop(0);
            }
        }
    }

    @Test
    public void testHttpFetchBlocksRedirectToNonHttpScheme() throws Exception {
        // An http fetch must only follow http(s) redirects. Here a JDK-recognized non-http scheme
        // (ftp) is rejected cleanly.
        HttpServer httpServer = null;
        try {
            httpServer = startHttpServer();
            var port = httpServer.getAddress().getPort();
            httpServer.createContext(
                    "/redirect",
                    new RedirectHttpHandler(String.format("ftp://127.0.0.1:%d/job.jar", port)));

            var ex =
                    Assertions.assertThrows(
                            IOException.class,
                            () ->
                                    artifactManager.fetch(
                                            String.format("http://127.0.0.1:%d/redirect", port),
                                            new Configuration(),
                                            tempDir.toString()));
            Assertions.assertTrue(ex.getMessage().contains("non-http(s) target"), ex.getMessage());
        } finally {
            if (httpServer != null) {
                httpServer.stop(0);
            }
        }
    }

    @Test
    public void testHttpFetchBlocksRedirectToFilesystemScheme() throws Exception {
        // An http server redirecting to an s3/hdfs target (a Flink filesystem scheme, not a
        // java.net URL protocol) must fail closed cleanly rather than with a raw error.
        HttpServer httpServer = null;
        try {
            httpServer = startHttpServer();
            var port = httpServer.getAddress().getPort();
            httpServer.createContext("/redirect", new RedirectHttpHandler("s3://bucket/job.jar"));

            var ex =
                    Assertions.assertThrows(
                            IOException.class,
                            () ->
                                    artifactManager.fetch(
                                            String.format("http://127.0.0.1:%d/redirect", port),
                                            new Configuration(),
                                            tempDir.toString()));
            Assertions.assertTrue(
                    ex.getMessage().contains("Refusing to follow redirect"), ex.getMessage());
        } finally {
            if (httpServer != null) {
                httpServer.stop(0);
            }
        }
    }

    @Test
    public void testHttpFetchBlocksTooManyRedirects() throws Exception {
        HttpServer httpServer = null;
        try {
            httpServer = startHttpServer();
            var port = httpServer.getAddress().getPort();
            httpServer.createContext(
                    "/loop",
                    new RedirectHttpHandler(String.format("http://127.0.0.1:%d/loop", port)));

            var ex =
                    Assertions.assertThrows(
                            IOException.class,
                            () ->
                                    artifactManager.fetch(
                                            String.format("http://127.0.0.1:%d/loop", port),
                                            new Configuration(),
                                            tempDir.toString()));
            Assertions.assertTrue(ex.getMessage().contains("Too many redirects"), ex.getMessage());
        } finally {
            if (httpServer != null) {
                httpServer.stop(0);
            }
        }
    }

    @Test
    public void testOperatorConfigControlsRestrictedHostPolicy() {
        // The restricted-host policy comes from the operator config; a value set in the per-job
        // config does not override it. No server is needed: the loopback host is rejected first.
        var strictManager = artifactManagerWithPolicy(List.of("http"), true);
        var jobConfig =
                new Configuration()
                        .set(
                                KubernetesOperatorConfigOptions.JAR_URI_DISALLOW_RESTRICTED_HOSTS,
                                false);

        var ex =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                strictManager.fetch(
                                        "http://127.0.0.1:9999/job.jar",
                                        jobConfig,
                                        tempDir.toString()));
        Assertions.assertTrue(ex.getMessage().contains("restricted address"), ex.getMessage());
    }

    @Test
    public void testOperatorConfigControlsSchemeAllowlist() {
        // The scheme allowlist comes from the operator config; a value set in the per-job config
        // does not override it.
        var strictManager = artifactManagerWithPolicy(List.of("https"), false);
        var jobConfig =
                new Configuration()
                        .set(
                                KubernetesOperatorConfigOptions.JAR_URI_ALLOWED_SCHEMES,
                                List.of("http"));

        var ex =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                strictManager.fetch(
                                        "http://127.0.0.1:9999/job.jar",
                                        jobConfig,
                                        tempDir.toString()));
        Assertions.assertTrue(ex.getMessage().contains("scheme 'http'"), ex.getMessage());
    }

    private HttpServer startHttpServer() throws IOException {
        int port = RandomUtils.nextInt(2000, 3000);
        HttpServer httpServer = null;
        while (httpServer == null && port <= 65536) {
            try {
                httpServer = HttpServer.create(new InetSocketAddress(port), 0);
                httpServer.setExecutor(null);
                httpServer.start();
            } catch (BindException e) {
                LOG.warn("Failed to start http server", e);
                port++;
            }
        }
        return httpServer;
    }

    private File mockTheJarFile() {
        String className = String.format("%s.class", ArtifactManagerTest.class.getSimpleName());
        URL url = ArtifactManagerTest.class.getResource(className);
        Assertions.assertNotNull(url);
        return new File(url.getPath());
    }

    /** Handler to mock download file. */
    public static class DownloadFileHttpHandler implements HttpHandler {

        private final File file;
        private final String contentType = "application/octet-stream";

        public DownloadFileHttpHandler(File fileToDownload) {
            Preconditions.checkArgument(
                    fileToDownload.exists(), "The file to be download not exists!");
            this.file = fileToDownload;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Content-Type", contentType);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, file.length());
            FileUtils.copyFile(this.file, exchange.getResponseBody());
            exchange.close();
        }
    }

    /** Handler that always responds with a 302 redirect to the configured location. */
    public static class RedirectHttpHandler implements HttpHandler {

        private final String location;

        public RedirectHttpHandler(String location) {
            this.location = location;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Location", location);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_MOVED_TEMP, -1);
            exchange.close();
        }
    }
}
