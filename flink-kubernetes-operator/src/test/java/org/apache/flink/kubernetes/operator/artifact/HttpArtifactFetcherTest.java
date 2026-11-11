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
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManagerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HttpArtifactFetcher}. */
class HttpArtifactFetcherTest {

    private static final byte[] ARTIFACT = "artifact-bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LOOPBACK_BYTES = {127, 0, 0, 1};
    private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();
    private static final String REJECTED_HOST = "rejected.invalid";

    @TempDir Path tempDir;

    private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
    private final List<HttpServer> servers = new ArrayList<>();
    private final List<Closeable> sockets = new ArrayList<>();
    private final RecordingValidator validator = new RecordingValidator();
    private final HttpArtifactFetcher fetcher = new HttpArtifactFetcher(validator);
    private HttpServer server;
    private int port;

    @BeforeEach
    void startServer() throws IOException {
        server = start(HttpServer.create(new InetSocketAddress(loopback(), 0), 0));
        server.createContext("/job.jar", serveArtifact(requests));
        port = server.getAddress().getPort();
    }

    @AfterEach
    void stopServers() throws IOException {
        servers.forEach(s -> s.stop(0));
        for (var socket : sockets) {
            socket.close();
        }
    }

    @Test
    void connectsToCheckedAddressAndSendsOriginalHost() throws Exception {
        var file = fetch("http://artifact.invalid:" + port + "/job.jar", Map.of());

        assertThat(file).hasBinaryContent(ARTIFACT);
        assertThat(requests).hasSize(1);
        assertThat(requests.get(0).header("Host")).isEqualTo("artifact.invalid:" + port);
    }

    @Test
    void validatesAndPinsEachRedirectHopAndDropsConfiguredHeadersOnHostChange() throws Exception {
        var start = "http://artifact.invalid:" + port + "/start";
        var target = "http://other.invalid:" + port + "/job.jar";
        server.createContext("/start", exchange -> redirect(exchange, target));

        fetch(start, Map.of("X-Token", "secret"));

        assertThat(validator.validatedUris).containsExactly(start, target);
        assertThat(requests).hasSize(2);
        assertThat(requests.get(0).header("Host")).isEqualTo("artifact.invalid:" + port);
        assertThat(requests.get(0).header("X-Token")).isEqualTo("secret");
        assertThat(requests.get(1).header("Host")).isEqualTo("other.invalid:" + port);
        assertThat(requests.get(1).header("X-Token")).isNull();
    }

    @Test
    void refusesRedirectToRejectedHost() {
        var target = "http://" + REJECTED_HOST + ":" + port + "/job.jar";
        server.createContext("/start", exchange -> redirect(exchange, target));

        assertThatThrownBy(() -> fetch("http://artifact.invalid:" + port + "/start", Map.of()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Refusing to fetch artifact from '" + target + "'");
        assertThat(requests).hasSize(1);
    }

    @Test
    void configuredHeadersCannotOverrideHost() throws Exception {
        fetch(
                "http://artifact.invalid:" + port + "/job.jar",
                Map.of("Host", "other.invalid", "X-Token", "secret"));

        assertThat(requests).hasSize(1);
        assertThat(requests.get(0).header("Host")).isEqualTo("artifact.invalid:" + port);
        assertThat(requests.get(0).header("X-Token")).isEqualTo("secret");
    }

    @Test
    void verifiesCertificateAndSendsSniForOriginalHostname() throws Exception {
        var httpsPort = startHttpsServer().getAddress().getPort();

        var file =
                fetchTrustingTestCertificate("https://artifact.invalid:" + httpsPort + "/job.jar");

        assertThat(file).hasBinaryContent(ARTIFACT);
        assertThat(requests).hasSize(1);
        assertThat(requests.get(0).header("Host")).isEqualTo("artifact.invalid:" + httpsPort);
        assertThat(requests.get(0).sniHostnames()).containsExactly("artifact.invalid");
    }

    @Test
    void rejectsCertificateIssuedForAnotherHostname() throws Exception {
        var httpsPort = startHttpsServer().getAddress().getPort();

        assertThatThrownBy(
                        () ->
                                fetchTrustingTestCertificate(
                                        "https://other.invalid:" + httpsPort + "/job.jar"))
                .isInstanceOf(SSLHandshakeException.class)
                .hasMessageContaining("other.invalid");
        assertThat(requests).isEmpty();
    }

    @Test
    void doesNotPinWhenFetchingThroughProxy() throws Exception {
        var proxyRequests = new CopyOnWriteArrayList<RecordedRequest>();
        var proxyServer = start(HttpServer.create(new InetSocketAddress(loopback(), 0), 0));
        proxyServer.createContext("/job.jar", serveArtifact(proxyRequests));
        var proxy = new Proxy(Proxy.Type.HTTP, proxyServer.getAddress());

        withProxySelector(
                List.of(proxy),
                () -> fetch("http://artifact.invalid:" + port + "/job.jar", Map.of()));

        assertThat(requests).isEmpty();
        assertThat(proxyRequests).hasSize(1);
        assertThat(proxyRequests.get(0).uri())
                .hasToString("http://artifact.invalid:" + port + "/job.jar");
    }

    @Test
    void doesNotPinHttpsWhenFetchingThroughProxy() throws Exception {
        try (var proxySocket = new ServerSocket(0, 1, loopback())) {
            var proxyPort = proxySocket.getLocalPort();
            var firstLine = CompletableFuture.supplyAsync(() -> refuseTunnel(proxySocket));
            var proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(loopback(), proxyPort));

            assertThatThrownBy(
                            () ->
                                    withProxySelector(
                                            List.of(proxy),
                                            () ->
                                                    fetch(
                                                            "https://artifact.invalid:"
                                                                    + proxyPort
                                                                    + "/job.jar",
                                                            Map.of())))
                    .isInstanceOf(IOException.class);
            assertThat(firstLine.get(10, TimeUnit.SECONDS))
                    .isEqualTo("CONNECT artifact.invalid:" + proxyPort + " HTTP/1.1");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "https"})
    void doesNotFallBackToDirectConnectionWhenProxyFails(String scheme) throws Exception {
        var unreachableProxy =
                new Proxy(Proxy.Type.HTTP, new InetSocketAddress(loopback(), unusedPort()));

        assertThatThrownBy(
                        () ->
                                withProxySelector(
                                        List.of(unreachableProxy, Proxy.NO_PROXY),
                                        () ->
                                                fetch(
                                                        scheme
                                                                + "://artifact.invalid:"
                                                                + port
                                                                + "/job.jar",
                                                        Map.of())))
                .isInstanceOf(ConnectException.class);
        assertThat(requests).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "https"})
    @Timeout(30)
    void pinnedConnectionsHonourConnectTimeout(String scheme) throws Exception {
        var uri = scheme + "://artifact.invalid:" + saturatedListenerPort() + "/job.jar";
        var operatorConfig = new Configuration();
        operatorConfig.set(
                KubernetesOperatorConfigOptions.JAR_FETCH_SOCKET_TIMEOUT, Duration.ofSeconds(1));

        assertThatThrownBy(
                        () ->
                                fetcher.fetch(
                                        uri,
                                        new Configuration(),
                                        FlinkOperatorConfiguration.fromConfiguration(
                                                operatorConfig),
                                        tempDir.toFile()))
                .isInstanceOf(SocketTimeoutException.class)
                .satisfies(
                        e -> assertThat(e.getMessage()).containsIgnoringCase("connect timed out"));
    }

    @Test
    void validateHopReturnsCheckedAddressWhenPolicyEnabled() throws Exception {
        assertThat(
                        HttpArtifactFetcher.validateHop(
                                "https://203.0.113.5/job.jar", List.of("https"), true))
                .map(InetAddress::getHostAddress)
                .hasValue("203.0.113.5");
    }

    @Test
    void validateHopDoesNotPinWhenPolicyDisabled() throws Exception {
        assertThat(
                        HttpArtifactFetcher.validateHop(
                                "https://203.0.113.5/job.jar", List.of("https"), false))
                .isEmpty();
    }

    @Test
    void validateHopRejectsRestrictedHost() {
        assertThatThrownBy(
                        () ->
                                HttpArtifactFetcher.validateHop(
                                        "http://127.0.0.1/job.jar", List.of("http"), true))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "Refusing to fetch artifact from 'http://127.0.0.1/job.jar': "
                                + "jarURI host '127.0.0.1' resolves to a restricted address");
    }

    private File fetch(String uri, Map<String, String> headers) throws Exception {
        var flinkConfiguration = new Configuration();
        if (!headers.isEmpty()) {
            flinkConfiguration.set(
                    KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER, headers);
        }
        return fetcher.fetch(
                uri,
                flinkConfiguration,
                FlinkOperatorConfiguration.fromConfiguration(new Configuration()),
                tempDir.toFile());
    }

    // The test certificate is self-signed for artifact.invalid, so it is also the trust anchor.
    private File fetchTrustingTestCertificate(String uri) throws Exception {
        var trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(testKeyStore());
        var sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        var originalFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        try {
            return fetch(uri, Map.of());
        } finally {
            HttpsURLConnection.setDefaultSSLSocketFactory(originalFactory);
        }
    }

    private <S extends HttpServer> S start(S httpServer) {
        httpServer.start();
        servers.add(httpServer);
        return httpServer;
    }

    private HttpsServer startHttpsServer() throws Exception {
        var keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(testKeyStore(), KEYSTORE_PASSWORD);
        var sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        var httpsServer = HttpsServer.create(new InetSocketAddress(loopback(), 0), 0);
        httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        httpsServer.createContext("/job.jar", serveArtifact(requests));
        return start(httpsServer);
    }

    private static HttpHandler serveArtifact(List<RecordedRequest> log) {
        return exchange -> {
            log.add(RecordedRequest.of(exchange));
            exchange.sendResponseHeaders(200, ARTIFACT.length);
            exchange.getResponseBody().write(ARTIFACT);
            exchange.close();
        };
    }

    private void redirect(HttpExchange exchange, String location) throws IOException {
        requests.add(RecordedRequest.of(exchange));
        exchange.getResponseHeaders().add("Location", location);
        exchange.sendResponseHeaders(302, -1);
        exchange.close();
    }

    // Reads the request line a client sends to the proxy, then refuses the tunnel.
    private static String refuseTunnel(ServerSocket proxySocket) {
        try (var client = proxySocket.accept()) {
            client.setSoTimeout(5_000);
            var reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    client.getInputStream(), StandardCharsets.US_ASCII));
            var requestLine = reader.readLine();
            OutputStream out = client.getOutputStream();
            out.write("HTTP/1.1 403 Forbidden\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            return requestLine;
        } catch (IOException e) {
            return "proxy read failed: " + e;
        }
    }

    private static KeyStore testKeyStore() throws Exception {
        var keyStore = KeyStore.getInstance("PKCS12");
        try (var in =
                HttpArtifactFetcherTest.class.getResourceAsStream("/artifact-fetch-pin-test.p12")) {
            keyStore.load(in, KEYSTORE_PASSWORD);
        }
        return keyStore;
    }

    // A listener whose accept queue is full: the kernel drops further connection attempts, so
    // connecting to it times out wherever the test runs, with or without a network.
    private int saturatedListenerPort() throws IOException {
        var listener = new ServerSocket(0, 1, loopback());
        sockets.add(listener);
        for (int i = 0; i < 16; i++) {
            var filler = new Socket();
            sockets.add(filler);
            try {
                filler.connect(listener.getLocalSocketAddress(), 200);
            } catch (SocketTimeoutException e) {
                return listener.getLocalPort();
            }
        }
        throw new IOException("Could not fill the accept queue of the test listener");
    }

    private static int unusedPort() throws IOException {
        try (var socket = new ServerSocket(0, 1, loopback())) {
            return socket.getLocalPort();
        }
    }

    private static <T> T withProxySelector(List<Proxy> proxies, Callable<T> action)
            throws Exception {
        var originalSelector = ProxySelector.getDefault();
        ProxySelector.setDefault(
                new ProxySelector() {
                    @Override
                    public List<Proxy> select(URI uri) {
                        return proxies;
                    }

                    @Override
                    public void connectFailed(URI uri, SocketAddress address, IOException e) {}
                });
        try {
            return action.call();
        } finally {
            ProxySelector.setDefault(originalSelector);
        }
    }

    private static InetAddress loopback() throws IOException {
        return InetAddress.getByAddress(LOOPBACK_BYTES);
    }

    /**
     * Stands in for the restricted-host check: every host but {@link #REJECTED_HOST} passes and
     * "resolves" to loopback. Like {@link InetAddress#getAllByName}, the returned address keeps the
     * hostname, which is under .invalid, so a fetch that connected by hostname would fail.
     */
    private static final class RecordingValidator implements HttpArtifactFetcher.UriValidator {
        private final List<String> validatedUris = new CopyOnWriteArrayList<>();

        @Override
        public Optional<InetAddress> validate(
                String uri, Collection<String> allowedSchemes, boolean disallowRestrictedHosts)
                throws IOException {
            validatedUris.add(uri);
            var host = URI.create(uri).getHost();
            if (REJECTED_HOST.equals(host)) {
                throw new IOException("Refusing to fetch artifact from '" + uri + "': rejected");
            }
            return Optional.of(InetAddress.getByAddress(host, LOOPBACK_BYTES));
        }
    }

    private record RecordedRequest(URI uri, Headers headers, List<String> sniHostnames) {

        static RecordedRequest of(HttpExchange exchange) {
            List<String> sniHostnames =
                    exchange instanceof HttpsExchange httpsExchange
                            ? ((ExtendedSSLSession) httpsExchange.getSSLSession())
                                    .getRequestedServerNames().stream()
                                            .map(name -> ((SNIHostName) name).getAsciiName())
                                            .collect(Collectors.toList())
                            : List.of();
            return new RecordedRequest(
                    exchange.getRequestURI(), exchange.getRequestHeaders(), sniHostnames);
        }

        String header(String name) {
            return headers.getFirst(name);
        }
    }
}
