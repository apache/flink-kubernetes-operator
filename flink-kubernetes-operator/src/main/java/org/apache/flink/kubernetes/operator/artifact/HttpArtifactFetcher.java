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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.utils.JarUriValidationUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Download the jar from the http resource. The scheme allowlist, restricted-host policy, fetch
 * timeouts and size cap come from the trusted operator configuration passed to {@link #fetch}, not
 * the (possibly tenant-influenced) {@code flinkConfiguration}, which only supplies the HTTP
 * headers. When the restricted-host policy is enabled and the connection is direct, each hop
 * connects to the address that passed the policy, not to whatever the host resolves to when the
 * connection is opened.
 */
public class HttpArtifactFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(HttpArtifactFetcher.class);
    public static final HttpArtifactFetcher INSTANCE =
            new HttpArtifactFetcher(HttpArtifactFetcher::validateHop);

    // Maximum number of redirects to follow before giving up.
    private static final int MAX_REDIRECTS = 5;

    // Chunk size used when streaming the response body to disk.
    private static final int COPY_BUFFER_SIZE = 8 * 1024;

    private static final String UNSUPPORTED_SOCKET = "Only the pinned socket path is supported";

    private final UriValidator uriValidator;

    @VisibleForTesting
    HttpArtifactFetcher(UriValidator uriValidator) {
        this.uriValidator = uriValidator;
    }

    public File fetch(
            String uri,
            Configuration flinkConfiguration,
            FlinkOperatorConfiguration operatorConfig,
            File targetDir)
            throws Exception {
        var start = System.currentTimeMillis();

        var allowedSchemes = operatorConfig.getJarUriAllowedSchemes();
        var disallowRestrictedHosts = operatorConfig.isJarUriDisallowRestrictedHosts();
        var socketTimeoutMillis = (int) operatorConfig.getJarFetchSocketTimeout().toMillis();
        var totalTimeout = operatorConfig.getJarFetchTotalTimeout();
        var maxArtifactSize = operatorConfig.getJarArtifactMaxSize().getBytes();
        // Overall wall-clock deadline for the whole fetch (all redirects + the body transfer).
        // This bounds the reconcile thread even against a host that keeps trickling data slowly
        // enough to never trip the socket timeout on its own.
        var deadline = start + totalTimeout.toMillis();

        // merged session job level header and cluster level header, session job level header take
        // precedence.
        Map<String, String> headers =
                flinkConfiguration.get(KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER);

        // Follow redirects manually so each hop is validated against the same policy as the
        // original URI.
        String currentUri = uri;
        URL originalUrl = null;
        URL currentUrl;
        HttpURLConnection conn;
        int redirects = 0;
        while (true) {
            if (System.currentTimeMillis() > deadline) {
                throw new IOException(
                        "Timed out (> "
                                + totalTimeout
                                + ") while fetching artifact from '"
                                + uri
                                + "'");
            }
            var pinnedAddress =
                    uriValidator.validate(currentUri, allowedSchemes, disallowRestrictedHosts);

            currentUrl = new URL(currentUri);
            if (originalUrl == null) {
                originalUrl = currentUrl;
            }
            if (pinnedAddress.isEmpty()) {
                conn = (HttpURLConnection) currentUrl.openConnection();
            } else {
                // Only direct connections are pinned. An HTTP proxy resolves the host itself; with
                // a SOCKS proxy the JDK resolves it locally and the connection is not pinned. Only
                // the selector's first choice is used: the JDK would move on to the next one if a
                // proxy fails, and a direct fallback would connect by hostname.
                var proxy = selectProxy(currentUri);
                conn =
                        proxy.type() == Proxy.Type.DIRECT
                                ? openPinnedConnection(currentUrl, pinnedAddress.get())
                                : (HttpURLConnection) currentUrl.openConnection(proxy);
            }
            conn.setInstanceFollowRedirects(false);
            conn.setConnectTimeout(socketTimeoutMillis);
            conn.setReadTimeout(socketTimeoutMillis);
            // Only send the configured headers to the original host; drop them on a cross-host
            // redirect.
            if (headers != null && originalUrl.getHost().equalsIgnoreCase(currentUrl.getHost())) {
                headers.forEach(conn::setRequestProperty);
            }
            conn.setRequestMethod("GET");

            // Release the connection on every path except the final (non-redirect) one, whose body
            // is streamed below. This covers getResponseCode() and the redirect handling throwing.
            boolean keepConnection = false;
            try {
                int status = conn.getResponseCode();
                if (!isRedirect(status)) {
                    keepConnection = true;
                    break;
                }

                String location = conn.getHeaderField("Location");
                if (location == null || location.isEmpty()) {
                    throw new IOException(
                            "Received redirect (status "
                                    + status
                                    + ") from '"
                                    + currentUri
                                    + "' without a Location header");
                }
                if (++redirects > MAX_REDIRECTS) {
                    throw new IOException(
                            "Too many redirects (>"
                                    + MAX_REDIRECTS
                                    + ") while fetching artifact from '"
                                    + uri
                                    + "'");
                }
                URL nextUrl;
                try {
                    nextUrl = new URL(currentUrl, location);
                } catch (MalformedURLException e) {
                    throw new IOException(
                            "Refusing to follow redirect from '"
                                    + currentUri
                                    + "' to '"
                                    + location
                                    + "': "
                                    + e.getMessage());
                }
                // An HTTP fetch only follows http(s) redirects, even if other schemes (e.g. s3,
                // hdfs) are in the jarURI allowlist for top-level use.
                var nextScheme = nextUrl.getProtocol();
                if (!"http".equalsIgnoreCase(nextScheme) && !"https".equalsIgnoreCase(nextScheme)) {
                    throw new IOException(
                            "Refusing to follow redirect from '"
                                    + currentUri
                                    + "' to non-http(s) target '"
                                    + nextUrl
                                    + "'");
                }
                currentUri = nextUrl.toString();
            } finally {
                if (!keepConnection) {
                    conn.disconnect();
                }
            }
        }

        // Fail fast if the server declares a size beyond the cap; a malicious/misconfigured
        // server can still lie about this, so the copy below enforces the cap regardless.
        long declaredLength = conn.getContentLengthLong();
        if (declaredLength > maxArtifactSize) {
            conn.disconnect();
            throw new IOException(
                    "Refusing to fetch artifact from '"
                            + uri
                            + "': declared size "
                            + declaredLength
                            + " bytes exceeds the configured limit of "
                            + maxArtifactSize
                            + " bytes");
        }

        // Name the file from the original jarURI, not the redirect target, so a redirect can't
        // change it (e.g. drop the .jar extension the JobManager upload requires).
        String fileName = FilenameUtils.getName(originalUrl.getPath());
        File targetFile = new File(targetDir, fileName);
        try (var inputStream = conn.getInputStream()) {
            copyBounded(inputStream, targetFile, maxArtifactSize, deadline, uri, totalTimeout);
        } catch (Exception e) {
            targetFile.delete();
            throw e;
        } finally {
            conn.disconnect();
        }
        LOG.debug(
                "Copied file from {} to {}, cost {} ms",
                uri,
                targetFile,
                System.currentTimeMillis() - start);
        return targetFile;
    }

    /**
     * Streams {@code inputStream} to {@code targetFile}, aborting if the total bytes written exceed
     * {@code maxBytes} or {@code deadline} (wall-clock millis) passes. Each individual {@link
     * InputStream#read} is already bounded by the connection's read timeout, so the deadline check
     * here is what catches a host that trickles data just fast enough to keep each read below that
     * timeout without ever finishing.
     */
    private static void copyBounded(
            InputStream inputStream,
            File targetFile,
            long maxBytes,
            long deadline,
            String uri,
            Duration totalTimeout)
            throws IOException {
        FileUtils.forceMkdirParent(targetFile);
        byte[] buffer = new byte[COPY_BUFFER_SIZE];
        long total = 0;
        try (var outputStream = new FileOutputStream(targetFile)) {
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                total += read;
                if (total > maxBytes) {
                    throw new IOException(
                            "Refusing to fetch artifact from '"
                                    + uri
                                    + "': downloaded size "
                                    + total
                                    + " bytes exceeds the configured limit of "
                                    + maxBytes
                                    + " bytes");
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new IOException(
                            "Timed out (> "
                                    + totalTimeout
                                    + ") while fetching artifact from '"
                                    + uri
                                    + "'");
                }
                outputStream.write(buffer, 0, read);
            }
        }
    }

    private static boolean isRedirect(int status) {
        return status == HttpURLConnection.HTTP_MOVED_PERM
                || status == HttpURLConnection.HTTP_MOVED_TEMP
                || status == HttpURLConnection.HTTP_SEE_OTHER
                || status == 307 // Temporary Redirect
                || status == 308; // Permanent Redirect
    }

    @VisibleForTesting
    static Optional<InetAddress> validateHop(
            String uri, Collection<String> allowedSchemes, boolean disallowRestrictedHosts)
            throws IOException {
        var validation =
                JarUriValidationUtils.validateAndResolve(
                        uri, allowedSchemes, disallowRestrictedHosts);
        if (validation.getError().isPresent()) {
            throw new IOException(
                    "Refusing to fetch artifact from '"
                            + uri
                            + "': "
                            + validation.getError().get());
        }
        return validation.getCheckedAddress();
    }

    private static Proxy selectProxy(String uri) {
        var selector = ProxySelector.getDefault();
        var proxies = selector == null ? List.<Proxy>of() : selector.select(URI.create(uri));
        return proxies.isEmpty() ? Proxy.NO_PROXY : proxies.get(0);
    }

    private static HttpURLConnection openPinnedConnection(URL url, InetAddress checkedAddress)
            throws IOException {
        // An address from getAllByName() keeps the hostname, which the JDK would resolve again
        // wherever it connects by host string, so connect by the raw IP only.
        var address = InetAddress.getByAddress(checkedAddress.getAddress());
        if ("https".equalsIgnoreCase(url.getProtocol())) {
            // The URL keeps the hostname, so Host, SNI and the certificate check still use it; only
            // the TCP socket goes to the checked address.
            var conn = (HttpsURLConnection) url.openConnection(Proxy.NO_PROXY);
            conn.setSSLSocketFactory(
                    new PinnedSSLSocketFactory(conn.getSSLSocketFactory(), address));
            return conn;
        }
        // Plain HTTP has no socket hook, so the request goes to the address as if it were a proxy:
        // in absolute form, which servers must accept (RFC 9112, section 3.2.2), with Host still
        // taken from the URL.
        var port = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
        return (HttpURLConnection)
                url.openConnection(
                        new Proxy(Proxy.Type.HTTP, new InetSocketAddress(address, port)));
    }

    /**
     * Validates one hop of a fetch, throwing if it is rejected, and returns the address to connect
     * to, or empty if the hop is not pinned.
     */
    @FunctionalInterface
    interface UriValidator {
        Optional<InetAddress> validate(
                String uri, Collection<String> allowedSchemes, boolean disallowRestrictedHosts)
                throws IOException;
    }

    // HttpsClient creates the socket with the no-arg createSocket() and layers TLS on it with
    // createSocket(Socket, ...). If layering fails it falls back to createSocket(host, port), which
    // would bypass the checked address, so that and the other overloads throw.
    private static final class PinnedSSLSocketFactory extends SSLSocketFactory {
        private final SSLSocketFactory delegate;
        private final InetAddress address;

        PinnedSSLSocketFactory(SSLSocketFactory delegate, InetAddress address) {
            this.delegate = delegate;
            this.address = address;
        }

        @Override
        public Socket createSocket() {
            return new Socket() {
                @Override
                public void connect(SocketAddress endpoint, int timeout) throws IOException {
                    var port = ((InetSocketAddress) endpoint).getPort();
                    super.connect(new InetSocketAddress(address, port), timeout);
                }
            };
        }

        @Override
        public Socket createSocket(Socket s, String host, int port, boolean autoClose)
                throws IOException {
            return delegate.createSocket(s, host, port, autoClose);
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            throw new SocketException(UNSUPPORTED_SOCKET);
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
                throws IOException {
            throw new SocketException(UNSUPPORTED_SOCKET);
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            throw new SocketException(UNSUPPORTED_SOCKET);
        }

        @Override
        public Socket createSocket(
                InetAddress host, int port, InetAddress localAddress, int localPort)
                throws IOException {
            throw new SocketException(UNSUPPORTED_SOCKET);
        }
    }
}
