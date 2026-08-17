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
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.utils.JarUriValidationUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/**
 * Download the jar from the http resource. The scheme allowlist and restricted-host policy are read
 * from the given configuration; {@link ArtifactManager} sets them from the operator configuration
 * before calling.
 */
public class HttpArtifactFetcher implements ArtifactFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(HttpArtifactFetcher.class);
    public static final HttpArtifactFetcher INSTANCE = new HttpArtifactFetcher();

    // Maximum number of redirects to follow before giving up.
    private static final int MAX_REDIRECTS = 5;

    @Override
    public File fetch(String uri, Configuration flinkConfiguration, File targetDir)
            throws Exception {
        var start = System.currentTimeMillis();

        // Scheme allowlist and restricted-host policy, set by ArtifactManager from the operator
        // configuration.
        var allowedSchemes =
                flinkConfiguration.get(KubernetesOperatorConfigOptions.JAR_URI_ALLOWED_SCHEMES);
        var disallowRestrictedHosts =
                flinkConfiguration.get(
                        KubernetesOperatorConfigOptions.JAR_URI_DISALLOW_RESTRICTED_HOSTS);

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
            var validationError =
                    JarUriValidationUtils.validateJarURI(
                            currentUri, allowedSchemes, disallowRestrictedHosts);
            if (validationError.isPresent()) {
                throw new IOException(
                        "Refusing to fetch artifact from '"
                                + currentUri
                                + "': "
                                + validationError.get());
            }

            currentUrl = new URL(currentUri);
            if (originalUrl == null) {
                originalUrl = currentUrl;
            }
            conn = (HttpURLConnection) currentUrl.openConnection();
            conn.setInstanceFollowRedirects(false);
            // Only send the configured headers to the original host; drop them on a cross-host
            // redirect.
            if (headers != null && originalUrl.getHost().equalsIgnoreCase(currentUrl.getHost())) {
                headers.forEach(conn::setRequestProperty);
            }
            conn.setRequestMethod("GET");

            int status = conn.getResponseCode();
            if (!isRedirect(status)) {
                break;
            }

            String location = conn.getHeaderField("Location");
            conn.disconnect();
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
            // An HTTP fetch only follows http(s) redirects, even if other schemes (e.g. s3, hdfs)
            // are in the jarURI allowlist for top-level use.
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
        }

        // Name the file from the original jarURI, not the redirect target, so a redirect can't
        // change it (e.g. drop the .jar extension the JobManager upload requires).
        String fileName = FilenameUtils.getName(originalUrl.getPath());
        File targetFile = new File(targetDir, fileName);
        try (var inputStream = conn.getInputStream()) {
            FileUtils.copyToFile(inputStream, targetFile);
        }
        LOG.debug(
                "Copied file from {} to {}, cost {} ms",
                uri,
                targetFile,
                System.currentTimeMillis() - start);
        return targetFile;
    }

    private static boolean isRedirect(int status) {
        return status == HttpURLConnection.HTTP_MOVED_PERM
                || status == HttpURLConnection.HTTP_MOVED_TEMP
                || status == HttpURLConnection.HTTP_SEE_OTHER
                || status == 307 // Temporary Redirect
                || status == 308; // Permanent Redirect
    }
}
