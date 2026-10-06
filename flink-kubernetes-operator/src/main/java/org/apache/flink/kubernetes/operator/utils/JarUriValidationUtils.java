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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shared jarURI validation (scheme allowlist plus restricted-host checks), used both at
 * admission/reconcile time and to re-validate every hop an artifact fetch is redirected through.
 */
public final class JarUriValidationUtils {

    private JarUriValidationUtils() {}

    public static Optional<String> validateJarURI(
            String jarURI, Collection<String> allowedSchemes, boolean disallowRestrictedHosts) {
        if (jarURI == null) {
            return Optional.empty();
        }

        URI uri;
        try {
            uri = new URI(jarURI);
        } catch (URISyntaxException e) {
            return Optional.of("jarURI is not a valid URI: " + e.getMessage());
        }

        String scheme = uri.getScheme();
        if (scheme == null) {
            return Optional.of("jarURI must include a scheme");
        }

        Set<String> normalizedAllowedSchemes = normalizeSchemes(allowedSchemes);
        Optional<String> schemeError = checkSchemeAllowed(scheme, normalizedAllowedSchemes);
        if (schemeError.isPresent()) {
            return schemeError;
        }

        if (isHttpOrHttps(scheme) && disallowRestrictedHosts) {
            String host = uri.getHost();
            if (host == null || host.isEmpty()) {
                return Optional.of("jarURI must include a host for http/https schemes");
            }
            InetAddress[] addresses;
            try {
                // Check every resolved address, not just the first, since a host can resolve to
                // multiple A/AAAA records.
                addresses = InetAddress.getAllByName(host);
            } catch (UnknownHostException e) {
                return Optional.of("jarURI host '" + host + "' cannot be resolved");
            }
            for (InetAddress addr : addresses) {
                if (isRestricted(addr)) {
                    return Optional.of(
                            "jarURI host '" + host + "' resolves to a restricted address");
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Like {@link #validateJarURI}, but also returns the resolved address for a validated
     * http/https jarURI, so callers that connect can pin to it instead of re-resolving the hostname
     * later and reopening the restricted-host check's DNS-rebinding gap. Resolution runs regardless
     * of {@code disallowRestrictedHosts}; only the restricted-address rejection is conditional on
     * that flag.
     */
    public static Result validateAndResolve(
            String jarURI, Collection<String> allowedSchemes, boolean disallowRestrictedHosts) {
        if (jarURI == null) {
            return Result.valid(null);
        }

        URI uri;
        try {
            uri = new URI(jarURI);
        } catch (URISyntaxException e) {
            return Result.invalid("jarURI is not a valid URI: " + e.getMessage());
        }

        String scheme = uri.getScheme();
        if (scheme == null) {
            return Result.invalid("jarURI must include a scheme");
        }

        Set<String> normalizedAllowedSchemes = normalizeSchemes(allowedSchemes);
        Optional<String> schemeError = checkSchemeAllowed(scheme, normalizedAllowedSchemes);
        if (schemeError.isPresent()) {
            return Result.invalid(schemeError.get());
        }

        if (!isHttpOrHttps(scheme)) {
            return Result.valid(null);
        }

        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            return Result.invalid("jarURI must include a host for http/https schemes");
        }
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            return Result.invalid("jarURI host '" + host + "' cannot be resolved");
        }
        if (disallowRestrictedHosts) {
            for (InetAddress addr : addresses) {
                if (isRestricted(addr)) {
                    return Result.invalid(
                            "jarURI host '" + host + "' resolves to a restricted address");
                }
            }
        }
        return Result.valid(addresses[0]);
    }

    private static Set<String> normalizeSchemes(Collection<String> allowedSchemes) {
        return allowedSchemes.stream()
                .map(s -> s.toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
    }

    private static Optional<String> checkSchemeAllowed(
            String scheme, Set<String> normalizedAllowedSchemes) {
        if (!normalizedAllowedSchemes.contains(scheme.toLowerCase(Locale.ROOT))) {
            return Optional.of(
                    String.format(
                            "jarURI scheme '%s' is not in the allowlist %s. Configure '%s' to extend the allowlist.",
                            scheme,
                            normalizedAllowedSchemes,
                            KubernetesOperatorConfigOptions.JAR_URI_ALLOWED_SCHEMES.key()));
        }
        return Optional.empty();
    }

    private static boolean isHttpOrHttps(String scheme) {
        return "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
    }

    private static boolean isRestricted(InetAddress addr) {
        return addr.isLoopbackAddress()
                || addr.isLinkLocalAddress()
                || addr.isSiteLocalAddress()
                || addr.isAnyLocalAddress()
                || addr.isMulticastAddress();
    }

    /**
     * Outcome of {@link #validateAndResolve}: an error, or the address to pin the connection to.
     */
    public static final class Result {
        private final String error;
        private final InetAddress resolvedAddress;

        private Result(String error, InetAddress resolvedAddress) {
            this.error = error;
            this.resolvedAddress = resolvedAddress;
        }

        private static Result invalid(String error) {
            return new Result(error, null);
        }

        private static Result valid(InetAddress resolvedAddress) {
            return new Result(null, resolvedAddress);
        }

        public Optional<String> getError() {
            return Optional.ofNullable(error);
        }

        public Optional<InetAddress> getResolvedAddress() {
            return Optional.ofNullable(resolvedAddress);
        }
    }
}
