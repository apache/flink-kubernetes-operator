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
        return validateAndResolve(jarURI, allowedSchemes, disallowRestrictedHosts).getError();
    }

    /**
     * Validates the jarURI against the scheme allowlist and, if {@code disallowRestrictedHosts} is
     * set, the restricted-host policy. For an http(s) jarURI that passed the policy, the result
     * carries the first resolved address (all of them passed), so a caller can connect to an
     * address that was checked.
     */
    public static Result validateAndResolve(
            String jarURI, Collection<String> allowedSchemes, boolean disallowRestrictedHosts) {
        if (jarURI == null) {
            return Result.accepted();
        }

        URI uri;
        try {
            uri = new URI(jarURI);
        } catch (URISyntaxException e) {
            return Result.rejected("jarURI is not a valid URI: " + e.getMessage());
        }

        String scheme = uri.getScheme();
        if (scheme == null) {
            return Result.rejected("jarURI must include a scheme");
        }

        Set<String> normalizedAllowedSchemes =
                allowedSchemes.stream()
                        .map(s -> s.toLowerCase(Locale.ROOT))
                        .collect(Collectors.toSet());
        if (!normalizedAllowedSchemes.contains(scheme.toLowerCase(Locale.ROOT))) {
            return Result.rejected(
                    String.format(
                            "jarURI scheme '%s' is not in the allowlist %s. Configure '%s' to extend the allowlist.",
                            scheme,
                            normalizedAllowedSchemes,
                            KubernetesOperatorConfigOptions.JAR_URI_ALLOWED_SCHEMES.key()));
        }

        if (("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                && disallowRestrictedHosts) {
            String host = uri.getHost();
            if (host == null || host.isEmpty()) {
                return Result.rejected("jarURI must include a host for http/https schemes");
            }
            InetAddress[] addresses;
            try {
                // Check every resolved address, not just the first, since a host can resolve to
                // multiple A/AAAA records.
                addresses = InetAddress.getAllByName(host);
            } catch (UnknownHostException e) {
                return Result.rejected("jarURI host '" + host + "' cannot be resolved");
            }
            for (InetAddress addr : addresses) {
                if (isRestricted(addr)) {
                    return Result.rejected(
                            "jarURI host '" + host + "' resolves to a restricted address");
                }
            }
            return Result.acceptedAt(addresses[0]);
        }
        return Result.accepted();
    }

    private static boolean isRestricted(InetAddress addr) {
        return addr.isLoopbackAddress()
                || addr.isLinkLocalAddress()
                || addr.isSiteLocalAddress()
                || addr.isAnyLocalAddress()
                || addr.isMulticastAddress();
    }

    /**
     * Outcome of {@link #validateAndResolve}: rejected with an error, or accepted, carrying the
     * checked address when the restricted-host policy resolved one.
     */
    public static final class Result {
        private final String error;
        private final InetAddress checkedAddress;

        private Result(String error, InetAddress checkedAddress) {
            this.error = error;
            this.checkedAddress = checkedAddress;
        }

        private static Result rejected(String error) {
            return new Result(error, null);
        }

        private static Result accepted() {
            return new Result(null, null);
        }

        private static Result acceptedAt(InetAddress checkedAddress) {
            return new Result(null, checkedAddress);
        }

        public Optional<String> getError() {
            return Optional.ofNullable(error);
        }

        public Optional<InetAddress> getCheckedAddress() {
            return Optional.ofNullable(checkedAddress);
        }
    }
}
