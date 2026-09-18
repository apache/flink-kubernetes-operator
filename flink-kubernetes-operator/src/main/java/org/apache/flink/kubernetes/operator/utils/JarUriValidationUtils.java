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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Shared jarURI validation (scheme allowlist plus restricted-host checks), used both at
 * admission/reconcile time and to re-validate every hop an artifact fetch is redirected through.
 */
public final class JarUriValidationUtils {

    private static final List<AddressRange> RESTRICTED_RANGES =
            Stream.of(
                            "0.0.0.0/8",
                            "10.0.0.0/8",
                            "100.64.0.0/10",
                            "127.0.0.0/8",
                            "169.254.0.0/16",
                            "172.16.0.0/12",
                            "192.0.0.0/24",
                            "192.0.2.0/24",
                            "192.88.99.0/24",
                            "192.168.0.0/16",
                            "198.18.0.0/15",
                            "198.51.100.0/24",
                            "203.0.113.0/24",
                            "224.0.0.0/4",
                            "240.0.0.0/4",
                            "::/128",
                            "::1/128",
                            "64:ff9b:1::/48",
                            "100::/64",
                            "2001:2::/48",
                            "2001:db8::/32",
                            "3fff::/20",
                            "5f00::/16",
                            "fc00::/7",
                            "fe80::/10",
                            "fec0::/10",
                            "ff00::/8")
                    .map(AddressRange::parse)
                    .collect(Collectors.toUnmodifiableList());

    private static final AddressRange IPV4_MAPPED =
            new AddressRange(
                    new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xff, (byte) 0xff, 0, 0, 0, 0},
                    96);
    private static final AddressRange IPV4_COMPATIBLE = AddressRange.parse("::/96");
    private static final AddressRange NAT64 = AddressRange.parse("64:ff9b::/96");
    private static final AddressRange SIX_TO_FOUR = AddressRange.parse("2002::/16");
    private static final AddressRange TEREDO = AddressRange.parse("2001::/32");

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

        Set<String> normalizedAllowedSchemes =
                allowedSchemes.stream()
                        .map(s -> s.toLowerCase(Locale.ROOT))
                        .collect(Collectors.toSet());
        if (!normalizedAllowedSchemes.contains(scheme.toLowerCase(Locale.ROOT))) {
            return Optional.of(
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

    private static boolean isRestricted(InetAddress addr) {
        return isRestricted(addr.getAddress());
    }

    private static boolean isRestricted(byte[] address) {
        if (RESTRICTED_RANGES.stream().anyMatch(range -> range.contains(address))) {
            return true;
        }
        return embeddedIpv4(address).map(JarUriValidationUtils::isRestricted).orElse(false);
    }

    private static Optional<byte[]> embeddedIpv4(byte[] address) {
        if (IPV4_MAPPED.contains(address)
                || IPV4_COMPATIBLE.contains(address)
                || NAT64.contains(address)) {
            return Optional.of(Arrays.copyOfRange(address, 12, 16));
        }
        if (SIX_TO_FOUR.contains(address)) {
            return Optional.of(Arrays.copyOfRange(address, 2, 6));
        }
        if (TEREDO.contains(address)) {
            byte[] client = Arrays.copyOfRange(address, 12, 16);
            for (int i = 0; i < client.length; i++) {
                client[i] = (byte) ~client[i];
            }
            return Optional.of(client);
        }
        return Optional.empty();
    }

    private static final class AddressRange {
        private final byte[] network;
        private final int prefixLength;

        private AddressRange(byte[] network, int prefixLength) {
            this.network = network;
            this.prefixLength = prefixLength;
        }

        private static AddressRange parse(String cidr) {
            String[] parts = cidr.split("/", 2);
            try {
                return new AddressRange(
                        InetAddress.getByName(parts[0]).getAddress(), Integer.parseInt(parts[1]));
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid address range " + cidr, e);
            }
        }

        private boolean contains(byte[] address) {
            if (address.length != network.length) {
                return false;
            }
            int fullBytes = prefixLength / 8;
            for (int i = 0; i < fullBytes; i++) {
                if (address[i] != network[i]) {
                    return false;
                }
            }
            int remainingBits = prefixLength % 8;
            if (remainingBits == 0) {
                return true;
            }
            int mask = (0xff << (8 - remainingBits)) & 0xff;
            return (address[fullBytes] & mask) == (network[fullBytes] & mask);
        }
    }
}
