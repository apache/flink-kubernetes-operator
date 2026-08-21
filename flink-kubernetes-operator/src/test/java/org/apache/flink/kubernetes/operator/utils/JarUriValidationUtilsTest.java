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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link JarUriValidationUtils#validateAndResolve}. Uses IP literals (never real
 * hostnames) so resolution is hermetic: {@code InetAddress.getAllByName} parses a literal address
 * directly without a DNS lookup, so these don't depend on network access or DNS behavior.
 */
public class JarUriValidationUtilsTest {

    // TEST-NET-3 (RFC 5737): a public, non-restricted IPv4 literal reserved for documentation.
    private static final String PUBLIC_IP = "203.0.113.5";

    @Test
    public void testReturnsResolvedAddressForAllowedHost() {
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "https://" + PUBLIC_IP + "/job.jar", List.of("https"), true);

        Assertions.assertTrue(result.getError().isEmpty());
        Assertions.assertEquals(PUBLIC_IP, result.getResolvedAddress().get().getHostAddress());
    }

    @Test
    public void testRejectsRestrictedHostAndReturnsNoAddress() {
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "https://127.0.0.1/job.jar", List.of("https"), true);

        Assertions.assertTrue(result.getError().isPresent());
        Assertions.assertTrue(
                result.getError().get().contains("resolves to a restricted address"),
                result.getError().get());
        Assertions.assertTrue(result.getResolvedAddress().isEmpty());
    }

    @Test
    public void testStillResolvesRestrictedHostWhenPolicyDisabled() {
        // Resolution (for pinning) runs regardless of disallowRestrictedHosts; only the
        // restricted-address rejection is conditional on that flag.
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "https://127.0.0.1/job.jar", List.of("https"), false);

        Assertions.assertTrue(result.getError().isEmpty());
        Assertions.assertEquals("127.0.0.1", result.getResolvedAddress().get().getHostAddress());
    }

    @Test
    public void testRejectsDisallowedScheme() {
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "https://" + PUBLIC_IP + "/job.jar", List.of("s3"), true);

        Assertions.assertTrue(result.getError().isPresent());
        Assertions.assertTrue(
                result.getError().get().contains("not in the allowlist"), result.getError().get());
        Assertions.assertTrue(result.getResolvedAddress().isEmpty());
    }

    @Test
    public void testPassesThroughNonHttpSchemeWithoutResolving() {
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "s3://bucket/job.jar", List.of("s3"), true);

        Assertions.assertTrue(result.getError().isEmpty());
        Assertions.assertTrue(result.getResolvedAddress().isEmpty());
    }

    @Test
    public void testHandlesNullJarUri() {
        var result = JarUriValidationUtils.validateAndResolve(null, List.of("https"), true);

        Assertions.assertTrue(result.getError().isEmpty());
        Assertions.assertTrue(result.getResolvedAddress().isEmpty());
    }

    @Test
    public void testRejectsMalformedUri() {
        var result =
                JarUriValidationUtils.validateAndResolve("ht tp://bad uri", List.of("https"), true);

        Assertions.assertTrue(result.getError().isPresent());
        Assertions.assertTrue(
                result.getError().get().contains("is not a valid URI"), result.getError().get());
    }

    @Test
    public void testRejectsMissingScheme() {
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "/no/scheme/job.jar", List.of("https"), true);

        Assertions.assertTrue(result.getError().isPresent());
        Assertions.assertTrue(
                result.getError().get().contains("must include a scheme"), result.getError().get());
    }

    @Test
    public void testRejectsMissingHostForHttpScheme() {
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "https:///job.jar", List.of("https"), true);

        Assertions.assertTrue(result.getError().isPresent());
        Assertions.assertTrue(
                result.getError().get().contains("must include a host"), result.getError().get());
    }
}
