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

/** Tests for {@link JarUriValidationUtils#validateAndResolve}. */
public class JarUriValidationUtilsTest {

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
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "https://127.0.0.1/job.jar", List.of("https"), false);

        Assertions.assertTrue(result.getError().isEmpty());
        Assertions.assertEquals("127.0.0.1", result.getResolvedAddress().get().getHostAddress());
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
    public void testRejectsInvalidInputsTheSameWayValidateJarUriDoes() {
        // validateJarURI's own checks are duplicated here, not shared, so this isn't redundant.
        record Case(String jarUri, String expectedErrorSubstring) {}
        var cases =
                List.of(
                        new Case("s3://bucket/job.jar", "not in the allowlist"),
                        new Case("ht tp://bad uri", "is not a valid URI"),
                        new Case("/no/scheme/job.jar", "must include a scheme"),
                        new Case("https:///job.jar", "must include a host"));
        for (var c : cases) {
            var result =
                    JarUriValidationUtils.validateAndResolve(c.jarUri(), List.of("https"), true);
            Assertions.assertTrue(result.getError().isPresent(), c.jarUri());
            Assertions.assertTrue(
                    result.getError().get().contains(c.expectedErrorSubstring()),
                    result.getError().get());
            Assertions.assertTrue(result.getResolvedAddress().isEmpty(), c.jarUri());
        }
    }
}
