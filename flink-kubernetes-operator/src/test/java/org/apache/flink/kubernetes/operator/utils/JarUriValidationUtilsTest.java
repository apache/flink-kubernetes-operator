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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JarUriValidationUtils#validateAndResolve}. */
class JarUriValidationUtilsTest {

    private static final String PUBLIC_IP = "203.0.113.5";

    @Test
    void returnsCheckedAddressWhenPolicyEnabled() {
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "https://" + PUBLIC_IP + "/job.jar", List.of("https"), true);

        assertThat(result.getError()).isEmpty();
        assertThat(result.getCheckedAddress()).map(InetAddress::getHostAddress).hasValue(PUBLIC_IP);
    }

    @Test
    void rejectsRestrictedHostWithoutAddress() {
        var result =
                JarUriValidationUtils.validateAndResolve(
                        "https://127.0.0.1/job.jar", List.of("https"), true);

        assertThat(result.getError())
                .hasValueSatisfying(
                        error -> assertThat(error).contains("resolves to a restricted address"));
        assertThat(result.getCheckedAddress()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "https://127.0.0.1/job.jar",
                "http://artifacts_repo.example.com/job.jar",
                "http://unresolvable.invalid/job.jar"
            })
    void skipsHostChecksWhenPolicyDisabled(String jarUri) {
        var result =
                JarUriValidationUtils.validateAndResolve(jarUri, List.of("http", "https"), false);

        assertThat(result.getError()).isEmpty();
        assertThat(result.getCheckedAddress()).isEmpty();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "s3://bucket/job.jar")
    void acceptsWithoutAddressWhenThereIsNoHttpHost(String jarUri) {
        var result = JarUriValidationUtils.validateAndResolve(jarUri, List.of("s3"), true);

        assertThat(result.getError()).isEmpty();
        assertThat(result.getCheckedAddress()).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
        "s3://bucket/job.jar, not in the allowlist",
        "'ht tp://bad uri', is not a valid URI",
        "/no/scheme/job.jar, must include a scheme",
        "https:///job.jar, must include a host"
    })
    void rejectsInvalidJarUri(String jarUri, String expectedError) {
        var result = JarUriValidationUtils.validateAndResolve(jarUri, List.of("https"), true);

        assertThat(result.getError())
                .hasValueSatisfying(error -> assertThat(error).contains(expectedError));
        assertThat(result.getCheckedAddress()).isEmpty();
    }
}
