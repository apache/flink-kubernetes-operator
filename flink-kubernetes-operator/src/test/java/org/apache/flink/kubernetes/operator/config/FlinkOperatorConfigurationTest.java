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

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.configuration.Configuration;

import io.javaoperatorsdk.operator.processing.event.rate.LinearRateLimiter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Operator retry config tests. */
public class FlinkOperatorConfigurationTest {

    @Test
    public void testRetryConfiguration() {
        // default values
        FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
        var retryConf = configManager.getOperatorConfiguration().getRetryConfiguration();

        Assertions.assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_RETRY_INITIAL_INTERVAL
                        .defaultValue()
                        .toMillis(),
                retryConf.getInitialInterval());
        Assertions.assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_RETRY_INTERVAL_MULTIPLIER.defaultValue(),
                retryConf.getIntervalMultiplier());
        Assertions.assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_RETRY_MAX_ATTEMPTS.defaultValue(),
                retryConf.getMaxAttempts());
        Assertions.assertEquals(-1, retryConf.getMaxInterval());

        // overrides
        var overrides =
                Configuration.fromMap(
                        Map.of(
                                KubernetesOperatorConfigOptions.OPERATOR_RETRY_INITIAL_INTERVAL
                                        .key(),
                                "1 s",
                                KubernetesOperatorConfigOptions.OPERATOR_RETRY_INTERVAL_MULTIPLIER
                                        .key(),
                                "2.0",
                                KubernetesOperatorConfigOptions.OPERATOR_RETRY_MAX_ATTEMPTS.key(),
                                "3",
                                KubernetesOperatorConfigOptions.OPERATOR_RETRY_MAX_INTERVAL.key(),
                                "4"));
        configManager.updateDefaultConfig(overrides);
        retryConf = configManager.getOperatorConfiguration().getRetryConfiguration();
        Assertions.assertEquals(1000L, retryConf.getInitialInterval());
        Assertions.assertEquals(2.0, retryConf.getIntervalMultiplier());
        Assertions.assertEquals(3, retryConf.getMaxAttempts());
        Assertions.assertEquals(4, retryConf.getMaxInterval());
    }

    @Test
    public void testRateLimiter() {
        // default values
        FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
        var rateLimiter =
                (LinearRateLimiter) configManager.getOperatorConfiguration().getRateLimiter();

        Assertions.assertTrue(rateLimiter.isActivated());
        Assertions.assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_RATE_LIMITER_PERIOD.defaultValue(),
                rateLimiter.getRefreshPeriod());
        Assertions.assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_RATE_LIMITER_LIMIT.defaultValue(),
                rateLimiter.getLimitForPeriod());
        configManager.updateDefaultConfig(
                Configuration.fromMap(
                        Map.of(
                                KubernetesOperatorConfigOptions.OPERATOR_RATE_LIMITER_LIMIT.key(),
                                "0")));

        rateLimiter = (LinearRateLimiter) configManager.getOperatorConfiguration().getRateLimiter();
        Assertions.assertFalse(rateLimiter.isActivated());
    }

    @ParameterizedTest
    @MethodSource("eventStackTraceMaxLengthArgs")
    void testEventStackTraceMaxLength(Map<String, String> config, int expectedMaxLength) {
        assertThat(
                        FlinkOperatorConfiguration.fromConfiguration(Configuration.fromMap(config))
                                .getReportedExceptionEventsMaxStackTraceLength())
                .isEqualTo(expectedMaxLength);
    }

    private static Stream<Arguments> eventStackTraceMaxLengthArgs() {
        String maxLength =
                KubernetesOperatorConfigOptions.OPERATOR_EVENT_EXCEPTION_STACKTRACE_MAX_LENGTH
                        .key();
        String lines =
                KubernetesOperatorConfigOptions.OPERATOR_EVENT_EXCEPTION_STACKTRACE_LINES.key();
        return Stream.of(
                Arguments.of(Map.of(), 2048),
                Arguments.of(Map.of(maxLength, "100"), 100),
                Arguments.of(Map.of(lines, "5"), 500),
                Arguments.of(Map.of(lines, "-1"), -100),
                Arguments.of(Map.of(maxLength, "100", lines, "5"), 100));
    }
}
