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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** {@link FlinkOperatorConfiguration.FlinkOperatorRetryConfiguration} tests. */
public class FlinkOperatorRetryConfigurationTest {

    @Test
    public void testRetryConfiguration() {

        // default values
        FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
        Assertions.assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_RETRY_INITIAL_INTERVAL
                        .defaultValue()
                        .toMillis(),
                configManager
                        .getOperatorConfiguration()
                        .getRetryConfiguration()
                        .getInitialInterval());
        Assertions.assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_RETRY_INTERVAL_MULTIPLIER.defaultValue(),
                configManager
                        .getOperatorConfiguration()
                        .getRetryConfiguration()
                        .getIntervalMultiplier());
        Assertions.assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_RETRY_MAX_ATTEMPTS.defaultValue(),
                configManager.getOperatorConfiguration().getRetryConfiguration().getMaxAttempts());

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
                                "3"));
        configManager.updateDefaultConfig(overrides);
        Assertions.assertEquals(
                1000L,
                configManager
                        .getOperatorConfiguration()
                        .getRetryConfiguration()
                        .getInitialInterval());
        Assertions.assertEquals(
                2.0,
                configManager
                        .getOperatorConfiguration()
                        .getRetryConfiguration()
                        .getIntervalMultiplier());
        Assertions.assertEquals(
                3,
                configManager.getOperatorConfiguration().getRetryConfiguration().getMaxAttempts());
        Assertions.assertEquals(
                8000L,
                configManager.getOperatorConfiguration().getRetryConfiguration().getMaxInterval());
    }
}
