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

package org.apache.flink.autoscaler.config;

import org.apache.flink.configuration.ConfigOption;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for AutoScalerOptions. */
public class AutoScalerOptionsTest {

    @Test
    void testAllConfigurationKeysHaveFallBackConfigured() {
        assertThat(retrieveAutoscalerConfigOptions())
                .allSatisfy(
                        configOption ->
                                assertThat(configOption.fallbackKeys())
                                        .anySatisfy(
                                                fallbackKey -> {
                                                    assertThat(fallbackKey.getKey())
                                                            .startsWith(
                                                                    AutoScalerOptions
                                                                            .OLD_K8S_OP_CONF_PREFIX);
                                                    assertThat(
                                                                    fallbackKey
                                                                            .getKey()
                                                                            .substring(
                                                                                    AutoScalerOptions
                                                                                            .OLD_K8S_OP_CONF_PREFIX
                                                                                            .length()))
                                                            .isEqualTo(configOption.key());
                                                }));
    }

    private static List<ConfigOption<?>> retrieveAutoscalerConfigOptions() {
        List<ConfigOption<?>> configOptions = new ArrayList<>();

        for (Field declaredField : AutoScalerOptions.class.getDeclaredFields()) {
            if (declaredField.getType().equals(ConfigOption.class)) {
                try {
                    ConfigOption<?> configOption = (ConfigOption<?>) declaredField.get(null);
                    configOptions.add(configOption);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(
                            "Could not access autoscaler option: " + declaredField, e);
                }
            }
        }

        assertThat(configOptions)
                .withFailMessage("No default autoscaler configuration discovered")
                .isNotEmpty();

        return configOptions;
    }
}
