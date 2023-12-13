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
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for AutoScalerOptions. */
public class AutoScalerOptionsTest {

    @Test
    void testPrefixForAutoscalerOptions() {
        assertThat(retrieveAutoscalerConfigOptions())
                .allSatisfy(
                        configOption -> {
                            assertThat(configOption.key())
                                    .startsWith(AutoScalerOptions.AUTOSCALER_CONF_PREFIX);
                            assertThat(configOption.fallbackKeys())
                                    .allSatisfy(
                                            fallback ->
                                                    assertThat(fallback.getKey())
                                                            .startsWith(
                                                                    AutoScalerOptions
                                                                            .AUTOSCALER_CONF_PREFIX));
                        });
    }

    @Test
    void testConfigMigration() {
        var config = new Configuration();
        String toBeMigratedKey = "kubernetes.operator.job.autoscaler.actual.config.key";
        config.setString(toBeMigratedKey, "0.23");
        config.setString("another.key", "another value");

        var migratedConfig = AutoScalerOptions.migrateOldConfigKeys(config);

        var configMap = migratedConfig.toMap();
        assertThat(configMap.size()).isEqualTo(2);
        assertThat(configMap).containsEntry("job.autoscaler.actual.config.key", "0.23");
        assertThat(configMap).doesNotContainKey(toBeMigratedKey);
        assertThat(configMap).containsEntry("another.key", "another value");
    }

    @Test
    void testConfigMigrationDoNotOverrideExistingKeys() {
        var config = new Configuration();
        config.setString("kubernetes.operator.job.autoscaler.config.key", "0.23");
        config.setString("job.autoscaler.config.key", "0.42");

        var migratedConfig = AutoScalerOptions.migrateOldConfigKeys(config);

        var configMap = migratedConfig.toMap();
        assertThat(configMap.size()).isEqualTo(1);
        assertThat(configMap).containsEntry("job.autoscaler.config.key", "0.42");
    }

    @Test
    void testConfigMigrationIgnoreNonAutoscalerKeys() {
        var config = new Configuration();
        config.setString("kubernetes.operator.config.key", "value");

        var migratedConfig = AutoScalerOptions.migrateOldConfigKeys(config);

        var configMap = migratedConfig.toMap();
        assertThat(configMap.size()).isEqualTo(1);
        assertThat(configMap).containsEntry("kubernetes.operator.config.key", "value");
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
