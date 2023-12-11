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

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyMigratingConfiguration}. */
public class KeyMigratingConfigurationTest {

    @Test
    void testMigration() {
        var config = new Configuration();
        String toBeMigratedKey = "prefix.actual.config.key";
        config.setString(toBeMigratedKey, "0.23");
        config.setString("another.key", "another value");

        var migratedConfig = new KeyMigratingConfiguration("prefix.", config);

        var configMap = migratedConfig.toMap();
        assertThat(configMap.size()).isEqualTo(2);
        assertThat(configMap).containsEntry("actual.config.key", "0.23");
        assertThat(configMap).doesNotContainKey(toBeMigratedKey);
        assertThat(configMap).containsEntry("another.key", "another value");
    }

    @Test
    void testDoNotOverrideExistingKeys() {
        var config = new Configuration();
        String toBeMigratedKey = "prefix.actual.config.key";
        config.setString("prefix.config.key", "0.23");
        config.setString("config.key", "0.42");

        var migratedConfig = new KeyMigratingConfiguration("prefix.", config);

        var configMap = migratedConfig.toMap();
        assertThat(configMap.size()).isEqualTo(1);
        assertThat(configMap).containsEntry("config.key", "0.42");
    }
}
