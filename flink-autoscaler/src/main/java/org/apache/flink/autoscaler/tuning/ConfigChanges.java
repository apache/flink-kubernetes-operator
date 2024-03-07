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

package org.apache.flink.autoscaler.tuning;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.FallbackKey;

import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Holds the configuration overrides and removals for a Flink Configuration. */
public class ConfigChanges {

    /** Overrides which will be applied on top of the existing config. */
    @Getter private final Map<String, String> overrides = new HashMap<>();

    /** Removals which will be removed from the existing config. */
    @Getter private final Set<String> removals = new HashSet<>();

    public <T> ConfigChanges addOverride(ConfigOption<T> configOption, T value) {
        overrides.put(configOption.key(), ConfigurationUtils.convertValue(value, String.class));
        return this;
    }

    public ConfigChanges addOverride(String key, String value) {
        overrides.put(key, value);
        return this;
    }

    public ConfigChanges addRemoval(ConfigOption<?> configOption) {
        removals.add(configOption.key());
        for (FallbackKey fallbackKey : configOption.fallbackKeys()) {
            removals.add(fallbackKey.getKey());
        }
        return this;
    }

    public Configuration newConfigWithOverrides(Configuration existingConfig) {
        Configuration config = new Configuration(existingConfig);
        for (String key : removals) {
            config.removeKey(key);
        }
        config.addAll(Configuration.fromMap(overrides));
        return config;
    }
}
