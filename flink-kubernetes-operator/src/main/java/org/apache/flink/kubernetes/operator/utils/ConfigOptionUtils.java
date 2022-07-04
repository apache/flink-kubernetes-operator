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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link ConfigOption} utilities. */
public class ConfigOptionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigOptionUtils.class);

    /**
     * Gets the value of {@link ConfigOption} with threshold.
     *
     * @param config The effective config
     * @param configOption The config option.
     * @param configThreshold The config threshold.
     * @param <T> Type of the config value.
     * @return The value of {@link ConfigOption} with threshold.
     */
    public static <T extends Comparable<T>> T getValueWithThreshold(
            Configuration config, ConfigOption<T> configOption, T configThreshold) {
        T configValue = config.get(configOption);
        if (configThreshold != null && configValue.compareTo(configThreshold) > 0) {
            LOG.warn(
                    "Uses the config threshold [{}] instead of the config value [{}] of '{}'.",
                    configThreshold,
                    configValue,
                    configOption.key());
            return configThreshold;
        }
        return configValue;
    }
}
