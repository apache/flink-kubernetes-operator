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
     * @param configOption The config option.
     * @param thresholdOption The threshold option.
     * @param effectiveConfig The effective config.
     * @param operatorConfig The operator config.
     * @return The value of {@link ConfigOption} with threshold.
     */
    public static <T extends Comparable<T>> T getValueWithThreshold(
            ConfigOption<T> configOption,
            ConfigOption<T> thresholdOption,
            Configuration effectiveConfig,
            Configuration operatorConfig) {
        T configValue = effectiveConfig.get(configOption);
        T configThreshold = operatorConfig.get(thresholdOption);
        if (configThreshold != null && configValue.compareTo(configThreshold) > 0) {
            LOG.warn(
                    "Uses the config threshold [{}] of '{}' instead of the config value [{}] of '{}'.",
                    configThreshold,
                    thresholdOption.key(),
                    configValue,
                    configOption.key());
            return configThreshold;
        }
        return configValue;
    }
}
