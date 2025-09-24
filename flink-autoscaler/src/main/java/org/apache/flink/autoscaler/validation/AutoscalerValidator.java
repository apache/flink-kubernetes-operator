/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler.validation;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.utils.CalendarUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.OBSERVED_SCALABILITY_COEFFICIENT_MIN;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.UTILIZATION_MAX;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.UTILIZATION_MIN;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.UTILIZATION_TARGET;

/** Validator for Autoscaler. */
public class AutoscalerValidator {

    /**
     * Validate autoscaler config and return optional error.
     *
     * @param flinkConf autoscaler config
     * @return Optional error string, should be present iff validation resulted in an error
     */
    public Optional<String> validateAutoscalerOptions(Configuration flinkConf) {

        if (!flinkConf.getBoolean(AutoScalerOptions.AUTOSCALER_ENABLED)) {
            return Optional.empty();
        }
        return firstPresent(
                validateNumber(flinkConf, AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.0d),
                validateNumber(flinkConf, AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.0d),
                validateNumber(flinkConf, UTILIZATION_TARGET, 0.0d, 1.0d),
                validateNumber(flinkConf, AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.0d),
                validateNumber(flinkConf, UTILIZATION_MAX, flinkConf.get(UTILIZATION_TARGET), 1.0d),
                validateNumber(flinkConf, UTILIZATION_MIN, 0.0d, flinkConf.get(UTILIZATION_TARGET)),
                validateNumber(flinkConf, OBSERVED_SCALABILITY_COEFFICIENT_MIN, 0.01d, 1d),
                CalendarUtils.validateExcludedPeriods(flinkConf));
    }

    @SafeVarargs
    private static Optional<String> firstPresent(Optional<String>... errOpts) {
        for (Optional<String> opt : errOpts) {
            if (opt.isPresent()) {
                return opt;
            }
        }
        return Optional.empty();
    }

    private static <T extends Number> Optional<String> validateNumber(
            Configuration flinkConfiguration,
            ConfigOption<T> autoScalerConfig,
            Double min,
            Double max) {
        try {
            var configValue = flinkConfiguration.get(autoScalerConfig);
            if (configValue != null) {
                double value = configValue.doubleValue();
                if ((min != null && value < min) || (max != null && value > max)) {
                    return Optional.of(
                            String.format(
                                    "The AutoScalerOption %s is invalid, it should be a value within the range [%s, %s]",
                                    autoScalerConfig.key(),
                                    min != null ? min.toString() : "-Infinity",
                                    max != null ? max.toString() : "+Infinity"));
                }
            }
            return Optional.empty();
        } catch (IllegalArgumentException e) {
            return Optional.of(
                    String.format(
                            "Invalid value in the autoscaler config %s", autoScalerConfig.key()));
        }
    }

    private static <T extends Number> Optional<String> validateNumber(
            Configuration flinkConfiguration, ConfigOption<T> autoScalerConfig, Double min) {
        return validateNumber(flinkConfiguration, autoScalerConfig, min, null);
    }
}
