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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AutoscalerValidator}. */
class AutoscalerValidatorTest {

    private final AutoscalerValidator validator = new AutoscalerValidator();

    private static Configuration enabledConf() {
        var conf = new Configuration();
        conf.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        return conf;
    }

    @Test
    void testDisabledAutoscalerSkipsValidation() {
        // Out-of-range value is ignored when the autoscaler is disabled.
        var conf = new Configuration();
        conf.set(AutoScalerOptions.AUTOSCALER_ENABLED, false);
        conf.set(AutoScalerOptions.SCALING_EFFECTIVENESS_THRESHOLD, 2.0d);

        assertThat(validator.validateAutoscalerOptions(conf)).isEmpty();
    }

    @Test
    void testDefaultsAreValid() {
        assertThat(validator.validateAutoscalerOptions(enabledConf())).isEmpty();
    }

    /**
     * Numeric options together with the lower/upper bound enforced and, where applicable, the
     * feature flag that must be enabled for the option to be validated.
     */
    static Stream<Arguments> boundedOptions() {
        return Stream.of(
                // Always validated when the autoscaler is enabled.
                Arguments.of(AutoScalerOptions.GC_PRESSURE_THRESHOLD, null, 0.0d, 1.0d),
                Arguments.of(AutoScalerOptions.HEAP_USAGE_THRESHOLD, null, 0.0d, 1.0d),
                // Only validated when their feature is enabled.
                Arguments.of(
                        AutoScalerOptions.SCALING_EFFECTIVENESS_THRESHOLD,
                        AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED,
                        0.0d,
                        1.0d),
                Arguments.of(
                        AutoScalerOptions.MEMORY_TUNING_OVERHEAD,
                        AutoScalerOptions.MEMORY_TUNING_ENABLED,
                        0.0d,
                        1.0d),
                Arguments.of(
                        AutoScalerOptions.OBSERVED_SCALABILITY_COEFFICIENT_MIN,
                        AutoScalerOptions.OBSERVED_SCALABILITY_ENABLED,
                        0.01d,
                        1.0d));
    }

    private static Configuration confWithFeatureEnabled(
            @Nullable ConfigOption<Boolean> enableFlag) {
        var conf = enabledConf();
        if (enableFlag != null) {
            conf.set(enableFlag, true);
        }
        return conf;
    }

    @ParameterizedTest
    @MethodSource("boundedOptions")
    void testValueAboveMaxIsRejected(
            ConfigOption<Double> option,
            @Nullable ConfigOption<Boolean> enableFlag,
            double min,
            double max) {
        var conf = confWithFeatureEnabled(enableFlag);
        conf.set(option, max + 0.0001d);

        assertThat(validator.validateAutoscalerOptions(conf))
                .hasValueSatisfying(error -> assertThat(error).contains(option.key()));
    }

    @ParameterizedTest
    @MethodSource("boundedOptions")
    void testValueBelowMinIsRejected(
            ConfigOption<Double> option,
            @Nullable ConfigOption<Boolean> enableFlag,
            double min,
            double max) {
        var conf = confWithFeatureEnabled(enableFlag);
        conf.set(option, min - 0.0001d);

        assertThat(validator.validateAutoscalerOptions(conf))
                .hasValueSatisfying(error -> assertThat(error).contains(option.key()));
    }

    @ParameterizedTest
    @MethodSource("boundedOptions")
    void testBoundaryValuesAreAccepted(
            ConfigOption<Double> option,
            @Nullable ConfigOption<Boolean> enableFlag,
            double min,
            double max) {
        var lower = confWithFeatureEnabled(enableFlag);
        lower.set(option, min);
        assertThat(validator.validateAutoscalerOptions(lower)).isEmpty();

        var upper = confWithFeatureEnabled(enableFlag);
        upper.set(option, max);
        assertThat(validator.validateAutoscalerOptions(upper)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("boundedOptions")
    void testOutOfRangeIgnoredWhenFeatureDisabled(
            ConfigOption<Double> option,
            @Nullable ConfigOption<Boolean> enableFlag,
            double min,
            double max) {
        if (enableFlag == null) {
            // Not a feature-gated option, nothing to assert here.
            return;
        }
        var conf = enabledConf();
        // Feature flag left disabled on purpose.
        conf.set(option, max + 1.0d);

        assertThat(validator.validateAutoscalerOptions(conf)).isEmpty();
    }
}
