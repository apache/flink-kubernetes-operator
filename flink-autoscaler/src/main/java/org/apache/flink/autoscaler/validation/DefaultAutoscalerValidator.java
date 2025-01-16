package org.apache.flink.autoscaler.validation;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.utils.CalendarUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

/** Default implementation of {@link AutoscalerValidator}. */
public class DefaultAutoscalerValidator implements AutoscalerValidator {

    @Override
    public Optional<String> validateAutoscalerOptions(Configuration configuration) {
        if (!configuration.get(AutoScalerOptions.AUTOSCALER_ENABLED)) {
            return Optional.empty();
        }
        return firstPresent(
                validateNumber(configuration, AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.0d),
                validateNumber(configuration, AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.0d),
                validateNumber(configuration, AutoScalerOptions.TARGET_UTILIZATION, 0.0d),
                validateNumber(configuration, AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.0d),
                CalendarUtils.validateExcludedPeriods(configuration));
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
