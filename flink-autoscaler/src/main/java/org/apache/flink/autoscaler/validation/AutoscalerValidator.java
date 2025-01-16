package org.apache.flink.autoscaler.validation;

import org.apache.flink.configuration.Configuration;

import java.util.Optional;

/** Validator for Autoscaler. */
public interface AutoscalerValidator {

    /**
     * Validate autoscaler config and return optional error.
     *
     * @param configuration autoscaler config
     * @return Optional error string, should be present iff validation resulted in an error
     */
    Optional<String> validateAutoscalerOptions(Configuration configuration);
}
