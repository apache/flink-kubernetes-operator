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

import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.FlinkResourceException;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Flink Resource Exception utilities. */
public final class FlinkResourceExceptionUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <R extends AbstractFlinkResource> void updateFlinkResourceException(
            Throwable throwable, R resource, FlinkOperatorConfiguration conf) {

        boolean stackTraceEnabled = conf.getExceptionStackTraceEnabled();
        int stackTraceLengthThreshold = conf.getExceptionStackTraceLengthThreshold();
        int lengthThreshold = conf.getExceptionFieldLengthThreshold();
        int throwableCountThreshold = conf.getExceptionThrowableCountThreshold();

        Preconditions.checkNotNull(stackTraceEnabled);

        FlinkResourceException flinkResourceException =
                getFlinkResourceException(
                        throwable,
                        stackTraceEnabled,
                        stackTraceLengthThreshold,
                        lengthThreshold,
                        throwableCountThreshold);

        try {
            ((AbstractFlinkResource<?, ?>) resource)
                    .getStatus()
                    .setError(convertToJson(flinkResourceException));
        } catch (Exception e) {
            // Rollback to setting error string/message to CRD
            ((AbstractFlinkResource<?, ?>) resource)
                    .getStatus()
                    .setError(
                            (e instanceof ReconciliationException)
                                    ? e.getCause().toString()
                                    : e.toString());
        }
    }

    private static FlinkResourceException getFlinkResourceException(
            Throwable throwable,
            boolean isStackTraceEnabled,
            int stackTraceLengthThreshold,
            int lengthThreshold,
            int throwableCountThreshold) {
        FlinkResourceException flinkResourceException =
                convertToFlinkResourceException(
                        throwable, isStackTraceEnabled, stackTraceLengthThreshold, lengthThreshold);

        flinkResourceException.setThrowableList(
                ExceptionUtils.getThrowableList(throwable.getCause()).stream()
                        .limit(throwableCountThreshold)
                        .map(
                                (t) ->
                                        convertToFlinkResourceException(
                                                t,
                                                false,
                                                stackTraceLengthThreshold,
                                                lengthThreshold))
                        .collect(Collectors.toList()));

        return flinkResourceException;
    }

    private static FlinkResourceException convertToFlinkResourceException(
            Throwable throwable,
            boolean stackTraceEnabled,
            int stackTraceLengthThreshold,
            int lengthThreshold) {
        FlinkResourceException flinkResourceException = FlinkResourceException.builder().build();

        getSubstringWithMaxLength(throwable.getClass().getName(), lengthThreshold)
                .ifPresent(flinkResourceException::setType);
        getSubstringWithMaxLength(throwable.getMessage(), lengthThreshold)
                .ifPresent(flinkResourceException::setMessage);

        if (stackTraceEnabled) {
            getSubstringWithMaxLength(ExceptionUtils.getStackTrace(throwable), lengthThreshold)
                    .ifPresent(flinkResourceException::setStackTrace);
        }

        enrichMetadata(throwable, flinkResourceException, lengthThreshold);

        return flinkResourceException;
    }

    public static Optional<String> getSubstringWithMaxLength(String str, int limit) {
        if (str == null) {
            return Optional.empty();
        } else {
            return Optional.of(str.substring(0, Math.min(str.length(), limit)));
        }
    }

    private static void enrichMetadata(
            Throwable throwable,
            FlinkResourceException flinkResourceException,
            int lengthThreshold) {
        if (throwable instanceof RestClientException) {
            flinkResourceException.setAdditionalMetadata(
                    Map.of(
                            "httpResponseCode",
                            ((RestClientException) throwable).getHttpResponseStatus().code()));
        }

        if (throwable instanceof DeploymentFailedException) {
            getSubstringWithMaxLength(
                            ((DeploymentFailedException) throwable).getReason(), lengthThreshold)
                    .ifPresent(
                            reason ->
                                    flinkResourceException.setAdditionalMetadata(
                                            Map.of("reason", reason)));
        }

        // This section can be extended to enrich more metadata in the future.
    }

    private static String convertToJson(FlinkResourceException flinkResourceException)
            throws JsonProcessingException {
        return objectMapper.writeValueAsString(flinkResourceException);
    }
}
