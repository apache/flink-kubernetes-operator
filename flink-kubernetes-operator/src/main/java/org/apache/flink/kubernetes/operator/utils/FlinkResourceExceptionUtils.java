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

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.FlinkResourceException;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Flink Resource Exception utilities. */
public final class FlinkResourceExceptionUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String LABELS = "labels";

    public static <R extends AbstractFlinkResource> void updateFlinkResourceException(
            Throwable throwable, R resource, FlinkOperatorConfiguration conf) {
        FlinkResourceException flinkResourceException = getFlinkResourceException(throwable, conf);

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

    public static void updateFlinkStateSnapshotException(
            Throwable throwable, FlinkStateSnapshot resource, FlinkOperatorConfiguration conf) {

        FlinkResourceException flinkResourceException = getFlinkResourceException(throwable, conf);

        try {
            resource.getStatus().setError(convertToJson(flinkResourceException));
        } catch (Exception e) {
            // Rollback to setting error string/message to CRD
            resource.getStatus()
                    .setError(
                            (e instanceof ReconciliationException)
                                    ? e.getCause().toString()
                                    : e.toString());
        }
    }

    private static FlinkResourceException getFlinkResourceException(
            Throwable throwable, FlinkOperatorConfiguration conf) {

        boolean stackTraceEnabled = conf.isExceptionStackTraceEnabled();
        int stackTraceLengthThreshold = conf.getExceptionStackTraceLengthThreshold();
        int lengthThreshold = conf.getExceptionFieldLengthThreshold();
        int throwableCountThreshold = conf.getExceptionThrowableCountThreshold();
        Map<String, String> labelMapper = conf.getExceptionLabelMapper();

        Preconditions.checkNotNull(stackTraceEnabled);

        FlinkResourceException flinkResourceException =
                convertToFlinkResourceException(
                        throwable,
                        stackTraceEnabled,
                        stackTraceLengthThreshold,
                        lengthThreshold,
                        labelMapper);

        flinkResourceException.setThrowableList(
                ExceptionUtils.getThrowableList(throwable.getCause()).stream()
                        .limit(throwableCountThreshold)
                        .map(
                                (t) ->
                                        convertToFlinkResourceException(
                                                t,
                                                false,
                                                stackTraceLengthThreshold,
                                                lengthThreshold,
                                                labelMapper))
                        .collect(Collectors.toList()));

        return flinkResourceException;
    }

    private static FlinkResourceException convertToFlinkResourceException(
            Throwable throwable,
            boolean stackTraceEnabled,
            int stackTraceLengthThreshold,
            int lengthThreshold,
            Map<String, String> labelMapper) {
        FlinkResourceException flinkResourceException = FlinkResourceException.builder().build();

        getSubstringWithMaxLength(throwable.getClass().getName(), lengthThreshold)
                .ifPresent(flinkResourceException::setType);
        getSubstringWithMaxLength(throwable.getMessage(), lengthThreshold)
                .ifPresent(flinkResourceException::setMessage);

        if (stackTraceEnabled) {
            getSubstringWithMaxLength(
                            ExceptionUtils.getStackTrace(throwable), stackTraceLengthThreshold)
                    .ifPresent(flinkResourceException::setStackTrace);
        }

        enrichMetadata(throwable, flinkResourceException, lengthThreshold, labelMapper);

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
            int lengthThreshold,
            Map<String, String> labelMapper) {
        if (flinkResourceException.getAdditionalMetadata() == null) {
            flinkResourceException.setAdditionalMetadata(new HashMap<>());
        }

        if (throwable instanceof RestClientException) {
            flinkResourceException
                    .getAdditionalMetadata()
                    .put(
                            "httpResponseCode",
                            ((RestClientException) throwable).getHttpResponseStatus().code());
        }

        if (throwable instanceof DeploymentFailedException) {
            getSubstringWithMaxLength(
                            ((DeploymentFailedException) throwable).getReason(), lengthThreshold)
                    .ifPresent(
                            reason ->
                                    flinkResourceException
                                            .getAdditionalMetadata()
                                            .put("reason", reason));
        }

        labelMapper
                .entrySet()
                .forEach(
                        (entry) -> {
                            Pattern pattern = Pattern.compile(entry.getKey());

                            org.apache.flink.util.ExceptionUtils.findThrowable(
                                            throwable,
                                            t ->
                                                    pattern.matcher(
                                                                    Optional.ofNullable(
                                                                                    t.getMessage())
                                                                            .orElse(""))
                                                            .find())
                                    .ifPresent(
                                            (t) -> {
                                                enrichMetadataWithLabelMapper(
                                                        flinkResourceException, entry.getValue());
                                            });
                        });

        // This section can be extended to enrich more metadata in the future.
    }

    private static void enrichMetadataWithLabelMapper(
            FlinkResourceException flinkResourceException, String value) {
        if (!flinkResourceException.getAdditionalMetadata().containsKey(LABELS)) {
            flinkResourceException.getAdditionalMetadata().put(LABELS, new ArrayList<String>());
        }
        ((ArrayList) flinkResourceException.getAdditionalMetadata().get(LABELS)).add(value);
    }

    private static String convertToJson(FlinkResourceException flinkResourceException)
            throws JsonProcessingException {
        return objectMapper.writeValueAsString(flinkResourceException);
    }
}
