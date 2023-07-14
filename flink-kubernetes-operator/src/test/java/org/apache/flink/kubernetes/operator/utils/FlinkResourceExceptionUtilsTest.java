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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.FlinkResourceException;
import org.apache.flink.kubernetes.operator.exception.MissingJobManagerException;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.operator.utils.FlinkResourceExceptionUtils.LABELS;
import static org.apache.flink.kubernetes.operator.utils.FlinkResourceExceptionUtils.getSubstringWithMaxLength;
import static org.apache.flink.kubernetes.operator.utils.FlinkResourceExceptionUtils.updateFlinkResourceException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test class for {@link FlinkResourceExceptionUtils}. */
public class FlinkResourceExceptionUtilsTest {

    @Test
    public void testUpdateFlinkResourceExceptionWithConfig() throws JsonProcessingException {

        Configuration testConfig = new Configuration();
        FlinkConfigManager configManager = new FlinkConfigManager(testConfig);

        assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_ENABLED
                        .defaultValue(),
                configManager.getOperatorConfiguration().isExceptionStackTraceEnabled());
        assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_MAX_LENGTH
                        .defaultValue(),
                configManager.getOperatorConfiguration().getExceptionStackTraceLengthThreshold());
        assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_THROWABLE_LIST_MAX_COUNT
                        .defaultValue(),
                configManager.getOperatorConfiguration().getExceptionThrowableCountThreshold());
        assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_FIELD_MAX_LENGTH.defaultValue(),
                configManager.getOperatorConfiguration().getExceptionFieldLengthThreshold());
        assertEquals(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_LABEL_MAPPER.defaultValue(),
                configManager.getOperatorConfiguration().getExceptionLabelMapper());

        testUpdateFlinkResourceException(configManager);

        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_ENABLED, true);
        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_MAX_LENGTH, 0);
        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_THROWABLE_LIST_MAX_COUNT, 0);
        testConfig.set(KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_FIELD_MAX_LENGTH, 0);
        testConfig.set(KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_LABEL_MAPPER, Map.of());

        configManager.updateDefaultConfig(testConfig);
        testUpdateFlinkResourceException(configManager);

        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_MAX_LENGTH, 100);
        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_THROWABLE_LIST_MAX_COUNT, 100);
        testConfig.set(KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_FIELD_MAX_LENGTH, 100);
        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_LABEL_MAPPER,
                Map.of("rest client .*", "rest exception found"));

        configManager.updateDefaultConfig(testConfig);
        testUpdateFlinkResourceException(configManager);
    }

    private void testUpdateFlinkResourceException(FlinkConfigManager configManager)
            throws JsonProcessingException {

        ReconciliationException reconciliationException = getTestException();

        List<AbstractFlinkResource> resources = getTestResources();

        for (AbstractFlinkResource resource : resources) {

            updateFlinkResourceException(
                    reconciliationException, resource, configManager.getOperatorConfiguration());

            String errorJson =
                    Optional.ofNullable(resource)
                            .map(r -> (CommonStatus) r.getStatus())
                            .map(CommonStatus::getError)
                            .orElseThrow(RuntimeException::new);

            FlinkResourceException flinkResourceException =
                    new ObjectMapper().readValue(errorJson, FlinkResourceException.class);

            assertEquals(
                    getSubstringWithMaxLength(
                                    "reconciliation exception message",
                                    configManager
                                            .getOperatorConfiguration()
                                            .getExceptionFieldLengthThreshold())
                            .get(),
                    flinkResourceException.getMessage());
            assertEquals(
                    getSubstringWithMaxLength(
                                    "org.apache.flink.kubernetes.operator.exception.ReconciliationException",
                                    configManager
                                            .getOperatorConfiguration()
                                            .getExceptionFieldLengthThreshold())
                            .get(),
                    flinkResourceException.getType());

            assertTrue(
                    flinkResourceException.getThrowableList().size()
                            <= configManager
                                    .getOperatorConfiguration()
                                    .getExceptionThrowableCountThreshold());

            configManager
                    .getOperatorConfiguration()
                    .getExceptionLabelMapper()
                    .forEach(
                            (key, value) -> {
                                assertTrue(
                                        flinkResourceException.getAdditionalMetadata().get(LABELS)
                                                instanceof ArrayList);
                                assertTrue(
                                        ((ArrayList<?>)
                                                        flinkResourceException
                                                                .getAdditionalMetadata()
                                                                .get(LABELS))
                                                .contains(value));
                            });

            flinkResourceException
                    .getThrowableList()
                    .forEach(
                            exception -> {
                                if (exception.getType().contains("RestClientException")) {
                                    assertEquals(
                                            "rest client exception message",
                                            exception.getMessage());
                                    assertEquals(
                                            400,
                                            exception
                                                    .getAdditionalMetadata()
                                                    .get("httpResponseCode"));
                                } else if (exception
                                        .getType()
                                        .contains("DeploymentFailedException")) {
                                    assertEquals("dfe message", exception.getMessage());
                                    assertEquals(
                                            "dfe reason",
                                            exception.getAdditionalMetadata().get("reason"));
                                }
                                assertTrue(
                                        exception.getMessage().length()
                                                <= configManager
                                                        .getOperatorConfiguration()
                                                        .getExceptionFieldLengthThreshold());
                                assertTrue(
                                        exception.getType().length()
                                                <= configManager
                                                        .getOperatorConfiguration()
                                                        .getExceptionFieldLengthThreshold());
                            });

            if (!configManager.getOperatorConfiguration().isExceptionStackTraceEnabled()) {
                assertNull(flinkResourceException.getStackTrace());
            } else {
                assertTrue(
                        flinkResourceException.getStackTrace().length()
                                <= configManager
                                        .getOperatorConfiguration()
                                        .getExceptionStackTraceLengthThreshold());
            }
        }
    }

    private static Stream<Arguments> labelMapperProvider() {
        return Stream.of(
                Arguments.of(Map.of(), List.of(), getTestException()),
                Arguments.of(
                        Map.of("test rest client", "rest exception found"),
                        List.of(),
                        getTestException()),
                Arguments.of(
                        Map.of("rest client .*", "rest exception found"),
                        List.of("rest exception found"),
                        getTestException()),
                Arguments.of(
                        Map.of(
                                "rest client .*",
                                "rest exception found",
                                ".*missing.*",
                                "job manager is missing"),
                        List.of("rest exception found", "job manager is missing"),
                        getTestException()),
                Arguments.of(
                        Map.of("test rest client", "rest exception found"),
                        List.of(),
                        new Exception()));
    }

    @ParameterizedTest
    @MethodSource("labelMapperProvider")
    public void testUpdateFlinkResourceExceptionWithLabelMapper(
            Map<String, String> labelMapper,
            List<String> expectedLabelMapperMetadata,
            Exception testException)
            throws JsonProcessingException {
        Configuration testConfig = new Configuration();
        FlinkConfigManager configManager = new FlinkConfigManager(testConfig);

        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_LABEL_MAPPER, labelMapper);
        configManager.updateDefaultConfig(testConfig);

        List<AbstractFlinkResource> resources = getTestResources();
        for (AbstractFlinkResource resource : resources) {
            updateFlinkResourceException(
                    testException, resource, configManager.getOperatorConfiguration());

            String errorJson =
                    Optional.ofNullable(resource)
                            .map(r -> (CommonStatus) r.getStatus())
                            .map(CommonStatus::getError)
                            .orElseThrow(RuntimeException::new);

            FlinkResourceException flinkResourceException =
                    new ObjectMapper().readValue(errorJson, FlinkResourceException.class);

            if (flinkResourceException.getAdditionalMetadata().get(LABELS) != null) {
                assertTrue(
                        flinkResourceException.getAdditionalMetadata().get(LABELS) instanceof List);
                List<String> labelMapperList =
                        (ArrayList) flinkResourceException.getAdditionalMetadata().get(LABELS);

                expectedLabelMapperMetadata.forEach(
                        (value) -> {
                            assertTrue(labelMapperList.contains(value));
                        });

                assertEquals(expectedLabelMapperMetadata.size(), labelMapperList.size());
            }
        }
    }

    private static ReconciliationException getTestException() {
        return new ReconciliationException(
                "reconciliation exception message",
                new MissingJobManagerException(
                        "missing job manager exception message",
                        new RestClientException(
                                "rest client exception message",
                                new FlinkRuntimeException(
                                        "flink runtime exception message",
                                        new DeploymentFailedException("dfe message", "dfe reason")),
                                new HttpResponseStatus(400, "http response status"))));
    }

    private static List<AbstractFlinkResource> getTestResources() {
        return List.of(new FlinkSessionJob(), new FlinkDeployment());
    }
}
