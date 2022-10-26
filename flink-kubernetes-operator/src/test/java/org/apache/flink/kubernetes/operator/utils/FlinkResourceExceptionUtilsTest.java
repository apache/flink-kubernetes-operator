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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;
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

import java.util.List;
import java.util.Optional;

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
                configManager.getOperatorConfiguration().getExceptionStackTraceEnabled());
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

        testUpdateFlinkResourceException(configManager);

        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_ENABLED, true);
        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_MAX_LENGTH, 0);
        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_THROWABLE_LIST_MAX_COUNT, 0);
        testConfig.set(KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_FIELD_MAX_LENGTH, 0);

        configManager.updateDefaultConfig(testConfig);
        testUpdateFlinkResourceException(configManager);

        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_MAX_LENGTH, 100);
        testConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_THROWABLE_LIST_MAX_COUNT, 100);
        testConfig.set(KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_FIELD_MAX_LENGTH, 100);

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

            assertNull(flinkResourceException.getAdditionalMetadata());
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

            if (!configManager.getOperatorConfiguration().getExceptionStackTraceEnabled()) {
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

    private ReconciliationException getTestException() {
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

    private List<AbstractFlinkResource> getTestResources() {
        return List.of(new FlinkSessionJob(), new FlinkDeployment());
    }
}
