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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.kubernetes.operator.utils.FlinkRuntimeConfigurationUtils;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CustomJobConfigInfo}. */
class CustomJobConfigInfoTest {

    @ParameterizedTest
    @MethodSource("flinkJobConfigResponses")
    void testJobConfigurationCompatibilityAcrossFlinkVersions(String json) throws Exception {
        var configInfo = parseJobConfig(json);

        assertThat(FlinkRuntimeConfigurationUtils.mapJobConfiguration(configInfo))
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                "parallelism.default", "4",
                                "pipeline.object-reuse", "true",
                                "user.param", "value1"));
    }

    @Test
    void testMapJobConfigurationDropsOperatorControlledGlobalParameters() throws Exception {
        var configInfo =
                parseJobConfig(
                        """
                        {
                          "execution-config": {
                            "job-parallelism": 4,
                            "object-reuse-mode": true,
                            "user-config": {
                              "user.param": "value1",
                              "kubernetes.operator.job.upgrade.last-state-fallback.enabled": "false",
                              "job.autoscaler.enabled": "false"
                            }
                          }
                        }
                        """);

        assertThat(FlinkRuntimeConfigurationUtils.mapJobConfiguration(configInfo))
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                "parallelism.default", "4",
                                "pipeline.object-reuse", "true",
                                "user.param", "value1"));
    }

    @Test
    void testMapJobConfigurationHandlesMissingFields() throws Exception {
        var userConfigOnly =
                parseJobConfig(
                        """
                        {
                          "execution-config": {
                            "user-config": {
                              "user.param": "value1"
                            }
                          }
                        }
                        """);

        assertThat(FlinkRuntimeConfigurationUtils.mapJobConfiguration(userConfigOnly))
                .containsExactly(Map.entry("user.param", "value1"));
        assertThat(
                        FlinkRuntimeConfigurationUtils.mapJobConfiguration(
                                parseJobConfig("{\"execution-config\":{}}")))
                .isEmpty();
        assertThat(FlinkRuntimeConfigurationUtils.mapJobConfiguration(parseJobConfig("{}")))
                .isEmpty();
        assertThat(FlinkRuntimeConfigurationUtils.mapJobConfiguration(null)).isEmpty();
    }

    private static Stream<String> flinkJobConfigResponses() {
        return Stream.of(
                // Flink 1.x includes the deprecated execution-mode field.
                """
                {
                  "jid": "bc8cc01941f17a4fb5f8873b45512e19",
                  "name": "test-job",
                  "execution-config": {
                    "execution-mode": "PIPELINED",
                    "restart-strategy": "fixedDelay",
                    "job-parallelism": 4,
                    "object-reuse-mode": true,
                    "user-config": {
                      "user.param": "value1"
                    }
                  }
                }
                """,
                // Flink 2.x removed execution-mode. Unknown future fields must also be tolerated.
                """
                {
                  "jid": "bc8cc01941f17a4fb5f8873b45512e19",
                  "name": "test-job",
                  "future-root-field": "ignored",
                  "execution-config": {
                    "restart-strategy": "Cluster level default restart strategy",
                    "job-parallelism": 4,
                    "object-reuse-mode": true,
                    "user-config": {
                      "user.param": "value1"
                    },
                    "future-execution-field": "ignored"
                  }
                }
                """);
    }

    private static CustomJobConfigInfo parseJobConfig(String json) throws Exception {
        var flexible =
                RestMapperUtils.getFlexibleObjectMapper()
                        .readValue(json, CustomJobConfigInfo.class);
        var strict =
                RestMapperUtils.getStrictObjectMapper().readValue(json, CustomJobConfigInfo.class);
        assertThat(strict).isEqualTo(flexible);
        return flexible;
    }
}
