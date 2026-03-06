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

package org.apache.flink.kubernetes.operator.api.utils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;

import io.fabric8.kubernetes.api.model.Condition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Test for {@link ConditionsUtils}. */
public class ConditionsUtilsTest {

    private static Stream<Arguments> updateLastTransitionTimeParams() {
        String existingConditionLastTransitionTime = Instant.now().toString();

        return Stream.of(
                // Test Null
                Arguments.of(Collections.emptyList(), null, null, true),

                // Test Empty Conditions
                Arguments.of(
                        Collections.emptyList(),
                        new Condition(
                                "lastTransitionTime", "message", 0L, "reason", "status", "type"),
                        new Condition(
                                "lastTransitionTime", "message", 0L, "reason", "status", "type"),
                        false),

                // Test Correct Update
                Arguments.of(
                        List.of(
                                new Condition(
                                        existingConditionLastTransitionTime,
                                        "message",
                                        0L,
                                        "reason",
                                        "status",
                                        "type")),
                        new Condition(
                                existingConditionLastTransitionTime,
                                "message",
                                0L,
                                "reason",
                                "status",
                                "type"),
                        new Condition(
                                "lastTransitionTime", "message", 0L, "reason", "status", "type"),
                        true),

                // Test Status Unchanged
                Arguments.of(
                        List.of(
                                new Condition(
                                        existingConditionLastTransitionTime,
                                        "message",
                                        0L,
                                        "reason",
                                        "status",
                                        "type")),
                        new Condition(
                                existingConditionLastTransitionTime,
                                "message",
                                0L,
                                "reason",
                                "status",
                                "type"),
                        new Condition(
                                existingConditionLastTransitionTime,
                                "message",
                                0L,
                                "reason",
                                "status",
                                "type"),
                        true),

                // Test Status Changed
                Arguments.of(
                        List.of(
                                new Condition(
                                        existingConditionLastTransitionTime,
                                        "message",
                                        0L,
                                        "reason",
                                        "status1",
                                        "type")),
                        new Condition(
                                existingConditionLastTransitionTime,
                                "message",
                                0L,
                                "reason",
                                "status2",
                                "type"),
                        new Condition(
                                existingConditionLastTransitionTime,
                                "message",
                                0L,
                                "reason",
                                "status2",
                                "type"),
                        false),

                // Test Multiple Conditions with Multiple Types
                Arguments.of(
                        List.of(
                                new Condition("time1", "message", 0L, "reason", "status", "type1"),
                                new Condition("time2", "message", 0L, "reason", "status", "type1"),
                                new Condition("time3", "message", 0L, "reason", "status", "type2")),
                        new Condition("time1", "message", 0L, "reason", "status", "type1"),
                        new Condition(
                                "lastTransitionTime", "message", 0L, "reason", "status", "type1"),
                        true),

                // Test No Matching Type
                Arguments.of(
                        List.of(
                                new Condition(
                                        existingConditionLastTransitionTime,
                                        "message",
                                        0L,
                                        "reason",
                                        "status",
                                        "type1")),
                        new Condition(
                                existingConditionLastTransitionTime,
                                "message",
                                0L,
                                "reason",
                                "status",
                                "type2"),
                        new Condition(
                                "lastTransitionTime", "message", 0L, "reason", "status", "type2"),
                        false));
    }

    @ParameterizedTest
    @MethodSource("updateLastTransitionTimeParams")
    void testUpdateLastTransitionTime(
            List<Condition> conditions, Condition expected, Condition actual, boolean equals) {
        ConditionsUtils.updateLastTransitionTime(conditions, actual);
        if (equals) {
            assertEquals(expected, actual);
        } else {
            assertNotEquals(expected, actual);
        }
    }

    private static Stream<Arguments> createApplicationModeConditionParams() {
        return Stream.of(
                Arguments.of(JobStatus.RUNNING, "True", "Running", "Job status RUNNING"),
                Arguments.of(JobStatus.FAILED, "False", "Failed", "Job status FAILED"),
                Arguments.of(JobStatus.FINISHED, "False", "Finished", "Job status FINISHED"),
                Arguments.of(JobStatus.CANCELED, "False", "Canceled", "Job status CANCELED"),
                Arguments.of(JobStatus.CREATED, "False", "Created", "Job status CREATED"),
                Arguments.of(JobStatus.SUSPENDED, "False", "Suspended", "Job status SUSPENDED"),
                Arguments.of(JobStatus.FAILING, "False", "Failing", "Job status FAILING"),
                Arguments.of(JobStatus.CANCELLING, "False", "Cancelling", "Job status CANCELLING"),
                Arguments.of(JobStatus.RESTARTING, "False", "Restarting", "Job status RESTARTING"),
                Arguments.of(
                        JobStatus.RECONCILING, "False", "Reconciling", "Job status RECONCILING"),
                Arguments.of(
                        JobStatus.INITIALIZING,
                        "False",
                        "Initializing",
                        "Job status INITIALIZING"));
    }

    @ParameterizedTest
    @MethodSource("createApplicationModeConditionParams")
    void testCreateApplicationModeCondition(
            JobStatus jobStatus,
            String expectedStatus,
            String expectedReason,
            String expectedMessage) {
        Condition condition = ConditionsUtils.createApplicationModeCondition(jobStatus);

        assertNotNull(condition);
        assertEquals(FlinkDeploymentStatus.CONDITION_TYPE_RUNNING, condition.getType());
        assertEquals(expectedStatus, condition.getStatus());
        assertEquals(expectedReason, condition.getReason());
        assertEquals(expectedMessage, condition.getMessage());
    }

    private static Stream<Arguments> createSessionModeConditionParams() {
        return Stream.of(
                Arguments.of(
                        JobManagerDeploymentStatus.READY,
                        "True",
                        JobManagerDeploymentStatus.READY.getReason(),
                        JobManagerDeploymentStatus.READY.getMessage()),
                Arguments.of(
                        JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                        "False",
                        JobManagerDeploymentStatus.DEPLOYED_NOT_READY.getReason(),
                        JobManagerDeploymentStatus.DEPLOYED_NOT_READY.getMessage()),
                Arguments.of(
                        JobManagerDeploymentStatus.DEPLOYING,
                        "False",
                        JobManagerDeploymentStatus.DEPLOYING.getReason(),
                        JobManagerDeploymentStatus.DEPLOYING.getMessage()),
                Arguments.of(
                        JobManagerDeploymentStatus.MISSING,
                        "False",
                        JobManagerDeploymentStatus.MISSING.getReason(),
                        JobManagerDeploymentStatus.MISSING.getMessage()),
                Arguments.of(
                        JobManagerDeploymentStatus.ERROR,
                        "False",
                        JobManagerDeploymentStatus.ERROR.getReason(),
                        JobManagerDeploymentStatus.ERROR.getMessage()));
    }

    @ParameterizedTest
    @MethodSource("createSessionModeConditionParams")
    void testCreateSessionModeCondition(
            JobManagerDeploymentStatus jmStatus,
            String expectedStatus,
            String expectedReason,
            String expectedMessage) {
        Condition condition = ConditionsUtils.createSessionModeCondition(jmStatus);

        assertNotNull(condition);
        assertEquals(FlinkDeploymentStatus.CONDITION_TYPE_RUNNING, condition.getType());
        assertEquals(expectedStatus, condition.getStatus());
        assertEquals(expectedReason, condition.getReason());
        assertEquals(expectedMessage, condition.getMessage());
    }
}
