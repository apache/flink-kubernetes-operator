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

package org.apache.flink.kubernetes.operator.api.utils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentReconciliationStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;

import io.fabric8.kubernetes.api.model.Condition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link ConditionUtils}. */
class ConditionUtilsTest {

    /**
     * Helper method to get a condition by type from a list of conditions.
     *
     * @param conditions the list of conditions
     * @param type the condition type to find
     * @return the condition with the specified type, or null if not found
     */
    private Condition getConditionByType(List<Condition> conditions, String type) {
        return conditions.stream().filter(c -> type.equals(c.getType())).findFirst().orElse(null);
    }

    @Test
    void testCreateConditionFromStatusWithNullReconciliationStatus() {
        FlinkDeploymentStatus status = new FlinkDeploymentStatus();
        status.setReconciliationStatus(null);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertTrue(
                conditions.isEmpty(),
                "Should return empty list when reconciliation status is null");
    }

    @Test
    void testCreateConditionFromStatusWithNullDeploymentSpec() {
        FlinkDeploymentStatus status = new FlinkDeploymentStatus();
        FlinkDeploymentReconciliationStatus reconciliationStatus =
                new FlinkDeploymentReconciliationStatus();
        status.setReconciliationStatus(reconciliationStatus);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertTrue(conditions.isEmpty(), "Should return empty list when deployment spec is null");
    }

    @Test
    void testApplicationModeConditionWithRunningJob() {
        FlinkDeploymentStatus status = BaseTestUtils.createApplicationModeStatus(JobStatus.RUNNING);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertEquals(1, conditions.size(), "Should return one condition");
        Condition condition = getConditionByType(conditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(condition);
        assertEquals(ConditionUtils.CONDITION_TYPE_RUNNING, condition.getType());
        assertEquals("True", condition.getStatus());
        assertEquals("Running", condition.getReason());
        assertEquals("Job status RUNNING", condition.getMessage());
        assertNotNull(condition.getLastTransitionTime());
    }

    @Test
    void testApplicationModeConditionWithFailedJob() {
        FlinkDeploymentStatus status = BaseTestUtils.createApplicationModeStatus(JobStatus.FAILED);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertEquals(1, conditions.size());
        Condition condition = getConditionByType(conditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(condition);
        assertEquals(ConditionUtils.CONDITION_TYPE_RUNNING, condition.getType());
        assertEquals("False", condition.getStatus());
        assertEquals("Failed", condition.getReason());
        assertEquals("Job status FAILED", condition.getMessage());
        assertNotNull(condition.getLastTransitionTime());
    }

    @Test
    void testApplicationModeConditionWithCanceledJob() {
        FlinkDeploymentStatus status =
                BaseTestUtils.createApplicationModeStatus(JobStatus.CANCELED);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertEquals(1, conditions.size());
        Condition condition = getConditionByType(conditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(condition);
        assertEquals("False", condition.getStatus());
        assertEquals("Canceled", condition.getReason());
        assertEquals("Job status CANCELED", condition.getMessage());
        assertNotNull(condition.getLastTransitionTime());
    }

    @Test
    void testApplicationModeConditionWithFinishedJob() {
        FlinkDeploymentStatus status =
                BaseTestUtils.createApplicationModeStatus(JobStatus.FINISHED);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertEquals(1, conditions.size());
        Condition condition = getConditionByType(conditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(condition);
        assertEquals("False", condition.getStatus());
        assertEquals("Finished", condition.getReason());
        assertEquals("Job status FINISHED", condition.getMessage());
        assertNotNull(condition.getLastTransitionTime());
    }

    @Test
    void testApplicationModeConditionWithCreatedJob() {
        FlinkDeploymentStatus status = BaseTestUtils.createApplicationModeStatus(JobStatus.CREATED);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertEquals(1, conditions.size());
        Condition condition = getConditionByType(conditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(condition);
        assertEquals("False", condition.getStatus());
        assertEquals("Created", condition.getReason());
        assertNotNull(condition.getLastTransitionTime());
    }

    @Test
    void testSessionModeConditionWithReadyJobManager() {
        FlinkDeploymentStatus status =
                BaseTestUtils.createSessionModeStatus(JobManagerDeploymentStatus.READY);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertEquals(1, conditions.size());
        Condition condition = getConditionByType(conditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(condition);
        assertEquals(ConditionUtils.CONDITION_TYPE_RUNNING, condition.getType());
        assertEquals("True", condition.getStatus());
        assertEquals(JobManagerDeploymentStatus.READY.getReason(), condition.getReason());
        assertEquals(JobManagerDeploymentStatus.READY.getMessage(), condition.getMessage());
        assertNotNull(condition.getLastTransitionTime());
    }

    @Test
    void testSessionModeConditionWithDeployingJobManager() {
        FlinkDeploymentStatus status =
                BaseTestUtils.createSessionModeStatus(JobManagerDeploymentStatus.DEPLOYING);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertEquals(1, conditions.size());
        Condition condition = getConditionByType(conditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(condition);
        assertEquals("False", condition.getStatus());
        assertEquals(JobManagerDeploymentStatus.DEPLOYING.getReason(), condition.getReason());
        assertNotNull(condition.getLastTransitionTime());
    }

    @Test
    void testSessionModeConditionWithMissingJobManager() {
        FlinkDeploymentStatus status =
                BaseTestUtils.createSessionModeStatus(JobManagerDeploymentStatus.MISSING);

        List<Condition> conditions = ConditionUtils.createConditionFromStatus(status);

        assertEquals(1, conditions.size());
        Condition condition = getConditionByType(conditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(condition);
        assertEquals("False", condition.getStatus());
        assertNotNull(condition.getLastTransitionTime());
    }

    @Test
    void testConditionTypeIsAlwaysRunning() {
        // Test application mode
        FlinkDeploymentStatus appStatus =
                BaseTestUtils.createApplicationModeStatus(JobStatus.RUNNING);
        List<Condition> appConditions = ConditionUtils.createConditionFromStatus(appStatus);
        Condition appCondition =
                getConditionByType(appConditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(appCondition);
        assertEquals(ConditionUtils.CONDITION_TYPE_RUNNING, appCondition.getType());

        // Test session mode
        FlinkDeploymentStatus sessionStatus =
                BaseTestUtils.createSessionModeStatus(JobManagerDeploymentStatus.READY);
        List<Condition> sessionConditions = ConditionUtils.createConditionFromStatus(sessionStatus);
        Condition sessionCondition =
                getConditionByType(sessionConditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(sessionCondition);
        assertEquals(ConditionUtils.CONDITION_TYPE_RUNNING, sessionCondition.getType());
    }

    @Test
    void testGetLastTransitionTimeStamp_StatusUnchanged() throws InterruptedException {
        // Test that timestamp is preserved when status doesn't change
        FlinkDeploymentStatus status = BaseTestUtils.createApplicationModeStatus(JobStatus.RUNNING);

        // First call - creates initial condition with timestamp
        List<Condition> firstConditions = ConditionUtils.createConditionFromStatus(status);
        Condition firstCondition =
                getConditionByType(firstConditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(firstCondition);
        String firstTimestamp = firstCondition.getLastTransitionTime();

        // Add the condition to status
        status.getConditions().add(firstCondition);

        // Second call - status unchanged (still RUNNING)
        List<Condition> secondConditions = ConditionUtils.createConditionFromStatus(status);
        Condition secondCondition =
                getConditionByType(secondConditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(secondCondition);
        String secondTimestamp = secondCondition.getLastTransitionTime();

        // Timestamp should be preserved since status didn't change
        assertEquals(firstTimestamp, secondTimestamp);
    }

    @Test
    void testGetLastTransitionTimeStamp_StatusChanged() throws InterruptedException {
        // Test that timestamp is updated when status changes
        FlinkDeploymentStatus status = BaseTestUtils.createApplicationModeStatus(JobStatus.RUNNING);

        // First call - creates initial condition with RUNNING status
        List<Condition> firstConditions = ConditionUtils.createConditionFromStatus(status);
        Condition firstCondition =
                getConditionByType(firstConditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(firstCondition);
        String firstTimestamp = firstCondition.getLastTransitionTime();

        // Add the condition to status
        status.getConditions().add(firstCondition);

        // Change status to FAILED
        status.getJobStatus().setState(JobStatus.FAILED);

        // Second call - status changed to FAILED
        List<Condition> secondConditions = ConditionUtils.createConditionFromStatus(status);
        Condition secondCondition =
                getConditionByType(secondConditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(secondCondition);
        String secondTimestamp = secondCondition.getLastTransitionTime();

        // Timestamp should be different since status changed
        assertTrue(
                !firstTimestamp.equals(secondTimestamp),
                "Timestamp should be updated when status changes");
    }

    @Test
    void testGetLastTransitionTimeStamp_SessionMode() throws InterruptedException {
        // Test timestamp behavior in session mode
        FlinkDeploymentStatus status =
                BaseTestUtils.createSessionModeStatus(JobManagerDeploymentStatus.READY);

        // First call
        List<Condition> firstConditions = ConditionUtils.createConditionFromStatus(status);
        Condition firstCondition =
                getConditionByType(firstConditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(firstCondition);
        String firstTimestamp = firstCondition.getLastTransitionTime();

        // Add condition to status
        status.getConditions().add(firstCondition);

        // Second call with same status
        List<Condition> secondConditions = ConditionUtils.createConditionFromStatus(status);
        Condition secondCondition =
                getConditionByType(secondConditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(secondCondition);
        String secondTimestamp = secondCondition.getLastTransitionTime();
        // Timestamp should be preserved
        assertEquals(firstTimestamp, secondTimestamp);

        // Change to DEPLOYING
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);

        // Third call with changed status
        List<Condition> thirdConditions = ConditionUtils.createConditionFromStatus(status);
        Condition thirdCondition =
                getConditionByType(thirdConditions, ConditionUtils.CONDITION_TYPE_RUNNING);
        assertNotNull(thirdCondition);
        String thirdTimestamp = thirdCondition.getLastTransitionTime();
        // Timestamp should be updated
        assertTrue(
                !secondTimestamp.equals(thirdTimestamp),
                "Timestamp should be updated when JobManager status changes");
    }
}
