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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobReconciliationStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.service.CheckpointHistoryWrapper;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.util.SerializedThrowable;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.JobStatus.CANCELLING;
import static org.apache.flink.api.common.JobStatus.RECONCILING;
import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.kubernetes.operator.TestUtils.MAX_RECONCILE_TIMES;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED;
import static org.apache.flink.kubernetes.operator.utils.EventRecorder.Reason.ValidationError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** {@link FlinkSessionJobController} tests. */
@EnableKubernetesMockClient(crud = true)
class FlinkSessionJobControllerTest {
    private KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private Context context;

    private TestingFlinkService flinkService = new TestingFlinkService();
    private TestingFlinkSessionJobController testController;
    private FlinkSessionJob sessionJob = TestUtils.buildSessionJob();
    private FlinkSessionJob suspendedSessionJob = TestUtils.buildSessionJob(JobState.SUSPENDED);

    @BeforeEach
    public void before() {
        flinkService = new TestingFlinkService();
        testController = new TestingFlinkSessionJobController(configManager, flinkService);
        sessionJob = TestUtils.buildSessionJob();
        suspendedSessionJob = TestUtils.buildSessionJob(JobState.SUSPENDED);
        kubernetesClient.resource(sessionJob).createOrReplace();
        context = TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient);
    }

    @Test
    public void testSubmitJobButException() {
        flinkService.setDeployFailure(true);

        try {
            testController.reconcile(sessionJob, context);
        } catch (Exception e) {
            // Ignore
        }

        Assertions.assertEquals(2, testController.events().size());
        // Discard submit event
        testController.events().remove();

        var event = testController.events().remove();
        Assertions.assertEquals(EventRecorder.Type.Warning.toString(), event.getType());
        Assertions.assertEquals("Error", event.getReason());

        testController.cleanup(sessionJob, context);
    }

    @Test
    public void verifyBasicReconcileLoop() throws Exception {
        UpdateControl<FlinkSessionJob> updateControl;

        assertEquals(
                ReconciliationState.UPGRADING,
                sessionJob.getStatus().getReconciliationStatus().getState());
        assertNull(sessionJob.getStatus().getJobStatus().getState());

        verifyNormalBasicReconcileLoop(sessionJob);

        // Send in invalid update
        sessionJob.getSpec().getJob().setParallelism(-1);
        updateControl = testController.reconcile(sessionJob, context);

        assertEquals(RUNNING, sessionJob.getStatus().getJobStatus().getState());
        assertEquals(6, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isPatchStatus());

        FlinkSessionJobReconciliationStatus reconciliationStatus =
                sessionJob.getStatus().getReconciliationStatus();
        assertTrue(
                sessionJob
                        .getStatus()
                        .getError()
                        .contains("Job parallelism must be larger than 0"));
        assertNotNull(reconciliationStatus.deserializeLastReconciledSpec().getJob());

        // Validate job status correct even with error
        var jobStatus = sessionJob.getStatus().getJobStatus();
        JobStatusMessage expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState(), jobStatus.getState());

        // Validate last stable spec is still the old one
        assertEquals(
                sessionJob.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                sessionJob.getStatus().getReconciliationStatus().getLastStableSpec());
    }

    @Test
    public void verifyBasicReconcileLoopForSuspendedSessionJob() throws Exception {
        assertEquals(
                ReconciliationState.UPGRADING,
                suspendedSessionJob.getStatus().getReconciliationStatus().getState());
        assertNull(suspendedSessionJob.getStatus().getJobStatus().getState());

        int reconcileTimes = 0;
        while (reconcileTimes < MAX_RECONCILE_TIMES) {
            verifyReconcileInitialSuspendedDeployment(suspendedSessionJob);
            reconcileTimes++;
        }

        suspendedSessionJob.getSpec().getJob().setState(JobState.RUNNING);
        verifyNormalBasicReconcileLoop(suspendedSessionJob);
    }

    @Test
    public void verifyReconcileLoopForInitialSuspendedSessionJobWithSavepoint() throws Exception {
        assertEquals(
                ReconciliationState.UPGRADING,
                suspendedSessionJob.getStatus().getReconciliationStatus().getState());
        assertNull(suspendedSessionJob.getStatus().getJobStatus().getState());

        int reconcileTimes = 0;
        while (reconcileTimes < MAX_RECONCILE_TIMES) {
            verifyReconcileInitialSuspendedDeployment(suspendedSessionJob);
            reconcileTimes++;
        }

        suspendedSessionJob.getSpec().getJob().setState(JobState.RUNNING);
        suspendedSessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        suspendedSessionJob.getSpec().getJob().setInitialSavepointPath("s0");
        verifyNormalBasicReconcileLoop(suspendedSessionJob);
        var jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);
    }

    @Test
    public void verifyUpgradeFromSavepointLegacy() throws Exception {
        UpdateControl<FlinkDeployment> updateControl;

        sessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        sessionJob.getSpec().getJob().setInitialSavepointPath("s0");
        sessionJob.getSpec().getFlinkConfiguration().put(SNAPSHOT_RESOURCE_ENABLED.key(), "false");
        testController.reconcile(sessionJob, context);
        var jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);
        assertEquals("s0", sessionJob.getStatus().getJobStatus().getUpgradeSavepointPath());

        var previousJobs = new ArrayList<>(jobs);
        sessionJob.getSpec().getJob().setInitialSavepointPath("s1");

        // Send in a no-op change
        testController.reconcile(sessionJob, context);
        assertEquals(previousJobs, new ArrayList<>(flinkService.listJobs()));
        assertEquals("s0", sessionJob.getStatus().getJobStatus().getUpgradeSavepointPath());

        // Upgrade job
        sessionJob.getSpec().getJob().setParallelism(100);
        updateControl = testController.reconcile(sessionJob, context);
        assertEquals(
                "savepoint_0", sessionJob.getStatus().getJobStatus().getUpgradeSavepointPath());

        assertEquals(0L, updateControl.getScheduleDelay().get());
        assertEquals(
                JobState.SUSPENDED,
                sessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());

        flinkService.clearJobsInTerminalState();

        testController.reconcile(sessionJob, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_0", jobs.get(0).f0);
        testController.reconcile(sessionJob, context);
        assertEquals(
                "savepoint_0", sessionJob.getStatus().getJobStatus().getUpgradeSavepointPath());

        // Suspend job
        sessionJob.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(sessionJob, context);
        flinkService.clearJobsInTerminalState();

        // Resume from last savepoint
        sessionJob.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(sessionJob, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_1", jobs.get(0).f0);
        assertEquals(
                "savepoint_1", sessionJob.getStatus().getJobStatus().getUpgradeSavepointPath());

        testController.reconcile(sessionJob, context);
        testController.cleanup(sessionJob, context);

        flinkService.clearJobsInTerminalState();
        jobs = flinkService.listJobs();
        assertEquals(0, jobs.size());
    }

    @Test
    public void verifyLastStateUpgrade() throws Exception {
        sessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        testController.reconcile(sessionJob, context);

        // Simulate completed checkpoints
        flinkService.setCheckpointInfo(
                Tuple2.of(
                        Optional.of(
                                new CheckpointHistoryWrapper.CompletedCheckpointInfo(
                                        0, "cp1", System.currentTimeMillis())),
                        Optional.empty()));

        // Trigger Update
        sessionJob.getSpec().setRestartNonce(3L);
        testController.reconcile(sessionJob, context);

        // Make sure we are cancelling
        assertEquals(CANCELLING, sessionJob.getStatus().getJobStatus().getState());

        // Once cancelling completed make sure that last reconciled spec is correctly upgraded and
        // job was started from cp
        testController.reconcile(sessionJob, context);
        assertEquals("cp1", sessionJob.getStatus().getJobStatus().getUpgradeSavepointPath());
        assertEquals(
                UpgradeMode.SAVEPOINT,
                sessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getUpgradeMode());
        assertEquals(RECONCILING, sessionJob.getStatus().getJobStatus().getState());
        assertEquals(
                ReconciliationState.DEPLOYED,
                sessionJob.getStatus().getReconciliationStatus().getState());

        flinkService.clearJobsInTerminalState();
        var jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("cp1", jobs.get(0).f0);

        testController.reconcile(sessionJob, context);
        assertEquals(RUNNING, sessionJob.getStatus().getJobStatus().getState());

        // Suspend job
        flinkService.setCheckpointInfo(
                Tuple2.of(
                        Optional.of(
                                new CheckpointHistoryWrapper.CompletedCheckpointInfo(
                                        0, "cp2", System.currentTimeMillis())),
                        Optional.empty()));
        sessionJob.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(sessionJob, context);
        testController.reconcile(sessionJob, context);
        assertEquals("cp2", sessionJob.getStatus().getJobStatus().getUpgradeSavepointPath());
    }

    @Test
    public void verifyLastStateUpgradeFailure() throws Exception {
        sessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        testController.reconcile(sessionJob, context);

        // Simulate completed checkpoints
        flinkService.setCheckpointInfo(
                Tuple2.of(
                        Optional.of(
                                new CheckpointHistoryWrapper.CompletedCheckpointInfo(
                                        0, "cp1", System.currentTimeMillis())),
                        Optional.empty()));

        // Trigger Update
        sessionJob.getSpec().setRestartNonce(3L);
        testController.events().clear();
        testController.reconcile(sessionJob, context);

        // Make sure we are cancelling
        assertEquals(CANCELLING, sessionJob.getStatus().getJobStatus().getState());
        testController.events().poll();
        assertEquals(
                testController.events().poll().getReason(),
                EventRecorder.Reason.SpecChanged.name());
        testController.events().clear();

        // Remove all jobs to trigger not found error
        flinkService.clear();

        testController.reconcile(sessionJob, context);
        assertEquals(JobStatusObserver.JOB_NOT_FOUND_ERR, sessionJob.getStatus().getError());
        assertEquals(RECONCILING, sessionJob.getStatus().getJobStatus().getState());
        assertEquals(
                ReconciliationState.DEPLOYED,
                sessionJob.getStatus().getReconciliationStatus().getState());

        testController.events().clear();
        testController.reconcile(sessionJob, context);
        assertEquals(
                testController.events().poll().getReason(), EventRecorder.Reason.Missing.name());
        assertTrue(testController.events().isEmpty());
        testController.reconcile(sessionJob, context);
        assertEquals(
                testController.events().poll().getReason(), EventRecorder.Reason.Missing.name());
        assertTrue(testController.events().isEmpty());

        // Deletion should still work
        var deleteControl = testController.cleanup(sessionJob, context);
        assertTrue(deleteControl.isRemoveFinalizer());
    }

    @Test
    public void verifyStatelessUpgrade() throws Exception {
        UpdateControl<FlinkDeployment> updateControl;

        sessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        sessionJob.getSpec().getJob().setInitialSavepointPath("s0");

        testController.reconcile(sessionJob, context);
        var jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);

        testController.reconcile(sessionJob, context);

        assertEquals(2, testController.events().size());
        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.JobStatusChanged,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));

        // Upgrade job
        sessionJob.getSpec().getJob().setParallelism(100);
        updateControl = testController.reconcile(sessionJob, context);
        assertEquals(2, testController.events().size());
        assertEquals(
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Suspended,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));

        assertEquals(
                configManager.getOperatorConfiguration().getProgressCheckInterval().toMillis(),
                updateControl.getScheduleDelay().get());
        assertEquals(
                JobState.RUNNING,
                sessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());

        updateControl = testController.reconcile(sessionJob, context);
        flinkService.clearJobsInTerminalState();

        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        testController.reconcile(sessionJob, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertNull(jobs.get(0).f0);

        assertEquals(3, testController.events().size());
        assertEquals(
                EventRecorder.Reason.JobStatusChanged,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.JobStatusChanged,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));

        // Suspend job
        sessionJob.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(sessionJob, context);

        assertEquals(2, testController.events().size());
        assertEquals(
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Suspended,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));
        testController.reconcile(sessionJob, context);
        testController.events().clear();

        // Resume from empty state
        sessionJob.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(sessionJob, context);
        flinkService.clearJobsInTerminalState();
        testController.reconcile(sessionJob, context);
        assertEquals(3, testController.events().size());
        assertEquals(
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.JobStatusChanged,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));

        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertNull(jobs.get(0).f0);

        // Inject validation error in the middle of the upgrade
        sessionJob.getSpec().setRestartNonce(123L);
        testController.reconcile(sessionJob, context);
        assertEquals(2, testController.events().size());
        assertEquals(
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Suspended,
                EventRecorder.Reason.valueOf(testController.events().poll().getReason()));

        sessionJob.getSpec().getJob().setParallelism(-1);
        testController.reconcile(sessionJob, context);
        flinkService.clearJobsInTerminalState();
        assertEquals(3, testController.events().size());
        testController.reconcile(sessionJob, context);
        var statusEvents =
                testController.events().stream()
                        .filter(e -> !e.getReason().equals(ValidationError.name()))
                        .collect(Collectors.toList());
        assertEquals(3, statusEvents.size());
        assertEquals(
                EventRecorder.Reason.JobStatusChanged,
                EventRecorder.Reason.valueOf(statusEvents.get(0).getReason()));
        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(statusEvents.get(1).getReason()));
        assertEquals(
                EventRecorder.Reason.JobStatusChanged,
                EventRecorder.Reason.valueOf(statusEvents.get(2).getReason()));

        assertEquals(RUNNING, sessionJob.getStatus().getJobStatus().getState());
        assertEquals(
                JobState.RUNNING,
                sessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
    }

    @Test
    public void verifyReconcileWithBadConfig() throws Exception {
        UpdateControl<FlinkDeployment> updateControl;
        // Override headers, and it should be saved in lastReconciledSpec once a successful
        // reconcile() finishes.
        sessionJob
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER.key(), "changed");
        updateControl = testController.reconcile(sessionJob, context);
        assertFalse(updateControl.isPatchStatus());
        assertEquals(RECONCILING, sessionJob.getStatus().getJobStatus().getState());

        // Check when the bad config is applied, observe() will change the cluster state correctly
        sessionJob.getSpec().getJob().setParallelism(-1);
        // Next reconcile will set error msg and observe with previous validated config
        updateControl = testController.reconcile(sessionJob, context);
        assertTrue(
                sessionJob
                        .getStatus()
                        .getError()
                        .contains("Job parallelism must be larger than 0"));
        assertFalse(updateControl.isPatchStatus());
        assertEquals(RUNNING, sessionJob.getStatus().getJobStatus().getState());

        // Make sure we do validation before getting effective config in reconcile().
        // Verify the saved headers in lastReconciledSpec is actually used in observe() by
        // utilizing listJobConsumer
        sessionJob
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER.key(), "again");
        flinkService.setListJobConsumer(
                (configuration) ->
                        assertEquals(
                                "changed",
                                configuration.get(
                                        KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER)));
        testController.reconcile(sessionJob, context);
        assertEquals(RUNNING, sessionJob.getStatus().getJobStatus().getState());
    }

    @Test
    public void testSuccessfulObservationShouldClearErrors() throws Exception {
        sessionJob.getSpec().getJob().setParallelism(-1);

        testController.reconcile(sessionJob, context);

        assertNull(sessionJob.getStatus().getReconciliationStatus().getLastStableSpec());

        // Failed Job deployment should set errors to the status
        assertTrue(
                sessionJob
                        .getStatus()
                        .getError()
                        .contains("Job parallelism must be larger than 0"));
        assertNull(sessionJob.getStatus().getJobStatus().getState());

        // Job deployment becomes ready and successful observation should clear the errors
        sessionJob.getSpec().getJob().setParallelism(1);
        testController.reconcile(sessionJob, context);
        assertNull(sessionJob.getStatus().getReconciliationStatus().getLastStableSpec());

        testController.reconcile(sessionJob, context);
        assertEquals(RUNNING, sessionJob.getStatus().getJobStatus().getState());
        assertNull(sessionJob.getStatus().getError());

        assertEquals(
                sessionJob.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                sessionJob.getStatus().getReconciliationStatus().getLastStableSpec());
    }

    @Test
    public void testValidationError() throws Exception {
        UpdateControl<FlinkDeployment> updateControl;

        sessionJob.getSpec().getJob().setParallelism(-1);
        updateControl = testController.reconcile(sessionJob, context);

        assertEquals(1, testController.events().size());
        assertNull(sessionJob.getStatus().getJobStatus().getState());

        var event = testController.events().remove();
        assertEquals("Warning", event.getType());
        assertEquals("ValidationError", event.getReason());
        assertTrue(event.getMessage().startsWith("Job parallelism "));

        // Failed spec should not be rescheduled
        assertEquals(Optional.empty(), updateControl.getScheduleDelay());
    }

    @Test
    public void testInitialSavepointOnError() throws Exception {
        sessionJob.getSpec().getJob().setInitialSavepointPath("msp");
        flinkService.setDeployFailure(true);
        try {
            testController.reconcile(sessionJob, context);
            fail();
        } catch (Exception expected) {
        }
        flinkService.setDeployFailure(false);
        testController.reconcile(sessionJob, context);
        assertEquals("msp", flinkService.listJobs().get(0).f0);
    }

    @Test
    public void testErrorOnReconcileWithChainedExceptions() throws Exception {
        sessionJob.getSpec().getJob().setInitialSavepointPath("msp");
        flinkService.setMakeItFailWith(
                new RuntimeException(
                        "Deployment Failure",
                        new IllegalStateException(
                                null,
                                new SerializedThrowable(new Exception("actual failure reason")))));
        try {
            testController.reconcile(sessionJob, context);
            fail();
        } catch (Exception expected) {
        }
        assertEquals(2, testController.events().size());

        var event = testController.events().remove();
        assertEquals("Submit", event.getReason());
        event = testController.events().remove();
        assertEquals("Error", event.getReason());
        assertEquals(
                "Deployment Failure -> IllegalStateException -> actual failure reason",
                event.getMessage());
    }

    @Test
    public void verifyCanaryHandling() throws Exception {
        var canary = TestUtils.createCanaryJob();
        kubernetesClient.resource(canary).create();
        assertTrue(testController.reconcile(canary, context).isNoUpdate());
        assertEquals(0, testController.getInternalStatusUpdateCount());
        assertEquals(1, testController.getCanaryResourceManager().getNumberOfActiveCanaries());
        testController.cleanup(canary, context);
        assertEquals(0, testController.getInternalStatusUpdateCount());
        assertEquals(0, testController.getCanaryResourceManager().getNumberOfActiveCanaries());
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUnsupportedVersions(FlinkVersion version) throws Exception {
        context =
                TestUtils.createContextWithReadyFlinkDeployment(
                        Map.of(), kubernetesClient, version);
        var updateControl = testController.reconcile(TestUtils.buildSessionJob(), context);
        var lastEvent = testController.events().poll();
        if (!version.isEqualOrNewer(FlinkVersion.v1_15)) {
            assertTrue(updateControl.getScheduleDelay().isEmpty());
            assertEquals(
                    EventRecorder.Reason.UnsupportedFlinkVersion.name(), lastEvent.getReason());
        } else {
            assertTrue(updateControl.getScheduleDelay().isPresent());
            assertEquals(EventRecorder.Reason.Submit.name(), lastEvent.getReason());
        }
    }

    @Test
    public void testCancelJobNotFound() throws Exception {
        testController.reconcile(sessionJob, context);

        var deleteControl = testController.cleanup(sessionJob, context);

        assertEquals(CANCELLING, sessionJob.getStatus().getJobStatus().getState());
        assertFalse(deleteControl.isRemoveFinalizer());
        assertEquals(
                ResourceLifecycleState.DELETING,
                testController
                        .getStatusUpdateCounter()
                        .currentResource
                        .getStatus()
                        .getLifecycleState());
        assertEquals(ResourceLifecycleState.DELETING, sessionJob.getStatus().getLifecycleState());
        assertEquals(
                configManager.getOperatorConfiguration().getProgressCheckInterval().toMillis(),
                deleteControl.getScheduleDelay().get());

        flinkService.clear();
        flinkService.setFlinkJobNotFound(true);
        deleteControl = testController.cleanup(sessionJob, context);
        assertTrue(deleteControl.isRemoveFinalizer());
        assertEquals(
                ResourceLifecycleState.DELETED,
                testController
                        .getStatusUpdateCounter()
                        .currentResource
                        .getStatus()
                        .getLifecycleState());
        assertEquals(ResourceLifecycleState.DELETED, sessionJob.getStatus().getLifecycleState());
    }

    private void verifyReconcileInitialSuspendedDeployment(FlinkSessionJob sessionJob)
            throws Exception {
        UpdateControl<FlinkDeployment> updateControl =
                testController.reconcile(suspendedSessionJob, context);
        // Reconciling
        assertEquals(JobState.SUSPENDED, suspendedSessionJob.getSpec().getJob().getState());
        assertNull(suspendedSessionJob.getStatus().getJobStatus().getState());
        assertEquals(1, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isPatchStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        // Validate reconciliation status
        FlinkSessionJobReconciliationStatus reconciliationStatus =
                suspendedSessionJob.getStatus().getReconciliationStatus();
        assertNull(suspendedSessionJob.getStatus().getError());
        assertNull(reconciliationStatus.deserializeLastReconciledSpec());
        assertNull(reconciliationStatus.getLastStableSpec());
    }

    private void verifyNormalBasicReconcileLoop(FlinkSessionJob sessionJob) throws Exception {
        UpdateControl<FlinkDeployment> updateControl =
                testController.reconcile(sessionJob, context);

        // Reconciling
        assertEquals(RECONCILING, sessionJob.getStatus().getJobStatus().getState());
        assertEquals(4, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isPatchStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        // Validate reconciliation status
        FlinkSessionJobReconciliationStatus reconciliationStatus =
                sessionJob.getStatus().getReconciliationStatus();
        assertNull(sessionJob.getStatus().getError());
        assertEquals(sessionJob.getSpec(), reconciliationStatus.deserializeLastReconciledSpec());
        assertNull(sessionJob.getStatus().getReconciliationStatus().getLastStableSpec());

        // Running
        updateControl = testController.reconcile(sessionJob, context);
        assertEquals(RUNNING, sessionJob.getStatus().getJobStatus().getState());
        assertEquals(5, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isPatchStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        // Stable loop
        updateControl = testController.reconcile(sessionJob, context);
        assertEquals(RUNNING, sessionJob.getStatus().getJobStatus().getState());
        assertEquals(5, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isPatchStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        // Validate job status
        var jobStatus = sessionJob.getStatus().getJobStatus();
        JobStatusMessage expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState(), jobStatus.getState());
        assertEquals(
                sessionJob.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                sessionJob.getStatus().getReconciliationStatus().getLastStableSpec());
    }
}
