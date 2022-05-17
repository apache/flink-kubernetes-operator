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

package org.apache.flink.kubernetes.operator.reconciler.sessionjob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkSessionJobReconciler}. */
public class FlinkSessionJobReconcilerTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @Test
    public void testSubmitAndCleanUp() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        FlinkSessionJobReconciler reconciler =
                new FlinkSessionJobReconciler(null, flinkService, configManager);
        // session not found
        reconciler.reconcile(sessionJob, TestUtils.createEmptyContext());
        assertEquals(0, flinkService.listSessionJobs().size());

        // session not ready
        reconciler.reconcile(sessionJob, TestUtils.createContextWithNotReadyFlinkDeployment());
        assertEquals(0, flinkService.listSessionJobs().size());

        // session ready
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        assertEquals(1, flinkService.listSessionJobs().size());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listSessionJobs());
        // clean up
        reconciler.cleanup(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        assertEquals(0, flinkService.listSessionJobs().size());
    }

    @Test
    public void testRestart() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        FlinkSessionJobReconciler reconciler =
                new FlinkSessionJobReconciler(null, flinkService, configManager);
        // session ready
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        assertEquals(1, flinkService.listSessionJobs().size());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listSessionJobs());
        sessionJob.getSpec().setRestartNonce(2L);
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        assertEquals(0, flinkService.listSessionJobs().size());
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listSessionJobs());
    }

    @Test
    public void testSubmitWithInitialSavepointPath() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        var initSavepointPath = "file:///init-sp";
        sessionJob.getSpec().getJob().setInitialSavepointPath(initSavepointPath);
        FlinkSessionJobReconciler reconciler =
                new FlinkSessionJobReconciler(null, flinkService, configManager);
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                initSavepointPath,
                flinkService.listSessionJobs());
    }

    @Test
    public void testStatelessUpgrade() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        var readyContext = TestUtils.createContextWithReadyFlinkDeployment();
        FlinkSessionJobReconciler reconciler =
                new FlinkSessionJobReconciler(null, flinkService, configManager);
        reconciler.reconcile(sessionJob, readyContext);
        assertEquals(1, flinkService.listSessionJobs().size());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listSessionJobs());

        var statelessSessionJob = ReconciliationUtils.clone(sessionJob);
        statelessSessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        statelessSessionJob.getSpec().getJob().setParallelism(2);
        // job suspended first
        reconciler.reconcile(statelessSessionJob, readyContext);
        assertTrue(flinkService.listSessionJobs().isEmpty());
        verifyJobState(statelessSessionJob, JobState.SUSPENDED, JobState.SUSPENDED.name());

        reconciler.reconcile(statelessSessionJob, readyContext);
        assertEquals(1, flinkService.listSessionJobs().size());
        verifyAndSetRunningJobsToStatus(
                statelessSessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listSessionJobs());
    }

    @Test
    public void testSavepointUpgrade() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();
        FlinkSessionJobReconciler reconciler =
                new FlinkSessionJobReconciler(null, flinkService, configManager);

        var readyContext = TestUtils.createContextWithReadyFlinkDeployment();
        reconciler.reconcile(sessionJob, readyContext);
        // start the job
        assertEquals(1, flinkService.listSessionJobs().size());
        assertTrue(
                sessionJob
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getSavepointHistory()
                        .isEmpty());

        // update job spec
        var statefulSessionJob = ReconciliationUtils.clone(sessionJob);
        statefulSessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        statefulSessionJob.getSpec().getJob().setParallelism(3);
        reconciler.reconcile(statefulSessionJob, readyContext);

        // job suspended first
        assertTrue(flinkService.listSessionJobs().isEmpty());
        verifyJobState(statefulSessionJob, JobState.SUSPENDED, JobState.SUSPENDED.name());
        assertEquals(
                1,
                statefulSessionJob
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getSavepointHistory()
                        .size());

        // upgraded
        reconciler.reconcile(statefulSessionJob, readyContext);
        assertEquals(1, flinkService.listSessionJobs().size());
        verifyAndSetRunningJobsToStatus(
                statefulSessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                "savepoint_0",
                flinkService.listSessionJobs());
    }

    @Test
    public void testUseTheEffectiveConfigToSubmit() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        var readyContext =
                TestUtils.createContextWithReadyFlinkDeployment(Map.of("key", "newValue"));

        FlinkSessionJobReconciler reconciler =
                new FlinkSessionJobReconciler(null, flinkService, configManager);

        reconciler.reconcile(sessionJob, readyContext);

        assertEquals(1, flinkService.listSessionJobs().size());
        var submittedJob =
                verifyAndReturnTheSubmittedJob(sessionJob, flinkService.listSessionJobs());
        assertEquals("newValue", submittedJob.effectiveConfig.getString("key", null));
    }

    @Test
    public void testTriggerSavepoint() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();
        assertFalse(SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));

        var readyContext = TestUtils.createContextWithReadyFlinkDeployment();
        FlinkSessionJobReconciler reconciler =
                new FlinkSessionJobReconciler(null, flinkService, configManager);
        reconciler.reconcile(sessionJob, readyContext);
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listSessionJobs());

        assertFalse(SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));

        // trigger savepoint
        var sp1SessionJob = ReconciliationUtils.clone(sessionJob);

        // do not trigger savepoint if nonce is null
        reconciler.reconcile(sp1SessionJob, readyContext);
        assertFalse(SavepointUtils.savepointInProgress(sp1SessionJob.getStatus().getJobStatus()));

        sp1SessionJob.getSpec().getJob().setSavepointTriggerNonce(2L);
        sp1SessionJob
                .getStatus()
                .getJobStatus()
                .setState(org.apache.flink.api.common.JobStatus.CREATED.name());
        reconciler.reconcile(sp1SessionJob, readyContext);
        // do not trigger savepoint if job is not running
        assertFalse(SavepointUtils.savepointInProgress(sp1SessionJob.getStatus().getJobStatus()));

        sp1SessionJob
                .getStatus()
                .getJobStatus()
                .setState(org.apache.flink.api.common.JobStatus.RUNNING.name());

        reconciler.reconcile(sp1SessionJob, readyContext);
        assertTrue(SavepointUtils.savepointInProgress(sp1SessionJob.getStatus().getJobStatus()));

        // the last reconcile nonce updated
        assertEquals(
                2L,
                sp1SessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getSavepointTriggerNonce());

        // don't trigger new savepoint when savepoint is in progress
        sp1SessionJob.getSpec().getJob().setSavepointTriggerNonce(3L);
        reconciler.reconcile(sp1SessionJob, readyContext);
        assertEquals(
                "trigger_0",
                sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        // don't trigger upgrade when savepoint is in progress
        assertEquals(
                1,
                sp1SessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getParallelism());
        sp1SessionJob.getSpec().getJob().setParallelism(100);
        reconciler.reconcile(sp1SessionJob, readyContext);
        assertEquals(
                "trigger_0",
                sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        // parallelism not changed
        assertEquals(
                1,
                sp1SessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getParallelism());

        sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().resetTrigger();

        // running -> suspended
        reconciler.reconcile(sp1SessionJob, readyContext);
        // suspended -> running
        reconciler.reconcile(sp1SessionJob, readyContext);
        // parallelism changed
        assertEquals(
                100,
                sp1SessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getParallelism());
        verifyAndSetRunningJobsToStatus(
                sp1SessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listSessionJobs());

        sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().resetTrigger();

        // don't trigger when nonce is the same
        sp1SessionJob.getSpec().getJob().setSavepointTriggerNonce(2L);
        reconciler.reconcile(sp1SessionJob, readyContext);
        assertFalse(SavepointUtils.savepointInProgress(sp1SessionJob.getStatus().getJobStatus()));

        // trigger when new nonce is defined
        sp1SessionJob.getSpec().getJob().setSavepointTriggerNonce(3L);
        reconciler.reconcile(sp1SessionJob, readyContext);
        assertEquals(
                "trigger_1",
                sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().resetTrigger();

        // don't trigger when nonce is cleared
        sp1SessionJob.getSpec().getJob().setSavepointTriggerNonce(null);
        reconciler.reconcile(sp1SessionJob, readyContext);
        assertFalse(SavepointUtils.savepointInProgress(sp1SessionJob.getStatus().getJobStatus()));
    }

    private TestingFlinkService.SubmittedJobInfo verifyAndReturnTheSubmittedJob(
            FlinkSessionJob sessionJob,
            Map<JobID, TestingFlinkService.SubmittedJobInfo> sessionJobs) {
        var jobID = JobID.fromHexString(sessionJob.getStatus().getJobStatus().getJobId());
        var submittedJobInfo = sessionJobs.get(jobID);
        Assertions.assertNotNull(submittedJobInfo);
        return submittedJobInfo;
    }

    private void verifyAndSetRunningJobsToStatus(
            FlinkSessionJob sessionJob,
            JobState expectedState,
            String jobStatusObserved,
            @Nullable String expectedSavepointPath,
            Map<JobID, TestingFlinkService.SubmittedJobInfo> sessionJobs) {

        var submittedJobInfo = verifyAndReturnTheSubmittedJob(sessionJob, sessionJobs);
        assertEquals(expectedSavepointPath, submittedJobInfo.savepointPath);

        verifyJobState(sessionJob, expectedState, jobStatusObserved);
        JobStatus jobStatus = sessionJob.getStatus().getJobStatus();
        jobStatus.setJobName(submittedJobInfo.jobStatusMessage.getJobName());
        jobStatus.setState("RUNNING");
    }

    private void verifyJobState(
            FlinkSessionJob sessionJob, JobState expectedState, String jobStatusObserved) {
        assertEquals(
                expectedState,
                sessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());

        assertEquals(jobStatusObserved, sessionJob.getStatus().getJobStatus().getState());
    }

    @Test
    public void testJobUpgradeIgnorePendingSavepoint() throws Exception {
        Context readyContext = TestUtils.createContextWithReadyFlinkDeployment();
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkSessionJobReconciler reconciler =
                new FlinkSessionJobReconciler(null, flinkService, configManager);
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();
        reconciler.reconcile(sessionJob, readyContext);
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listSessionJobs());

        FlinkSessionJob spSessionJob = ReconciliationUtils.clone(sessionJob);
        spSessionJob
                .getSpec()
                .getJob()
                .setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(spSessionJob, readyContext);
        assertEquals(
                "trigger_0",
                spSessionJob.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertEquals(JobState.RUNNING.name(), spSessionJob.getStatus().getJobStatus().getState());

        configManager.updateDefaultConfig(
                Configuration.fromMap(
                        Map.of(
                                KubernetesOperatorConfigOptions.JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT
                                        .key(),
                                "true")));
        // Force upgrade when savepoint is in progress.
        reconciler = new FlinkSessionJobReconciler(null, flinkService, configManager);
        spSessionJob.getSpec().getJob().setParallelism(100);
        reconciler.reconcile(spSessionJob, readyContext);
        assertEquals(
                "trigger_0",
                spSessionJob.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertEquals(JobState.SUSPENDED.name(), spSessionJob.getStatus().getJobStatus().getState());
    }
}
