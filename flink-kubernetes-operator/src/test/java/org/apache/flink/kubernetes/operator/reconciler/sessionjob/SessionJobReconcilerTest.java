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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingFlinkServiceFactory;
import org.apache.flink.kubernetes.operator.TestingStatusRecorder;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkServiceFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link SessionJobReconciler}. */
@EnableKubernetesMockClient(crud = true)
public class SessionJobReconcilerTest {

    private KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private TestingFlinkService flinkService = new TestingFlinkService();
    private EventRecorder eventRecorder;
    private SessionJobReconciler reconciler;
    private StatusRecorder<FlinkSessionJob, FlinkSessionJobStatus> statusRecoder;
    private FlinkServiceFactory flinkServiceFactory;

    @BeforeEach
    public void before() {
        flinkService = new TestingFlinkService();
        flinkServiceFactory = new TestingFlinkServiceFactory(flinkService);
        eventRecorder =
                new EventRecorder(null, (r, e) -> {}) {
                    @Override
                    public boolean triggerEvent(
                            AbstractFlinkResource<?, ?> resource,
                            Type type,
                            String reason,
                            String message,
                            Component component) {
                        return false;
                    }
                };
        statusRecoder = new TestingStatusRecorder<>();
        reconciler =
                new SessionJobReconciler(
                        kubernetesClient,
                        flinkServiceFactory,
                        configManager,
                        eventRecorder,
                        statusRecoder);
        kubernetesClient.resource(TestUtils.buildSessionJob()).createOrReplace();
    }

    @Test
    public void testSubmitAndCleanUp() throws Exception {
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        // session not found
        reconciler.reconcile(sessionJob, TestUtils.createEmptyContext());
        assertEquals(0, flinkService.listJobs().size());

        // session not ready
        reconciler.reconcile(sessionJob, TestUtils.createContextWithNotReadyFlinkDeployment());
        assertEquals(0, flinkService.listJobs().size());

        // session ready
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        assertEquals(1, flinkService.listJobs().size());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listJobs());
        // clean up
        reconciler.cleanup(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED,
                flinkService.listJobs().get(0).f1.getJobState());
    }

    @Test
    public void testRestart() throws Exception {
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        // session ready
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        assertEquals(1, flinkService.listJobs().size());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listJobs());
        sessionJob.getSpec().setRestartNonce(2L);
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED,
                flinkService.listJobs().get(0).f1.getJobState());
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listJobs());
    }

    @Test
    public void testSubmitWithInitialSavepointPath() throws Exception {
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        var initSavepointPath = "file:///init-sp";
        sessionJob.getSpec().getJob().setInitialSavepointPath(initSavepointPath);
        reconciler.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                initSavepointPath,
                flinkService.listJobs());
    }

    @Test
    public void testStatelessUpgrade() throws Exception {
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        var readyContext = TestUtils.createContextWithReadyFlinkDeployment();
        reconciler.reconcile(sessionJob, readyContext);
        assertEquals(1, flinkService.listJobs().size());
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listJobs());

        var statelessSessionJob = ReconciliationUtils.clone(sessionJob);
        statelessSessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        statelessSessionJob.getSpec().getJob().setParallelism(2);
        // job suspended first
        reconciler.reconcile(statelessSessionJob, readyContext);
        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED,
                flinkService.listJobs().get(0).f1.getJobState());
        verifyJobState(statelessSessionJob, JobState.SUSPENDED, "FINISHED");

        flinkService.clear();
        reconciler.reconcile(statelessSessionJob, readyContext);
        assertEquals(1, flinkService.listJobs().size());
        verifyAndSetRunningJobsToStatus(
                statelessSessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listJobs());
    }

    @Test
    public void testSavepointUpgrade() throws Exception {
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        var readyContext = TestUtils.createContextWithReadyFlinkDeployment();
        reconciler.reconcile(sessionJob, readyContext);
        // start the job
        assertEquals(1, flinkService.listJobs().size());
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

        verifyAndSetRunningJobsToStatus(
                statefulSessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listJobs());

        reconciler.reconcile(statefulSessionJob, readyContext);

        // job suspended first
        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED,
                flinkService.listJobs().get(0).f1.getJobState());
        verifyJobState(statefulSessionJob, JobState.SUSPENDED, "FINISHED");
        assertEquals(
                1,
                statefulSessionJob
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getSavepointHistory()
                        .size());
        assertEquals(
                SavepointTriggerType.UPGRADE,
                statefulSessionJob
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getTriggerType());

        flinkService.clear();
        // upgraded
        reconciler.reconcile(statefulSessionJob, readyContext);
        assertEquals(1, flinkService.listJobs().size());
        verifyAndSetRunningJobsToStatus(
                statefulSessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                "savepoint_0",
                flinkService.listJobs());
    }

    @Test
    public void testTriggerSavepoint() throws Exception {
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();
        assertFalse(SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));

        var readyContext = TestUtils.createContextWithReadyFlinkDeployment();
        reconciler.reconcile(sessionJob, readyContext);
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listJobs());

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
        assertNull(
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
        assertEquals(
                SavepointTriggerType.MANUAL,
                sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().getTriggerType());

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
        ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(
                sp1SessionJob.getStatus().getJobStatus().getSavepointInfo(), sp1SessionJob);

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
                flinkService.listJobs());

        sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().resetTrigger();
        ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(
                sp1SessionJob.getStatus().getJobStatus().getSavepointInfo(), sp1SessionJob);

        // trigger when new nonce is defined
        sp1SessionJob.getSpec().getJob().setSavepointTriggerNonce(4L);
        reconciler.reconcile(sp1SessionJob, readyContext);
        assertEquals(
                "trigger_1",
                sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        sp1SessionJob.getStatus().getJobStatus().getSavepointInfo().resetTrigger();
        ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(
                sp1SessionJob.getStatus().getJobStatus().getSavepointInfo(), sp1SessionJob);

        // don't trigger when nonce is cleared
        sp1SessionJob.getSpec().getJob().setSavepointTriggerNonce(null);
        reconciler.reconcile(sp1SessionJob, readyContext);
        assertFalse(SavepointUtils.savepointInProgress(sp1SessionJob.getStatus().getJobStatus()));
    }

    private Tuple2<String, JobStatusMessage> verifyAndReturnTheSubmittedJob(
            FlinkSessionJob sessionJob, List<Tuple2<String, JobStatusMessage>> jobs) {
        var jobID = JobID.fromHexString(sessionJob.getStatus().getJobStatus().getJobId());
        var submittedJobInfo =
                jobs.stream().filter(t -> t.f1.getJobId().equals(jobID)).findAny().get();
        Assertions.assertNotNull(submittedJobInfo);
        return submittedJobInfo;
    }

    private void verifyAndSetRunningJobsToStatus(
            FlinkSessionJob sessionJob,
            JobState expectedState,
            String jobStatusObserved,
            @Nullable String expectedSavepointPath,
            List<Tuple2<String, JobStatusMessage>> jobs) {

        var submittedJobInfo = verifyAndReturnTheSubmittedJob(sessionJob, jobs);
        assertEquals(expectedSavepointPath, submittedJobInfo.f0);

        verifyJobState(sessionJob, expectedState, jobStatusObserved);
        JobStatus jobStatus = sessionJob.getStatus().getJobStatus();
        jobStatus.setJobName(submittedJobInfo.f1.getJobName());
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
        var readyContext = TestUtils.createContextWithReadyFlinkDeployment();
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();
        reconciler.reconcile(sessionJob, readyContext);
        verifyAndSetRunningJobsToStatus(
                sessionJob,
                JobState.RUNNING,
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                null,
                flinkService.listJobs());

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
        reconciler =
                new SessionJobReconciler(
                        null, flinkServiceFactory, configManager, eventRecorder, statusRecoder);
        spSessionJob.getSpec().getJob().setParallelism(100);
        reconciler.reconcile(spSessionJob, readyContext);
        assertEquals(
                "trigger_0",
                spSessionJob.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertEquals("FINISHED", spSessionJob.getStatus().getJobStatus().getState());
    }
}
