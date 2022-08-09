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

package org.apache.flink.kubernetes.operator.observer.sessionjob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingFlinkServiceFactory;
import org.apache.flink.kubernetes.operator.TestingStatusRecorder;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkServiceFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link SessionJobObserver}. */
@EnableKubernetesMockClient(crud = true)
public class SessionJobObserverTest {

    private KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private TestingFlinkService flinkService;
    private SessionJobObserver observer;
    private SessionJobReconciler reconciler;

    @BeforeEach
    public void before() {
        kubernetesClient.resource(TestUtils.buildSessionJob()).createOrReplace();
        var eventRecorder = new EventRecorder(kubernetesClient, (r, e) -> {});
        var statusRecorder = new TestingStatusRecorder<FlinkSessionJob, FlinkSessionJobStatus>();
        flinkService = new TestingFlinkService();
        FlinkServiceFactory flinkServiceFactory = new TestingFlinkServiceFactory(flinkService);
        observer = new SessionJobObserver(flinkServiceFactory, configManager, eventRecorder);
        reconciler =
                new SessionJobReconciler(
                        kubernetesClient,
                        flinkServiceFactory,
                        configManager,
                        eventRecorder,
                        statusRecorder);
    }

    @Test
    public void testBasicObserve() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment();

        // observe the brand new job, nothing to do.
        observer.observe(sessionJob, readyContext);

        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        // observe with empty context will do nothing
        observer.observe(sessionJob, TestUtils.createEmptyContext());
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        var reconStatus = sessionJob.getStatus().getReconciliationStatus();
        Assertions.assertNotEquals(
                reconStatus.getLastReconciledSpec(), reconStatus.getLastStableSpec());

        // observe with ready context
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());
        Assertions.assertEquals(
                reconStatus.getLastReconciledSpec(), reconStatus.getLastStableSpec());

        flinkService.setPortReady(false);
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        sessionJob.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
        // no matched job id, update the state to unknown
        flinkService.setPortReady(true);
        sessionJob.getStatus().getJobStatus().setJobId(new JobID().toHexString());
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());
        sessionJob.getStatus().getJobStatus().setJobId(jobID);

        // testing multi job

        var sessionJob2 = TestUtils.buildSessionJob();
        // submit the second job
        reconciler.reconcile(sessionJob2, readyContext);
        var jobID2 = sessionJob2.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertNotEquals(jobID, jobID2);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());
        observer.observe(sessionJob2, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob2.getStatus().getJobStatus().getState());
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());
    }

    @Test
    public void testObserveWithEffectiveConfig() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var readyContext =
                TestUtils.createContextWithReadyFlinkDeployment(
                        Map.of(RestOptions.PORT.key(), "8088"));

        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        flinkService.setListJobConsumer(
                configuration ->
                        Assertions.assertEquals(8088, configuration.getInteger(RestOptions.PORT)));
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());
    }

    @Test
    public void testObserveSavepoint() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment();

        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());

        var savepointInfo = sessionJob.getStatus().getJobStatus().getSavepointInfo();
        Assertions.assertFalse(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));

        flinkService.triggerSavepoint(
                jobID, SavepointTriggerType.MANUAL, savepointInfo, new Configuration());
        Assertions.assertTrue(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
        Assertions.assertEquals("trigger_0", savepointInfo.getTriggerId());

        flinkService.triggerSavepoint(
                jobID, SavepointTriggerType.MANUAL, savepointInfo, new Configuration());
        Assertions.assertTrue(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
        Assertions.assertEquals("trigger_1", savepointInfo.getTriggerId());
        flinkService.triggerSavepoint(
                jobID, SavepointTriggerType.MANUAL, savepointInfo, new Configuration());
        Assertions.assertTrue(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
        observer.observe(sessionJob, readyContext); // pending
        observer.observe(sessionJob, readyContext); // success
        Assertions.assertEquals("savepoint_0", savepointInfo.getLastSavepoint().getLocation());
        Assertions.assertFalse(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
    }

    @Test
    public void testObserveAlreadySubmitted() {
        final var sessionJob = TestUtils.buildSessionJob();
        sessionJob.getMetadata().setGeneration(10L);
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment();

        flinkService.setSessionJobSubmittedCallback(
                () -> {
                    throw new RuntimeException("Failed after submitted job");
                });
        // submit job
        Assertions.assertThrows(
                RuntimeException.class, () -> reconciler.reconcile(sessionJob, readyContext));
        Assertions.assertNotNull(sessionJob.getStatus().getReconciliationStatus());
        Assertions.assertEquals(
                ReconciliationState.UPGRADING,
                sessionJob.getStatus().getReconciliationStatus().getState());
        Assertions.assertNull(sessionJob.getStatus().getJobStatus().getJobId());

        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                ReconciliationState.DEPLOYED,
                sessionJob.getStatus().getReconciliationStatus().getState());
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(10, JobID.fromHexString(jobID).getUpperPart());
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());
    }

    @Test
    public void testObserveAlreadyUpgraded() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        sessionJob.getMetadata().setGeneration(10L);
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment();

        reconciler.reconcile(sessionJob, readyContext);
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                ReconciliationState.DEPLOYED,
                sessionJob.getStatus().getReconciliationStatus().getState());
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(10, JobID.fromHexString(jobID).getUpperPart());
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());

        flinkService.setSessionJobSubmittedCallback(
                () -> {
                    throw new RuntimeException("Failed after submitted job");
                });
        sessionJob.getSpec().getJob().setParallelism(10);
        sessionJob.getMetadata().setGeneration(11L);

        // upgrade
        Assertions.assertThrows(
                RuntimeException.class,
                () -> {
                    // suspend
                    reconciler.reconcile(sessionJob, readyContext);
                    // upgrade
                    reconciler.reconcile(sessionJob, readyContext);
                });

        Assertions.assertEquals(
                ReconciliationState.UPGRADING,
                sessionJob.getStatus().getReconciliationStatus().getState());
        // jobID not changed
        Assertions.assertEquals(jobID, sessionJob.getStatus().getJobStatus().getJobId());

        observer.observe(sessionJob, readyContext);

        Assertions.assertEquals(
                ReconciliationState.DEPLOYED,
                sessionJob.getStatus().getReconciliationStatus().getState());
        Assertions.assertEquals(
                11L,
                JobID.fromHexString(sessionJob.getStatus().getJobStatus().getJobId())
                        .getUpperPart());
    }

    @Test
    public void testOrphanedJob() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        sessionJob.getMetadata().setGeneration(10L);
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment();

        reconciler.reconcile(sessionJob, readyContext);
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                ReconciliationState.DEPLOYED,
                sessionJob.getStatus().getReconciliationStatus().getState());
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(10, JobID.fromHexString(jobID).getUpperPart());
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());

        flinkService.setSessionJobSubmittedCallback(
                () -> {
                    throw new RuntimeException("Failed after submitted job");
                });
        sessionJob.getSpec().getJob().setParallelism(10);
        sessionJob.getMetadata().setGeneration(11L);
        // upgrade
        Assertions.assertThrows(
                RuntimeException.class,
                () -> {
                    // suspend
                    reconciler.reconcile(sessionJob, readyContext);
                    // upgrade
                    reconciler.reconcile(sessionJob, readyContext);
                });

        Assertions.assertEquals(
                ReconciliationState.UPGRADING,
                sessionJob.getStatus().getReconciliationStatus().getState());
        // jobID not changed
        Assertions.assertEquals(jobID, sessionJob.getStatus().getJobStatus().getJobId());

        // mock a job with different id of the target CR occurs
        var jobs = flinkService.listJobs();
        for (Tuple2<String, JobStatusMessage> job : jobs) {
            if (!job.f1.getJobState().isGloballyTerminalState()
                    && !job.f1.getJobId().toHexString().equals(jobID)) {
                job.f1 =
                        new JobStatusMessage(
                                FlinkUtils.generateSessionJobFixedJobID(
                                        sessionJob.getMetadata().getUid(), -1L),
                                job.f1.getJobName(),
                                job.f1.getJobState(),
                                job.f1.getStartTime());
            }
        }

        var exception =
                Assertions.assertThrows(
                        RuntimeException.class, () -> observer.observe(sessionJob, readyContext));
        Assertions.assertTrue(
                exception.getMessage().contains("doesn't match upgrade target generation"));
    }

    @Test
    public void validateLastReconciledClearedOnInitialFailure() {
        var sessionJob = TestUtils.buildSessionJob();
        sessionJob.getMetadata().setGeneration(123L);

        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(sessionJob, new Configuration());

        assertFalse(sessionJob.getStatus().getReconciliationStatus().isFirstDeployment());
        observer.observe(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        assertTrue(sessionJob.getStatus().getReconciliationStatus().isFirstDeployment());
    }
}
