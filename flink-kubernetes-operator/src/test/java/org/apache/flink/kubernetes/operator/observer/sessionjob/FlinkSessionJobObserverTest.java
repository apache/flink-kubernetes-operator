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
import org.apache.flink.autoscaler.NoopJobAutoscaler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.TestObserverAdapter;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.TestReconcilerAdapter;
import org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler;
import org.apache.flink.kubernetes.operator.utils.SnapshotUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkSessionJobObserver}. */
@EnableKubernetesMockClient(crud = true)
public class FlinkSessionJobObserverTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;

    private TestObserverAdapter<FlinkSessionJob> observer;
    private TestReconcilerAdapter<FlinkSessionJob, FlinkSessionJobSpec, FlinkSessionJobStatus>
            reconciler;

    @Override
    public void setup() {
        observer = new TestObserverAdapter<>(this, new FlinkSessionJobObserver(eventRecorder));
        reconciler =
                new TestReconcilerAdapter<>(
                        this,
                        new SessionJobReconciler(
                                eventRecorder, statusRecorder, new NoopJobAutoscaler<>()));
    }

    @Test
    public void testBasicObserve() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient);

        // observe the brand new job, nothing to do.
        observer.observe(sessionJob, readyContext);

        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING, sessionJob.getStatus().getJobStatus().getState());

        // observe with empty context will do nothing
        observer.observe(sessionJob, TestUtils.createEmptyContext());
        Assertions.assertEquals(
                JobStatus.RECONCILING, sessionJob.getStatus().getJobStatus().getState());

        var reconStatus = sessionJob.getStatus().getReconciliationStatus();
        Assertions.assertNotEquals(
                reconStatus.getLastReconciledSpec(), reconStatus.getLastStableSpec());

        // observe with ready context
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING, sessionJob.getStatus().getJobStatus().getState());
        Assertions.assertEquals(
                reconStatus.getLastReconciledSpec(), reconStatus.getLastStableSpec());

        flinkService.setPortReady(false);
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RECONCILING, sessionJob.getStatus().getJobStatus().getState());

        sessionJob.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        // no matched job id, update the state to unknown
        flinkService.setPortReady(true);
        sessionJob.getStatus().getJobStatus().setJobId(new JobID().toHexString());
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RECONCILING, sessionJob.getStatus().getJobStatus().getState());
        Assertions.assertEquals(
                JobState.SUSPENDED,
                sessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());

        // reset
        sessionJob.getStatus().getJobStatus().setJobId(jobID);
        ReconciliationUtils.updateLastReconciledSpec(
                sessionJob, (s, m) -> s.getJob().setState(JobState.RUNNING));

        // testing multi job

        var sessionJob2 = TestUtils.buildSessionJob();
        // submit the second job
        reconciler.reconcile(sessionJob2, readyContext);
        var jobID2 = sessionJob2.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertNotEquals(jobID, jobID2);
        Assertions.assertEquals(
                JobStatus.RECONCILING, sessionJob.getStatus().getJobStatus().getState());
        observer.observe(sessionJob2, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING, sessionJob2.getStatus().getJobStatus().getState());
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING, sessionJob.getStatus().getJobStatus().getState());

        // test error behaviour if job not present
        flinkService.clear();

        flinkResourceEventCollector.events.clear();

        observer.observe(sessionJob2, readyContext);
        Assertions.assertEquals(
                JobStatus.RECONCILING, sessionJob2.getStatus().getJobStatus().getState());
        Assertions.assertTrue(
                sessionJob2.getStatus().getError().contains(JobStatusObserver.JOB_NOT_FOUND_ERR));
        Assertions.assertEquals(
                JobStatusObserver.JOB_NOT_FOUND_ERR,
                flinkResourceEventCollector.events.peek().getMessage());
    }

    @Test
    public void testObserveWithEffectiveConfig() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var readyContext =
                TestUtils.createContextWithReadyFlinkDeployment(
                        Map.of(RestOptions.PORT.key(), "8088"), kubernetesClient);

        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING, sessionJob.getStatus().getJobStatus().getState());

        flinkService.setListJobConsumer(
                (configuration) ->
                        Assertions.assertEquals(8088, configuration.getInteger(RestOptions.PORT)));
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING, sessionJob.getStatus().getJobStatus().getState());
    }

    @Test
    public void testObserveSavepoint() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient);
        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING, sessionJob.getStatus().getJobStatus().getState());

        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING, sessionJob.getStatus().getJobStatus().getState());

        var savepointInfo = sessionJob.getStatus().getJobStatus().getSavepointInfo();
        Assertions.assertFalse(
                SnapshotUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));

        Long firstNonce = 123L;
        sessionJob.getSpec().getJob().setSavepointTriggerNonce(firstNonce);
        flinkService.triggerSavepointLegacy(
                jobID, SnapshotTriggerType.MANUAL, sessionJob, new Configuration());
        Assertions.assertTrue(
                SnapshotUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
        Assertions.assertEquals("savepoint_trigger_0", savepointInfo.getTriggerId());

        Long secondNonce = 456L;
        sessionJob.getSpec().getJob().setSavepointTriggerNonce(secondNonce);
        flinkService.triggerSavepointLegacy(
                jobID, SnapshotTriggerType.MANUAL, sessionJob, new Configuration());
        Assertions.assertTrue(
                SnapshotUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
        Assertions.assertEquals("savepoint_trigger_1", savepointInfo.getTriggerId());
        flinkService.triggerSavepointLegacy(
                jobID, SnapshotTriggerType.MANUAL, sessionJob, new Configuration());
        Assertions.assertTrue(
                SnapshotUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
        observer.observe(sessionJob, readyContext); // pending
        observer.observe(sessionJob, readyContext); // success
        Assertions.assertEquals("savepoint_0", savepointInfo.getLastSavepoint().getLocation());
        Assertions.assertEquals(secondNonce, savepointInfo.getLastSavepoint().getTriggerNonce());
        Assertions.assertFalse(
                SnapshotUtils.checkpointInProgress(sessionJob.getStatus().getJobStatus()));
    }

    @Test
    public void testObserveCheckpoint() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient);
        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING, sessionJob.getStatus().getJobStatus().getState());

        observer.observe(sessionJob, readyContext);
        assertEquals(JobStatus.RUNNING, sessionJob.getStatus().getJobStatus().getState());

        var checkpointInfo = sessionJob.getStatus().getJobStatus().getCheckpointInfo();
        assertFalse(SnapshotUtils.checkpointInProgress(sessionJob.getStatus().getJobStatus()));

        Long firstNonce = 123L;
        sessionJob.getSpec().getJob().setCheckpointTriggerNonce(firstNonce);
        var triggerId =
                flinkService.triggerCheckpoint(jobID, CheckpointType.FULL, new Configuration());
        checkpointInfo.setTrigger(
                triggerId,
                SnapshotTriggerType.MANUAL,
                org.apache.flink.kubernetes.operator.api.status.CheckpointType.FULL);
        assertTrue(SnapshotUtils.checkpointInProgress(sessionJob.getStatus().getJobStatus()));

        Long secondNonce = 456L;
        sessionJob.getSpec().getJob().setCheckpointTriggerNonce(secondNonce);
        triggerId = flinkService.triggerCheckpoint(jobID, CheckpointType.FULL, new Configuration());
        checkpointInfo.setTrigger(
                triggerId,
                SnapshotTriggerType.MANUAL,
                org.apache.flink.kubernetes.operator.api.status.CheckpointType.FULL);
        assertTrue(SnapshotUtils.checkpointInProgress(sessionJob.getStatus().getJobStatus()));
        triggerId = flinkService.triggerCheckpoint(jobID, CheckpointType.FULL, new Configuration());
        checkpointInfo.setTrigger(
                triggerId,
                SnapshotTriggerType.MANUAL,
                org.apache.flink.kubernetes.operator.api.status.CheckpointType.FULL);
        assertTrue(SnapshotUtils.checkpointInProgress(sessionJob.getStatus().getJobStatus()));
        observer.observe(sessionJob, readyContext); // pending
        observer.observe(sessionJob, readyContext); // success
        assertEquals(secondNonce, checkpointInfo.getLastCheckpoint().getTriggerNonce());
        assertFalse(SnapshotUtils.checkpointInProgress(sessionJob.getStatus().getJobStatus()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testObserveAlreadySubmitted(boolean submitted) {
        var sessionJob = TestUtils.buildSessionJob();
        sessionJob.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
        sessionJob.getMetadata().setGeneration(10L);
        var readyContext = TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient);

        flinkService.setSessionJobSubmittedCallback(
                () -> {
                    throw new RuntimeException("Failed after submitted job");
                });
        // submit job but fail during submission
        Assertions.assertThrows(
                RuntimeException.class, () -> reconciler.reconcile(sessionJob, readyContext));
        Assertions.assertEquals(
                ReconciliationState.UPGRADING,
                sessionJob.getStatus().getReconciliationStatus().getState());

        if (!submitted) {
            // Pretend that job was never submitted
            flinkService.clear();
        }

        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                submitted ? ReconciliationState.DEPLOYED : ReconciliationState.UPGRADING,
                sessionJob.getStatus().getReconciliationStatus().getState());
        Assertions.assertEquals(
                submitted ? JobStatus.RUNNING : JobStatus.RECONCILING,
                sessionJob.getStatus().getJobStatus().getState());
    }

    @Test
    public void validateLastReconciledClearedOnInitialFailure() {
        var sessionJob = TestUtils.buildSessionJob();
        sessionJob.getMetadata().setGeneration(123L);

        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(sessionJob, new Configuration());

        assertFalse(sessionJob.getStatus().getReconciliationStatus().isBeforeFirstDeployment());
        observer.observe(
                sessionJob, TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient));
        assertTrue(sessionJob.getStatus().getReconciliationStatus().isBeforeFirstDeployment());
    }
}
