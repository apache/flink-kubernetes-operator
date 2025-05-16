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

package org.apache.flink.kubernetes.operator.observer.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.observer.TestObserverAdapter;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.SnapshotUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getCheckpointInfo;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getJobStatus;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link ApplicationObserver} unit tests. */
@EnableKubernetesMockClient(crud = true)
public class ApplicationObserverTest extends OperatorTestBase {
    @Getter private KubernetesClient kubernetesClient;

    private Context<FlinkDeployment> readyContext;
    private TestObserverAdapter<FlinkDeployment> observer;
    private FlinkDeployment deployment;

    @Override
    public void setup() {
        observer = new TestObserverAdapter<>(this, new ApplicationObserver(eventRecorder));
        readyContext = TestUtils.createContextWithReadyJobManagerDeployment(kubernetesClient);
        deployment = TestUtils.buildApplicationCluster();
        var jobId = new JobID().toHexString();
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key(), jobId);
        deployment.getStatus().getJobStatus().setJobId(jobId);
    }

    @Test
    public void observeApplicationCluster() throws Exception {
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());

        observer.observe(deployment, TestUtils.createEmptyContext());
        assertNull(deployment.getStatus().getReconciliationStatus().getLastStableSpec());

        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        // Validate port check logic
        flinkService.setPortReady(false);

        // Port not ready
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertNull(deployment.getStatus().getReconciliationStatus().getLastStableSpec());

        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertNull(deployment.getStatus().getReconciliationStatus().getLastStableSpec());

        flinkService.setPortReady(true);
        // Port ready but we have to recheck once again
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertNull(deployment.getStatus().getReconciliationStatus().getLastStableSpec());
        assertTrue(deployment.getStatus().getClusterInfo().isEmpty());

        // Stable ready
        observer.observe(deployment, readyContext);
        assertEquals(TestingFlinkService.CLUSTER_INFO, deployment.getStatus().getClusterInfo());
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                deployment.getStatus().getJobStatus().getState());
        assertEquals(
                deployment.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                deployment.getStatus().getReconciliationStatus().getLastStableSpec());

        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                deployment.getStatus().getJobStatus().getState());

        assertEquals(
                deployment.getMetadata().getName(),
                deployment.getStatus().getJobStatus().getJobName());
        assertTrue(
                Long.valueOf(deployment.getStatus().getJobStatus().getUpdateTime())
                                .compareTo(
                                        Long.valueOf(
                                                deployment
                                                        .getStatus()
                                                        .getJobStatus()
                                                        .getStartTime()))
                        >= 0);

        // Test job manager is unavailable suddenly
        flinkService.setPortReady(false);
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());
        // Job manager recovers
        flinkService.setPortReady(true);
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        // Test listing failure
        deployment.getStatus().getReconciliationStatus().setLastStableSpec(null);
        flinkService.clear();
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RECONCILING,
                deployment.getStatus().getJobStatus().getState());
        assertNull(deployment.getStatus().getReconciliationStatus().getLastStableSpec());
    }

    @Test
    public void testEventGeneratedWhenStatusChanged() throws Exception {
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);

        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);

        observer.observe(deployment, readyContext);
        var eventList =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems();
        Assertions.assertEquals(1, eventList.size());
        Assertions.assertEquals("Job status changed to RUNNING", eventList.get(0).getMessage());
        observer.observe(deployment, readyContext);
        Assertions.assertEquals(
                1,
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems()
                        .size());
    }

    @Test
    public void testErrorForwardToStatusWhenJobFailed() throws Exception {
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);

        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);

        observer.observe(deployment, readyContext);
        Assertions.assertEquals(1, flinkService.getRunningCount());
        flinkService.markApplicationJobFailedWithError(
                JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId()),
                "Job failed");
        observer.observe(deployment, readyContext);
        Assertions.assertEquals(0, flinkService.getRunningCount());
        Assertions.assertTrue(deployment.getStatus().getError().contains("Job failed"));
    }

    @Test
    public void observeSavepoint() throws Exception {
        Long timedOutNonce = 1L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(timedOutNonce);
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED.key(), "false");
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);
        bringToReadyStatus(deployment);
        assertTrue(ReconciliationUtils.isJobRunning(deployment.getStatus()));

        flinkService.triggerSavepointLegacy(
                deployment.getStatus().getJobStatus().getJobId(),
                SnapshotTriggerType.MANUAL,
                deployment,
                conf);
        // pending savepoint
        assertEquals(
                "savepoint_trigger_0",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        observer.observe(deployment, readyContext);
        assertTrue(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        deployment.getStatus().getJobStatus().getSavepointInfo().setTriggerId("unknown");
        // savepoint error within grace period
        assertEquals(
                0,
                (int)
                        kubernetesClient
                                .v1()
                                .events()
                                .inNamespace(deployment.getMetadata().getNamespace())
                                .list()
                                .getItems()
                                .stream()
                                .filter(e -> e.getReason().contains("SavepointError"))
                                .count());
        observer.observe(deployment, readyContext);
        assertFalse(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
        assertEquals(
                1,
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems()
                        .stream()
                        .filter(e -> e.getReason().contains("SavepointError"))
                        .count());

        deployment.getStatus().getJobStatus().getSavepointInfo().setTriggerId("unknown");
        deployment
                .getStatus()
                .getJobStatus()
                .getSavepointInfo()
                .setTriggerType(SnapshotTriggerType.MANUAL);
        deployment
                .getStatus()
                .getJobStatus()
                .getSavepointInfo()
                .setTriggerTimestamp(Instant.now().minus(Duration.ofHours(1)).toEpochMilli());

        observer.observe(deployment, readyContext);
        assertFalse(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
        assertEquals(
                1,
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems()
                        .stream()
                        .filter(e -> e.getReason().contains("SavepointError"))
                        .filter(
                                e ->
                                        e.getMessage()
                                                .equals(
                                                        "Savepoint failed for savepointTriggerNonce: "
                                                                + timedOutNonce))
                        .count());
        assertEquals(
                2,
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems()
                        .stream()
                        .filter(e -> e.getReason().contains("SavepointError"))
                        .filter(
                                e ->
                                        e.getMessage()
                                                .equals(
                                                        "Savepoint failed for savepointTriggerNonce: "
                                                                + timedOutNonce))
                        .collect(Collectors.toList())
                        .get(0)
                        .getCount());

        // savepoint success
        Long firstNonce = 123L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(firstNonce);
        flinkService.triggerSavepointLegacy(
                deployment.getStatus().getJobStatus().getJobId(),
                SnapshotTriggerType.MANUAL,
                deployment,
                conf);
        assertEquals(
                "savepoint_trigger_1",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        observer.observe(deployment, readyContext);
        observer.observe(deployment, readyContext);

        assertEquals(
                "savepoint_0",
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());
        assertEquals(
                firstNonce,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getTriggerNonce());
        assertFalse(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        // second attempt success
        Long secondNonce = 456L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(secondNonce);
        flinkService.triggerSavepointLegacy(
                deployment.getStatus().getJobStatus().getJobId(),
                SnapshotTriggerType.MANUAL,
                deployment,
                conf);
        assertEquals(
                "savepoint_trigger_2",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertTrue(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        observer.observe(deployment, readyContext);
        observer.observe(deployment, readyContext);

        assertEquals(
                "savepoint_1",
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());
        assertEquals(
                secondNonce,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getTriggerNonce());
        assertFalse(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        // application failure after checkpoint trigger
        Long thirdNonce = 789L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(thirdNonce);
        flinkService.triggerSavepointLegacy(
                deployment.getStatus().getJobStatus().getJobId(),
                SnapshotTriggerType.MANUAL,
                deployment,
                conf);
        assertEquals(
                "savepoint_trigger_3",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertTrue(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
        flinkService.setPortReady(false);
        observer.observe(deployment, readyContext);
        assertEquals(
                "savepoint_1",
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());
        assertEquals(
                secondNonce,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getTriggerNonce());
        assertFalse(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        assertEquals(
                1,
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems()
                        .stream()
                        .filter(e -> e.getReason().contains("SavepointError"))
                        .filter(
                                e ->
                                        e.getMessage()
                                                .equals(
                                                        "Savepoint failed for savepointTriggerNonce: "
                                                                + thirdNonce))
                        .count());
        assertEquals(
                1,
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems()
                        .stream()
                        .filter(e -> e.getReason().contains("SavepointError"))
                        .filter(
                                e ->
                                        e.getMessage()
                                                .equals(
                                                        "Savepoint failed for savepointTriggerNonce: "
                                                                + thirdNonce))
                        .collect(Collectors.toList())
                        .get(0)
                        .getCount());

        flinkService.setPortReady(true);
        observer.observe(deployment, readyContext);
        // Simulate Failed job
        var jobTuple = flinkService.listJobs().get(0);
        jobTuple.f0 = "last-SP";
        jobTuple.f1 =
                new JobStatusMessage(
                        jobTuple.f1.getJobId(),
                        jobTuple.f1.getJobName(),
                        org.apache.flink.api.common.JobStatus.FAILED,
                        jobTuple.f1.getStartTime());
        deployment.getStatus().getJobStatus().getSavepointInfo().setTriggerId("test");
        deployment.getStatus().getJobStatus().getSavepointInfo().setTriggerTimestamp(123L);

        observer.observe(deployment, readyContext);
        assertEquals(
                org.apache.flink.api.common.JobStatus.FAILED,
                deployment.getStatus().getJobStatus().getState());
        assertEquals("last-SP", deployment.getStatus().getJobStatus().getUpgradeSavepointPath());
        assertFalse(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        observer.observe(deployment, readyContext);
        assertEquals(
                2,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getSavepointHistory()
                        .size());

        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT.key(),
                        "1");
        observer.observe(deployment, readyContext);
        assertEquals(
                1,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getSavepointHistory()
                        .size());
    }

    @Test
    public void observeCheckpoint() throws Exception {
        Long timedOutNonce = 1L;
        final var errorReason = EventRecorder.Reason.CheckpointError.name();
        final var namespace = deployment.getMetadata().getNamespace();
        final var timedOutMessage =
                "Checkpoint failed for checkpointTriggerNonce: " + timedOutNonce;

        deployment.getSpec().getJob().setCheckpointTriggerNonce(timedOutNonce);
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);
        bringToReadyStatus(deployment);
        assertTrue(ReconciliationUtils.isJobRunning(deployment.getStatus()));

        var triggerId =
                flinkService.triggerCheckpoint(
                        deployment.getStatus().getJobStatus().getJobId(),
                        CheckpointType.FULL,
                        conf);
        getCheckpointInfo(deployment)
                .setTrigger(
                        triggerId,
                        SnapshotTriggerType.MANUAL,
                        org.apache.flink.kubernetes.operator.api.status.CheckpointType.FULL);
        // pending checkpoint
        assertEquals("checkpoint_trigger_0", getCheckpointInfo(deployment).getTriggerId());
        observer.observe(deployment, readyContext);
        assertTrue(SnapshotUtils.checkpointInProgress(deployment.getStatus().getJobStatus()));

        getCheckpointInfo(deployment).setTriggerId("unknown");
        // checkpoint error within grace period
        assertEquals(0, countErrorEvents(errorReason, namespace, timedOutMessage));
        observer.observe(deployment, readyContext);
        assertFalse(SnapshotUtils.checkpointInProgress(deployment.getStatus().getJobStatus()));
        assertEquals(1, countErrorEvents(errorReason, namespace, timedOutMessage));

        getCheckpointInfo(deployment).setTriggerId("unknown");
        deployment
                .getStatus()
                .getJobStatus()
                .getCheckpointInfo()
                .setTriggerType(SnapshotTriggerType.MANUAL);
        deployment
                .getStatus()
                .getJobStatus()
                .getCheckpointInfo()
                .setTriggerTimestamp(Instant.now().minus(Duration.ofHours(1)).toEpochMilli());

        observer.observe(deployment, readyContext);
        assertFalse(SnapshotUtils.checkpointInProgress(deployment.getStatus().getJobStatus()));
        assertEquals(2, countErrorEvents(errorReason, namespace, timedOutMessage));

        // checkpoint success
        Long firstNonce = 123L;
        deployment.getSpec().getJob().setCheckpointTriggerNonce(firstNonce);
        triggerId =
                flinkService.triggerCheckpoint(
                        deployment.getStatus().getJobStatus().getJobId(),
                        CheckpointType.FULL,
                        conf);
        getCheckpointInfo(deployment)
                .setTrigger(
                        triggerId,
                        SnapshotTriggerType.MANUAL,
                        org.apache.flink.kubernetes.operator.api.status.CheckpointType.FULL);
        assertEquals("checkpoint_trigger_1", getCheckpointInfo(deployment).getTriggerId());
        observer.observe(deployment, readyContext);
        observer.observe(deployment, readyContext);

        assertEquals(
                firstNonce, getCheckpointInfo(deployment).getLastCheckpoint().getTriggerNonce());
        assertFalse(SnapshotUtils.checkpointInProgress(getJobStatus(deployment)));

        // second attempt success
        Long secondNonce = 456L;
        deployment.getSpec().getJob().setCheckpointTriggerNonce(secondNonce);
        triggerId =
                flinkService.triggerCheckpoint(
                        getJobStatus(deployment).getJobId(), CheckpointType.FULL, conf);
        getCheckpointInfo(deployment)
                .setTrigger(
                        triggerId,
                        SnapshotTriggerType.MANUAL,
                        org.apache.flink.kubernetes.operator.api.status.CheckpointType.FULL);
        assertEquals("checkpoint_trigger_2", getCheckpointInfo(deployment).getTriggerId());
        assertTrue(SnapshotUtils.checkpointInProgress(getJobStatus(deployment)));

        observer.observe(deployment, readyContext);
        observer.observe(deployment, readyContext);

        assertEquals(
                secondNonce, getCheckpointInfo(deployment).getLastCheckpoint().getTriggerNonce());
        assertFalse(SnapshotUtils.checkpointInProgress(getJobStatus(deployment)));

        // application failure after checkpoint trigger
        Long thirdNonce = 789L;
        deployment.getSpec().getJob().setCheckpointTriggerNonce(thirdNonce);
        triggerId =
                flinkService.triggerCheckpoint(
                        getJobStatus(deployment).getJobId(), CheckpointType.FULL, conf);
        getCheckpointInfo(deployment)
                .setTrigger(
                        triggerId,
                        SnapshotTriggerType.MANUAL,
                        org.apache.flink.kubernetes.operator.api.status.CheckpointType.FULL);
        assertEquals("checkpoint_trigger_3", getCheckpointInfo(deployment).getTriggerId());
        assertTrue(SnapshotUtils.checkpointInProgress(getJobStatus(deployment)));
        flinkService.setPortReady(false);
        observer.observe(deployment, readyContext);
        assertEquals(
                secondNonce, getCheckpointInfo(deployment).getLastCheckpoint().getTriggerNonce());
        assertFalse(SnapshotUtils.checkpointInProgress(getJobStatus(deployment)));

        final var thirdNonceMessage = "Checkpoint failed for checkpointTriggerNonce: " + thirdNonce;
        assertEquals(1, countErrorEvents(errorReason, namespace, thirdNonceMessage));

        flinkService.setPortReady(true);
        observer.observe(deployment, readyContext);
        // Simulate Failed job
        var jobTuple = flinkService.listJobs().get(0);
        jobTuple.f0 = "last-SP";
        jobTuple.f1 =
                new JobStatusMessage(
                        jobTuple.f1.getJobId(),
                        jobTuple.f1.getJobName(),
                        org.apache.flink.api.common.JobStatus.FAILED,
                        jobTuple.f1.getStartTime());
        getCheckpointInfo(deployment).setTriggerId("test");
        getCheckpointInfo(deployment).setTriggerTimestamp(123L);

        observer.observe(deployment, readyContext);
        assertEquals(
                org.apache.flink.api.common.JobStatus.FAILED, getJobStatus(deployment).getState());
        assertFalse(SnapshotUtils.checkpointInProgress(getJobStatus(deployment)));
    }

    private Integer countErrorEvents(String errorReason, String namespace, String timedOutMessage) {
        return kubernetesClient.v1().events().inNamespace(namespace).list().getItems().stream()
                .filter(e -> e.getReason().contains(errorReason))
                .filter(e -> e.getMessage().equals(timedOutMessage))
                .map(Event::getCount)
                .findFirst()
                .orElse(0);
    }

    @Test
    public void testSavepointFormat() throws Exception {
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);
        bringToReadyStatus(deployment);
        assertTrue(ReconciliationUtils.isJobRunning(deployment.getStatus()));

        // canonical savepoint
        Long firstNonce = 123L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(firstNonce);
        flinkService.triggerSavepointLegacy(
                deployment.getStatus().getJobStatus().getJobId(),
                SnapshotTriggerType.MANUAL,
                deployment,
                conf);

        observer.observe(deployment, readyContext);
        observer.observe(deployment, readyContext);
        assertFalse(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
        assertEquals(
                firstNonce,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getTriggerNonce());
        assertEquals(
                SavepointFormatType.CANONICAL,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getFormatType());

        // native savepoint
        Long secondNonce = 456L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(secondNonce);
        deployment
                .getSpec()
                .setFlinkConfiguration(
                        Map.of(
                                OPERATOR_SAVEPOINT_FORMAT_TYPE.key(),
                                org.apache.flink.core.execution.SavepointFormatType.NATIVE.name()));
        conf = configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.triggerSavepointLegacy(
                deployment.getStatus().getJobStatus().getJobId(),
                SnapshotTriggerType.MANUAL,
                deployment,
                conf);

        observer.observe(deployment, readyContext);
        observer.observe(deployment, readyContext);
        assertFalse(SnapshotUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
        assertEquals(
                secondNonce,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getTriggerNonce());
        assertEquals(
                SavepointFormatType.NATIVE,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getFormatType());
    }

    private void bringToReadyStatus(FlinkDeployment deployment) {
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        JobStatus jobStatus = deployment.getStatus().getJobStatus();
        jobStatus.setJobName("jobname");
        jobStatus.setState(org.apache.flink.api.common.JobStatus.RUNNING);
        deployment.getStatus().setJobStatus(jobStatus);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
    }

    @ParameterizedTest
    @MethodSource("containerFailureReasons")
    public void observeListJobsError(String reason, boolean initError) {
        bringToReadyStatus(deployment);
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        // simulate deployment failure
        String podFailedMessage = "list jobs error";
        if (initError) {
            flinkService.setPodList(
                    TestUtils.createFailedInitContainerPodList(podFailedMessage, reason));
        } else {
            flinkService.setPodList(TestUtils.createFailedPodList(podFailedMessage, reason));
        }
        flinkService.setPortReady(false);
        Exception exception =
                assertThrows(
                        DeploymentFailedException.class,
                        () ->
                                observer.observe(
                                        deployment,
                                        TestUtils.createContextWithInProgressDeployment(
                                                kubernetesClient)));
        assertEquals("[c1] " + podFailedMessage, exception.getMessage());
    }

    private static Stream<Arguments> containerFailureReasons() {
        return DeploymentFailedException.CONTAINER_ERROR_REASONS.stream()
                .flatMap(
                        reason ->
                                Stream.of(Arguments.of(reason, true), Arguments.of(reason, false)));
    }

    @Test
    public void observeAlreadyUpgraded() {
        var kubernetesDeployment = TestUtils.createDeployment(true);
        kubernetesDeployment.getMetadata().setAnnotations(new HashMap<>());

        var context = TestUtils.createContextWithDeployment(kubernetesDeployment, kubernetesClient);

        deployment.getMetadata().setGeneration(123L);

        var status = deployment.getStatus();
        var reconStatus = status.getReconciliationStatus();

        // New deployment
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(
                deployment,
                new FlinkConfigManager(new Configuration())
                        .getDeployConfig(deployment.getMetadata(), deployment.getSpec()));
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        // Test regular upgrades
        deployment.getSpec().getJob().setParallelism(5);
        deployment.getMetadata().setGeneration(321L);
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(
                deployment,
                new FlinkConfigManager(new Configuration())
                        .getDeployConfig(deployment.getMetadata(), deployment.getSpec()));
        status = deployment.getStatus();
        reconStatus = status.getReconciliationStatus();

        assertEquals(status.getReconciliationStatus().getState(), ReconciliationState.UPGRADING);
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);

        // Kubernetes Deployment is not there yet
        observer.observe(deployment, TestUtils.createEmptyContext());
        assertEquals(JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());

        // Kubernetes Deployment is there but without the correct label
        observer.observe(deployment, context);
        assertEquals(JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());

        // We set the correct generation label on the kubernetes deployment
        kubernetesDeployment
                .getMetadata()
                .getAnnotations()
                .put(FlinkUtils.CR_GENERATION_LABEL, "321");

        deployment.getMetadata().setGeneration(322L);
        deployment.getSpec().getJob().setParallelism(4);

        // Simulate marked for deletion, make sure we don't recognize this as a valid deployment
        kubernetesDeployment.getMetadata().setDeletionTimestamp(Instant.now().toString());
        observer.observe(deployment, context);
        assertEquals(ReconciliationState.UPGRADING, reconStatus.getState());
        assertEquals(JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());

        // Reset deletion flag
        kubernetesDeployment.getMetadata().setDeletionTimestamp(null);

        // Simulate non-missing deployment, this happens in the middle of savepoint upgrades
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        observer.observe(deployment, context);

        assertEquals(ReconciliationState.UPGRADING, reconStatus.getState());
        assertEquals(JobManagerDeploymentStatus.READY, status.getJobManagerDeploymentStatus());

        // Reset to missing
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);

        // Deployment is missing and kubernetes deployment matches the target generation
        // should be recognized as deployed
        observer.observe(deployment, context);
        assertEquals(ReconciliationState.DEPLOYED, reconStatus.getState());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                status.getJobManagerDeploymentStatus());

        var specWithMeta = status.getReconciliationStatus().deserializeLastReconciledSpecWithMeta();
        assertEquals(321L, status.getObservedGeneration());
        assertEquals(JobState.RUNNING, specWithMeta.getSpec().getJob().getState());
        assertEquals(5, specWithMeta.getSpec().getJob().getParallelism());
    }

    @Test
    public void validateLastReconciledClearedOnInitialFailure() {
        deployment.getMetadata().setGeneration(123L);

        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(
                deployment,
                new FlinkConfigManager(new Configuration())
                        .getDeployConfig(deployment.getMetadata(), deployment.getSpec()));
        var reconStatus = deployment.getStatus().getReconciliationStatus();

        assertTrue(
                reconStatus.deserializeLastReconciledSpecWithMeta().getMeta().isFirstDeployment());
        assertFalse(reconStatus.isBeforeFirstDeployment());

        observer.observe(deployment, TestUtils.createEmptyContext());
        assertTrue(reconStatus.isBeforeFirstDeployment());
    }

    @Test
    public void jobStatusNotOverwrittenWhenTerminal() throws Exception {
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);
        bringToReadyStatus(deployment);

        deployment
                .getStatus()
                .getJobStatus()
                .setState(org.apache.flink.api.common.JobStatus.FINISHED);

        // Simulate missing deployment
        var emptyContext = TestUtils.createEmptyContext();
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        observer.observe(deployment, emptyContext);

        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED,
                deployment.getStatus().getJobStatus().getState());
    }

    @Test
    public void getLastCheckpointShouldHandleCheckpointingNotEnabled() throws Exception {
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);
        bringToReadyStatus(deployment);

        deployment
                .getStatus()
                .getJobStatus()
                .setState(org.apache.flink.api.common.JobStatus.FINISHED);
        var jobs = flinkService.listJobs();
        var oldStatus = jobs.get(0).f1;
        jobs.get(0).f1 =
                new JobStatusMessage(
                        oldStatus.getJobId(),
                        oldStatus.getJobName(),
                        org.apache.flink.api.common.JobStatus.FINISHED,
                        oldStatus.getStartTime());

        flinkService.setThrowCheckpointingDisabledError(true);
        observer.observe(deployment, readyContext);

        assertEquals(
                0,
                countErrorEvents(
                        EventRecorder.Reason.CheckpointError.name(),
                        deployment.getMetadata().getNamespace(),
                        "Checkpointing has not been enabled"));
    }
}
