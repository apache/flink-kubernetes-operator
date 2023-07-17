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
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.observer.TestObserverAdapter;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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

    private final Context<FlinkDeployment> readyContext =
            TestUtils.createContextWithReadyJobManagerDeployment();
    private TestObserverAdapter<FlinkDeployment> observer;

    @Override
    public void setup() {
        observer = new TestObserverAdapter<>(this, new ApplicationObserver(eventRecorder));
    }

    @Test
    public void observeApplicationCluster() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
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
        assertEquals(JobState.RUNNING.name(), deployment.getStatus().getJobStatus().getState());
        assertEquals(
                deployment.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                deployment.getStatus().getReconciliationStatus().getLastStableSpec());

        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertEquals(JobState.RUNNING.name(), deployment.getStatus().getJobStatus().getState());

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
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                deployment.getStatus().getJobStatus().getState());
        assertNull(deployment.getStatus().getReconciliationStatus().getLastStableSpec());
    }

    @Test
    public void testEventGeneratedWhenStatusChanged() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);

        deployment.setStatus(deployment.initStatus());
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
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);

        deployment.setStatus(deployment.initStatus());
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
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Long timedOutNonce = 1L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(timedOutNonce);
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);
        bringToReadyStatus(deployment);
        assertTrue(ReconciliationUtils.isJobRunning(deployment.getStatus()));

        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                SavepointTriggerType.MANUAL,
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                conf);
        // pending savepoint
        assertEquals(
                "trigger_0",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        observer.observe(deployment, readyContext);
        assertTrue(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        deployment.getStatus().getJobStatus().getSavepointInfo().setTriggerId("unknown");
        // savepoint error within grace period
        assertEquals(
                0,
                (int)
                        kubernetesClient.v1().events()
                                .inNamespace(deployment.getMetadata().getNamespace()).list()
                                .getItems().stream()
                                .filter(e -> e.getReason().contains("SavepointError"))
                                .count());
        observer.observe(deployment, readyContext);
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
        assertEquals(
                1,
                kubernetesClient.v1().events().inNamespace(deployment.getMetadata().getNamespace())
                        .list().getItems().stream()
                        .filter(e -> e.getReason().contains("SavepointError"))
                        .count());

        deployment.getStatus().getJobStatus().getSavepointInfo().setTriggerId("unknown");
        deployment
                .getStatus()
                .getJobStatus()
                .getSavepointInfo()
                .setTriggerType(SavepointTriggerType.MANUAL);
        deployment
                .getStatus()
                .getJobStatus()
                .getSavepointInfo()
                .setTriggerTimestamp(Instant.now().minus(Duration.ofHours(1)).toEpochMilli());

        observer.observe(deployment, readyContext);
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
        assertEquals(
                1,
                kubernetesClient.v1().events().inNamespace(deployment.getMetadata().getNamespace())
                        .list().getItems().stream()
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
                kubernetesClient.v1().events().inNamespace(deployment.getMetadata().getNamespace())
                        .list().getItems().stream()
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
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                SavepointTriggerType.MANUAL,
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                conf);
        assertEquals(
                "trigger_1",
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
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        // second attempt success
        Long secondNonce = 456L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(secondNonce);
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                SavepointTriggerType.MANUAL,
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                conf);
        assertEquals(
                "trigger_2",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertTrue(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

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
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        // application failure after checkpoint trigger
        Long thirdNonce = 789L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(thirdNonce);
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                SavepointTriggerType.MANUAL,
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                conf);
        assertEquals(
                "trigger_3",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertTrue(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
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
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        assertEquals(
                1,
                kubernetesClient.v1().events().inNamespace(deployment.getMetadata().getNamespace())
                        .list().getItems().stream()
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
                kubernetesClient.v1().events().inNamespace(deployment.getMetadata().getNamespace())
                        .list().getItems().stream()
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
                org.apache.flink.api.common.JobStatus.FAILED.name(),
                deployment.getStatus().getJobStatus().getState());
        assertEquals(
                "last-SP",
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        observer.observe(deployment, readyContext);
        assertEquals(
                3,
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
    public void testSavepointFormat() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf, false);
        bringToReadyStatus(deployment);
        assertTrue(ReconciliationUtils.isJobRunning(deployment.getStatus()));

        // canonical savepoint
        Long firstNonce = 123L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(firstNonce);
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                SavepointTriggerType.MANUAL,
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                conf);

        observer.observe(deployment, readyContext);
        observer.observe(deployment, readyContext);
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
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
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                SavepointTriggerType.MANUAL,
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                conf);

        observer.observe(deployment, readyContext);
        observer.observe(deployment, readyContext);
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
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

        // canonical for flink savepoint
        Long thirdNonce = 789L;
        deployment.getSpec().getJob().setSavepointTriggerNonce(thirdNonce);
        deployment.getSpec().setFlinkVersion(FlinkVersion.v1_14);
        deployment
                .getSpec()
                .setFlinkConfiguration(
                        Map.of(
                                OPERATOR_SAVEPOINT_FORMAT_TYPE.key(),
                                org.apache.flink.core.execution.SavepointFormatType.NATIVE.name()));
        conf = configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                SavepointTriggerType.MANUAL,
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                conf);

        observer.observe(deployment, readyContext);
        observer.observe(deployment, readyContext);
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
        assertEquals(
                thirdNonce,
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
    }

    private void bringToReadyStatus(FlinkDeployment deployment) {
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobName("jobname");
        jobStatus.setJobId("0000000000");
        jobStatus.setState(JobState.RUNNING.name());
        deployment.getStatus().setJobStatus(jobStatus);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
    }

    @Test
    public void observeListJobsError() {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        bringToReadyStatus(deployment);
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        // simulate deployment failure
        String podFailedMessage = "list jobs error";
        flinkService.setPodList(
                TestUtils.createFailedPodList(
                        podFailedMessage, DeploymentFailedException.REASON_CRASH_LOOP_BACKOFF));
        flinkService.setPortReady(false);
        Exception exception =
                assertThrows(
                        DeploymentFailedException.class,
                        () ->
                                observer.observe(
                                        deployment,
                                        TestUtils.createContextWithInProgressDeployment()));
        assertEquals(podFailedMessage, exception.getMessage());
    }

    @Test
    public void observeAlreadyUpgraded() {
        var kubernetesDeployment = TestUtils.createDeployment(true);
        kubernetesDeployment.getMetadata().setAnnotations(new HashMap<>());

        var context = TestUtils.createContextWithDeployment(kubernetesDeployment);

        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
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

        observer.observe(deployment, TestUtils.createEmptyContext());
        assertEquals(JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());

        observer.observe(deployment, context);
        assertEquals(JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());

        kubernetesDeployment
                .getMetadata()
                .getAnnotations()
                .put(FlinkUtils.CR_GENERATION_LABEL, "321");

        deployment.getMetadata().setGeneration(322L);
        deployment.getSpec().getJob().setParallelism(4);

        observer.observe(deployment, context);

        assertEquals(ReconciliationState.DEPLOYED, reconStatus.getState());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                status.getJobManagerDeploymentStatus());

        var specWithMeta = status.getReconciliationStatus().deserializeLastReconciledSpecWithMeta();
        assertEquals(321L, specWithMeta.getMeta().getMetadata().getGeneration());
        assertEquals(JobState.RUNNING, specWithMeta.getSpec().getJob().getState());
        assertEquals(5, specWithMeta.getSpec().getJob().getParallelism());
    }

    @Test
    public void observeAlreadyScaled() {
        var deployment = TestUtils.buildApplicationCluster();

        // Update status for for running job
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(
                deployment,
                new FlinkConfigManager(new Configuration())
                        .getDeployConfig(deployment.getMetadata(), deployment.getSpec()));
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());

        var conf = new Configuration();
        var v1 = new JobVertexID();
        conf.set(PipelineOptions.PARALLELISM_OVERRIDES, Map.of(v1.toHexString(), "2"));
        deployment.getSpec().setFlinkConfiguration(conf.toMap());

        // Update status after triggering scale operation
        ReconciliationUtils.updateAfterScaleUp(
                deployment,
                new Configuration(),
                Clock.systemDefaultZone(),
                FlinkService.ScalingResult.SCALING_TRIGGERED);
        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());

        // Assert that we remain in upgrading until scaling completes
        flinkService.setScalingCompleted(false);
        observer.observe(deployment, context);
        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());

        // Assert that we move to deployed when scaling completes
        flinkService.setScalingCompleted(true);
        observer.observe(deployment, context);
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
    }

    @Test
    public void validateLastReconciledClearedOnInitialFailure() {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
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
}
