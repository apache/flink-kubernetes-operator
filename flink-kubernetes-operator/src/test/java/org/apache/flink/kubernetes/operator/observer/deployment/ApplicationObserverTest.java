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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingStatusRecorder;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link ApplicationObserver} unit tests. */
@EnableKubernetesMockClient(crud = true)
public class ApplicationObserverTest {
    private KubernetesClient kubernetesClient;
    private final Context readyContext = TestUtils.createContextWithReadyJobManagerDeployment();
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private final TestingFlinkService flinkService = new TestingFlinkService();
    private ApplicationObserver observer;

    @BeforeEach
    public void before() {
        var eventRecorder = new EventRecorder(kubernetesClient, (r, e) -> {});
        observer =
                new ApplicationObserver(
                        flinkService, configManager, new TestingStatusRecorder<>(), eventRecorder);
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
                0,
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems()
                        .size());

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
                        .count());
        assertEquals(
                1,
                kubernetesClient.v1().events().inNamespace(deployment.getMetadata().getNamespace())
                        .list().getItems().stream()
                        .filter(e -> e.getReason().contains("SavepointError"))
                        .collect(Collectors.toList())
                        .get(0)
                        .getCount());

        // savepoint success
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
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        // second attempt success
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
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        // application failure after checkpoint trigger
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
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));

        assertEquals(
                1,
                kubernetesClient.v1().events().inNamespace(deployment.getMetadata().getNamespace())
                        .list().getItems().stream()
                        .filter(e -> e.getReason().contains("SavepointError"))
                        .count());
        assertEquals(
                2,
                kubernetesClient.v1().events().inNamespace(deployment.getMetadata().getNamespace())
                        .list().getItems().stream()
                        .filter(e -> e.getReason().contains("SavepointError"))
                        .collect(Collectors.toList())
                        .get(0)
                        .getCount());

        flinkService.setPortReady(true);
        observer.observe(deployment, readyContext);
        // Simulate Failed job
        Tuple2<String, JobStatusMessage> jobTuple = flinkService.listJobs().get(0);
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
        flinkService.setJmPodList(TestUtils.createFailedPodList(podFailedMessage));
        flinkService.setPortReady(false);
        Exception exception =
                assertThrows(
                        DeploymentFailedException.class,
                        () -> {
                            observer.observe(
                                    deployment, TestUtils.createContextWithInProgressDeployment());
                        });
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
        assertEquals(321L, specWithMeta.f1.get("metadata").get("generation").asLong());
        assertEquals(JobState.RUNNING, specWithMeta.f0.getJob().getState());
        assertEquals(5, specWithMeta.f0.getJob().getParallelism());
    }
}
