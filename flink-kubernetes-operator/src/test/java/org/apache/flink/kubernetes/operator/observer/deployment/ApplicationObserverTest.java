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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link ApplicationObserver} unit tests. */
public class ApplicationObserverTest {

    private final Context readyContext = TestUtils.createContextWithReadyJobManagerDeployment();
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @Test
    public void observeApplicationCluster() {
        TestingFlinkService flinkService = new TestingFlinkService();
        ApplicationObserver observer = new ApplicationObserver(flinkService, configManager);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.setStatus(new FlinkDeploymentStatus());

        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());

        observer.observe(deployment, TestUtils.createEmptyContext());
        assertNull(deployment.getStatus().getReconciliationStatus().getLastStableSpec());

        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec());
        deployment.getStatus().setJobStatus(new JobStatus());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf);

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

        // Stable ready
        observer.observe(deployment, readyContext);
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
                AbstractDeploymentObserver.JOB_STATE_UNKNOWN,
                deployment.getStatus().getJobStatus().getState());
        assertNull(deployment.getStatus().getReconciliationStatus().getLastStableSpec());
    }

    @Test
    public void observeSavepoint() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        ApplicationObserver observer = new ApplicationObserver(flinkService, configManager);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration conf =
                configManager.getDeployConfig(deployment.getMetadata(), deployment.getSpec());
        flinkService.submitApplicationCluster(deployment.getSpec().getJob(), conf);
        bringToReadyStatus(deployment);
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                conf);
        assertEquals(
                "trigger_0",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        observer.observe(deployment, readyContext);
        assertEquals(
                "savepoint_0",
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());
        assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerTimestamp());

        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                conf);
        assertEquals(
                "trigger_1",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        observer.observe(deployment, readyContext);
        assertEquals(
                "savepoint_1",
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());
        assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerTimestamp());
    }

    private void bringToReadyStatus(FlinkDeployment deployment) {
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec());
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobName("jobname");
        jobStatus.setJobId("0000000000");
        jobStatus.setState(JobState.RUNNING.name());
        deployment.getStatus().setJobStatus(jobStatus);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
    }

    @Test
    public void observeListJobsError() {
        TestingFlinkService flinkService = new TestingFlinkService();
        ApplicationObserver observer = new ApplicationObserver(flinkService, configManager);
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
}
