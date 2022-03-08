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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** @link Observer unit tests */
public class ObserverTest {

    private final Context readyContext = TestUtils.createContextWithReadyJobManagerDeployment();

    @Test
    public void observeSessionCluster() {
        FlinkService flinkService = new TestingFlinkService();
        Observer observer =
                new Observer(
                        flinkService,
                        FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(deployment.getSpec());

        observer.observe(
                deployment,
                readyContext,
                FlinkUtils.getEffectiveConfig(deployment, new Configuration()));

        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        observer.observe(
                deployment,
                readyContext,
                FlinkUtils.getEffectiveConfig(deployment, new Configuration()));

        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
    }

    @Test
    public void observeApplicationCluster() {
        TestingFlinkService flinkService = new TestingFlinkService();
        Observer observer =
                new Observer(
                        flinkService,
                        FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration conf = FlinkUtils.getEffectiveConfig(deployment, new Configuration());

        observer.observe(deployment, TestUtils.createEmptyContext(), conf);

        deployment.setStatus(new FlinkDeploymentStatus());
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(deployment.getSpec());
        deployment.getStatus().setJobStatus(new JobStatus());
        flinkService.submitApplicationCluster(deployment, conf);

        // Validate port check logic
        flinkService.setPortReady(false);

        // Port not ready
        observer.observe(deployment, readyContext, conf);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());

        observer.observe(deployment, readyContext, conf);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());

        flinkService.setPortReady(true);
        // Port ready but we have to recheck once again
        observer.observe(deployment, readyContext, conf);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        // Stable ready
        observer.observe(deployment, readyContext, conf);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertEquals(JobState.RUNNING.name(), deployment.getStatus().getJobStatus().getState());

        observer.observe(deployment, readyContext, conf);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertEquals(JobState.RUNNING.name(), deployment.getStatus().getJobStatus().getState());

        assertEquals(
                deployment.getMetadata().getName(),
                deployment.getStatus().getJobStatus().getJobName());

        // Test listing failure
        flinkService.clear();
        observer.observe(deployment, readyContext, conf);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertEquals(Observer.JOB_STATE_UNKNOWN, deployment.getStatus().getJobStatus().getState());
    }

    @Test
    public void observeSavepoint() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        Observer observer =
                new Observer(
                        flinkService,
                        FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration conf = FlinkUtils.getEffectiveConfig(deployment, new Configuration());
        flinkService.submitApplicationCluster(deployment, conf);
        bringToReadyStatus(deployment);
        observer.observe(deployment, readyContext, conf);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        flinkService.triggerSavepoint(deployment, conf);

        assertEquals(
                "trigger_0",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        observer.observe(deployment, readyContext, conf);
        assertEquals(
                "savepoint_0",
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());
        assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        flinkService.triggerSavepoint(deployment, conf);
        assertEquals(
                "trigger_1",
                deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        observer.observe(deployment, readyContext, conf);
        assertEquals(
                "savepoint_1",
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());
        assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
    }

    private void bringToReadyStatus(FlinkDeployment deployment) {
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(ReconciliationUtils.clone(deployment.getSpec()));
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobName("jobname");
        jobStatus.setJobId("0000000000");
        jobStatus.setState(JobState.RUNNING.name());
        deployment.getStatus().setJobStatus(jobStatus);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
    }
}
