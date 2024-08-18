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
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.observer.SnapshotObserver;
import org.apache.flink.kubernetes.operator.service.CheckpointHistoryWrapper;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;

import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_JOB_RESTART_FAILED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @link Unhealthy deployment restart tests
 */
@EnableKubernetesMockClient(crud = true)
public class FailedDeploymentRestartTest extends OperatorTestBase {
    private FlinkConfigManager configManager;

    private Context<FlinkDeployment> context;
    private TestingFlinkDeploymentController testController;
    private SnapshotObserver<FlinkDeployment, FlinkDeploymentStatus> observer;

    @Getter private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        var configuration = new Configuration();
        configuration.set(OPERATOR_JOB_RESTART_FAILED, true);
        configManager = new FlinkConfigManager(configuration);
        context = flinkService.getContext();
        testController = new TestingFlinkDeploymentController(configManager, flinkService);
        kubernetesClient.resource(TestUtils.buildApplicationCluster()).createOrReplace();
        observer = new SnapshotObserver<>(eventRecorder);
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersionsAndUpgradeModes")
    public void verifyFailedApplicationRecovery(FlinkVersion flinkVersion, UpgradeMode upgradeMode)
            throws Exception {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(upgradeMode);

        // Start a healthy deployment
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(RUNNING, appCluster.getStatus().getJobStatus().getState());

        // Make deployment unhealthy
        flinkService.markApplicationJobFailedWithError(
                flinkService.listJobs().get(0).f1.getJobId(), "Failed job");
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // After restart the deployment is healthy again
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(RUNNING, appCluster.getStatus().getJobStatus().getState());

        // We started without savepoint
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        assertEquals(
                appCluster.getSpec(),
                appCluster.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());
    }

    @ParameterizedTest
    @EnumSource(UpgradeMode.class)
    public void verifyFailedApplicationRecoveryWithCheckpoint(UpgradeMode upgradeMode)
            throws Exception {
        var savepointPath = "/tmp/savepoint";
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().getJob().setUpgradeMode(upgradeMode);

        // Start a healthy deployment
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        // Mark job_id
        String jobId = appCluster.getStatus().getJobStatus().getJobId();
        assertNotNull(jobId);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(RUNNING, appCluster.getStatus().getJobStatus().getState());
        assertNull(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH));

        // trigger checkpoint
        long now = System.currentTimeMillis();
        flinkService.setCheckpointInfo(
                Tuple2.of(
                        Optional.of(
                                new CheckpointHistoryWrapper.CompletedCheckpointInfo(
                                        0, savepointPath, now - 30000)),
                        Optional.of(
                                new CheckpointHistoryWrapper.PendingCheckpointInfo(
                                        0, now - 61000))));

        // Make deployment unhealthy
        flinkService.markApplicationJobFailedWithError(
                flinkService.listJobs().get(0).f1.getJobId(), "Failed job");
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // After restart the deployment is healthy again
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(RUNNING, appCluster.getStatus().getJobStatus().getState());

        // check savepoint_path
        if (upgradeMode != UpgradeMode.STATELESS) {
            assertEquals(
                    flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH),
                    savepointPath);
        } else {
            assertNull(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH));
        }
    }
}
