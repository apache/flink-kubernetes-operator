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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_JOB_RESTART_FAILED;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** @link Unhealthy deployment restart tests */
@EnableKubernetesMockClient(crud = true)
public class FailedDeploymentRestartTest {
    private FlinkConfigManager configManager;

    private TestingFlinkService flinkService;
    private Context<FlinkDeployment> context;
    private TestingFlinkDeploymentController testController;

    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        var configuration = new Configuration();
        configuration.set(OPERATOR_JOB_RESTART_FAILED, true);
        configManager = new FlinkConfigManager(configuration);
        flinkService = new TestingFlinkService(kubernetesClient);
        context = flinkService.getContext();
        testController = new TestingFlinkDeploymentController(configManager, flinkService);
        kubernetesClient.resource(TestUtils.buildApplicationCluster()).createOrReplace();
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
        assertEquals("RUNNING", appCluster.getStatus().getJobStatus().getState());

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
        assertEquals("RUNNING", appCluster.getStatus().getJobStatus().getState());

        // We started without savepoint
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        assertEquals(
                appCluster.getSpec(),
                appCluster.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());
    }
}
