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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** @link Missing deployment recovery tests */
@EnableKubernetesMockClient(crud = true)
public class DeploymentRecoveryTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    private TestingFlinkService flinkService;
    private Context context;
    private FlinkDeploymentController testController;

    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService();
        context = flinkService.getContext();
        testController =
                TestUtils.createTestController(configManager, kubernetesClient, flinkService);
    }

    @ParameterizedTest
    @MethodSource("applicationTestParams")
    public void verifyApplicationJmRecovery(
            FlinkVersion flinkVersion, UpgradeMode upgradeMode, boolean enabled) {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(upgradeMode);
        if (enabled) {
            appCluster
                    .getSpec()
                    .getFlinkConfiguration()
                    .put(
                            KubernetesOperatorConfigOptions.OPERATOR_RECOVER_JM_DEPLOYMENT_ENABLED
                                    .key(),
                            "true");
        }
        FlinkDeploymentStatus status = appCluster.getStatus();

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        assertEquals(JobManagerDeploymentStatus.READY, status.getJobManagerDeploymentStatus());

        // Remove deployment
        flinkService.setPortReady(false);
        flinkService.clear();

        // Make sure we do not try to recover JM deployment errors (only missing)
        testController.reconcile(
                appCluster, TestUtils.createContextWithFailedJobManagerDeployment());
        testController.reconcile(
                appCluster, TestUtils.createContextWithFailedJobManagerDeployment());
        assertEquals(JobManagerDeploymentStatus.ERROR, status.getJobManagerDeploymentStatus());

        testController.reconcile(appCluster, context);
        if (flinkVersion.isNewerVersionThan(FlinkVersion.v1_14) || enabled) {
            assertEquals(
                    JobManagerDeploymentStatus.DEPLOYING, status.getJobManagerDeploymentStatus());
        } else {
            assertEquals(
                    JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());
        }
        flinkService.setPortReady(true);

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        if (flinkVersion.isNewerVersionThan(FlinkVersion.v1_14) || enabled) {
            assertEquals(JobManagerDeploymentStatus.READY, status.getJobManagerDeploymentStatus());
            assertEquals(JobStatus.RUNNING.name(), status.getJobStatus().getState());
        } else {
            assertEquals(
                    JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());
            assertEquals(JobStatus.FAILED.name(), status.getJobStatus().getState());
        }

        // Remove deployment
        flinkService.setPortReady(false);
        flinkService.clear();
        // Trigger update
        appCluster.getSpec().setRestartNonce(123L);

        testController.reconcile(appCluster, context);
        if (upgradeMode == UpgradeMode.SAVEPOINT) {
            // If deployment goes missing during an upgrade we should throw an error as savepoint
            // information cannot be recovered with complete certainty
            assertEquals(JobManagerDeploymentStatus.ERROR, status.getJobManagerDeploymentStatus());
        } else {
            flinkService.setPortReady(true);
            testController.reconcile(appCluster, context);
            testController.reconcile(appCluster, context);
            testController.reconcile(appCluster, context);
            assertEquals(JobManagerDeploymentStatus.READY, status.getJobManagerDeploymentStatus());
            assertEquals("RUNNING", status.getJobStatus().getState());
            assertEquals(
                    appCluster.getSpec(),
                    status.getReconciliationStatus().deserializeLastReconciledSpec());
        }
    }

    @ParameterizedTest
    @MethodSource("sessionTestParams")
    public void verifySessionJmRecovery(FlinkVersion flinkVersion, boolean enabled) {
        FlinkDeployment appCluster = TestUtils.buildSessionCluster(flinkVersion);
        if (enabled) {
            appCluster
                    .getSpec()
                    .getFlinkConfiguration()
                    .put(
                            KubernetesOperatorConfigOptions.OPERATOR_RECOVER_JM_DEPLOYMENT_ENABLED
                                    .key(),
                            "true");
        }
        FlinkDeploymentStatus status = appCluster.getStatus();

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        assertEquals(JobManagerDeploymentStatus.READY, status.getJobManagerDeploymentStatus());

        // Remove deployment
        flinkService.setPortReady(false);
        flinkService.clear();
        testController.reconcile(appCluster, context);
        flinkService.setPortReady(true);

        if (flinkVersion.isNewerVersionThan(FlinkVersion.v1_14) || enabled) {
            assertEquals(
                    JobManagerDeploymentStatus.DEPLOYING, status.getJobManagerDeploymentStatus());
            testController.reconcile(appCluster, context);
            testController.reconcile(appCluster, context);
            assertEquals(JobManagerDeploymentStatus.READY, status.getJobManagerDeploymentStatus());
        } else {
            assertEquals(
                    JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());
            testController.reconcile(appCluster, context);
            assertEquals(
                    JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());
        }
    }

    private static Stream<Arguments> applicationTestParams() {
        List<Arguments> args = new ArrayList<>();
        for (FlinkVersion version : FlinkVersion.values()) {
            for (UpgradeMode upgradeMode : UpgradeMode.values()) {
                args.add(arguments(version, upgradeMode, true));
                args.add(arguments(version, upgradeMode, false));
            }
        }
        return args.stream();
    }

    private static Stream<Arguments> sessionTestParams() {
        List<Arguments> args = new ArrayList<>();
        for (FlinkVersion version : FlinkVersion.values()) {
            args.add(arguments(version, true));
            args.add(arguments(version, false));
        }
        return args.stream();
    }
}
