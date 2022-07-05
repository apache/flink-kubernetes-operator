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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
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
    private TestingFlinkDeploymentController testController;

    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService(kubernetesClient);
        context = flinkService.getContext();
        testController =
                new TestingFlinkDeploymentController(configManager, kubernetesClient, flinkService);
        kubernetesClient.resource(TestUtils.buildApplicationCluster()).createOrReplace();
    }

    @ParameterizedTest
    @MethodSource("applicationTestParams")
    public void verifyApplicationJmRecovery(FlinkVersion flinkVersion, UpgradeMode upgradeMode)
            throws Exception {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(upgradeMode);

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // Remove deployment
        flinkService.setPortReady(false);
        flinkService.clear();

        // Make sure we do not try to recover JM deployment errors (only missing)
        testController.reconcile(
                appCluster, TestUtils.createContextWithFailedJobManagerDeployment());
        testController.reconcile(
                appCluster, TestUtils.createContextWithFailedJobManagerDeployment());
        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        flinkService.setPortReady(true);

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(JobStatus.RUNNING.name(), appCluster.getStatus().getJobStatus().getState());

        // Remove deployment
        flinkService.setPortReady(false);
        flinkService.clear();
        // Trigger update
        appCluster.getSpec().setRestartNonce(123L);
        if (upgradeMode == UpgradeMode.SAVEPOINT) {
            flinkService.setHaDataAvailable(false);
        }

        testController.reconcile(appCluster, context);
        if (upgradeMode == UpgradeMode.SAVEPOINT) {
            // If deployment goes missing during an upgrade we should throw an error as savepoint
            // information cannot be recovered with complete certainty
            assertEquals(
                    JobManagerDeploymentStatus.ERROR,
                    appCluster.getStatus().getJobManagerDeploymentStatus());
        } else {
            flinkService.setPortReady(true);
            testController.reconcile(appCluster, context);
            testController.reconcile(appCluster, context);
            testController.reconcile(appCluster, context);
            assertEquals(
                    JobManagerDeploymentStatus.READY,
                    appCluster.getStatus().getJobManagerDeploymentStatus());
            assertEquals("RUNNING", appCluster.getStatus().getJobStatus().getState());
            assertEquals(
                    appCluster.getSpec(),
                    appCluster
                            .getStatus()
                            .getReconciliationStatus()
                            .deserializeLastReconciledSpec());
        }
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void verifySessionJmRecovery(FlinkVersion flinkVersion) throws Exception {
        FlinkDeployment appCluster = TestUtils.buildSessionCluster(flinkVersion);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // Remove deployment
        flinkService.setPortReady(false);
        flinkService.clear();
        testController.reconcile(appCluster, context);
        flinkService.setPortReady(true);

        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
    }

    private static Stream<Arguments> applicationTestParams() {
        List<Arguments> args = new ArrayList<>();
        for (FlinkVersion version : FlinkVersion.values()) {
            for (UpgradeMode upgradeMode : UpgradeMode.values()) {
                args.add(arguments(version, upgradeMode));
            }
        }
        return args.stream();
    }
}
