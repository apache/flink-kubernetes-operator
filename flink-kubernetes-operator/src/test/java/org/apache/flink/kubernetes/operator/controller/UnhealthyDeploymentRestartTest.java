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
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;

import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_WINDOW;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD;
import static org.apache.flink.kubernetes.operator.observer.ClusterHealthEvaluator.getLastValidClusterHealthInfo;
import static org.apache.flink.kubernetes.operator.observer.ClusterHealthEvaluator.setLastValidClusterHealthInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @link Unhealthy deployment restart tests
 */
@EnableKubernetesMockClient(crud = true)
public class UnhealthyDeploymentRestartTest {

    private static final String NUM_RESTARTS_METRIC_NAME = "numRestarts";

    private static final String NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME =
            "numberOfCompletedCheckpoints";

    private FlinkConfigManager configManager;

    private TestingFlinkService flinkService;
    private Context<FlinkDeployment> context;
    private TestingFlinkDeploymentController testController;

    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        var configuration = new Configuration();
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED, true);
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD, 64);
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_ENABLED, true);
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_WINDOW, Duration.ZERO);
        configManager = new FlinkConfigManager(configuration);
        flinkService = new TestingFlinkService(kubernetesClient);
        context = flinkService.getContext();
        testController = new TestingFlinkDeploymentController(configManager, flinkService);
        kubernetesClient.resource(TestUtils.buildApplicationCluster()).createOrReplace();

        flinkService.setMetricValue(NUM_RESTARTS_METRIC_NAME, "0");
        flinkService.setMetricValue(NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME, "1");
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersionsAndUpgradeModes")
    public void verifyApplicationUnhealthyJmRecovery(
            FlinkVersion flinkVersion, UpgradeMode upgradeMode) throws Exception {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(upgradeMode);

        // Start a healthy deployment
        flinkService.setMetricValue(NUM_RESTARTS_METRIC_NAME, "0");
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(RUNNING, appCluster.getStatus().getJobStatus().getState());

        // Make deployment unhealthy
        flinkService.setMetricValue(NUM_RESTARTS_METRIC_NAME, "100");
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // After restart the deployment is healthy again
        flinkService.setMetricValue(NUM_RESTARTS_METRIC_NAME, "0");
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(RUNNING, appCluster.getStatus().getJobStatus().getState());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersionsAndUpgradeModes")
    public void verifyApplicationNoCompletedCheckpointsJmRecovery(
            FlinkVersion flinkVersion, UpgradeMode upgradeMode) throws Exception {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(upgradeMode);

        // Start a healthy deployment
        flinkService.setMetricValue(NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME, "1");
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(RUNNING, appCluster.getStatus().getJobStatus().getState());

        // Make deployment unhealthy
        flinkService.setMetricValue(NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME, "1");
        ClusterHealthInfo clusterHealthInfo =
                getLastValidClusterHealthInfo(appCluster.getStatus().getClusterInfo());
        // Ensure the last savepoint has been taken more than 10 minutes ago (Default checkpoint
        // interval)
        clusterHealthInfo.setNumCompletedCheckpointsIncreasedTimeStamp(
                clusterHealthInfo.getNumCompletedCheckpointsIncreasedTimeStamp() - 1200000);
        setLastValidClusterHealthInfo(appCluster.getStatus().getClusterInfo(), clusterHealthInfo);
        testController.getStatusRecorder().patchAndCacheStatus(appCluster, kubernetesClient);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // After restart the deployment is healthy again
        flinkService.setMetricValue(NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME, "2");
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(RUNNING, appCluster.getStatus().getJobStatus().getState());
    }
}
