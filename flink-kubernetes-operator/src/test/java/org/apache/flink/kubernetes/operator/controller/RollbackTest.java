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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.util.function.ThrowingRunnable;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link RollBack logic tests */
@EnableKubernetesMockClient(crud = true)
public class RollbackTest {

    private TestingFlinkService flinkService;
    private Context<FlinkDeployment> context;

    private TestingFlinkDeploymentController testController;

    private KubernetesClient kubernetesClient;

    private static final int ROLLBACK_DELAY = 600;

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService(kubernetesClient);
        context = flinkService.getContext();
        testController =
                new TestingFlinkDeploymentController(
                        new FlinkConfigManager(new Configuration()),
                        kubernetesClient,
                        flinkService);
        kubernetesClient.resource(TestUtils.buildApplicationCluster()).createOrReplace();
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testRollbackWithSavepoint(FlinkVersion flinkVersion) throws Exception {
        var dep = TestUtils.buildApplicationCluster(flinkVersion);
        dep.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        var flinkConfiguration = dep.getSpec().getFlinkConfiguration();
        flinkConfiguration.put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), "sd");

        List<String> savepoints = new ArrayList<>();
        testRollback(
                dep,
                () -> {
                    dep.getSpec().getJob().setParallelism(9999);
                    testController.reconcile(dep, context);
                    savepoints.add(
                            dep.getStatus()
                                    .getJobStatus()
                                    .getSavepointInfo()
                                    .getLastSavepoint()
                                    .getLocation());
                    assertEquals(
                            JobState.SUSPENDED,
                            dep.getStatus()
                                    .getReconciliationStatus()
                                    .deserializeLastReconciledSpec()
                                    .getJob()
                                    .getState());
                    testController.reconcile(dep, context);

                    // Trigger rollback by delaying the recovery
                    Thread.sleep(ROLLBACK_DELAY);
                    testController.reconcile(dep, context);
                },
                () -> {
                    assertEquals("RUNNING", dep.getStatus().getJobStatus().getState());
                    assertEquals(1, flinkService.listJobs().size());
                    // Make sure we rolled back using the savepoint taken during upgrade
                    assertEquals(savepoints.get(0), flinkService.listJobs().get(0).f0);
                    dep.getSpec().setRestartNonce(10L);
                    testController.reconcile(dep, context);
                },
                false);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testRollbackWithLastState(FlinkVersion flinkVersion) throws Exception {
        var dep = TestUtils.buildApplicationCluster(flinkVersion);
        dep.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);

        testRollback(
                dep,
                () -> {
                    dep.getSpec().getJob().setParallelism(9999);
                    testController.reconcile(dep, context);
                    assertEquals(
                            JobState.SUSPENDED,
                            dep.getStatus()
                                    .getReconciliationStatus()
                                    .deserializeLastReconciledSpec()
                                    .getJob()
                                    .getState());
                    testController.reconcile(dep, context);

                    // Trigger rollback by delaying the recovery
                    Thread.sleep(ROLLBACK_DELAY);
                    testController.reconcile(dep, context);
                },
                () -> {
                    assertEquals("RUNNING", dep.getStatus().getJobStatus().getState());
                    assertEquals(1, flinkService.listJobs().size());
                    dep.getSpec().setRestartNonce(10L);
                    testController.reconcile(dep, context);
                },
                true);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testRollbackFailureWithLastState(FlinkVersion flinkVersion) throws Exception {
        var dep = TestUtils.buildApplicationCluster(flinkVersion);
        dep.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        dep.getSpec().getFlinkConfiguration().put("t", "1");

        testRollback(
                dep,
                () -> {
                    dep.getSpec().getJob().setParallelism(9999);
                    dep.getSpec().getFlinkConfiguration().remove("t");
                    testController.reconcile(dep, context);
                    assertEquals(
                            JobState.SUSPENDED,
                            dep.getStatus()
                                    .getReconciliationStatus()
                                    .deserializeLastReconciledSpec()
                                    .getJob()
                                    .getState());
                    testController.reconcile(dep, context);

                    // Trigger rollback by delaying the recovery
                    Thread.sleep(ROLLBACK_DELAY);
                    testController.reconcile(dep, context);
                },
                () -> {
                    assertEquals("RUNNING", dep.getStatus().getJobStatus().getState());
                    assertEquals(1, flinkService.listJobs().size());

                    // Trigger deployment recovery
                    flinkService.clear();
                    flinkService.setPortReady(false);

                    testController.reconcile(dep, context);
                    flinkService.setPortReady(true);
                    testController.reconcile(dep, context);
                    var jobs = flinkService.listJobs();
                    // Make sure deployment was recovered with correct spec/config
                    assertTrue(jobs.get(jobs.size() - 1).f2.containsKey("t"));

                    // Remove job to simulate rollback failure
                    flinkService.clear();
                    flinkService.setPortReady(false);

                    dep.getSpec().setRestartNonce(10L);
                    testController.reconcile(dep, context);
                    flinkService.setPortReady(true);
                },
                false);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testRollbackStateless(FlinkVersion flinkVersion) throws Exception {
        var dep = TestUtils.buildApplicationCluster(flinkVersion);
        dep.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);

        testRollback(
                dep,
                () -> {
                    dep.getSpec()
                            .getFlinkConfiguration()
                            .put(
                                    KubernetesOperatorConfigOptions.DEPLOYMENT_ROLLBACK_ENABLED
                                            .key(),
                                    "false");
                    dep.getSpec().getJob().setParallelism(9999);
                    testController.reconcile(dep, context);
                    assertEquals(
                            JobState.SUSPENDED,
                            dep.getStatus()
                                    .getReconciliationStatus()
                                    .deserializeLastReconciledSpec()
                                    .getJob()
                                    .getState());
                    testController.reconcile(dep, context);
                    // Validate that rollback config is picked up from latest deploy conf
                    dep.getSpec()
                            .getFlinkConfiguration()
                            .put(
                                    KubernetesOperatorConfigOptions.DEPLOYMENT_ROLLBACK_ENABLED
                                            .key(),
                                    "true");

                    // Trigger rollback by delaying the recovery
                    Thread.sleep(ROLLBACK_DELAY);
                    dep.getStatus()
                            .getJobStatus()
                            .getSavepointInfo()
                            .updateLastSavepoint(
                                    Savepoint.of("test", SavepointTriggerType.UPGRADE));
                    testController.reconcile(dep, context);
                },
                () -> {
                    assertEquals("RUNNING", dep.getStatus().getJobStatus().getState());
                    // Make sure we started from empty state even if savepoint was available
                    assertNull(new LinkedList<>(flinkService.listJobs()).getLast().f0);

                    dep.getSpec().setRestartNonce(10L);
                    testController.reconcile(dep, context);
                },
                true);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testRollbackSession(FlinkVersion flinkVersion) throws Exception {
        var dep = TestUtils.buildSessionCluster(flinkVersion);
        testRollback(
                dep,
                () -> {
                    dep.getSpec().getFlinkConfiguration().put("random", "config");
                    testController.reconcile(dep, context);
                    // Trigger rollback by delaying the recovery
                    Thread.sleep(ROLLBACK_DELAY);
                    testController.reconcile(dep, context);
                },
                () -> {
                    assertEquals(
                            JobManagerDeploymentStatus.READY,
                            dep.getStatus().getJobManagerDeploymentStatus());
                    dep.getSpec().setRestartNonce(10L);
                },
                false);
    }

    public void testRollback(
            FlinkDeployment deployment,
            ThrowingRunnable<Exception> triggerRollback,
            ThrowingRunnable<Exception> validateAndRecover,
            boolean injectValidationError)
            throws Exception {

        var flinkConfiguration = deployment.getSpec().getFlinkConfiguration();
        flinkConfiguration.put(
                KubernetesOperatorConfigOptions.DEPLOYMENT_ROLLBACK_ENABLED.key(), "true");
        flinkConfiguration.put(
                KubernetesOperatorConfigOptions.DEPLOYMENT_READINESS_TIMEOUT.key(), "400");

        testController.reconcile(deployment, context);

        // Validate reconciliation status

        testController.reconcile(deployment, context);
        testController.reconcile(deployment, context);

        // Validate stable job
        assertTrue(deployment.getStatus().getReconciliationStatus().isLastReconciledSpecStable());

        triggerRollback.run();

        assertFalse(deployment.getStatus().getReconciliationStatus().isLastReconciledSpecStable());
        assertEquals(
                ReconciliationState.ROLLING_BACK,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(
                "Deployment is not ready within the configured timeout, rolling back.",
                deployment.getStatus().getError());

        if (injectValidationError) {
            deployment.getSpec().setLogConfiguration(Map.of("invalid", "entry"));
        }

        testController.reconcile(deployment, context);
        assertEquals(
                ReconciliationState.ROLLED_BACK,
                deployment.getStatus().getReconciliationStatus().getState());
        deployment.getSpec().setLogConfiguration(null);

        testController.reconcile(deployment, context);
        testController.reconcile(deployment, context);

        assertEquals(
                ReconciliationState.ROLLED_BACK,
                deployment.getStatus().getReconciliationStatus().getState());
        assertFalse(deployment.getStatus().getReconciliationStatus().isLastReconciledSpecStable());

        validateAndRecover.run();
        // Test update
        testController.reconcile(deployment, context);
        assertEquals(
                deployment.getSpec(),
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());
        testController.reconcile(deployment, context);
        testController.reconcile(deployment, context);
        assertTrue(deployment.getStatus().getReconciliationStatus().isLastReconciledSpecStable());
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
        assertNull(deployment.getStatus().getError());

        deployment.getSpec().setRestartNonce(456L);
        triggerRollback.run();
        testController.reconcile(deployment, context);
        assertEquals(
                ReconciliationState.ROLLED_BACK,
                deployment.getStatus().getReconciliationStatus().getState());
        assertNotEquals(
                deployment.getStatus().getReconciliationStatus().deserializeLastStableSpec(),
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());

        deployment.setSpec(
                deployment.getStatus().getReconciliationStatus().deserializeLastStableSpec());
        testController.reconcile(deployment, context);
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(
                deployment.getStatus().getReconciliationStatus().deserializeLastStableSpec(),
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());

        if (deployment.getSpec().getJob() != null) {
            deployment.getSpec().getJob().setState(JobState.SUSPENDED);
            deployment.getSpec().getJob().setParallelism(1);
            testController.reconcile(deployment, context);
            testController.reconcile(deployment, context);
            assertTrue(
                    deployment.getStatus().getReconciliationStatus().isLastReconciledSpecStable());
            assertEquals(
                    ReconciliationState.DEPLOYED,
                    deployment.getStatus().getReconciliationStatus().getState());
            assertNull(deployment.getStatus().getError());

            deployment.getSpec().getJob().setState(JobState.RUNNING);
            testController.reconcile(deployment, context);
            // Make sure we do not roll back to suspended state
            Thread.sleep(ROLLBACK_DELAY);
            testController.reconcile(deployment, context);
            testController.reconcile(deployment, context);
            assertTrue(
                    deployment.getStatus().getReconciliationStatus().isLastReconciledSpecStable());
            assertEquals(
                    ReconciliationState.DEPLOYED,
                    deployment.getStatus().getReconciliationStatus().getState());
            assertNull(deployment.getStatus().getError());

            // Verify suspending a rolled back job
            triggerRollback.run();
            testController.reconcile(deployment, context);
            assertEquals(
                    ReconciliationState.ROLLED_BACK,
                    deployment.getStatus().getReconciliationStatus().getState());
            testController.reconcile(deployment, context);
            testController.reconcile(deployment, context);

            deployment.getSpec().getJob().setState(JobState.SUSPENDED);
            testController.reconcile(deployment, context);
            assertTrue(
                    deployment.getStatus().getReconciliationStatus().isLastReconciledSpecStable());
            assertEquals(
                    ReconciliationState.DEPLOYED,
                    deployment.getStatus().getReconciliationStatus().getState());
            assertNull(deployment.getStatus().getError());
        }
    }
}
