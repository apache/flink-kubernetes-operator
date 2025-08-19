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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.ABORT_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.RECONCILIATION_RESCHEDULING_INTERVAL;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.SAMPLE_JAR;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_DEPLOYMENT_NAME;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_NAMESPACE;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.instantStrToMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link FlinkBlueGreenDeploymentController} tests. */
@EnableKubernetesMockClient(crud = true)
public class FlinkBlueGreenDeploymentControllerTest {

    public static final String SERVICE_ACCOUNT = "flink-operator";
    public static final String FLINK_VERSION = "latest";
    public static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    public static final String IMAGE_POLICY = "IfNotPresent";

    private static final String CUSTOM_CONFIG_FIELD = "custom-configuration-field";
    private static final int DEFAULT_DELETION_DELAY_VALUE = 500;
    private static final int ALT_DELETION_DELAY_VALUE = 1000;
    private static final int MINIMUM_ABORT_GRACE_PERIOD = 1000;
    private static final String TEST_CHECKPOINT_PATH = "/tmp/checkpoints";
    private static final String TEST_INITIAL_SAVEPOINT_PATH = "/tmp/savepoints";
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private TestingFlinkService flinkService;
    private Context<FlinkBlueGreenDeployment> context;
    private TestingFlinkBlueGreenDeploymentController testController;

    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService(kubernetesClient);
        context = flinkService.getContext();
        testController = new TestingFlinkBlueGreenDeploymentController(configManager, flinkService);
    }

    @ParameterizedTest
    @MethodSource("flinkVersionsAndSavepointPaths")
    public void verifyBasicDeployment(FlinkVersion flinkVersion, String initialSavepointPath)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        initialSavepointPath,
                        UpgradeMode.STATELESS);
        executeBasicDeployment(flinkVersion, blueGreenDeployment, true, initialSavepointPath);
    }

    @ParameterizedTest
    @MethodSource("flinkVersionsAndSavepointPathsAndUpgradeModes")
    public void verifyBasicTransition(
            FlinkVersion flinkVersion, String initialSavepointPath, UpgradeMode upgradeMode)
            throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion, null, upgradeMode);
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false, null);

        // Simulate a change in the spec to trigger a Green deployment
        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(
                rs.deployment, customValue, ALT_DELETION_DELAY_VALUE, initialSavepointPath);

        var expectedSavepointPath =
                upgradeMode == UpgradeMode.LAST_STATE ? TEST_CHECKPOINT_PATH : initialSavepointPath;

        if (upgradeMode == UpgradeMode.SAVEPOINT) {
            // In this case there will ALWAYS be a savepoint generated with this value,
            // regardless of the initialSavepointPath
            expectedSavepointPath = "savepoint_1";
            rs = handleSavepoint(rs);
        }

        // Transitioning to the Green deployment

        testTransitionToGreen(rs, customValue, expectedSavepointPath);
    }

    @NotNull
    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult handleSavepoint(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs)
            throws Exception {
        var triggers = flinkService.getSavepointTriggers();
        triggers.clear();

        rs = reconcile(rs.deployment);

        // Simulating a pending savepoint
        triggers.put(rs.deployment.getStatus().getSavepointTriggerId(), false);

        // Should be in SAVEPOINTING_BLUE state first
        assertEquals(
                FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE,
                rs.reconciledStatus.getBlueGreenState());
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(rs.updateControl.getScheduleDelay().isPresent());

        // This next reconciliation should continue waiting on the pending savepoint
        var rs2 = reconcile(rs.deployment);

        assertTrue(rs2.updateControl.isNoUpdate());
        assertTrue(rs2.updateControl.getScheduleDelay().isPresent());

        // Completing the savepoint
        triggers.put(rs.deployment.getStatus().getSavepointTriggerId(), true);

        // This next reconciliation should move on to the next state
        rs = reconcile(rs.deployment);

        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE, rs.reconciledStatus.getBlueGreenState());
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(rs.updateControl.getScheduleDelay().isPresent());
        return rs;
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyFailureBeforeTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        TEST_INITIAL_SAVEPOINT_PATH,
                        UpgradeMode.STATELESS);
        var rs =
                executeBasicDeployment(
                        flinkVersion, blueGreenDeployment, false, TEST_INITIAL_SAVEPOINT_PATH);

        // Simulate a change in the spec to trigger a Blue deployment
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0, null);

        // Simulate a failure in the running deployment
        simulateJobFailure(getFlinkDeployments().get(0));

        // Initiate the Green deployment
        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(
                minReconciliationTs
                        < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));

        // Assert job status/state is left the way it is and that the Blue job never got submitted
        assertEquals(JobStatus.FAILING, rs.reconciledStatus.getJobStatus().getState());
        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        assertEquals(
                JobStatus.RECONCILING,
                flinkDeployments.get(0).getStatus().getJobStatus().getState());
        assertEquals(
                ReconciliationState.UPGRADING,
                flinkDeployments.get(0).getStatus().getReconciliationStatus().getState());

        // No update
        rs = reconcile(rs.deployment);
        assertTrue(rs.updateControl.isNoUpdate());
    }

    @ParameterizedTest
    @MethodSource({"org.apache.flink.kubernetes.operator.TestUtils#flinkVersions"})
    public void verifyFailureDuringTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        null,
                        UpgradeMode.STATELESS);

        // Overriding the maxNumRetries and Reschedule Interval
        var abortGracePeriodMs = 1200;
        var reconciliationReschedulingIntervalMs = 3000;
        Map<String, String> configuration =
                blueGreenDeployment.getSpec().getTemplate().getConfiguration();
        configuration.put(ABORT_GRACE_PERIOD.key(), String.valueOf(abortGracePeriodMs));
        configuration.put(
                RECONCILIATION_RESCHEDULING_INTERVAL.key(),
                String.valueOf(reconciliationReschedulingIntervalMs));

        var rs =
                executeBasicDeployment(
                        flinkVersion, blueGreenDeployment, false, TEST_INITIAL_SAVEPOINT_PATH);

        // Simulate a change in the spec to trigger a Blue deployment
        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue, 0, null);

        // Initiate the Green deployment
        rs = reconcile(rs.deployment);

        // We should be TRANSITIONING_TO_GREEN at this point
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState());
        assertEquals(
                customValue,
                rs.deployment
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getFlinkConfiguration()
                        .get(CUSTOM_CONFIG_FIELD));

        // Simulating the Blue deployment doesn't start correctly (status will remain the same)
        Long reschedDelayMs = 0L;
        for (int i = 0; i < 2; i++) {
            rs = reconcile(rs.deployment);
            assertTrue(rs.updateControl.isPatchStatus());
            assertFalse(rs.updateControl.isPatchResource());
            assertTrue(rs.updateControl.getScheduleDelay().isPresent());
            reschedDelayMs = rs.updateControl.getScheduleDelay().get();
            assertTrue(reschedDelayMs == reconciliationReschedulingIntervalMs && reschedDelayMs > 0);
            assertTrue(
                    instantStrToMillis(rs.reconciledStatus.getAbortTimestamp())
                            > System.currentTimeMillis());
        }

        // Wait until the delay
        Thread.sleep(reschedDelayMs);

        // After the retries are exhausted
        rs = reconcile(rs.deployment);

        assertTrue(rs.updateControl.isPatchStatus());

        // The first job should be RUNNING, the second should be SUSPENDED
        assertEquals(JobStatus.FAILING, rs.reconciledStatus.getJobStatus().getState());
        // No longer TRANSITIONING_TO_GREEN and rolled back to ACTIVE_BLUE
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE, rs.reconciledStatus.getBlueGreenState());
        var flinkDeployments = getFlinkDeployments();
        assertEquals(2, flinkDeployments.size());
        assertEquals(
                JobStatus.RUNNING, flinkDeployments.get(0).getStatus().getJobStatus().getState());
        assertEquals(
                ReconciliationState.DEPLOYED,
                flinkDeployments.get(0).getStatus().getReconciliationStatus().getState());
        // The B/G controller changes the State = SUSPENDED, the actual suspension is done by the
        // FlinkDeploymentController
        assertEquals(JobState.SUSPENDED, flinkDeployments.get(1).getSpec().getJob().getState());
        assertEquals(
                ReconciliationState.UPGRADING,
                flinkDeployments.get(1).getStatus().getReconciliationStatus().getState());
        assertTrue(instantStrToMillis(rs.reconciledStatus.getAbortTimestamp()) > 0);

        // Simulate another change in the spec to trigger a redeployment
        customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue, ALT_DELETION_DELAY_VALUE, null);

        // Initiate the redeployment
        testTransitionToGreen(rs, customValue, null);
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySpecChangeDuringTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        TEST_INITIAL_SAVEPOINT_PATH,
                        UpgradeMode.STATELESS);

        // Initiate the Blue deployment
        var originalSpec = blueGreenDeployment.getSpec();
        var rs = initialPhaseBasicDeployment(blueGreenDeployment, false);

        // Job starting...
        simulateSubmitAndSuccessfulJobStart(getFlinkDeployments().get(0));

        // Simulate a spec change before the transition is complete
        simulateChangeInSpec(rs.deployment, "MODIFIED_VALUE", 0, null);
        rs = reconcile(rs.deployment);

        // The spec should have been reverted
        assertEquals(
                SpecUtils.writeSpecAsJSON(originalSpec, "spec"),
                SpecUtils.writeSpecAsJSON(rs.deployment.getSpec(), "spec"));
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyFailureBeforeFirstDeployment(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        TEST_INITIAL_SAVEPOINT_PATH,
                        UpgradeMode.STATELESS);

        // Initiate the Blue deployment
        var rs = initialPhaseBasicDeployment(blueGreenDeployment, false);

        // Simulating the job did not start correctly before the AbortGracePeriodMs
        Thread.sleep(MINIMUM_ABORT_GRACE_PERIOD);

        rs = reconcile(rs.deployment);

        assertEquals(JobStatus.FAILING, rs.reconciledStatus.getJobStatus().getState());
        // No longer TRANSITIONING_TO_GREEN and rolled back to INITIALIZING_BLUE
        assertEquals(
                FlinkBlueGreenDeploymentState.INITIALIZING_BLUE,
                rs.reconciledStatus.getBlueGreenState());
        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        // The B/G controller changes the State = SUSPENDED, the actual suspension is done by the
        // FlinkDeploymentController
        assertEquals(JobState.SUSPENDED, flinkDeployments.get(0).getSpec().getJob().getState());

        // No-op if the spec remains the same
        var rs2 = reconcile(rs.deployment);
        assertTrue(rs2.updateControl.isNoUpdate());

        simulateChangeInSpec(rs.deployment, "MODIFIED_VALUE", 0, null);

        // Resubmitting should re-start the Initialization to Blue
        rs = reconcile(rs.deployment);

        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(
                rs.updateControl.getScheduleDelay().isPresent()
                        && rs.updateControl.getScheduleDelay().get() > 0);
        flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                rs.reconciledStatus.getBlueGreenState());
    }

    @ParameterizedTest
    @MethodSource("patchScenarioProvider")
    public void verifyPatchScenario(FlinkVersion flinkVersion, PatchTestCase testCase)
            throws Exception {
        var rs = setupActiveBlueDeployment(flinkVersion);

        testCase.applyChanges(rs.deployment, kubernetesClient);

        var result = reconcileAndVerifyPatchBehavior(rs);

        testCase.verifySpecificBehavior(result, getFlinkDeployments());

        assertFinalized(
                result.minReconciliationTs, result.rs, FlinkBlueGreenDeploymentState.ACTIVE_BLUE);
    }

    // ==================== Parameterized Test Inputs ====================

    static Stream<Arguments> patchScenarioProvider() {
        // Extract FlinkVersions from TestUtils and combine with PatchTypes
        return TestUtils.flinkVersions()
                .flatMap(
                        args -> {
                            FlinkVersion version = (FlinkVersion) args.get()[0];
                            return Stream.of(
                                    Arguments.of(version, new PatchChildTestCase()),
                                    Arguments.of(version, new PatchTopLevelTestCase()),
                                    Arguments.of(version, new PatchBothTestCase()));
                        });
    }

    static Stream<Arguments> flinkVersionsAndSavepointPaths() {
        return TestUtils.flinkVersions()
                .flatMap(
                        args -> {
                            FlinkVersion version = (FlinkVersion) args.get()[0];
                            return Stream.of(
                                    Arguments.of(version, null),
                                    Arguments.of(version, TEST_INITIAL_SAVEPOINT_PATH));
                        });
    }

    static Stream<Arguments> flinkVersionsAndSavepointPathsAndUpgradeModes() {
        return TestUtils.flinkVersions()
                .flatMap(
                        args -> {
                            FlinkVersion version = (FlinkVersion) args.get()[0];
                            return Stream.of(
                                    Arguments.of(version, null, UpgradeMode.SAVEPOINT),
                                    Arguments.of(version, null, UpgradeMode.LAST_STATE),
                                    Arguments.of(version, null, UpgradeMode.STATELESS),
                                    Arguments.of(
                                            version,
                                            TEST_INITIAL_SAVEPOINT_PATH,
                                            UpgradeMode.SAVEPOINT),
                                    Arguments.of(
                                            version,
                                            TEST_INITIAL_SAVEPOINT_PATH,
                                            UpgradeMode.LAST_STATE),
                                    Arguments.of(
                                            version,
                                            TEST_INITIAL_SAVEPOINT_PATH,
                                            UpgradeMode.STATELESS));
                        });
    }

    // ==================== Test Case Interfaces and Implementations ====================

    interface PatchTestCase {
        void applyChanges(FlinkBlueGreenDeployment deployment, KubernetesClient client);

        void verifySpecificBehavior(ReconcileResult result, List<FlinkDeployment> deployments);
    }

    static class PatchChildTestCase implements PatchTestCase {
        @Override
        public void applyChanges(FlinkBlueGreenDeployment deployment, KubernetesClient client) {
            FlinkDeploymentSpec spec = deployment.getSpec().getTemplate().getSpec();
            spec.getJob().setSavepointRedeployNonce(12345L);
            deployment.getSpec().getTemplate().setSpec(spec);
            client.resource(deployment).createOrReplace();
        }

        @Override
        public void verifySpecificBehavior(
                ReconcileResult result, List<FlinkDeployment> deployments) {
            assertEquals(1, deployments.size());
            assertEquals(
                    12345L,
                    (long) deployments.get(0).getSpec().getJob().getSavepointRedeployNonce());
        }
    }

    static class PatchTopLevelTestCase implements PatchTestCase {
        @Override
        public void applyChanges(FlinkBlueGreenDeployment deployment, KubernetesClient client) {
            FlinkDeploymentTemplateSpec template = deployment.getSpec().getTemplate();
            Map<String, String> configuration = new HashMap<>(template.getConfiguration());
            configuration.put("custom.top.level", "custom-top-level-value");
            template.setConfiguration(configuration);
            deployment.getSpec().setTemplate(template);
            client.resource(deployment).createOrReplace();
        }

        @Override
        public void verifySpecificBehavior(
                ReconcileResult result, List<FlinkDeployment> deployments) {
            assertEquals(1, deployments.size());
            var existingDeployment = result.existingFlinkDeployment;
            var currentDeployment = deployments.get(0);

            // FlinkDeployment should remain unchanged for top-level only changes
            assertEquals(existingDeployment, currentDeployment);
        }
    }

    static class PatchBothTestCase implements PatchTestCase {
        @Override
        public void applyChanges(FlinkBlueGreenDeployment deployment, KubernetesClient client) {
            FlinkDeploymentTemplateSpec template = deployment.getSpec().getTemplate();

            // 1. Add top-level configuration change
            Map<String, String> configuration = new HashMap<>(template.getConfiguration());
            configuration.put("custom.both.level", "custom-both-level-value");
            template.setConfiguration(configuration);

            // 2. Add nested spec change
            FlinkDeploymentSpec spec = template.getSpec();
            spec.getJob().setSavepointRedeployNonce(67890L);
            template.setSpec(spec);

            deployment.getSpec().setTemplate(template);
            client.resource(deployment).createOrReplace();
        }

        @Override
        public void verifySpecificBehavior(
                ReconcileResult result, List<FlinkDeployment> deployments) {
            assertEquals(1, deployments.size());
            var updatedDeployment = deployments.get(0);

            // Child spec change should be applied to FlinkDeployment
            assertEquals(
                    67890L,
                    (long) updatedDeployment.getSpec().getJob().getSavepointRedeployNonce());

            // Top-level changes should be preserved in reconciled spec
            assertNotNull(result.rs.reconciledStatus.getLastReconciledSpec());
            assertEquals(
                    SpecUtils.writeSpecAsJSON(result.rs.deployment.getSpec(), "spec"),
                    result.rs.reconciledStatus.getLastReconciledSpec());
        }
    }

    // ==================== Helper Classes ====================

    static class ReconcileResult {
        final long minReconciliationTs;
        final TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs;
        final FlinkDeployment existingFlinkDeployment;

        ReconcileResult(
                long minReconciliationTs,
                TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
                FlinkDeployment existingFlinkDeployment) {
            this.minReconciliationTs = minReconciliationTs;
            this.rs = rs;
            this.existingFlinkDeployment = existingFlinkDeployment;
        }
    }

    // ==================== Common Test Helper Methods ====================

    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
            setupActiveBlueDeployment(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(
                        TEST_DEPLOYMENT_NAME,
                        TEST_NAMESPACE,
                        flinkVersion,
                        TEST_INITIAL_SAVEPOINT_PATH,
                        UpgradeMode.STATELESS);
        return executeBasicDeployment(
                flinkVersion, blueGreenDeployment, false, TEST_INITIAL_SAVEPOINT_PATH);
    }

    private ReconcileResult reconcileAndVerifyPatchBehavior(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs)
            throws Exception {

        var flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        var existingFlinkDeployment = flinkDeployments.get(0);

        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertPatchOperationTriggered(rs, minReconciliationTs);
        assertTransitioningState(rs);

        minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        return new ReconcileResult(minReconciliationTs, rs, existingFlinkDeployment);
    }

    private void assertPatchOperationTriggered(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            long minReconciliationTs) {
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(rs.updateControl.getScheduleDelay().isPresent());
        assertTrue(rs.updateControl.getScheduleDelay().get() > 0);
        assertTrue(
                minReconciliationTs
                        < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));
    }

    private void assertTransitioningState(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs) {
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                rs.reconciledStatus.getBlueGreenState());
        assertEquals(JobStatus.RECONCILING, rs.reconciledStatus.getJobStatus().getState());
    }

    private void assertFinalized(
            long minReconciliationTs,
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            FlinkBlueGreenDeploymentState expectedBGDeploymentState)
            throws Exception {
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(
                minReconciliationTs
                        < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));
        assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(
                SpecUtils.writeSpecAsJSON(rs.deployment.getSpec(), "spec"),
                rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(expectedBGDeploymentState, rs.reconciledStatus.getBlueGreenState());
        assertEquals(JobStatus.RUNNING, rs.reconciledStatus.getJobStatus().getState());
        assertEquals(0, instantStrToMillis(rs.reconciledStatus.getDeploymentReadyTimestamp()));
        assertEquals(0, instantStrToMillis(rs.reconciledStatus.getAbortTimestamp()));

        // Subsequent reconciliation calls after finalization = NO-OP
        rs = reconcile(rs.deployment);
        assertTrue(rs.updateControl.isNoUpdate());
    }

    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
            executeBasicDeployment(
                    FlinkVersion flinkVersion,
                    FlinkBlueGreenDeployment blueGreenDeployment,
                    boolean execAssertions,
                    String expectedInitialSavepointPath)
                    throws Exception {

        // 1. Initiate the Blue deployment
        var rs = initialPhaseBasicDeployment(blueGreenDeployment, execAssertions);

        var flinkDeployments = getFlinkDeployments();
        var deploymentA = flinkDeployments.get(0);

        if (execAssertions) {
            assertEquals(1, flinkDeployments.size());
            verifyOwnerReferences(rs.deployment, deploymentA);
            assertEquals(
                    expectedInitialSavepointPath,
                    deploymentA.getSpec().getJob().getInitialSavepointPath());
        }

        simulateSubmitAndSuccessfulJobStart(deploymentA);

        // 2. Mark the Blue deployment ready and finalize it
        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        if (execAssertions) {
            assertFinalized(minReconciliationTs, rs, FlinkBlueGreenDeploymentState.ACTIVE_BLUE);
        }

        return rs;
    }

    @NotNull
    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
            initialPhaseBasicDeployment(
                    FlinkBlueGreenDeployment blueGreenDeployment, boolean execAssertions)
                    throws Exception {
        Long minReconciliationTs = System.currentTimeMillis() - 1;

        // 1a. Initializing deploymentStatus with this call
        var rs = reconcile(blueGreenDeployment);

        if (execAssertions) {
            assertTrue(rs.updateControl.getScheduleDelay().isPresent());
            assertTrue(rs.updateControl.getScheduleDelay().get() > 0);
            assertEquals(
                    FlinkBlueGreenDeploymentState.INITIALIZING_BLUE,
                    rs.reconciledStatus.getBlueGreenState());
        }

        // 1b. Executing the actual deployment
        rs = reconcile(rs.deployment);

        if (execAssertions) {
            assertTrue(rs.updateControl.isPatchStatus());
            assertTrue(
                    minReconciliationTs
                            < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));

            // check the status (reconciled spec, reconciled ts, a/b state)
            assertEquals(
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                    rs.reconciledStatus.getBlueGreenState());
            assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
            assertEquals(JobStatus.RECONCILING, rs.reconciledStatus.getJobStatus().getState());
            assertNull(rs.reconciledStatus.getDeploymentReadyTimestamp());
        }

        return rs;
    }

    private void assertDeploymentDeleted(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            long expectedDeletionDelay,
            FlinkBlueGreenDeploymentSpec bgSpecBefore)
            throws Exception {
        var deletionDelay = rs.updateControl.getScheduleDelay().get();

        assertTrue(rs.updateControl.isPatchStatus());
        assertEquals(expectedDeletionDelay, deletionDelay);
        assertTrue(instantStrToMillis(rs.reconciledStatus.getDeploymentReadyTimestamp()) > 0);
        assertEquals(
                SpecUtils.writeSpecAsJSON(bgSpecBefore, "spec"),
                rs.reconciledStatus.getLastReconciledSpec());

        // A reconciliation before the deletion delay has expired should result in no-op
        var rs2 = reconcile(rs.deployment);
        var remainingDeletionDelay = rs2.updateControl.getScheduleDelay().get();
        assertTrue(remainingDeletionDelay <= expectedDeletionDelay);
        assertTrue(rs2.updateControl.isNoUpdate());

        Thread.sleep(remainingDeletionDelay);
    }

    private void testTransitionToGreen(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            String customValue,
            String expectedSavepointPath)
            throws Exception {

        // Initiate the Green deployment
        Long minReconciliationTs = System.currentTimeMillis() - 1;
        var bgSpecBefore = rs.deployment.getSpec();
        rs = reconcile(rs.deployment);

        var flinkDeployments = getFlinkDeployments();
        var greenDeploymentName = flinkDeployments.get(1).getMetadata().getName();

        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(
                minReconciliationTs
                        < instantStrToMillis(rs.reconciledStatus.getLastReconciledTimestamp()));
        assertEquals(2, flinkDeployments.size());
        assertNull(flinkDeployments.get(0).getSpec().getJob().getInitialSavepointPath());
        assertEquals(
                expectedSavepointPath,
                flinkDeployments.get(1).getSpec().getJob().getInitialSavepointPath());

        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState());
        assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(0, instantStrToMillis(rs.reconciledStatus.getDeploymentReadyTimestamp()));
        assertEquals(
                customValue,
                rs.deployment
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getFlinkConfiguration()
                        .get(CUSTOM_CONFIG_FIELD));

        // Initiate and mark the Green deployment ready
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs = reconcile(rs.deployment);

        // Logic for the deployment to get deleted
        assertDeploymentDeleted(rs, ALT_DELETION_DELAY_VALUE, bgSpecBefore);

        // Calling the rescheduled reconciliation (will delete the deployment)
        reconcile(rs.deployment);

        // Old Blue deployment deleted, Green is the active one
        flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        assertEquals(greenDeploymentName, flinkDeployments.get(0).getMetadata().getName());

        minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertTrue(rs.updateControl.isPatchStatus());
        assertFinalized(minReconciliationTs, rs, FlinkBlueGreenDeploymentState.ACTIVE_GREEN);
    }

    private void simulateChangeInSpec(
            FlinkBlueGreenDeployment blueGreenDeployment,
            String customFieldValue,
            int customDeletionDelayMs,
            String initialSavepointPath) {
        FlinkDeploymentTemplateSpec template = blueGreenDeployment.getSpec().getTemplate();

        if (customDeletionDelayMs > 0) {
            template.getConfiguration()
                    .put(DEPLOYMENT_DELETION_DELAY.key(), String.valueOf(customDeletionDelayMs));
        }

        FlinkDeploymentSpec spec = template.getSpec();
        spec.getFlinkConfiguration().put(CUSTOM_CONFIG_FIELD, customFieldValue);

        if (initialSavepointPath != null) {
            spec.getJob().setInitialSavepointPath(initialSavepointPath);
        }

        template.setSpec(spec);
        kubernetesClient.resource(blueGreenDeployment).createOrReplace();
    }

    /*
    Convenience function to reconcile and get the frequently used `BlueGreenReconciliationResult`
     */
    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult reconcile(
            FlinkBlueGreenDeployment blueGreenDeployment) throws Exception {
        UpdateControl<FlinkBlueGreenDeployment> updateControl =
                testController.reconcile(blueGreenDeployment, context);

        return new TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult(
                updateControl,
                updateControl.getResource().orElse(null),
                updateControl.isNoUpdate() ? null : updateControl.getResource().get().getStatus());
    }

    private void simulateSubmitAndSuccessfulJobStart(FlinkDeployment deployment) throws Exception {
        // TODO: is this correct? Doing this to give the TestingFlinkService awareness of the job
        JobSpec jobSpec = deployment.getSpec().getJob();
        Configuration conf = new Configuration();
        conf.set(SavepointConfigOptions.SAVEPOINT_PATH, TEST_CHECKPOINT_PATH);
        flinkService.submitApplicationCluster(jobSpec, conf, false);
        var jobId = flinkService.listJobs().get(0).f1.getJobId().toString();
        deployment.getStatus().getJobStatus().setJobId(jobId);
        simulateSuccessfulJobStart(deployment);
    }

    private void simulateSuccessfulJobStart(FlinkDeployment deployment) {
        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        deployment.getStatus().getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        deployment.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
        kubernetesClient.resource(deployment).update();
    }

    private void simulateJobFailure(FlinkDeployment deployment) {
        deployment.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
        deployment.getStatus().getReconciliationStatus().setState(ReconciliationState.UPGRADING);
        kubernetesClient.resource(deployment).update();
    }

    private static void verifyOwnerReferences(
            FlinkBlueGreenDeployment parent, FlinkDeployment child) {
        var ownerReferences = child.getMetadata().getOwnerReferences();
        assertEquals(1, ownerReferences.size());
        var ownerRef = ownerReferences.get(0);
        assertEquals(parent.getMetadata().getName(), ownerRef.getName());
        assertEquals(parent.getKind(), ownerRef.getKind());
        assertEquals(parent.getApiVersion(), ownerRef.getApiVersion());
    }

    private List<FlinkDeployment> getFlinkDeployments() {
        return kubernetesClient
                .resources(FlinkDeployment.class)
                .inNamespace(TEST_NAMESPACE)
                .list()
                .getItems();
    }

    private static FlinkBlueGreenDeployment buildSessionCluster(
            String name,
            String namespace,
            FlinkVersion version,
            String initialSavepointPath,
            UpgradeMode upgradeMode) {
        var deployment = new FlinkBlueGreenDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withCreationTimestamp(Instant.now().toString())
                        .withUid(UUID.randomUUID().toString())
                        .withResourceVersion("1")
                        .build());
        var bgDeploymentSpec = getTestFlinkDeploymentSpec(version);

        bgDeploymentSpec
                .getTemplate()
                .getSpec()
                .setJob(
                        JobSpec.builder()
                                .jarURI(SAMPLE_JAR)
                                .parallelism(1)
                                .upgradeMode(upgradeMode)
                                .state(JobState.RUNNING)
                                .initialSavepointPath(initialSavepointPath)
                                .build());

        deployment.setSpec(bgDeploymentSpec);
        return deployment;
    }

    private static FlinkBlueGreenDeploymentSpec getTestFlinkDeploymentSpec(FlinkVersion version) {
        Map<String, String> conf = new HashMap<>();
        conf.put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "2");
        conf.put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), "test-savepoint-dir");
        conf.put(CheckpointingOptions.INCREMENTAL_CHECKPOINTS.key(), "true");
        conf.put(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(), "10");
        conf.put(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
                "file:///test/test-checkpoint-dir");

        var flinkDeploymentSpec =
                FlinkDeploymentSpec.builder()
                        .image(IMAGE)
                        .imagePullPolicy(IMAGE_POLICY)
                        .serviceAccount(SERVICE_ACCOUNT)
                        .flinkVersion(version)
                        .flinkConfiguration(conf)
                        .jobManager(new JobManagerSpec(new Resource(1.0, "2048m", "2G"), 1, null))
                        .taskManager(
                                new TaskManagerSpec(new Resource(1.0, "2048m", "2G"), null, null))
                        .build();

        Map<String, String> configuration = new HashMap<>();
        configuration.put(ABORT_GRACE_PERIOD.key(), "1");
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "500");
        configuration.put(
                DEPLOYMENT_DELETION_DELAY.key(), String.valueOf(DEFAULT_DELETION_DELAY_VALUE));

        var flinkDeploymentTemplateSpec =
                FlinkDeploymentTemplateSpec.builder()
                        .configuration(configuration)
                        .spec(flinkDeploymentSpec)
                        .build();

        return new FlinkBlueGreenDeploymentSpec(flinkDeploymentTemplateSpec);
    }
}
