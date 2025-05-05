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
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionMode;
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

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.naming.OperationNotSupportedException;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.SAMPLE_JAR;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_DEPLOYMENT_NAME;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_NAMESPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private TestingFlinkService flinkService;
    private Context<FlinkBlueGreenDeployment> context;
    private TestingFlinkBlueGreenDeploymentController testController;

    private KubernetesMockServer mockServer;
    private KubernetesClient kubernetesClient;

    Event mockedEvent =
            new EventBuilder()
                    .withNewMetadata()
                    .withName("name")
                    .endMetadata()
                    .withType("type")
                    .withReason("reason")
                    .build();

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService(kubernetesClient);
        context = flinkService.getContext();
        testController = new TestingFlinkBlueGreenDeploymentController(configManager, flinkService);
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyBasicDeployment(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);
        executeBasicDeployment(flinkVersion, blueGreenDeployment, true);
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyBasicTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false);

        // Simulate a change in the spec to trigger a Green deployment
        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue, ALT_DELETION_DELAY_VALUE);

        // Transitioning to the Green deployment
        var bgUpdatedSpec = rs.deployment.getSpec();
        testTransitionToGreen(rs, customValue, bgUpdatedSpec);
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyFailureBeforeTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);
        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false);

        // Simulate a change in the spec to trigger a Blue deployment
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString(), 0);

        // Simulate a failure in the running deployment
        simulateJobFailure(getFlinkDeployments().get(0));

        // Initiate the Green deployment
        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(minReconciliationTs < rs.reconciledStatus.getLastReconciledTimestamp());

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
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);

        // Overriding the maxNumRetries and Reschedule Interval
        var abortGracePeriodMs = 1200;
        var reconciliationReschedulingIntervalMs = 5000;
        blueGreenDeployment.getSpec().getTemplate().setAbortGracePeriodMs(abortGracePeriodMs);
        blueGreenDeployment
                .getSpec()
                .getTemplate()
                .setReconciliationReschedulingIntervalMs(reconciliationReschedulingIntervalMs);

        var rs = executeBasicDeployment(flinkVersion, blueGreenDeployment, false);

        // Simulate a change in the spec to trigger a Blue deployment
        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue, 0);

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
            assertFalse(rs.updateControl.isUpdateResource());
            assertTrue(rs.updateControl.getScheduleDelay().isPresent());
            reschedDelayMs = rs.updateControl.getScheduleDelay().get();
            assertTrue(reschedDelayMs < abortGracePeriodMs && reschedDelayMs > 0);
            assertTrue(rs.reconciledStatus.getAbortTimestamp() > System.currentTimeMillis());
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
        assertTrue(rs.reconciledStatus.getAbortTimestamp() > 0);

        // Simulate another change in the spec to trigger a redeployment
        customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue, ALT_DELETION_DELAY_VALUE);

        // Initiate the redeployment
        var bgUpdatedSpec = rs.deployment.getSpec();
        testTransitionToGreen(rs, customValue, bgUpdatedSpec);
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifySpecChangeDuringTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);

        // Initiate the Blue deployment
        var originalSpec = blueGreenDeployment.getSpec();
        var rs = initialPhaseBasicDeployment(blueGreenDeployment, false);

        // Job starting...
        simulateSubmitAndSuccessfulJobStart(getFlinkDeployments().get(0));

        // Simulate a spec change before the transition is complete
        simulateChangeInSpec(rs.deployment, "MODIFIED_VALUE", 0);
        rs = reconcile(rs.deployment);

        // The spec should have been reverted
        assertEquals(
                SpecUtils.serializeObject(originalSpec, "spec"),
                SpecUtils.serializeObject(rs.deployment.getSpec(), "spec"));
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyFailureBeforeFirstDeployment(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);

        // Initiate the Blue deployment
        var rs = initialPhaseBasicDeployment(blueGreenDeployment, false);

        // Simulating the job did not start correctly before the AbortGracePeriodMs
        Thread.sleep(FlinkBlueGreenDeploymentController.minimumAbortGracePeriodMs);

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

        simulateChangeInSpec(rs.deployment, "MODIFIED_VALUE", 0);

        // Resubmitting should re-start the Initialization to Blue
        rs = reconcile(rs.deployment);

        assertTrue(rs.updateControl.isUpdateStatus());
        assertTrue(
                rs.updateControl.getScheduleDelay().isPresent()
                        && rs.updateControl.getScheduleDelay().get() > 0);
        flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                rs.reconciledStatus.getBlueGreenState());
    }

    // TODO: this test is only for FLIP-503, remove later.
    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyOnlyBasicTransitionIsAllowed(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);

        blueGreenDeployment.getSpec().getTemplate().setTransitionMode(null);
        assertThrows(OperationNotSupportedException.class, () -> reconcile(blueGreenDeployment));

        blueGreenDeployment.getSpec().getTemplate().setTransitionMode(TransitionMode.ADVANCED);
        assertThrows(OperationNotSupportedException.class, () -> reconcile(blueGreenDeployment));
    }

    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
            executeBasicDeployment(
                    FlinkVersion flinkVersion,
                    FlinkBlueGreenDeployment blueGreenDeployment,
                    boolean execAssertions)
                    throws Exception {

        // 1. Initiate the Blue deployment
        var bgSpecBefore = blueGreenDeployment.getSpec();

        var rs = initialPhaseBasicDeployment(blueGreenDeployment, execAssertions);

        var flinkDeployments = getFlinkDeployments();
        var deploymentA = flinkDeployments.get(0);

        if (execAssertions) {
            assertEquals(1, flinkDeployments.size());
            verifyOwnerReferences(rs.deployment, deploymentA);
        }

        simulateSubmitAndSuccessfulJobStart(deploymentA);

        // 2. Mark the Blue deployment ready
        rs = reconcile(rs.deployment);

        // 3. Logic for the deployment to get deleted
        assertDeploymentDeleted(rs, DEFAULT_DELETION_DELAY_VALUE, bgSpecBefore);

        // 4. Finalize the Blue deployment
        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        if (execAssertions) {
            assertEquals(JobStatus.RUNNING, rs.reconciledStatus.getJobStatus().getState());
            assertTrue(minReconciliationTs < rs.reconciledStatus.getLastReconciledTimestamp());
            assertEquals(0, rs.reconciledStatus.getDeploymentReadyTimestamp());
            assertEquals(
                    FlinkBlueGreenDeploymentState.ACTIVE_BLUE,
                    rs.reconciledStatus.getBlueGreenState());

            // 5. Subsequent reconciliation calls = NO-OP
            var rs2 = reconcile(rs.deployment);
            assertTrue(rs2.updateControl.isNoUpdate());
        }

        return rs;
    }

    @NotNull
    private TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult
            initialPhaseBasicDeployment(
                    FlinkBlueGreenDeployment blueGreenDeployment, boolean execAssertions)
                    throws Exception {
        if (blueGreenDeployment.getSpec().getTemplate().getTransitionMode()
                != TransitionMode.BASIC) {
            throw new OperationNotSupportedException(
                    "Only TransitionMode == BASIC is currently supported");
        }

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
            assertTrue(minReconciliationTs < rs.reconciledStatus.getLastReconciledTimestamp());

            // check the status (reconciled spec, reconciled ts, a/b state)
            assertEquals(
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                    rs.reconciledStatus.getBlueGreenState());
            assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
            assertEquals(JobStatus.RECONCILING, rs.reconciledStatus.getJobStatus().getState());
            assertEquals(0, rs.reconciledStatus.getDeploymentReadyTimestamp());
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
        assertTrue(rs.reconciledStatus.getDeploymentReadyTimestamp() > 0);
        assertEquals(
                SpecUtils.serializeObject(bgSpecBefore, "spec"),
                rs.reconciledStatus.getLastReconciledSpec());

        // A reconciliation before the deletion delay has expired should result in no-op
        var rs2 = reconcile(rs.deployment);
        var remainingDeletionDelay = rs2.updateControl.getScheduleDelay().get();
        assertTrue(remainingDeletionDelay < expectedDeletionDelay);
        assertTrue(rs2.updateControl.isNoUpdate());

        Thread.sleep(remainingDeletionDelay);
    }

    private void testTransitionToGreen(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            String customValue,
            FlinkBlueGreenDeploymentSpec bgUpdatedSpec)
            throws Exception {

        // Initiate the Green deployment
        Long minReconciliationTs = System.currentTimeMillis() - 1;
        var bgSpecBefore = rs.deployment.getSpec();
        rs = reconcile(rs.deployment);

        var flinkDeployments = getFlinkDeployments();
        var greenDeploymentName = flinkDeployments.get(1).getMetadata().getName();

        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(minReconciliationTs < rs.reconciledStatus.getLastReconciledTimestamp());
        assertEquals(2, flinkDeployments.size());
        assertNull(flinkDeployments.get(0).getSpec().getJob().getInitialSavepointPath());
        assertNotNull(flinkDeployments.get(1).getSpec().getJob().getInitialSavepointPath());

        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN,
                rs.reconciledStatus.getBlueGreenState());
        assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(0, rs.reconciledStatus.getDeploymentReadyTimestamp());
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
        assertTrue(minReconciliationTs < rs.reconciledStatus.getLastReconciledTimestamp());
        assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(
                SpecUtils.serializeObject(bgUpdatedSpec, "spec"),
                rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState());
        assertEquals(JobStatus.RUNNING, rs.reconciledStatus.getJobStatus().getState());
        assertEquals(0, rs.reconciledStatus.getDeploymentReadyTimestamp());
        assertEquals(0, rs.reconciledStatus.getAbortTimestamp());
    }

    private void simulateChangeInSpec(
            FlinkBlueGreenDeployment blueGreenDeployment,
            String customFieldValue,
            int customDeletionDelayMs) {
        FlinkDeploymentTemplateSpec template = blueGreenDeployment.getSpec().getTemplate();

        if (customDeletionDelayMs > 0) {
            template.setDeploymentDeletionDelayMs(customDeletionDelayMs);
        }

        FlinkDeploymentSpec spec = template.getSpec();
        spec.getFlinkConfiguration().put(CUSTOM_CONFIG_FIELD, customFieldValue);

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
                updateControl.getResource(),
                updateControl.isNoUpdate() ? null : updateControl.getResource().getStatus());
    }

    private void simulateSubmitAndSuccessfulJobStart(FlinkDeployment deployment) throws Exception {
        // TODO: is this correct? Doing this to give the TestingFlinkService awareness of the job
        JobSpec jobSpec = deployment.getSpec().getJob();
        Configuration conf = new Configuration();
        conf.set(SavepointConfigOptions.SAVEPOINT_PATH, "/tmp/savepoint");
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
            String name, String namespace, FlinkVersion version) {
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
                                .upgradeMode(UpgradeMode.STATELESS)
                                .state(JobState.RUNNING)
                                .build());

        bgDeploymentSpec.getTemplate().setTransitionMode(TransitionMode.BASIC);
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

        var flinkDeploymentTemplateSpec =
                FlinkDeploymentTemplateSpec.builder()
                        .deploymentDeletionDelayMs(DEFAULT_DELETION_DELAY_VALUE)
                        .abortGracePeriodMs(1)
                        .reconciliationReschedulingIntervalMs(500)
                        .spec(flinkDeploymentSpec)
                        .build();

        return new FlinkBlueGreenDeploymentSpec(flinkDeploymentTemplateSpec);
    }
}
