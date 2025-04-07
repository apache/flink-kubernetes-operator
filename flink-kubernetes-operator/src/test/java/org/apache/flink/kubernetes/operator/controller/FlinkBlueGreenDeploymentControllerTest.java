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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link FlinkBlueGreenDeploymentController} tests. */
@EnableKubernetesMockClient(crud = true)
public class FlinkBlueGreenDeploymentControllerTest {

    public static final String SERVICE_ACCOUNT = "flink-operator";
    public static final String FLINK_VERSION = "latest";
    public static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    public static final String IMAGE_POLICY = "IfNotPresent";

    private static final String CUSTOM_CONFIG_FIELD = "custom-configuration-field";
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

        // 1. Initiate the Green deployment
        var bgSpecBefore = blueGreenDeployment.getSpec();
        Long minReconciliationTs = System.currentTimeMillis() - 1;
        var rs = reconcile(blueGreenDeployment);

        assertSpec(rs, minReconciliationTs);

        // check the status (reconciled spec, reconciled ts, a/b state)
        assertEquals(
                FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                rs.reconciledStatus.getBlueGreenState());
        assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
        assertNull(rs.reconciledStatus.getJobStatus().getState());

        var flinkDeploymentList = getFlinkDeployments();
        assertEquals(1, flinkDeploymentList.size());
        var deploymentA = flinkDeploymentList.get(0);

        verifyOwnerReferences(rs.deployment, deploymentA);

        simulateSubmitAndSuccessfulJobStart(deploymentA);

        // 2. Finalize the Green deployment
        minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertSpec(rs, minReconciliationTs);

        assertEquals(
                SpecUtils.serializeObject(bgSpecBefore, "spec"),
                rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_BLUE, rs.reconciledStatus.getBlueGreenState());
        assertEquals(JobStatus.RUNNING, rs.reconciledStatus.getJobStatus().getState());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyBasicTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);

        // 1. Initiate the Blue deployment
        var rs = reconcile(blueGreenDeployment);

        // 2. Finalize the Blue deployment
        simulateSubmitAndSuccessfulJobStart(getFlinkDeployments().get(0));
        rs = reconcile(rs.deployment);

        // Verify noUpdate if reconciliation is triggered without a spec change
        var rs2 = reconcile(rs.deployment);
        assertTrue(rs2.updateControl.isNoUpdate());

        // 3. Simulate a change in the spec to trigger a Green deployment
        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue);

        // 4. Initiate the Green deployment
        var bgUpdatedSpec = rs.deployment.getSpec();
        Long minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        testTransitionToGreen(rs, minReconciliationTs, customValue, bgUpdatedSpec);
    }

    private void testTransitionToGreen(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            Long minReconciliationTs,
            String customValue,
            FlinkBlueGreenDeploymentSpec bgUpdatedSpec)
            throws Exception {
        TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs2;
        var flinkDeployments = getFlinkDeployments();
        var greenDeploymentName = flinkDeployments.get(1).getMetadata().getName();

        assertSpec(rs, minReconciliationTs);

        assertEquals(2, flinkDeployments.size());
        assertNull(flinkDeployments.get(0).getSpec().getJob().getInitialSavepointPath());
        assertNotNull(flinkDeployments.get(1).getSpec().getJob().getInitialSavepointPath());

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
        assertNotNull(rs.reconciledStatus.getLastReconciledSpec());

        // New Blue deployment successfully started
        simulateSuccessfulJobStart(getFlinkDeployments().get(1));
        rs2 = reconcile(rs.deployment);
        assertTrue(rs2.updateControl.isNoUpdate());

        // Old Blue deployment deleted, Green is the active one
        flinkDeployments = getFlinkDeployments();
        assertEquals(1, flinkDeployments.size());
        assertEquals(greenDeploymentName, flinkDeployments.get(0).getMetadata().getName());

        minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        // Spec should still be the new one
        assertSpec(rs, minReconciliationTs);

        assertNotNull(rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(
                SpecUtils.serializeObject(bgUpdatedSpec, "spec"),
                rs.reconciledStatus.getLastReconciledSpec());
        assertEquals(
                FlinkBlueGreenDeploymentState.ACTIVE_GREEN,
                rs.reconciledStatus.getBlueGreenState());
        assertEquals(JobStatus.RUNNING, rs.reconciledStatus.getJobStatus().getState());
        assertTrue(rs.updateControl.isPatchStatus());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyFailureBeforeTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);
        var originalSpec = blueGreenDeployment.getSpec();

        // 1. Initiate the Green deployment
        var rs = reconcile(blueGreenDeployment);

        // 2. Finalize the Green deployment
        simulateSubmitAndSuccessfulJobStart(getFlinkDeployments().get(0));
        rs = reconcile(rs.deployment);

        // 3. Simulate a change in the spec to trigger a Blue deployment
        simulateChangeInSpec(rs.deployment, UUID.randomUUID().toString());

        // TODO: simulate a failure in the running deployment
        simulateJobFailure(getFlinkDeployments().get(0));

        // 4. Initiate the Blue deployment
        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertSpec(rs, minReconciliationTs);

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

        // 5. No update
        rs = reconcile(rs.deployment);
        assertTrue(rs.updateControl.isNoUpdate());
    }

    @ParameterizedTest
    @MethodSource({"org.apache.flink.kubernetes.operator.TestUtils#flinkVersions"})
    public void verifyFailureDuringTransition(FlinkVersion flinkVersion) throws Exception {
        var blueGreenDeployment =
                buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, flinkVersion);
        var originalSpec = blueGreenDeployment.getSpec();

        // Overriding the maxNumRetries and Reschedule Interval
        var maxNumRetries = 2;
        var reconciliationReschedulingIntervalMs = 5000;
        blueGreenDeployment.getSpec().getTemplate().setMaxNumRetries(maxNumRetries);
        blueGreenDeployment
                .getSpec()
                .getTemplate()
                .setReconciliationReschedulingIntervalMs(reconciliationReschedulingIntervalMs);

        // 1. Initiate the Green deployment
        var rs = reconcile(blueGreenDeployment);

        // 2. Finalize the Green deployment
        simulateSubmitAndSuccessfulJobStart(getFlinkDeployments().get(0));
        rs = reconcile(rs.deployment);

        // 3. Simulate a change in the spec to trigger a Blue deployment
        String customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue);

        // 4. Initiate the Blue deployment
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

        // 4a. Simulating the Blue deployment doesn't start correctly (status will remain the same)
        //  Asserting the status retry count is incremented by 1
        long lastTs = System.currentTimeMillis();
        for (int i = 1; i <= maxNumRetries; i++) {
            Thread.sleep(1);
            rs = reconcile(rs.deployment);
            assertTrue(rs.updateControl.isPatchStatus());
            assertFalse(rs.updateControl.isUpdateResource());
            assertTrue(rs.updateControl.getScheduleDelay().isPresent());
            assertEquals(
                    reconciliationReschedulingIntervalMs,
                    rs.updateControl.getScheduleDelay().get());
            assertEquals(i, rs.reconciledStatus.getNumRetries());
            assertTrue(rs.reconciledStatus.getLastReconciledTimestamp() > lastTs);
            lastTs = rs.reconciledStatus.getLastReconciledTimestamp();
            System.out.println();
        }

        // 4b. After the retries are exhausted
        var minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        assertSpec(rs, minReconciliationTs);

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
        assertEquals(
                JobStatus.SUSPENDED, flinkDeployments.get(1).getStatus().getJobStatus().getState());
        assertEquals(
                ReconciliationState.UPGRADING,
                flinkDeployments.get(1).getStatus().getReconciliationStatus().getState());

        // 5. Simulate another change in the spec to trigger a redeployment
        customValue = UUID.randomUUID().toString();
        simulateChangeInSpec(rs.deployment, customValue);

        // 6. Initiate the redeployment
        var bgUpdatedSpec = rs.deployment.getSpec();
        minReconciliationTs = System.currentTimeMillis() - 1;
        rs = reconcile(rs.deployment);

        testTransitionToGreen(rs, minReconciliationTs, customValue, bgUpdatedSpec);
    }

    private static void assertSpec(
            TestingFlinkBlueGreenDeploymentController.BlueGreenReconciliationResult rs,
            long minReconciliationTs) {
        assertTrue(rs.updateControl.isPatchStatus());
        assertTrue(minReconciliationTs < rs.reconciledStatus.getLastReconciledTimestamp());
    }

    private void simulateChangeInSpec(
            FlinkBlueGreenDeployment blueGreenDeployment, String customValue) {
        FlinkDeploymentSpec spec = blueGreenDeployment.getSpec().getTemplate().getSpec();
        spec.getFlinkConfiguration().put(CUSTOM_CONFIG_FIELD, customValue);
        blueGreenDeployment.getSpec().getTemplate().setSpec(spec);
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
                        .deploymentDeletionDelaySec(1)
                        .maxNumRetries(1)
                        .reconciliationReschedulingIntervalMs(2000)
                        .spec(flinkDeploymentSpec)
                        .build();

        return new FlinkBlueGreenDeploymentSpec(flinkDeploymentTemplateSpec);
    }
}
