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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingApplicationReconciler;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingStatusRecorder;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/** @link JobStatusObserver unit tests */
@EnableKubernetesMockClient(crud = true)
public class ApplicationReconcilerUpgradeModeTest {

    private KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private TestingFlinkService flinkService;
    private ApplicationReconciler reconciler;
    private Context<FlinkDeployment> context;

    @BeforeEach
    public void before() {
        kubernetesClient.resource(TestUtils.buildApplicationCluster()).createOrReplace();
        var eventRecorder = new EventRecorder(kubernetesClient, (r, e) -> {});
        var statusRecoder = new TestingStatusRecorder<FlinkDeployment, FlinkDeploymentStatus>();
        flinkService = new TestingFlinkService(kubernetesClient);
        context = flinkService.getContext();
        reconciler =
                new TestingApplicationReconciler(
                        kubernetesClient,
                        flinkService,
                        configManager,
                        eventRecorder,
                        statusRecoder,
                        TestUtils.createTestMetricGroup(new Configuration()));
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUpgradeFromStatelessToStateless(FlinkVersion flinkVersion) throws Exception {
        testUpgradeToStateless(flinkVersion, UpgradeMode.STATELESS);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUpgradeFromSavepointToStateless(FlinkVersion flinkVersion) throws Exception {
        testUpgradeToStateless(flinkVersion, UpgradeMode.SAVEPOINT);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUpgradeFromLastStateToStateless(FlinkVersion flinkVersion) throws Exception {
        testUpgradeToStateless(flinkVersion, UpgradeMode.LAST_STATE);
    }

    private void testUpgradeToStateless(FlinkVersion flinkVersion, UpgradeMode fromUpgradeMode)
            throws Exception {
        FlinkDeployment deployment = buildApplicationCluster(flinkVersion, fromUpgradeMode);

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        FlinkDeployment modifiedDeployment =
                cloneDeploymentWithUpgradeMode(deployment, UpgradeMode.STATELESS);

        reconciler.reconcile(modifiedDeployment, context);
        assertEquals(0, flinkService.getRunningCount());

        reconciler.reconcile(modifiedDeployment, context);
        runningJobs = flinkService.listJobs();
        assertEquals(1, flinkService.getRunningCount());
        assertNull(runningJobs.get(0).f0);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUpgradeFromStatelessToSavepoint(FlinkVersion flinkVersion) throws Exception {
        testUpgradeToSavepoint(flinkVersion, UpgradeMode.STATELESS);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUpgradeFromSavepointToSavepoint(FlinkVersion flinkVersion) throws Exception {
        testUpgradeToSavepoint(flinkVersion, UpgradeMode.SAVEPOINT);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUpgradeFromLastStateToSavepoint(FlinkVersion flinkVersion) throws Exception {
        testUpgradeToSavepoint(flinkVersion, UpgradeMode.LAST_STATE);
    }

    private void testUpgradeToSavepoint(FlinkVersion flinkVersion, UpgradeMode fromUpgradeMode)
            throws Exception {
        FlinkDeployment deployment = buildApplicationCluster(flinkVersion, fromUpgradeMode);

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        FlinkDeployment modifiedDeployment =
                cloneDeploymentWithUpgradeMode(deployment, UpgradeMode.SAVEPOINT);
        modifiedDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), "test-savepoint-dir");

        reconciler.reconcile(modifiedDeployment, context);
        assertEquals(0, flinkService.getRunningCount());

        reconciler.reconcile(modifiedDeployment, context);
        runningJobs = flinkService.listJobs();
        assertEquals(1, flinkService.getRunningCount());
        assertEquals("savepoint_0", runningJobs.get(0).f0);
        assertEquals(
                SavepointTriggerType.UPGRADE,
                modifiedDeployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getTriggerType());
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUpgradeFromStatelessToLastState(FlinkVersion flinkVersion) throws Exception {
        testUpgradeToLastState(flinkVersion, UpgradeMode.STATELESS);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUpgradeFromSavepointToLastState(FlinkVersion flinkVersion) throws Exception {
        testUpgradeToLastState(flinkVersion, UpgradeMode.SAVEPOINT);
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUpgradeFromLastStateToLastState(FlinkVersion flinkVersion) throws Exception {
        testUpgradeToLastState(flinkVersion, UpgradeMode.LAST_STATE);
    }

    private void testUpgradeToLastState(FlinkVersion flinkVersion, UpgradeMode fromUpgradeMode)
            throws Exception {
        FlinkDeployment deployment = buildApplicationCluster(flinkVersion, fromUpgradeMode);

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().setRestartNonce(100L);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastStableSpec(
                        deployment.getStatus().getReconciliationStatus().getLastReconciledSpec());
        flinkService.setHaDataAvailable(false);
        deployment.getStatus().getJobStatus().setState("RECONCILING");

        Assertions.assertThrows(
                RecoveryFailureException.class,
                () -> {
                    deployment
                            .getStatus()
                            .setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
                    reconciler.reconcile(deployment, context);
                    fail();
                });

        Assertions.assertThrows(
                RecoveryFailureException.class,
                () -> {
                    deployment
                            .getStatus()
                            .setJobManagerDeploymentStatus(JobManagerDeploymentStatus.ERROR);
                    reconciler.reconcile(deployment, context);
                });

        flinkService.clear();
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().setRestartNonce(200L);
        flinkService.setHaDataAvailable(false);
        deployment
                .getStatus()
                .getJobStatus()
                .getSavepointInfo()
                .setLastSavepoint(Savepoint.of("finished_sp", SavepointTriggerType.UPGRADE));
        deployment.getStatus().getJobStatus().setState("FINISHED");
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), "test-savepoint-dir");
        reconciler.reconcile(deployment, context);
        reconciler.reconcile(deployment, context);

        assertEquals(1, flinkService.getRunningCount());
        assertEquals("finished_sp", runningJobs.get(0).f0);
    }

    private FlinkDeployment cloneDeploymentWithUpgradeMode(
            FlinkDeployment deployment, UpgradeMode upgradeMode) {
        FlinkDeployment result = ReconciliationUtils.clone(deployment);

        result.getSpec().getJob().setUpgradeMode(upgradeMode);
        result.getSpec().getFlinkConfiguration().put("new", "conf");

        return result;
    }

    @ParameterizedTest
    @MethodSource("testUpgradeJmDeployCannotStartParams")
    public void testUpgradeJmDeployCannotStart(UpgradeMode fromMode, UpgradeMode toMode)
            throws Exception {

        flinkService.setHaDataAvailable(true);
        flinkService.setJobManagerReady(true);

        // Prepare running deployment
        var deployment = TestUtils.buildApplicationCluster();
        var jobSpec = deployment.getSpec().getJob();
        jobSpec.setUpgradeMode(fromMode);

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        // Suspend running deployment and assert that correct upgradeMode is set
        jobSpec.setState(JobState.SUSPENDED);
        reconciler.reconcile(deployment, context);

        var lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        assertEquals(JobState.SUSPENDED, lastReconciledSpec.getJob().getState());
        assertEquals(fromMode, lastReconciledSpec.getJob().getUpgradeMode());

        // Restore deployment and assert that correct upgradeMode is set
        jobSpec.setState(JobState.RUNNING);
        jobSpec.setUpgradeMode(toMode);
        reconciler.reconcile(deployment, context);

        lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        assertEquals(JobState.RUNNING, lastReconciledSpec.getJob().getState());
        assertEquals(
                toMode == UpgradeMode.STATELESS ? UpgradeMode.STATELESS : fromMode,
                lastReconciledSpec.getJob().getUpgradeMode());

        // Simulate JM failure after deployment, we need this to test the actual upgrade behaviour
        // with a jobmanager that never started
        flinkService.setJobManagerReady(false);
        flinkService.setHaDataAvailable(false);

        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);

        // Send in a new upgrade while the jobmanager still not started
        jobSpec.setState(JobState.RUNNING);
        jobSpec.setEntryClass("newClass");
        reconciler.reconcile(deployment, context);
        lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();

        // Make sure the upgrade was executed as long as we have the savepoint information
        if (fromMode == UpgradeMode.LAST_STATE && toMode != UpgradeMode.STATELESS) {
            // We cant make progress as no HA meta available after LAST_STATE, upgrade. It means the
            // job started and terminated, but we didn't see...
            assertEquals(
                    JobManagerDeploymentStatus.DEPLOYING,
                    deployment.getStatus().getJobManagerDeploymentStatus());
            assertEquals(JobState.RUNNING, lastReconciledSpec.getJob().getState());
        } else {
            assertEquals(
                    JobManagerDeploymentStatus.MISSING,
                    deployment.getStatus().getJobManagerDeploymentStatus());
            assertEquals(JobState.SUSPENDED, lastReconciledSpec.getJob().getState());
            assertEquals(
                    toMode == UpgradeMode.STATELESS ? UpgradeMode.STATELESS : UpgradeMode.SAVEPOINT,
                    lastReconciledSpec.getJob().getUpgradeMode());

            // Complete upgrade and recover succesfully with the latest savepoint
            reconciler.reconcile(deployment, context);
            lastReconciledSpec =
                    deployment
                            .getStatus()
                            .getReconciliationStatus()
                            .deserializeLastReconciledSpec();

            assertEquals(JobState.RUNNING, lastReconciledSpec.getJob().getState());
            assertEquals(1, flinkService.listJobs().size());
            if (fromMode == UpgradeMode.STATELESS || toMode == UpgradeMode.STATELESS) {
                assertNull(flinkService.listJobs().get(0).f0);
            } else {
                assertEquals("savepoint_0", flinkService.listJobs().get(0).f0);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testInitialJmDeployCannotStartParams")
    public void testInitialJmDeployCannotStart(UpgradeMode upgradeMode, boolean initSavepoint)
            throws Exception {

        // We simulate JM failure to test the initial submission/upgrade behavior when the JM can
        // never start initially
        flinkService.setHaDataAvailable(false);
        flinkService.setJobManagerReady(false);

        var deployment = TestUtils.buildApplicationCluster();
        if (initSavepoint) {
            deployment.getSpec().getJob().setInitialSavepointPath("init-sp");
        }

        reconciler.reconcile(deployment, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());

        var lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();

        // Make sure savepoint path is recorded in status and upgradeMode set correctly for initial
        // startup. Either stateless or savepoint depending only on the initialSavepointPath
        // setting.
        if (initSavepoint) {
            assertEquals("init-sp", flinkService.listJobs().get(0).f0);
            assertEquals(
                    "init-sp",
                    deployment
                            .getStatus()
                            .getJobStatus()
                            .getSavepointInfo()
                            .getLastSavepoint()
                            .getLocation());
            assertEquals(UpgradeMode.SAVEPOINT, lastReconciledSpec.getJob().getUpgradeMode());
        } else {
            assertNull(flinkService.listJobs().get(0).f0);
            assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint());
            assertEquals(UpgradeMode.STATELESS, lastReconciledSpec.getJob().getUpgradeMode());
        }

        // JM is failed, but we submit an upgrade, this should always be possible on initial deploy
        // failure
        final String newImage = "new-image-1";
        deployment.getSpec().getJob().setUpgradeMode(upgradeMode);
        deployment.getSpec().setImage(newImage);
        reconciler.reconcile(deployment, context);
        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());
        lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();

        // We make sure that stateless upgrade request is respected (drop state)
        assertEquals(
                upgradeMode == UpgradeMode.STATELESS
                        ? UpgradeMode.STATELESS
                        : UpgradeMode.SAVEPOINT,
                lastReconciledSpec.getJob().getUpgradeMode());

        reconciler.reconcile(deployment, context);
        lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        assertEquals(newImage, lastReconciledSpec.getImage());
        assertEquals(
                upgradeMode == UpgradeMode.STATELESS
                        ? UpgradeMode.STATELESS
                        : UpgradeMode.SAVEPOINT,
                lastReconciledSpec.getJob().getUpgradeMode());
        assertEquals(1, flinkService.listJobs().size());
        assertEquals(
                initSavepoint && upgradeMode != UpgradeMode.STATELESS ? "init-sp" : null,
                flinkService.listJobs().get(0).f0);
    }

    private static Stream<Arguments> testInitialJmDeployCannotStartParams() {
        return Stream.of(
                Arguments.of(UpgradeMode.LAST_STATE, true),
                Arguments.of(UpgradeMode.LAST_STATE, false),
                Arguments.of(UpgradeMode.SAVEPOINT, true),
                Arguments.of(UpgradeMode.SAVEPOINT, false),
                Arguments.of(UpgradeMode.STATELESS, true),
                Arguments.of(UpgradeMode.STATELESS, false));
    }

    private static Stream<Arguments> testUpgradeJmDeployCannotStartParams() {
        var args = new ArrayList<Arguments>();
        for (UpgradeMode from : UpgradeMode.values()) {
            for (UpgradeMode to : UpgradeMode.values()) {
                args.add(Arguments.of(from, to));
            }
        }
        return args.stream();
    }

    @Test
    public void testLastStateOnDeletedDeployment() throws Exception {
        // Bootstrap running deployment
        var deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);

        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        // Delete cluster and keep HA metadata
        flinkService.deleteClusterDeployment(
                deployment.getMetadata(), deployment.getStatus(), false);
        flinkService.setHaDataAvailable(true);

        // Submit upgrade
        deployment.getSpec().setRestartNonce(123L);
        reconciler.reconcile(deployment, context);

        var lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();

        // Make sure we correctly record upgrade mode to last state
        assertEquals(UpgradeMode.LAST_STATE, lastReconciledSpec.getJob().getUpgradeMode());
        assertEquals(JobState.SUSPENDED, lastReconciledSpec.getJob().getState());
    }

    @Test
    public void testUpgradeModeChangeFromSavepointToLastState() throws Exception {
        final String expectedSavepointPath = "savepoint_0";
        final FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        // Suspend FlinkDeployment with savepoint upgrade mode
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        deployment.getSpec().getJob().setState(JobState.SUSPENDED);
        deployment.getSpec().setImage("new-image-1");

        reconciler.reconcile(deployment, context);
        assertEquals(0, flinkService.getRunningCount());
        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED.name(),
                deployment.getStatus().getJobStatus().getState());

        assertEquals(
                expectedSavepointPath,
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());

        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().getJob().setState(JobState.RUNNING);
        deployment.getSpec().setImage("new-image-2");

        reconciler.reconcile(deployment, context);
        runningJobs = flinkService.listJobs();
        assertEquals(1, flinkService.getRunningCount());
        assertEquals(expectedSavepointPath, runningJobs.get(0).f0);
    }

    @Test
    public void testUpgradeModeChangedToLastStateShouldTriggerSavepointWhileHADisabled()
            throws Exception {
        flinkService.setHaDataAvailable(false);

        final FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().getFlinkConfiguration().remove(HighAvailabilityOptions.HA_MODE.key());

        reconciler.reconcile(deployment, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());

        // Not ready for spec changes, the reconciliation is not performed
        final String newImage = "new-image-1";
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().setImage(newImage);
        reconciler.reconcile(deployment, context);
        reconciler.reconcile(deployment, context);
        assertNull(flinkService.listJobs().get(0).f0);
        assertNotEquals(
                newImage,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getImage());

        // Ready for spec changes, the reconciliation should be performed
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());
        reconciler.reconcile(deployment, context);
        reconciler.reconcile(deployment, context);
        assertEquals(
                newImage,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getImage());
        // Upgrade mode changes from stateless to last-state should trigger a savepoint
        final String expectedSavepointPath = "savepoint_0";
        var runningJobs = flinkService.listJobs();
        assertEquals(expectedSavepointPath, runningJobs.get(0).f0);
    }

    @Test
    public void testUpgradeModeChangedToLastStateShouldNotTriggerSavepointWhileHAEnabled()
            throws Exception {
        final FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        assertNotEquals(
                UpgradeMode.LAST_STATE,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getUpgradeMode());

        final String newImage = "new-image-1";
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().setImage(newImage);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());
        reconciler.reconcile(deployment, context);
        reconciler.reconcile(deployment, context);
        assertEquals(
                newImage,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getImage());
        // Upgrade mode changes from stateless to last-state while HA enabled previously should not
        // trigger a savepoint
        assertNull(flinkService.listJobs().get(0).f0);
    }

    public static FlinkDeployment buildApplicationCluster(
            FlinkVersion flinkVersion, UpgradeMode upgradeMode) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster(flinkVersion);
        deployment.getSpec().getJob().setUpgradeMode(upgradeMode);
        Map<String, String> conf = deployment.getSpec().getFlinkConfiguration();

        switch (upgradeMode) {
            case STATELESS:
                conf.remove(HighAvailabilityOptions.HA_MODE.key());
                conf.remove(HighAvailabilityOptions.HA_STORAGE_PATH.key());
                conf.remove(CheckpointingOptions.SAVEPOINT_DIRECTORY.key());
                conf.remove(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key());
                break;

            case SAVEPOINT:
                conf.remove(HighAvailabilityOptions.HA_MODE.key());
                conf.remove(HighAvailabilityOptions.HA_STORAGE_PATH.key());
                break;

            case LAST_STATE:
                conf.remove(CheckpointingOptions.SAVEPOINT_DIRECTORY.key());
                break;

            default:
                throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
        }

        return deployment;
    }

    private void verifyAndSetRunningJobsToStatus(
            FlinkDeployment deployment,
            List<Tuple3<String, JobStatusMessage, Configuration>> runningJobs) {
        assertEquals(1, runningJobs.size());
        assertNull(runningJobs.get(0).f0);
        deployment
                .getStatus()
                .setJobStatus(
                        new JobStatus()
                                .toBuilder()
                                .jobId(runningJobs.get(0).f1.getJobId().toHexString())
                                .jobName(runningJobs.get(0).f1.getJobName())
                                .state("RUNNING")
                                .build());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
    }
}
