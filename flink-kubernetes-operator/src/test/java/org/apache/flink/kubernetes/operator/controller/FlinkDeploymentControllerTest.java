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
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentReconciliationStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.api.status.TaskManagerInfo;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractFlinkResourceReconciler;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.util.SerializedThrowable;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.TestUtils.MAX_RECONCILE_TIMES;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_JOB_UPGRADE_LAST_STATE_FALLBACK_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED;
import static org.apache.flink.kubernetes.operator.utils.EventRecorder.Reason.ValidationError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** {@link FlinkDeploymentController} tests. */
@EnableKubernetesMockClient(crud = true)
public class FlinkDeploymentControllerTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private TestingFlinkService flinkService;
    private Context<FlinkDeployment> context;
    private TestingFlinkDeploymentController testController;

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
        testController = new TestingFlinkDeploymentController(configManager, flinkService);
        kubernetesClient.resource(TestUtils.buildApplicationCluster()).createOrReplace();
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyBasicReconcileLoop(FlinkVersion flinkVersion) throws Exception {

        UpdateControl<FlinkDeployment> updateControl;

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        assertEquals(
                JobManagerDeploymentStatus.MISSING,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertNull(appCluster.getStatus().getJobStatus().getState());

        // Normal reconcile life cycle
        verifyReconcileNormalLifecycle(appCluster);

        // Send in invalid update
        appCluster.getSpec().setJob(null);
        updateControl = testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                appCluster.getStatus().getJobStatus().getState());
        assertEquals(7, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isUpdateStatus());

        FlinkDeploymentReconciliationStatus reconciliationStatus =
                appCluster.getStatus().getReconciliationStatus();
        assertTrue(
                appCluster
                        .getStatus()
                        .getError()
                        .contains("Cannot switch from job to session cluster"));
        assertNotNull(reconciliationStatus.deserializeLastReconciledSpec().getJob());

        // Validate job status correct even with error
        JobStatus jobStatus = appCluster.getStatus().getJobStatus();
        JobStatusMessage expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState(), jobStatus.getState());

        // Validate last stable spec is still the old one
        assertEquals(
                appCluster.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                appCluster.getStatus().getReconciliationStatus().getLastStableSpec());

        testController.cleanup(appCluster, context);
        // Make sure status is recorded and sent out at the end of cleanup
        assertEquals(
                ResourceLifecycleState.DELETED,
                testController
                        .getStatusUpdateCounter()
                        .currentResource
                        .getStatus()
                        .getLifecycleState());
        assertEquals(ResourceLifecycleState.DELETED, appCluster.getStatus().getLifecycleState());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyReconcileLoopForInitialSuspendedDeployment(FlinkVersion flinkVersion)
            throws Exception {
        UpdateControl<FlinkDeployment> updateControl;
        FlinkDeployment appCluster =
                TestUtils.buildApplicationCluster(flinkVersion, JobState.SUSPENDED);
        assertEquals(
                JobManagerDeploymentStatus.MISSING,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertNull(appCluster.getStatus().getJobStatus().getState());

        int reconcileTimes = 0;
        while (reconcileTimes < MAX_RECONCILE_TIMES) {
            verifyReconcileInitialSuspendedDeployment(appCluster);
            reconcileTimes++;
        }

        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        verifyReconcileNormalLifecycle(appCluster);
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyReconcileLoopForInitialSuspendedDeploymentWithSavepoint(
            FlinkVersion flinkVersion) throws Exception {
        UpdateControl<FlinkDeployment> updateControl;
        FlinkDeployment appCluster =
                TestUtils.buildApplicationCluster(flinkVersion, JobState.SUSPENDED);
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");
        appCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                        "file:///flink-data/savepoints");

        int reconcileTimes = 0;
        while (reconcileTimes < MAX_RECONCILE_TIMES) {
            verifyReconcileInitialSuspendedDeployment(appCluster);
            reconcileTimes++;
        }

        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        verifyReconcileNormalLifecycle(appCluster);
        var jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);
        assertEquals(
                new TaskManagerInfo(
                        "component=taskmanager,app=" + appCluster.getMetadata().getName(), 1),
                appCluster.getStatus().getTaskManager());
    }

    @Test
    public void verifyFailedDeployment() throws Exception {

        var submittedEventValidatingResponseProvider =
                new TestUtils.ValidatingResponseProvider<>(
                        mockedEvent,
                        r ->
                                assertTrue(
                                        r.getBody()
                                                .readUtf8()
                                                .contains(
                                                        AbstractFlinkResourceReconciler
                                                                .MSG_SUBMIT)));
        mockServer
                .expect()
                .post()
                .withPath("/api/v1/namespaces/flink-operator-test/events")
                .andReply(submittedEventValidatingResponseProvider)
                .once();

        var validatingResponseProvider =
                new TestUtils.ValidatingResponseProvider<>(
                        mockedEvent,
                        r ->
                                assertTrue(
                                        r.getBody()
                                                .readUtf8()
                                                .contains(TestUtils.DEPLOYMENT_ERROR)));
        mockServer
                .expect()
                .post()
                .withPath("/api/v1/namespaces/flink-operator-test/events")
                .andReply(validatingResponseProvider)
                .once();

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        UpdateControl<FlinkDeployment> updateControl;

        testController.reconcile(appCluster, context);
        updateControl =
                testController.reconcile(
                        appCluster,
                        TestUtils.createContextWithFailedJobManagerDeployment(kubernetesClient));
        submittedEventValidatingResponseProvider.assertValidated();
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        validatingResponseProvider.assertValidated();

        // Validate status
        assertNotNull(appCluster.getStatus().getError());

        // next cycle should not create another event
        updateControl =
                testController.reconcile(
                        appCluster,
                        TestUtils.createContextWithFailedJobManagerDeployment(kubernetesClient));
        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                ReconciliationUtils.rescheduleAfter(
                                JobManagerDeploymentStatus.ERROR,
                                appCluster,
                                configManager.getOperatorConfiguration())
                        .toMillis(),
                updateControl.getScheduleDelay().get());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyUpgradeFromSavepointLegacyMode(FlinkVersion flinkVersion) throws Exception {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");
        appCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                        "file:///flink-data/savepoints");
        appCluster.getSpec().getFlinkConfiguration().put(SNAPSHOT_RESOURCE_ENABLED.key(), "false");
        testController.reconcile(appCluster, context);
        var jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);
        assertEquals(
                new TaskManagerInfo(
                        "component=taskmanager,app=" + appCluster.getMetadata().getName(), 1),
                appCluster.getStatus().getTaskManager());
        assertEquals("s0", appCluster.getStatus().getJobStatus().getUpgradeSavepointPath());

        var previousJobs = new ArrayList<>(jobs);
        appCluster.getSpec().getJob().setInitialSavepointPath("s1");

        // Send in a no-op change
        testController.reconcile(appCluster, context);
        assertEquals(previousJobs, new ArrayList<>(flinkService.listJobs()));
        assertEquals("s0", appCluster.getStatus().getJobStatus().getUpgradeSavepointPath());

        // Upgrade job
        appCluster.getSpec().getJob().setParallelism(100);

        assertEquals(0L, testController.reconcile(appCluster, context).getScheduleDelay().get());
        assertEquals(
                JobState.SUSPENDED,
                appCluster
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertEquals(new TaskManagerInfo("", 0), appCluster.getStatus().getTaskManager());
        assertEquals(
                "savepoint_0", appCluster.getStatus().getJobStatus().getUpgradeSavepointPath());

        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_0", jobs.get(0).f0);
        testController.reconcile(appCluster, context);

        // Suspend job
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                "savepoint_1", appCluster.getStatus().getJobStatus().getUpgradeSavepointPath());

        // Resume from last savepoint
        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_1", jobs.get(0).f0);

        testController.reconcile(appCluster, context);
        testController.cleanup(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(0, jobs.size());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyStatelessUpgrade(FlinkVersion flinkVersion) throws Exception {
        testController.flinkResourceEvents().clear();
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");

        testController.reconcile(appCluster, context);
        var jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);
        assertEquals(1, testController.flinkResourceEvents().size());
        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        assertEquals(1, testController.flinkResourceEvents().size());
        assertEquals(
                EventRecorder.Reason.JobStatusChanged,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));

        // Upgrade job
        appCluster.getSpec().getJob().setParallelism(100);

        UpdateControl<FlinkDeployment> updateControl =
                testController.reconcile(appCluster, context);

        assertEquals(2, testController.flinkResourceEvents().size());
        assertEquals(
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Suspended,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));

        assertEquals(0, updateControl.getScheduleDelay().get());
        assertEquals(
                JobState.SUSPENDED,
                appCluster
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());

        updateControl = testController.reconcile(appCluster, context);
        assertEquals(1, testController.flinkResourceEvents().size());
        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));

        assertEquals(
                ReconciliationUtils.rescheduleAfter(
                                JobManagerDeploymentStatus.DEPLOYING,
                                appCluster,
                                configManager.getOperatorConfiguration())
                        .toMillis(),
                updateControl.getScheduleDelay().get());

        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertNull(jobs.get(0).f0);

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        assertEquals(1, testController.flinkResourceEvents().size());
        assertEquals(
                EventRecorder.Reason.JobStatusChanged,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));

        // Suspend job
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, context);

        assertEquals(2, testController.flinkResourceEvents().size());
        assertEquals(
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Suspended,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));

        // Resume from empty state
        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(appCluster, context);
        assertEquals(2, testController.flinkResourceEvents().size());
        assertEquals(
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertNull(jobs.get(0).f0);

        // Inject validation error in the middle of the upgrade
        appCluster.getSpec().setRestartNonce(123L);
        testController.reconcile(appCluster, context);
        assertEquals(2, testController.flinkResourceEvents().size());
        assertEquals(
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Suspended,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        assertEquals(
                JobState.SUSPENDED,
                appCluster
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        appCluster.getSpec().setLogConfiguration(Map.of("invalid", "conf"));
        testController.reconcile(
                appCluster, TestUtils.createEmptyContextWithClient(kubernetesClient));
        assertEquals(2, testController.flinkResourceEvents().size());
        testController.flinkResourceEvents().remove();
        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().remove().getReason()));
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        var statusEvents =
                testController.flinkResourceEvents().stream()
                        .filter(e -> !e.getReason().equals(ValidationError.name()))
                        .collect(Collectors.toList());
        assertEquals(1, statusEvents.size());
        assertEquals(
                EventRecorder.Reason.JobStatusChanged,
                EventRecorder.Reason.valueOf(statusEvents.get(0).getReason()));

        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                appCluster.getStatus().getJobStatus().getState());
        assertEquals(
                JobState.RUNNING,
                appCluster
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void testUpgradeNotReadyClusterSession(FlinkVersion flinkVersion) throws Exception {
        testUpgradeNotReadyCluster(TestUtils.buildSessionCluster(flinkVersion));
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersionsAndUpgradeModes")
    public void testUpgradeNotReadyClusterApplication(
            FlinkVersion flinkVersion, UpgradeMode upgradeMode) throws Exception {
        var appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(upgradeMode);
        testUpgradeNotReadyCluster(ReconciliationUtils.clone(appCluster));
        assertEquals(upgradeMode, appCluster.getSpec().getJob().getUpgradeMode());
    }

    @Test
    public void verifyReconcileWithBadConfig() throws Exception {

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        UpdateControl<FlinkDeployment> updateControl;
        // Override rest port, and it should be saved in lastReconciledSpec once a successful
        // reconcile() finishes.
        appCluster.getSpec().getFlinkConfiguration().put(RestOptions.PORT.key(), "8088");
        updateControl = testController.reconcile(appCluster, context);
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // Check when the bad config is applied, observe() will change the cluster state correctly
        appCluster.getSpec().getJobManager().setReplicas(-1);
        // Next reconcile will set error msg and observe with previous validated config
        updateControl = testController.reconcile(appCluster, context);
        assertTrue(
                appCluster
                        .getStatus()
                        .getError()
                        .contains("JobManager replicas should not be configured less than one."));
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // Make sure we do validation before getting effective config in reconcile().
        appCluster.getSpec().getJobManager().setReplicas(1);
        appCluster.getSpec().getJob().setParallelism(0);
        // Verify the saved rest port in lastReconciledSpec is actually used in observe() by
        // utilizing listJobConsumer
        appCluster.getSpec().getFlinkConfiguration().put(RestOptions.PORT.key(), "12345");
        flinkService.setListJobConsumer(
                (configuration) -> assertEquals(8088, configuration.get(RestOptions.PORT)));
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
    }

    @Test
    public void verifyReconcileWithAChangedOperatorModeToSession() throws Exception {

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        UpdateControl<FlinkDeployment> updateControl;

        updateControl = testController.reconcile(appCluster, context);
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        updateControl = testController.reconcile(appCluster, context);
        JobStatus jobStatus = appCluster.getStatus().getJobStatus();
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        // jobStatus has not been set at this time
        assertEquals(org.apache.flink.api.common.JobStatus.RECONCILING, jobStatus.getState());

        // Switches operator mode to SESSION
        appCluster.getSpec().setJob(null);
        // Validation fails and JobObserver should still be used
        updateControl = testController.reconcile(appCluster, context);
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertTrue(
                appCluster
                        .getStatus()
                        .getError()
                        .contains("Cannot switch from job to session cluster"));

        assertNotNull(ReconciliationUtils.getDeployedSpec(appCluster).getJob());
        // Verify jobStatus is running
        jobStatus = appCluster.getStatus().getJobStatus();
        JobStatusMessage expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState(), jobStatus.getState());
    }

    @Test
    public void verifyReconcileWithAChangedOperatorModeToApplication() throws Exception {

        FlinkDeployment appCluster = TestUtils.buildSessionCluster();
        UpdateControl<FlinkDeployment> updateControl;

        updateControl = testController.reconcile(appCluster, context);
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        updateControl = testController.reconcile(appCluster, context);
        JobStatus jobStatus = appCluster.getStatus().getJobStatus();
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        // jobStatus has not been set at this time
        assertNull(jobStatus.getState());

        // Switches operator mode to APPLICATION
        appCluster.getSpec().setJob(TestUtils.buildSessionJob().getSpec().getJob());
        // Validation fails and JobObserver should still be used
        updateControl = testController.reconcile(appCluster, context);
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertTrue(
                appCluster
                        .getStatus()
                        .getError()
                        .contains("Cannot switch from session to job cluster"));
        assertNull(ReconciliationUtils.getDeployedSpec(appCluster).getJob());
    }

    private void testUpgradeNotReadyCluster(FlinkDeployment appCluster) throws Exception {
        flinkService.clear();
        testController.reconcile(appCluster, context);
        var specClone = ReconciliationUtils.clone(appCluster.getSpec());
        if (specClone.getJob() != null) {
            specClone.getJob().setUpgradeMode(UpgradeMode.STATELESS);
        }
        assertEquals(
                specClone,
                appCluster.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());

        flinkService.setPortReady(false);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // trigger change
        appCluster.getSpec().setServiceAccount(appCluster.getSpec().getServiceAccount() + "-2");
        testController.reconcile(appCluster, context);

        // Verify that even in DEPLOYING state we still redeploy when HA meta is available
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        var expectedSpec = ReconciliationUtils.clone(appCluster.getSpec());
        if (expectedSpec.getJob() != null
                && expectedSpec.getJob().getUpgradeMode() != UpgradeMode.STATELESS) {
            expectedSpec.getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        }

        assertEquals(
                expectedSpec,
                appCluster.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());

        flinkService.setPortReady(true);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        if (appCluster.getSpec().getJob() != null) {
            assertEquals(
                    org.apache.flink.api.common.JobStatus.RUNNING,
                    appCluster.getStatus().getJobStatus().getState());
        } else {
            assertEquals(
                    org.apache.flink.api.common.JobStatus.FINISHED,
                    appCluster.getStatus().getJobStatus().getState());
        }
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        if (appCluster.getSpec().getJob() == null) {
            return;
        }

        // Move into new deploying stage by submitting a valid upgrade
        appCluster.getSpec().setServiceAccount(appCluster.getSpec().getServiceAccount() + "-3");
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        // We do not let it go into running state
        flinkService.setPortReady(false);
        flinkService.setHaDataAvailable(false);

        // Trigger a new upgrade now with HA data missing
        appCluster.getSpec().setServiceAccount(appCluster.getSpec().getServiceAccount() + "-4");
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        if (appCluster.getSpec().getJob().getUpgradeMode() == UpgradeMode.STATELESS) {
            assertEquals(
                    appCluster.getSpec(),
                    appCluster
                            .getStatus()
                            .getReconciliationStatus()
                            .deserializeLastReconciledSpec());
            return;
        }

        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertNotEquals(
                appCluster.getSpec(),
                appCluster.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());

        // As soon as the HA data is available we can upgrade
        flinkService.setHaDataAvailable(true);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.MISSING,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                UpgradeMode.LAST_STATE,
                appCluster
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getUpgradeMode());
        flinkService.setPortReady(true);

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                appCluster.getStatus().getJobStatus().getState());
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // triggering upgrade with no last-state fallback on non-healthy app
        flinkService.setPortReady(false);
        appCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(OPERATOR_JOB_UPGRADE_LAST_STATE_FALLBACK_ENABLED.key(), "false");
        appCluster.getSpec().setServiceAccount(appCluster.getSpec().getServiceAccount() + "-5");
        // not upgrading the cluster with no last-state fallback
        testController.reconcile(appCluster, context);
        assertNotEquals(
                appCluster.getSpec(),
                appCluster.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());

        // once the job is ready however the upgrade continues
        flinkService.setPortReady(true);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                appCluster.getStatus().getJobStatus().getState());
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
    }

    @Test
    public void testSuccessfulObservationShouldClearErrors() throws Exception {
        final String crashLoopMessage = "deploy errors";
        flinkService.setPodList(TestUtils.createFailedPodList(crashLoopMessage, "ErrImagePull"));

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();

        testController.reconcile(appCluster, context);
        testController.reconcile(
                appCluster, TestUtils.createContextWithInProgressDeployment(kubernetesClient));

        assertNull(appCluster.getStatus().getReconciliationStatus().getLastStableSpec());

        // Failed JobManager deployment should set errors to the status
        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertTrue(appCluster.getStatus().getError().contains(crashLoopMessage));

        // JobManager deployment becomes ready and successful observation should clear the errors
        testController.reconcile(appCluster, context);
        assertNull(appCluster.getStatus().getReconciliationStatus().getLastStableSpec());

        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertNull(appCluster.getStatus().getError());

        assertEquals(
                appCluster.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                appCluster.getStatus().getReconciliationStatus().getLastStableSpec());
    }

    @Test
    public void testValidationError() throws Exception {
        assertTrue(testController.flinkResourceEvents().isEmpty());
        var flinkDeployment = TestUtils.buildApplicationCluster();
        flinkDeployment.getSpec().getJob().setParallelism(-1);
        testController.reconcile(flinkDeployment, context);

        assertEquals(1, testController.flinkResourceEvents().size());
        assertEquals(
                ResourceLifecycleState.FAILED, flinkDeployment.getStatus().getLifecycleState());

        var event = testController.flinkResourceEvents().remove();
        assertEquals("Warning", event.getType());
        assertEquals("ValidationError", event.getReason());
        assertTrue(event.getMessage().startsWith("Job parallelism "));
    }

    @Test
    public void testEventOfNonDeploymentFailedException() throws Exception {
        assertTrue(testController.flinkResourceEvents().isEmpty());
        var flinkDeployment = TestUtils.buildApplicationCluster();

        flinkService.setDeployFailure(true);
        try {
            testController.reconcile(flinkDeployment, context);
            fail();
        } catch (Exception expected) {
        }
        assertEquals(2, testController.flinkResourceEvents().size());

        var event = testController.flinkResourceEvents().remove();
        assertEquals("Submit", event.getReason());
        event = testController.flinkResourceEvents().remove();
        assertEquals("Error", event.getReason());
        assertEquals("Deployment failure", event.getMessage());
    }

    @Test
    public void testEventOfNonDeploymentFailedChainedException() {
        assertTrue(testController.flinkResourceEvents().isEmpty());
        var flinkDeployment = TestUtils.buildApplicationCluster();

        flinkService.setMakeItFailWith(
                new RuntimeException(
                        "Deployment Failure",
                        new IllegalStateException(
                                null,
                                new SerializedThrowable(new Exception("actual failure reason")))));
        try {
            testController.reconcile(flinkDeployment, context);
            fail();
        } catch (Exception expected) {
        }
        assertEquals(2, testController.flinkResourceEvents().size());

        var event = testController.flinkResourceEvents().remove();
        assertEquals("Submit", event.getReason());
        event = testController.flinkResourceEvents().remove();
        assertEquals("Error", event.getReason());
        assertEquals(
                "Deployment Failure -> IllegalStateException -> actual failure reason",
                event.getMessage());
    }

    @Test
    public void cleanUpNewDeployment() {
        FlinkDeployment flinkDeployment = TestUtils.buildApplicationCluster();
        var resourceMetricGroup =
                testController
                        .getContextFactory()
                        .getResourceContext(flinkDeployment, context)
                        .getResourceMetricGroup();
        var deleteControl = testController.cleanup(flinkDeployment, context);
        assertNotNull(deleteControl);
        assertTrue(resourceMetricGroup.isClosed());
        assertTrue(testController.getContextFactory().getMetricGroups().isEmpty());
    }

    @Test
    public void testIngressLifeCycle() throws Exception {
        FlinkDeployment appNoIngress = TestUtils.buildApplicationCluster();
        testController.reconcile(appNoIngress, context);
        // deploy without ingress
        assertNull(
                kubernetesClient
                        .network()
                        .v1()
                        .ingresses()
                        .inNamespace(appNoIngress.getMetadata().getNamespace())
                        .withName(appNoIngress.getMetadata().getName())
                        .get());

        // deploy with ingress
        FlinkDeployment appWithIngress = TestUtils.buildApplicationCluster();
        IngressSpec.IngressSpecBuilder builder = IngressSpec.builder();
        builder.template("{{name}}.{{namespace}}.example.com");
        IngressSpec ingressSpec = builder.build();
        appWithIngress.getSpec().setIngress(ingressSpec);
        testController.reconcile(appWithIngress, context);
        testController.reconcile(appWithIngress, context);

        HasMetadata ingress = getIngress(appWithIngress);
        Assertions.assertNotNull(ingress);

        Assertions.assertEquals(
                getIngressHost(ingress),
                IngressUtils.getIngressUrl(
                                "{{name}}.{{namespace}}.example.com",
                                appWithIngress.getMetadata().getName(),
                                appWithIngress.getMetadata().getNamespace())
                        .getHost());

        // upgrade with new ingress
        builder.template("http://{{name}}.{{namespace}}.foo.bar");
        ingressSpec = builder.build();
        appWithIngress.getSpec().setIngress(ingressSpec);
        testController.reconcile(appWithIngress, context);
        testController.reconcile(appWithIngress, context);

        ingress = getIngress(appWithIngress);
        Assertions.assertEquals(
                getIngressHost(ingress),
                IngressUtils.getIngressUrl(
                                "{{name}}.{{namespace}}.foo.bar",
                                appWithIngress.getMetadata().getName(),
                                appWithIngress.getMetadata().getNamespace())
                        .getHost());
    }

    @Test
    public void testInitialSavepointOnError() throws Exception {
        FlinkDeployment flinkDeployment = TestUtils.buildApplicationCluster();
        flinkDeployment.getSpec().getJob().setInitialSavepointPath("msp");
        flinkService.setDeployFailure(true);
        try {
            testController.reconcile(flinkDeployment, context);
            fail();
        } catch (Exception expected) {
        }
        flinkService.setDeployFailure(false);
        testController.reconcile(flinkDeployment, context);
        assertEquals("msp", flinkService.listJobs().get(0).f0);
    }

    @Test
    public void testErrorOnReconcileWithChainedExceptions() throws Exception {
        FlinkDeployment flinkDeployment = TestUtils.buildApplicationCluster();
        flinkDeployment.getSpec().getJob().setInitialSavepointPath("msp");
        flinkService.setMakeItFailWith(
                new RuntimeException(
                        "Deployment Failure",
                        new IllegalStateException(
                                null,
                                new SerializedThrowable(new Exception("actual failure reason")))));
        try {
            testController.reconcile(flinkDeployment, context);
            fail();
        } catch (Exception expected) {
        }
        assertEquals(2, testController.flinkResourceEvents().size());

        var event = testController.flinkResourceEvents().remove();
        assertEquals("Submit", event.getReason());
        event = testController.flinkResourceEvents().remove();
        assertEquals("Error", event.getReason());
        assertEquals(
                "Deployment Failure -> IllegalStateException -> actual failure reason",
                event.getMessage());
    }

    @Test
    public void testInitialHaError() throws Exception {
        var appCluster = TestUtils.buildApplicationCluster(FlinkVersion.v1_20);
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, context);

        flinkService.setHaDataAvailable(false);
        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        testController.flinkResourceEvents().clear();
        testController.reconcile(appCluster, context);

        assertEquals(3, testController.flinkResourceEvents().size());
        assertEquals(
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        assertTrue(
                testController
                        .flinkResourceEvents()
                        .poll()
                        .getMessage()
                        .contains("HA metadata not available to restore from last state."));

        testController.flinkResourceEvents().clear();
        testController.reconcile(appCluster, context);

        assertEquals(
                EventRecorder.Reason.Submit,
                EventRecorder.Reason.valueOf(
                        testController.flinkResourceEvents().poll().getReason()));
        assertTrue(
                testController
                        .flinkResourceEvents()
                        .poll()
                        .getMessage()
                        .contains("HA metadata not available to restore from last state."));

        flinkService.setHaDataAvailable(true);
        testController.flinkResourceEvents().clear();
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                appCluster.getStatus().getJobStatus().getState());
    }

    @Test
    public void verifyCanaryHandling() throws Exception {
        var canary = TestUtils.createCanaryDeployment();
        kubernetesClient.resource(canary).create();
        assertTrue(testController.reconcile(canary, context).isNoUpdate());
        assertEquals(0, testController.getInternalStatusUpdateCount());
        assertEquals(1, testController.getCanaryResourceManager().getNumberOfActiveCanaries());
        testController.cleanup(canary, context);
        assertEquals(0, testController.getInternalStatusUpdateCount());
        assertEquals(0, testController.getCanaryResourceManager().getNumberOfActiveCanaries());
    }

    private void verifyReconcileInitialSuspendedDeployment(FlinkDeployment appCluster)
            throws Exception {
        UpdateControl<FlinkDeployment> updateControl =
                testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.MISSING,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertNull(appCluster.getStatus().getJobStatus().getState());
        assertEquals(1, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        // Validate reconciliation status
        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                appCluster.getStatus().getReconciliationStatus();
        assertNull(appCluster.getStatus().getError());
        assertNull(reconciliationStatus.deserializeLastReconciledSpec());
        assertNull(reconciliationStatus.getLastStableSpec());
    }

    private void verifyReconcileNormalLifecycle(FlinkDeployment appCluster) throws Exception {
        UpdateControl<FlinkDeployment> updateControl =
                testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RECONCILING,
                appCluster.getStatus().getJobStatus().getState());
        assertEquals(4, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(
                        configManager
                                .getOperatorConfiguration()
                                .getProgressCheckInterval()
                                .toMillis()),
                updateControl.getScheduleDelay());
        // Validate reconciliation status
        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                appCluster.getStatus().getReconciliationStatus();
        assertNull(appCluster.getStatus().getError());
        assertEquals(appCluster.getSpec(), reconciliationStatus.deserializeLastReconciledSpec());
        assertNull(appCluster.getStatus().getReconciliationStatus().getLastStableSpec());

        updateControl = testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RECONCILING,
                appCluster.getStatus().getJobStatus().getState());
        assertEquals(5, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getRestApiReadyDelay().toMillis()),
                updateControl.getScheduleDelay());

        updateControl = testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                appCluster.getStatus().getJobStatus().getState());
        assertEquals(6, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        // Stable loop
        updateControl = testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                appCluster.getStatus().getJobStatus().getState());
        assertEquals(6, testController.getInternalStatusUpdateCount());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        // Validate job status
        JobStatus jobStatus = appCluster.getStatus().getJobStatus();
        JobStatusMessage expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState(), jobStatus.getState());
        assertEquals(
                appCluster.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                appCluster.getStatus().getReconciliationStatus().getLastStableSpec());
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void testUnsupportedVersions(FlinkVersion version) throws Exception {
        var appCluster = TestUtils.buildApplicationCluster(version);
        var updateControl = testController.reconcile(appCluster, context);
        var lastEvent = testController.flinkResourceEvents().poll();
        if (!version.isEqualOrNewer(FlinkVersion.v1_15)) {
            assertTrue(updateControl.getScheduleDelay().isEmpty());
            assertEquals(
                    EventRecorder.Reason.UnsupportedFlinkVersion.name(), lastEvent.getReason());
        } else {
            assertTrue(updateControl.getScheduleDelay().isPresent());
            assertEquals(EventRecorder.Reason.Submit.name(), lastEvent.getReason());
        }
    }

    private HasMetadata getIngress(FlinkDeployment deployment) {
        if (IngressUtils.ingressInNetworkingV1(kubernetesClient)) {
            return kubernetesClient
                    .network()
                    .v1()
                    .ingresses()
                    .inNamespace(deployment.getMetadata().getNamespace())
                    .withName(deployment.getMetadata().getName())
                    .get();
        } else {
            return kubernetesClient
                    .network()
                    .v1beta1()
                    .ingresses()
                    .inNamespace(deployment.getMetadata().getNamespace())
                    .withName(deployment.getMetadata().getName())
                    .get();
        }
    }

    private String getIngressHost(HasMetadata ingress) {
        IngressRule ingressRule = null;
        io.fabric8.kubernetes.api.model.networking.v1beta1.IngressRule ingressRuleV1beta1 = null;

        if (IngressUtils.ingressInNetworkingV1(kubernetesClient)) {
            ingressRule = ((Ingress) ingress).getSpec().getRules().stream().findFirst().get();
            Assertions.assertNotNull(ingressRule);
            return ingressRule.getHost();
        } else {
            ingressRuleV1beta1 =
                    ((io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress) ingress)
                            .getSpec().getRules().stream().findFirst().get();
            Assertions.assertNotNull(ingressRuleV1beta1);
            return ingressRuleV1beta1.getHost();
        }
    }
}
