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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** @link FlinkDeploymentController tests */
@EnableKubernetesMockClient(crud = true)
public class FlinkDeploymentControllerTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    private TestingFlinkService flinkService;
    private Context context;
    private FlinkDeploymentController testController;

    private KubernetesMockServer mockServer;
    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService();
        context = flinkService.getContext();
        testController =
                TestUtils.createTestController(configManager, kubernetesClient, flinkService);
        kubernetesClient.resource(TestUtils.buildApplicationCluster()).createOrReplace();
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void verifyBasicReconcileLoop(FlinkVersion flinkVersion) {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);

        UpdateControl<FlinkDeployment> updateControl;

        updateControl = testController.reconcile(appCluster, context);
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
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getRestApiReadyDelay().toMillis()),
                updateControl.getScheduleDelay());

        updateControl = testController.reconcile(appCluster, context);
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
        assertEquals(expectedJobStatus.getJobState().toString(), jobStatus.getState());
        assertEquals(
                appCluster.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                appCluster.getStatus().getReconciliationStatus().getLastStableSpec());

        // Send in invalid update
        appCluster.getSpec().setJob(null);
        updateControl = testController.reconcile(appCluster, context);
        assertFalse(updateControl.isUpdateStatus());

        reconciliationStatus = appCluster.getStatus().getReconciliationStatus();
        assertEquals(
                "Cannot switch from job to session cluster", appCluster.getStatus().getError());
        assertNotNull(reconciliationStatus.deserializeLastReconciledSpec().getJob());

        // Validate job status correct even with error
        jobStatus = appCluster.getStatus().getJobStatus();
        expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState().toString(), jobStatus.getState());

        // Validate last stable spec is still the old one
        assertEquals(
                appCluster.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                appCluster.getStatus().getReconciliationStatus().getLastStableSpec());
    }

    @Test
    public void verifyFailedDeployment() throws Exception {
        var validatingResponseProvider =
                new TestUtils.ValidatingResponseProvider<>(
                        new EventBuilder().withNewMetadata().endMetadata().build(),
                        r -> {
                            assertTrue(r.getBody().readUtf8().contains(TestUtils.DEPLOYMENT_ERROR));
                        });
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
                        appCluster, TestUtils.createContextWithFailedJobManagerDeployment());
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
                        appCluster, TestUtils.createContextWithFailedJobManagerDeployment());
        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.ERROR
                        .rescheduleAfter(appCluster, configManager.getOperatorConfiguration())
                        .toMillis(),
                updateControl.getScheduleDelay().get());
    }

    @Test
    public void verifyInProgressDeploymentWithCrashLoopBackoff() throws Exception {
        String crashLoopMessage = "container fails";

        var validatingResponseProvider =
                new TestUtils.ValidatingResponseProvider<>(
                        new EventBuilder().withNewMetadata().endMetadata().build(),
                        r -> {
                            String recordedRequestBody = r.getBody().readUtf8();
                            assertTrue(
                                    recordedRequestBody.contains(
                                            DeploymentFailedException.REASON_CRASH_LOOP_BACKOFF));
                            assertTrue(recordedRequestBody.contains(crashLoopMessage));
                        });
        mockServer
                .expect()
                .post()
                .withPath("/api/v1/namespaces/flink-operator-test/events")
                .andReply(validatingResponseProvider)
                .once();

        flinkService.setJmPodList(TestUtils.createFailedPodList(crashLoopMessage));

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        UpdateControl<FlinkDeployment> updateControl;

        testController.reconcile(appCluster, context);
        updateControl =
                testController.reconcile(
                        appCluster, TestUtils.createContextWithInProgressDeployment());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(
                        configManager.getOperatorConfiguration().getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RECONCILING.name(),
                appCluster.getStatus().getJobStatus().getState());

        // Validate status status
        assertNotNull(appCluster.getStatus().getError());

        // next cycle should not create another event
        updateControl =
                testController.reconcile(
                        appCluster, TestUtils.createContextWithFailedJobManagerDeployment());
        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.READY
                        .rescheduleAfter(appCluster, configManager.getOperatorConfiguration())
                        .toMillis(),
                updateControl.getScheduleDelay().get());
        validatingResponseProvider.assertValidated();
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    public void verifyUpgradeFromSavepoint(FlinkVersion flinkVersion) {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");
        appCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                        "file:///flink-data/savepoints");

        testController.reconcile(appCluster, context);
        List<Tuple2<String, JobStatusMessage>> jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);

        List<Tuple2<String, JobStatusMessage>> previousJobs = new ArrayList<>(jobs);
        appCluster.getSpec().getJob().setInitialSavepointPath("s1");

        // Send in a no-op change
        testController.reconcile(appCluster, context);
        assertEquals(previousJobs, new ArrayList<>(flinkService.listJobs()));

        // Upgrade job
        appCluster.getSpec().getJob().setParallelism(100);

        assertTrue(
                appCluster
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getSavepointHistory()
                        .isEmpty());
        assertEquals(0, testController.reconcile(appCluster, context).getScheduleDelay().get());
        assertEquals(
                JobState.SUSPENDED,
                appCluster
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertEquals(
                1,
                appCluster
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getSavepointHistory()
                        .size());

        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_0", jobs.get(0).f0);
        testController.reconcile(appCluster, context);
        assertEquals(
                1,
                appCluster
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getSavepointHistory()
                        .size());

        // Suspend job
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, context);
        assertEquals(
                flinkVersion.isNewerVersionThan(FlinkVersion.v1_14)
                        ? JobManagerDeploymentStatus.READY
                        : JobManagerDeploymentStatus.MISSING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

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
    @EnumSource(FlinkVersion.class)
    public void verifyStatelessUpgrade(FlinkVersion flinkVersion) {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");

        testController.reconcile(appCluster, context);
        List<Tuple2<String, JobStatusMessage>> jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        // Upgrade job
        appCluster.getSpec().getJob().setParallelism(100);

        UpdateControl<FlinkDeployment> updateControl =
                testController.reconcile(appCluster, context);
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
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING
                        .rescheduleAfter(appCluster, configManager.getOperatorConfiguration())
                        .toMillis(),
                updateControl.getScheduleDelay().get());

        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals(null, jobs.get(0).f0);

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        // Suspend job
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, context);

        // Resume from empty state
        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals(null, jobs.get(0).f0);

        // Inject validation error in the middle of the upgrade
        appCluster.getSpec().setRestartNonce(123L);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobState.SUSPENDED,
                appCluster
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        appCluster.getSpec().setLogConfiguration(Map.of("invalid", "conf"));
        testController.reconcile(appCluster, TestUtils.createEmptyContext());
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING.name(),
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
    @EnumSource(FlinkVersion.class)
    public void testUpgradeNotReadyClusterSession(FlinkVersion flinkVersion) {
        testUpgradeNotReadyCluster(TestUtils.buildSessionCluster(flinkVersion));
    }

    @ParameterizedTest
    @MethodSource("applicationTestParams")
    public void testUpgradeNotReadyClusterApplication(
            FlinkVersion flinkVersion, UpgradeMode upgradeMode) {
        var appCluster = TestUtils.buildApplicationCluster(flinkVersion);
        appCluster.getSpec().getJob().setUpgradeMode(upgradeMode);
        testUpgradeNotReadyCluster(appCluster);
    }

    @Test
    public void verifyReconcileWithBadConfig() {

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
        assertEquals(
                "JobManager replicas should not be configured less than one.",
                appCluster.getStatus().getError());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // Make sure we do validation before getting effective config in reconcile().
        appCluster.getSpec().getJobManager().setReplicas(1);
        appCluster.getSpec().getJob().setJarURI(null);
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
    public void verifyReconcileWithAChangedOperatorMode() {

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
        assertEquals(
                org.apache.flink.api.common.JobStatus.RECONCILING.name(), jobStatus.getState());

        // Switches operator mode to SESSION
        appCluster.getSpec().setJob(null);
        // Validation fails and JobObserver should still be used
        updateControl = testController.reconcile(appCluster, context);
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertNotNull(ReconciliationUtils.getDeployedSpec(appCluster).getJob());
        // Verify jobStatus is running
        jobStatus = appCluster.getStatus().getJobStatus();
        JobStatusMessage expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState().toString(), jobStatus.getState());
    }

    private void testUpgradeNotReadyCluster(FlinkDeployment appCluster) {
        flinkService.clear();
        testController.reconcile(appCluster, context);
        assertEquals(
                appCluster.getSpec(),
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
        assertEquals(
                appCluster.getSpec(),
                appCluster.getStatus().getReconciliationStatus().deserializeLastReconciledSpec());

        flinkService.setPortReady(true);
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        if (appCluster.getSpec().getJob() != null) {
            assertEquals(
                    org.apache.flink.api.common.JobStatus.RUNNING.name(),
                    appCluster.getStatus().getJobStatus().getState());
        } else {
            assertEquals(
                    org.apache.flink.api.common.JobStatus.FINISHED.name(),
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
                org.apache.flink.api.common.JobStatus.RUNNING.name(),
                appCluster.getStatus().getJobStatus().getState());
        assertEquals(
                JobManagerDeploymentStatus.READY,
                appCluster.getStatus().getJobManagerDeploymentStatus());
    }

    @Test
    public void testPrepareEventSource() {
        // Test watch all
        testController.setEffectiveNamespaces(Collections.emptySet());
        List<EventSource> eventSources = testController.prepareEventSources(null);
        assertEquals(1, eventSources.size());
        assertEquals("all", eventSources.get(0).name());

        // Test watch namespaces
        Set<String> namespaces = Set.of("ns1", "ns2", "ns3");
        testController.setEffectiveNamespaces(namespaces);
        eventSources = testController.prepareEventSources(null);
        assertEquals(3, eventSources.size());
        assertEquals(
                namespaces,
                eventSources.stream().map(EventSource::name).collect(Collectors.toSet()));
    }

    @Test
    public void testSuccessfulObservationShouldClearErrors() {
        final String crashLoopMessage = "deploy errors";
        flinkService.setJmPodList(TestUtils.createFailedPodList(crashLoopMessage));

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, TestUtils.createContextWithInProgressDeployment());

        assertNull(appCluster.getStatus().getReconciliationStatus().getLastStableSpec());

        // Failed JobManager deployment should set errors to the status
        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());
        assertEquals(crashLoopMessage, appCluster.getStatus().getError());

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
