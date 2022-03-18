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
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.observer.ObserverFactory;
import org.apache.flink.kubernetes.operator.reconciler.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.validation.DefaultDeploymentValidator;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link JobStatusObserver unit tests */
@EnableKubernetesMockClient(crud = true)
public class FlinkDeploymentControllerTest {

    private final Context context = TestUtils.createContextWithReadyJobManagerDeployment();
    private final FlinkOperatorConfiguration operatorConfiguration =
            new FlinkOperatorConfiguration(
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(3),
                    Duration.ofSeconds(4),
                    null,
                    Collections.emptySet());

    private TestingFlinkService flinkService;
    private FlinkDeploymentController testController;

    private KubernetesMockServer mockServer;
    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService();
        testController = createTestController(kubernetesClient, flinkService);
    }

    @Test
    public void verifyBasicReconcileLoop() {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();

        UpdateControl<FlinkDeployment> updateControl;

        updateControl = testController.reconcile(appCluster, TestUtils.createEmptyContext());
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(operatorConfiguration.getProgressCheckInterval().toMillis()),
                updateControl.getScheduleDelay());

        // Validate reconciliation status
        ReconciliationStatus reconciliationStatus =
                appCluster.getStatus().getReconciliationStatus();
        assertTrue(reconciliationStatus.isSuccess());
        assertNull(reconciliationStatus.getError());
        assertEquals(appCluster.getSpec(), reconciliationStatus.getLastReconciledSpec());

        updateControl = testController.reconcile(appCluster, context);
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(operatorConfiguration.getRestApiReadyDelay().toMillis()),
                updateControl.getScheduleDelay());

        updateControl = testController.reconcile(appCluster, context);
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(operatorConfiguration.getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        // Validate job status
        JobStatus jobStatus = appCluster.getStatus().getJobStatus();
        JobStatusMessage expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState().toString(), jobStatus.getState());

        // Send in invalid update
        appCluster = ReconciliationUtils.clone(appCluster);
        appCluster.getSpec().setJob(null);
        updateControl = testController.reconcile(appCluster, context);
        assertTrue(updateControl.isUpdateStatus());
        assertFalse(updateControl.getScheduleDelay().isPresent());

        reconciliationStatus = appCluster.getStatus().getReconciliationStatus();
        assertFalse(reconciliationStatus.isSuccess());
        assertEquals("Cannot switch from job to session cluster", reconciliationStatus.getError());
        assertNotNull(reconciliationStatus.getLastReconciledSpec().getJob());

        // Validate job status correct even with error
        jobStatus = appCluster.getStatus().getJobStatus();
        expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState().toString(), jobStatus.getState());
    }

    @Test
    public void verifyFailedDeployment() throws Exception {
        mockServer
                .expect()
                .post()
                .withPath("/api/v1/namespaces/flink-operator-test/events")
                .andReturn(
                        HttpURLConnection.HTTP_CREATED,
                        new EventBuilder().withNewMetadata().endMetadata().build())
                .once();

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        UpdateControl<FlinkDeployment> updateControl;

        testController.reconcile(appCluster, TestUtils.createEmptyContext());
        updateControl =
                testController.reconcile(
                        appCluster, TestUtils.createContextWithFailedJobManagerDeployment());
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(operatorConfiguration.getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        RecordedRequest recordedRequest = mockServer.getLastRequest();
        assertEquals("POST", recordedRequest.getMethod());
        assertTrue(recordedRequest.getBody().readUtf8().contains(TestUtils.DEPLOYMENT_ERROR));
        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // Validate reconciliation status
        ReconciliationStatus reconciliationStatus =
                appCluster.getStatus().getReconciliationStatus();
        assertFalse(reconciliationStatus.isSuccess());
        assertNotNull(reconciliationStatus.getError());

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
                        .rescheduleAfter(appCluster, operatorConfiguration)
                        .toMillis(),
                updateControl.getScheduleDelay().get());
    }

    @Test
    public void verifyInProgressDeploymentWithCrashLoopBackoff() throws Exception {
        mockServer
                .expect()
                .post()
                .withPath("/api/v1/namespaces/flink-operator-test/events")
                .andReturn(
                        HttpURLConnection.HTTP_CREATED,
                        new EventBuilder().withNewMetadata().endMetadata().build())
                .once();

        String crashLoopMessage = "container fails";
        ContainerStatus cs =
                new ContainerStatusBuilder()
                        .withNewState()
                        .withNewWaiting()
                        .withReason(DeploymentFailedException.REASON_CRASH_LOOP_BACKOFF)
                        .withMessage(crashLoopMessage)
                        .endWaiting()
                        .endState()
                        .build();

        Pod pod = TestUtils.getTestPod("host", "apiVersion", Collections.emptyList());
        pod.setStatus(
                new PodStatusBuilder()
                        .withContainerStatuses(Collections.singletonList(cs))
                        .build());
        flinkService.setJmPodList(new PodListBuilder().withItems(pod).build());

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        UpdateControl<FlinkDeployment> updateControl;

        testController.reconcile(appCluster, TestUtils.createEmptyContext());
        updateControl =
                testController.reconcile(
                        appCluster, TestUtils.createContextWithInProgressDeployment());
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                Optional.of(operatorConfiguration.getReconcileInterval().toMillis()),
                updateControl.getScheduleDelay());

        RecordedRequest recordedRequest = mockServer.getLastRequest();
        assertEquals("POST", recordedRequest.getMethod());
        String recordedRequestBody = recordedRequest.getBody().readUtf8();
        assertTrue(
                recordedRequestBody.contains(DeploymentFailedException.REASON_CRASH_LOOP_BACKOFF));
        assertTrue(recordedRequestBody.contains(crashLoopMessage));
        assertEquals(
                JobManagerDeploymentStatus.ERROR,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // Validate reconciliation status
        ReconciliationStatus reconciliationStatus =
                appCluster.getStatus().getReconciliationStatus();
        assertFalse(reconciliationStatus.isSuccess());

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
                        .rescheduleAfter(appCluster, operatorConfiguration)
                        .toMillis(),
                updateControl.getScheduleDelay().get());
    }

    @Test
    public void verifyUpgradeFromSavepoint() {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");

        testController.reconcile(appCluster, TestUtils.createEmptyContext());
        List<Tuple2<String, JobStatusMessage>> jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);

        List<Tuple2<String, JobStatusMessage>> previousJobs = new ArrayList<>(jobs);
        appCluster = ReconciliationUtils.clone(appCluster);
        appCluster.getSpec().getJob().setInitialSavepointPath("s1");

        // Send in a no-op change
        testController.reconcile(appCluster, context);
        assertEquals(previousJobs, new ArrayList<>(flinkService.listJobs()));

        // Upgrade job
        appCluster = ReconciliationUtils.clone(appCluster);
        appCluster.getSpec().getJob().setParallelism(100);

        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_0", jobs.get(0).f0);
        testController.reconcile(appCluster, context);

        // Suspend job
        appCluster = ReconciliationUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.MISSING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // Resume from last savepoint
        appCluster = ReconciliationUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(appCluster, TestUtils.createEmptyContext());
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_1", jobs.get(0).f0);

        testController.reconcile(appCluster, context);
        testController.cleanup(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(0, jobs.size());
    }

    @Test
    public void verifyStatelessUpgrade() {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");

        testController.reconcile(appCluster, TestUtils.createEmptyContext());
        List<Tuple2<String, JobStatusMessage>> jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        // Upgrade job
        appCluster = ReconciliationUtils.clone(appCluster);
        appCluster.getSpec().getJob().setParallelism(100);

        UpdateControl<FlinkDeployment> updateControl =
                testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING
                        .rescheduleAfter(appCluster, operatorConfiguration)
                        .toMillis(),
                updateControl.getScheduleDelay().get());
        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals(null, jobs.get(0).f0);

        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);

        // Suspend job
        appCluster = ReconciliationUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, context);

        // Resume from empty state
        appCluster = ReconciliationUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals(null, jobs.get(0).f0);
    }

    @Test
    public void testUpgradeNotReadyCluster() {
        testUpgradeNotReadyCluster(TestUtils.buildSessionCluster(), true);

        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        testUpgradeNotReadyCluster(appCluster, true);

        appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        testUpgradeNotReadyCluster(appCluster, true);

        appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        testUpgradeNotReadyCluster(appCluster, false);
    }

    public void testUpgradeNotReadyCluster(FlinkDeployment appCluster, boolean allowUpgrade) {
        testController.reconcile(appCluster, TestUtils.createEmptyContext());
        assertEquals(
                appCluster.getSpec(),
                appCluster.getStatus().getReconciliationStatus().getLastReconciledSpec());

        flinkService.setPortReady(false);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // trigger change
        appCluster.getSpec().setServiceAccount(appCluster.getSpec().getServiceAccount() + "-2");

        // Verify that even in DEPLOYING state we still redeploy
        testController.reconcile(appCluster, context);
        testController.reconcile(appCluster, context);
        if (allowUpgrade) {
            assertEquals(
                    JobManagerDeploymentStatus.DEPLOYING,
                    appCluster.getStatus().getJobManagerDeploymentStatus());
            assertEquals(
                    appCluster.getSpec(),
                    appCluster.getStatus().getReconciliationStatus().getLastReconciledSpec());

            flinkService.setPortReady(true);
            testController.reconcile(appCluster, context);
            testController.reconcile(appCluster, context);

            if (appCluster.getSpec().getJob() != null) {
                assertEquals("RUNNING", appCluster.getStatus().getJobStatus().getState());
            } else {
                assertNull(appCluster.getStatus().getJobStatus().getState());
            }
            assertEquals(
                    JobManagerDeploymentStatus.READY,
                    appCluster.getStatus().getJobManagerDeploymentStatus());
        } else {
            assertEquals(
                    JobManagerDeploymentStatus.DEPLOYING,
                    appCluster.getStatus().getJobManagerDeploymentStatus());
            assertNotEquals(
                    appCluster.getSpec(),
                    appCluster.getStatus().getReconciliationStatus().getLastReconciledSpec());

            flinkService.setPortReady(true);
            testController.reconcile(appCluster, context);
            testController.reconcile(appCluster, context);

            assertEquals(
                    JobManagerDeploymentStatus.DEPLOYING,
                    appCluster.getStatus().getJobManagerDeploymentStatus());
            assertEquals(
                    appCluster.getSpec(),
                    appCluster.getStatus().getReconciliationStatus().getLastReconciledSpec());

            testController.reconcile(appCluster, context);
            assertEquals(
                    JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                    appCluster.getStatus().getJobManagerDeploymentStatus());

            testController.reconcile(appCluster, context);

            assertEquals("RUNNING", appCluster.getStatus().getJobStatus().getState());
            assertEquals(
                    JobManagerDeploymentStatus.READY,
                    appCluster.getStatus().getJobManagerDeploymentStatus());
        }
    }

    @Test
    public void testPrepareEventSource() {
        // Test watch all
        testController.setControllerConfig(
                new FlinkControllerConfig(testController, Collections.emptySet()));
        List<EventSource> eventSources = testController.prepareEventSources(null);
        assertEquals(1, eventSources.size());
        assertEquals("all", eventSources.get(0).name());

        // Test watch namespaces
        Set<String> namespaces = Set.of("ns1", "ns2", "ns3");
        testController.setControllerConfig(new FlinkControllerConfig(testController, namespaces));
        eventSources = testController.prepareEventSources(null);
        assertEquals(3, eventSources.size());
        assertEquals(
                namespaces,
                eventSources.stream().map(EventSource::name).collect(Collectors.toSet()));
    }

    private FlinkDeploymentController createTestController(
            KubernetesClient kubernetesClient, TestingFlinkService flinkService) {

        FlinkDeploymentController controller =
                new FlinkDeploymentController(
                        FlinkUtils.loadDefaultConfig(),
                        operatorConfiguration,
                        kubernetesClient,
                        "test",
                        new DefaultDeploymentValidator(),
                        new ReconcilerFactory(
                                kubernetesClient, flinkService, operatorConfiguration),
                        new ObserverFactory(flinkService, operatorConfiguration));
        controller.setControllerConfig(
                new FlinkControllerConfig(controller, Collections.emptySet()));
        return controller;
    }
}
