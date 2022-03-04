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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.reconciler.FlinkReconcilerFactory;
import org.apache.flink.kubernetes.operator.reconciler.JobReconcilerTest;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.validation.DefaultDeploymentValidator;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** @link JobStatusObserver unit tests */
public class FlinkDeploymentControllerTest {

    private final Context context = JobReconcilerTest.createContextWithReadyJobManagerDeployment();
    private final FlinkOperatorConfiguration operatorConfiguration =
            FlinkOperatorConfiguration.fromConfiguration(new Configuration());

    private TestingFlinkService flinkService;
    private FlinkDeploymentController testController;

    private KubernetesMockServer mockServer;
    private NamespacedKubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        flinkService = new TestingFlinkService();
        mockServer = new KubernetesMockServer(new MockWebServer(), new HashMap<>(), true);
        mockServer.init();
        kubernetesClient = mockServer.createClient();
        testController = createTestController(kubernetesClient, flinkService);
    }

    @AfterEach
    public void tearDown() {
        kubernetesClient.close();
        mockServer.shutdown();
    }

    @Test
    public void verifyBasicReconcileLoop() {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();

        UpdateControl<FlinkDeployment> updateControl;

        updateControl = testController.reconcile(appCluster, TestUtils.createEmptyContext());
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING
                        .toUpdateControl(appCluster, operatorConfiguration)
                        .getScheduleDelay(),
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
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY
                        .toUpdateControl(appCluster, operatorConfiguration)
                        .getScheduleDelay(),
                updateControl.getScheduleDelay());

        updateControl = testController.reconcile(appCluster, context);
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                JobManagerDeploymentStatus.READY
                        .toUpdateControl(appCluster, operatorConfiguration)
                        .getScheduleDelay(),
                updateControl.getScheduleDelay());

        // Validate job status
        JobStatus jobStatus = appCluster.getStatus().getJobStatus();
        JobStatusMessage expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState().toString(), jobStatus.getState());

        // Send in invalid update
        appCluster = TestUtils.clone(appCluster);
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
    public void verifyUpgradeFromSavepoint() {
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");

        testController.reconcile(appCluster, TestUtils.createEmptyContext());
        List<Tuple2<String, JobStatusMessage>> jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);

        List<Tuple2<String, JobStatusMessage>> previousJobs = new ArrayList<>(jobs);
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setInitialSavepointPath("s1");

        // Send in a no-op change
        testController.reconcile(appCluster, context);
        assertEquals(previousJobs, new ArrayList<>(flinkService.listJobs()));

        // Upgrade job
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setParallelism(100);

        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_0", jobs.get(0).f0);
        testController.reconcile(appCluster, context);

        // Suspend job
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.MISSING,
                appCluster.getStatus().getJobManagerDeploymentStatus());

        // Resume from last savepoint
        appCluster = TestUtils.clone(appCluster);
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

        // Upgrade job
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setParallelism(100);

        UpdateControl<FlinkDeployment> updateControl =
                testController.reconcile(appCluster, context);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY
                        .toUpdateControl(appCluster, operatorConfiguration)
                        .getScheduleDelay(),
                updateControl.getScheduleDelay());
        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals(null, jobs.get(0).f0);

        // Suspend job
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, context);

        // Resume from empty state
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(appCluster, context);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals(null, jobs.get(0).f0);
    }

    @Test
    public void testPrepareEventSource() {
        // Test watch all
        testController.setControllerConfig(
                new FlinkControllerConfig(testController) {
                    @Override
                    public Set<String> getEffectiveNamespaces() {
                        return Set.of();
                    }
                });
        List<EventSource> eventSources = testController.prepareEventSources(null);
        assertEquals(1, eventSources.size());
        assertTrue(eventSources.get(0).name().endsWith("-all"));

        // Test watch namespaces
        Set<String> namespaces = Set.of("ns1", "ns2", "ns3");
        testController.setControllerConfig(
                new FlinkControllerConfig(testController) {
                    @Override
                    public Set<String> getEffectiveNamespaces() {
                        return namespaces;
                    }
                });
        eventSources = testController.prepareEventSources(null);
        assertEquals(3, eventSources.size());
        assertEquals(
                namespaces,
                eventSources.stream()
                        .map(EventSource::name)
                        .map(s -> s.substring(s.length() - 3))
                        .collect(Collectors.toSet()));
    }

    private FlinkDeploymentController createTestController(
            KubernetesClient kubernetesClient, TestingFlinkService flinkService) {
        Observer observer = new Observer(flinkService);

        FlinkDeploymentController controller =
                new FlinkDeploymentController(
                        FlinkUtils.loadDefaultConfig(),
                        operatorConfiguration,
                        kubernetesClient,
                        "test",
                        new DefaultDeploymentValidator(),
                        observer,
                        new FlinkReconcilerFactory(
                                kubernetesClient, flinkService, operatorConfiguration));
        controller.setControllerConfig(new FlinkControllerConfig(controller));
        return controller;
    }
}
