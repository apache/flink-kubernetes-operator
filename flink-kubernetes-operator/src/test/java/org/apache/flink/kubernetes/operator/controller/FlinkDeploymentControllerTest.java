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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.reconciler.JobReconciler;
import org.apache.flink.kubernetes.operator.reconciler.SessionReconciler;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** @link JobStatusObserver unit tests */
public class FlinkDeploymentControllerTest {

    @Test
    public void verifyBasicReconcileLoop() {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkDeploymentController testController = createTestController(flinkService);
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();

        UpdateControl<FlinkDeployment> updateControl;

        updateControl = testController.reconcile(appCluster, null);
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                FlinkDeploymentController.REFRESH_SECONDS * 1000,
                (long) updateControl.getScheduleDelay().get());

        // Validate reconciliation status
        ReconciliationStatus reconciliationStatus =
                appCluster.getStatus().getReconciliationStatus();
        assertTrue(reconciliationStatus.isSuccess());
        assertNull(reconciliationStatus.getError());
        assertEquals(appCluster.getSpec(), reconciliationStatus.getLastReconciledSpec());

        updateControl = testController.reconcile(appCluster, null);
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                FlinkDeploymentController.REFRESH_SECONDS * 1000,
                (long) updateControl.getScheduleDelay().get());

        // Validate job status
        JobStatus jobStatus = appCluster.getStatus().getJobStatus();
        JobStatusMessage expectedJobStatus = flinkService.listJobs().get(0).f1;
        assertEquals(expectedJobStatus.getJobId().toHexString(), jobStatus.getJobId());
        assertEquals(expectedJobStatus.getJobName(), jobStatus.getJobName());
        assertEquals(expectedJobStatus.getJobState().toString(), jobStatus.getState().toString());

        // Send in invalid update
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().setJob(null);
        updateControl = testController.reconcile(appCluster, null);
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
        assertEquals(expectedJobStatus.getJobState().toString(), jobStatus.getState().toString());
    }

    @Test
    public void verifyUpgradeFromSavepoint() {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkDeploymentController testController = createTestController(flinkService);
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");

        testController.reconcile(appCluster, null);
        List<Tuple2<String, JobStatusMessage>> jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);

        List<Tuple2<String, JobStatusMessage>> previousJobs = new ArrayList<>(jobs);
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setInitialSavepointPath("s1");

        // Send in a no-op change
        testController.reconcile(appCluster, null);
        assertEquals(previousJobs, new ArrayList<>(flinkService.listJobs()));

        // Upgrade job
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setParallelism(100);

        testController.reconcile(appCluster, null);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_0", jobs.get(0).f0);

        // Suspend job
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, null);

        // Resume from last savepoint
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(appCluster, null);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("savepoint_1", jobs.get(0).f0);
    }

    @Test
    public void verifyStatelessUpgrade() {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkDeploymentController testController = createTestController(flinkService);
        FlinkDeployment appCluster = TestUtils.buildApplicationCluster();
        appCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        appCluster.getSpec().getJob().setInitialSavepointPath("s0");

        testController.reconcile(appCluster, null);
        List<Tuple2<String, JobStatusMessage>> jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals("s0", jobs.get(0).f0);

        // Upgrade job
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setParallelism(100);

        testController.reconcile(appCluster, null);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals(null, jobs.get(0).f0);

        // Suspend job
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        testController.reconcile(appCluster, null);

        // Resume from empty state
        appCluster = TestUtils.clone(appCluster);
        appCluster.getSpec().getJob().setState(JobState.RUNNING);
        testController.reconcile(appCluster, null);
        jobs = flinkService.listJobs();
        assertEquals(1, jobs.size());
        assertEquals(null, jobs.get(0).f0);
    }

    private FlinkDeploymentController createTestController(TestingFlinkService flinkService) {
        JobStatusObserver observer = new JobStatusObserver(flinkService);
        JobReconciler jobReconciler = new JobReconciler(null, flinkService);
        SessionReconciler sessionReconciler = new SessionReconciler(null, flinkService);

        return new FlinkDeploymentController(
                FlinkUtils.loadDefaultConfig(),
                null,
                "test",
                observer,
                jobReconciler,
                sessionReconciler);
    }
}
