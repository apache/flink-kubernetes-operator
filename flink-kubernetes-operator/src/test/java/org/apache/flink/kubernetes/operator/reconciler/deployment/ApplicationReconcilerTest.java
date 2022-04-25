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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link JobStatusObserver unit tests */
public class ApplicationReconcilerTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @Test
    public void testUpgrade() throws Exception {
        Context context = TestUtils.createContextWithReadyJobManagerDeployment();
        TestingFlinkService flinkService = new TestingFlinkService();

        ApplicationReconciler reconciler =
                new ApplicationReconciler(null, flinkService, configManager);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        List<Tuple2<String, JobStatusMessage>> runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        // Test stateless upgrade
        FlinkDeployment statelessUpgrade = ReconciliationUtils.clone(deployment);
        statelessUpgrade.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        statelessUpgrade.getSpec().getFlinkConfiguration().put("new", "conf");
        reconciler.reconcile(statelessUpgrade, context);

        runningJobs = flinkService.listJobs();
        assertEquals(0, runningJobs.size());

        reconciler.reconcile(statelessUpgrade, context);

        runningJobs = flinkService.listJobs();
        assertEquals(1, runningJobs.size());
        assertNull(runningJobs.get(0).f0);

        deployment
                .getStatus()
                .getJobStatus()
                .setJobId(runningJobs.get(0).f1.getJobId().toHexString());

        // Test stateful upgrade
        FlinkDeployment statefulUpgrade = ReconciliationUtils.clone(deployment);
        statefulUpgrade.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        statefulUpgrade.getSpec().getFlinkConfiguration().put("new", "conf2");

        reconciler.reconcile(statefulUpgrade, context);

        runningJobs = flinkService.listJobs();
        assertEquals(0, runningJobs.size());

        reconciler.reconcile(statefulUpgrade, context);

        runningJobs = flinkService.listJobs();
        assertEquals(1, runningJobs.size());
        assertEquals("savepoint_0", runningJobs.get(0).f0);
    }

    @Test
    public void testUpgradeModeChangeFromSavepointToLastState() throws Exception {
        final String expectedSavepointPath = "savepoint_0";
        final Context context = TestUtils.createContextWithReadyJobManagerDeployment();
        final TestingFlinkService flinkService = new TestingFlinkService();

        final ApplicationReconciler reconciler =
                new ApplicationReconciler(null, flinkService, configManager);
        final FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        List<Tuple2<String, JobStatusMessage>> runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        // Suspend FlinkDeployment with savepoint upgrade mode
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        deployment.getSpec().getJob().setState(JobState.SUSPENDED);
        deployment.getSpec().setImage("new-image-1");

        reconciler.reconcile(deployment, context);
        assertEquals(0, flinkService.listJobs().size());
        assertTrue(
                JobState.SUSPENDED
                        .name()
                        .equalsIgnoreCase(deployment.getStatus().getJobStatus().getState()));

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
        assertEquals(1, runningJobs.size());
        assertEquals(expectedSavepointPath, runningJobs.get(0).f0);
    }

    @Test
    public void triggerSavepoint() throws Exception {
        Context context = TestUtils.createContextWithReadyJobManagerDeployment();
        TestingFlinkService flinkService = new TestingFlinkService();
        ApplicationReconciler reconciler =
                new ApplicationReconciler(null, flinkService, configManager);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        List<Tuple2<String, JobStatusMessage>> runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);
        assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        // trigger savepoint
        FlinkDeployment spDeployment = ReconciliationUtils.clone(deployment);

        // don't trigger if nonce is missing
        reconciler.reconcile(spDeployment, context);
        assertNull(spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        // trigger when nonce is defined
        spDeployment
                .getSpec()
                .getJob()
                .setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(spDeployment, context);
        assertEquals(
                "trigger_0",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        // don't trigger when savepoint is in progress
        reconciler.reconcile(spDeployment, context);
        assertEquals(
                "trigger_0",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        spDeployment.getStatus().getJobStatus().getSavepointInfo().setTriggerId(null);

        // don't trigger when nonce is the same
        reconciler.reconcile(spDeployment, context);
        assertNull(spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        spDeployment.getStatus().getJobStatus().getSavepointInfo().setTriggerId(null);

        // trigger when new nonce is defined
        spDeployment
                .getSpec()
                .getJob()
                .setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(spDeployment, context);
        assertEquals(
                "trigger_1",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());

        spDeployment.getStatus().getJobStatus().getSavepointInfo().setTriggerId(null);

        // don't trigger nonce is cleared
        spDeployment.getSpec().getJob().setSavepointTriggerNonce(null);
        reconciler.reconcile(spDeployment, context);
        assertNull(spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
    }

    @Test
    public void testUpgradeModeChangedToLastStateShouldTriggerSavepointWhileHADisabled()
            throws Exception {
        final Context context = TestUtils.createContextWithReadyJobManagerDeployment();
        final TestingFlinkService flinkService = new TestingFlinkService();

        final ApplicationReconciler reconciler =
                new ApplicationReconciler(null, flinkService, configManager);
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
        final List<Tuple2<String, JobStatusMessage>> runningJobs = flinkService.listJobs();
        assertEquals(expectedSavepointPath, runningJobs.get(0).f0);
    }

    @Test
    public void testUpgradeModeChangedToLastStateShouldNotTriggerSavepointWhileHAEnabled()
            throws Exception {
        final Context context = TestUtils.createContextWithReadyJobManagerDeployment();
        final TestingFlinkService flinkService = new TestingFlinkService();

        final ApplicationReconciler reconciler =
                new ApplicationReconciler(null, flinkService, configManager);
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

    @Test
    public void triggerRestart() throws Exception {
        Context context = TestUtils.createContextWithReadyJobManagerDeployment();
        TestingFlinkService flinkService = new TestingFlinkService();

        ApplicationReconciler reconciler =
                new ApplicationReconciler(null, flinkService, configManager);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        List<Tuple2<String, JobStatusMessage>> runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        // Test restart job
        FlinkDeployment restartJob = ReconciliationUtils.clone(deployment);
        restartJob.getSpec().setRestartNonce(1L);
        reconciler.reconcile(restartJob, context);
        assertEquals(
                JobState.SUSPENDED,
                restartJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        runningJobs = flinkService.listJobs();
        assertEquals(0, runningJobs.size());

        reconciler.reconcile(restartJob, context);
        assertEquals(
                JobState.RUNNING,
                restartJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        runningJobs = flinkService.listJobs();
        assertEquals(1, runningJobs.size());
        assertEquals(
                1L,
                restartJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getRestartNonce());
    }

    private void verifyAndSetRunningJobsToStatus(
            FlinkDeployment deployment, List<Tuple2<String, JobStatusMessage>> runningJobs) {
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

    @Test
    public void testJobUpgradeIgnorePendingSavepoint() throws Exception {
        Context context = TestUtils.createContextWithReadyJobManagerDeployment();
        TestingFlinkService flinkService = new TestingFlinkService();
        ApplicationReconciler reconciler =
                new ApplicationReconciler(null, flinkService, configManager);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        reconciler.reconcile(deployment, context);
        List<Tuple2<String, JobStatusMessage>> runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        FlinkDeployment spDeployment = ReconciliationUtils.clone(deployment);
        spDeployment
                .getSpec()
                .getJob()
                .setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(spDeployment, context);
        assertEquals(
                "trigger_0",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertEquals(JobState.RUNNING.name(), spDeployment.getStatus().getJobStatus().getState());

        // Force upgrade when savepoint is in progress.
        spDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        KubernetesOperatorConfigOptions.JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT.key(),
                        "true");
        reconciler.reconcile(spDeployment, context);
        assertEquals(
                "trigger_0",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertEquals(JobState.SUSPENDED.name(), spDeployment.getStatus().getJobStatus().getState());
    }
}
