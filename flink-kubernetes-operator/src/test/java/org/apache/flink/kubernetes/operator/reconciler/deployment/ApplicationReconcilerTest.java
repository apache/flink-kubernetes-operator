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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthEvaluator;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.TestReconcilerAdapter;
import org.apache.flink.kubernetes.operator.service.NativeFlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.SavepointStatus;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.commons.util.StringUtils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;
import static org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractFlinkResourceReconciler.MSG_SUBMIT;
import static org.apache.flink.kubernetes.operator.reconciler.deployment.ApplicationReconciler.MSG_RECOVERY;
import static org.apache.flink.kubernetes.operator.reconciler.deployment.ApplicationReconciler.MSG_RESTART_UNHEALTHY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** @link JobStatusObserver unit tests */
@EnableKubernetesMockClient(crud = true)
public class ApplicationReconcilerTest extends OperatorTestBase {

    private TestReconcilerAdapter<FlinkDeployment, FlinkDeploymentSpec, FlinkDeploymentStatus>
            reconciler;

    @Getter private KubernetesClient kubernetesClient;
    private ApplicationReconciler appReconciler;

    private FlinkOperatorConfiguration operatorConfig;
    private ExecutorService executorService;

    @Override
    public void setup() {
        appReconciler =
                new ApplicationReconciler(
                        kubernetesClient,
                        eventRecorder,
                        statusRecorder,
                        new NoopJobAutoscalerFactory());
        reconciler = new TestReconcilerAdapter<>(this, appReconciler);
        operatorConfig = configManager.getOperatorConfiguration();
        executorService = Executors.newDirectExecutorService();
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void testSubmitAndCleanUpWithSavepoint(FlinkVersion flinkVersion) throws Exception {
        var conf = configManager.getDefaultConfig();
        conf.set(KubernetesOperatorConfigOptions.SAVEPOINT_ON_DELETION, true);
        configManager.updateDefaultConfig(conf);

        FlinkDeployment deployment = TestUtils.buildApplicationCluster(flinkVersion);

        // session ready
        reconciler.reconcile(deployment, TestUtils.createContextWithReadyFlinkDeployment());
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        // clean up
        assertEquals(
                null, deployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint());
        reconciler.cleanup(deployment, TestUtils.createContextWithReadyFlinkDeployment());
        assertEquals(
                "savepoint_0",
                deployment
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getLocation());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void testUpgrade(FlinkVersion flinkVersion) throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster(flinkVersion);

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);
        assertTrue(
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpecWithMeta()
                        .getMeta()
                        .isFirstDeployment());

        JobID jobId = runningJobs.get(0).f1.getJobId();
        verifyJobId(deployment, runningJobs.get(0).f1, runningJobs.get(0).f2, jobId);

        // Test stateless upgrade
        FlinkDeployment statelessUpgrade = ReconciliationUtils.clone(deployment);
        statelessUpgrade.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        statelessUpgrade.getSpec().getFlinkConfiguration().put("new", "conf");
        reconciler.reconcile(statelessUpgrade, context);
        assertFalse(
                statelessUpgrade
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpecWithMeta()
                        .getMeta()
                        .isFirstDeployment());

        assertEquals(0, flinkService.getRunningCount());

        reconciler.reconcile(statelessUpgrade, context);
        assertFalse(
                statelessUpgrade
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpecWithMeta()
                        .getMeta()
                        .isFirstDeployment());
        assertEquals(null, SavepointUtils.getLastSavepointStatus(statelessUpgrade));
        runningJobs = flinkService.listJobs();
        assertEquals(1, flinkService.getRunningCount());
        assertNull(runningJobs.get(0).f0);

        assertNotEquals(
                runningJobs.get(0).f2.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID), jobId);
        jobId = runningJobs.get(0).f1.getJobId();

        deployment.getStatus().getJobStatus().setJobId(jobId.toHexString());

        // Test stateful upgrade
        FlinkDeployment statefulUpgrade = ReconciliationUtils.clone(deployment);
        statefulUpgrade.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        statefulUpgrade.getSpec().getFlinkConfiguration().put("new", "conf2");

        reconciler.reconcile(statefulUpgrade, context);

        assertEquals(0, flinkService.getRunningCount());

        reconciler.reconcile(statefulUpgrade, context);

        runningJobs = flinkService.listJobs();
        assertEquals(1, flinkService.getRunningCount());
        assertEquals("savepoint_0", runningJobs.get(0).f0);
        assertEquals(
                SavepointTriggerType.UPGRADE,
                statefulUpgrade
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getLastSavepoint()
                        .getTriggerType());
        assertEquals(
                SavepointStatus.SUCCEEDED, SavepointUtils.getLastSavepointStatus(statefulUpgrade));
        verifyJobId(deployment, runningJobs.get(0).f1, runningJobs.get(0).f2, jobId);

        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().setRestartNonce(100L);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastStableSpec(
                        deployment.getStatus().getReconciliationStatus().getLastReconciledSpec());
        flinkService.setHaDataAvailable(false);
        deployment.getStatus().getJobStatus().setState("RECONCILING");

        try {
            deployment
                    .getStatus()
                    .setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
            reconciler.reconcile(deployment, context);
            fail();
        } catch (RecoveryFailureException expected) {
        }

        try {
            deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.ERROR);
            reconciler.reconcile(deployment, context);
            fail();
        } catch (RecoveryFailureException expected) {
        }

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
        reconciler.reconcile(deployment, context);
        reconciler.reconcile(deployment, context);

        assertEquals(1, flinkService.getRunningCount());
        assertEquals("finished_sp", runningJobs.get(0).f0);
        verifyJobId(deployment, runningJobs.get(0).f1, runningJobs.get(0).f2, jobId);
    }

    private void verifyJobId(
            FlinkDeployment deployment, JobStatusMessage status, Configuration conf, JobID jobId) {
        // jobId set by operator
        assertEquals(jobId, status.getJobId());
        assertEquals(conf.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID), jobId.toHexString());
    }

    @Test
    public void triggerSavepoint() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);
        assertFalse(SavepointUtils.savepointInProgress(deployment.getStatus().getJobStatus()));
        assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint());
        assertEquals(null, SavepointUtils.getLastSavepointStatus(deployment));

        FlinkDeployment spDeployment = ReconciliationUtils.clone(deployment);

        // don't trigger if nonce is missing
        reconciler.reconcile(spDeployment, context);
        assertFalse(SavepointUtils.savepointInProgress(spDeployment.getStatus().getJobStatus()));
        assertNull(spDeployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint());
        assertEquals(null, SavepointUtils.getLastSavepointStatus(spDeployment));

        // trigger when nonce is defined
        spDeployment
                .getSpec()
                .getJob()
                .setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(spDeployment, context);
        assertNull(
                spDeployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getSavepointTriggerNonce());
        assertEquals(
                "trigger_0",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertTrue(SavepointUtils.savepointInProgress(spDeployment.getStatus().getJobStatus()));
        assertEquals(SavepointStatus.PENDING, SavepointUtils.getLastSavepointStatus(spDeployment));

        // don't trigger when savepoint is in progress
        reconciler.reconcile(spDeployment, context);
        assertEquals(
                "trigger_0",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertEquals(
                SavepointTriggerType.MANUAL,
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerType());

        ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(
                spDeployment.getStatus().getJobStatus().getSavepointInfo(), spDeployment);
        spDeployment.getStatus().getJobStatus().getSavepointInfo().resetTrigger();

        // don't trigger when nonce is the same
        reconciler.reconcile(spDeployment, context);
        assertFalse(SavepointUtils.savepointInProgress(spDeployment.getStatus().getJobStatus()));
        assertEquals(null, SavepointUtils.getLastSavepointStatus(spDeployment));

        // trigger when new nonce is defined
        spDeployment
                .getSpec()
                .getJob()
                .setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(spDeployment, context);
        assertEquals(
                "trigger_1",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertEquals(
                SavepointTriggerType.MANUAL,
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerType());

        // re-trigger after reset
        spDeployment.getStatus().getJobStatus().getSavepointInfo().resetTrigger();
        reconciler.reconcile(spDeployment, context);
        assertEquals(
                "trigger_2",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertEquals(
                SavepointTriggerType.MANUAL,
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerType());
        //  reconciled and savepoint is updated
        ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(
                spDeployment.getStatus().getJobStatus().getSavepointInfo(), spDeployment);
        updateLastSavepoint(spDeployment);
        assertEquals(
                SavepointStatus.SUCCEEDED, SavepointUtils.getLastSavepointStatus(spDeployment));

        // re-trigger, reconciled but savepoint is not updated
        spDeployment
                .getSpec()
                .getJob()
                .setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(spDeployment, context);
        ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(
                spDeployment.getStatus().getJobStatus().getSavepointInfo(), spDeployment);
        spDeployment.getStatus().getJobStatus().getSavepointInfo().resetTrigger();
        assertEquals(
                SavepointStatus.ABANDONED, SavepointUtils.getLastSavepointStatus(spDeployment));

        // don't trigger when nonce is cleared
        spDeployment.getSpec().getJob().setSavepointTriggerNonce(null);
        reconciler.reconcile(spDeployment, context);
        assertFalse(SavepointUtils.savepointInProgress(spDeployment.getStatus().getJobStatus()));

        // trigger by periodic settings
        spDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL.key(), "1");
        reconciler.reconcile(spDeployment, context);
        assertTrue(SavepointUtils.savepointInProgress(spDeployment.getStatus().getJobStatus()));
        assertEquals(SavepointStatus.PENDING, SavepointUtils.getLastSavepointStatus(spDeployment));
    }

    private void updateLastSavepoint(FlinkDeployment deployment) {
        var savepointInfo = deployment.getStatus().getJobStatus().getSavepointInfo();
        var savepoint =
                new Savepoint(
                        savepointInfo.getTriggerTimestamp(),
                        // To make sure the new savepoint has a new path
                        savepointInfo.getTriggerId()
                                + savepointInfo.getTriggerTimestamp()
                                + savepointInfo.getTriggerId()
                                + deployment.getSpec().getJob().getSavepointTriggerNonce(),
                        savepointInfo.getTriggerType(),
                        savepointInfo.getFormatType(),
                        SavepointTriggerType.MANUAL == savepointInfo.getTriggerType()
                                ? deployment.getSpec().getJob().getSavepointTriggerNonce()
                                : null);
        savepointInfo.updateLastSavepoint(savepoint);
    }

    @Test
    public void triggerRestart() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
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
        assertEquals(0, flinkService.getRunningCount());

        reconciler.reconcile(restartJob, context);
        assertEquals(
                JobState.RUNNING,
                restartJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertEquals(1, flinkService.getRunningCount());
        assertEquals(
                1L,
                restartJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getRestartNonce());
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
                                .updateTime(Long.toString(System.currentTimeMillis()))
                                .state("RUNNING")
                                .build());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
    }

    @Test
    public void testJobUpgradeIgnorePendingSavepoint() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
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
        spDeployment.getSpec().setImage("flink:greatest");
        reconciler.reconcile(spDeployment, context);
        assertEquals(
                "trigger_0",
                spDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED.name(),
                spDeployment.getStatus().getJobStatus().getState());
    }

    @Test
    public void testRandomJobResultStorePath() throws Exception {
        FlinkDeployment flinkApp = TestUtils.buildApplicationCluster();
        final String haStoragePath = "file:///flink-data/ha";
        flinkApp.getSpec()
                .getFlinkConfiguration()
                .put(HighAvailabilityOptions.HA_STORAGE_PATH.key(), haStoragePath);

        ObjectMeta deployMeta = flinkApp.getMetadata();
        FlinkDeploymentStatus status = flinkApp.getStatus();
        FlinkDeploymentSpec spec = flinkApp.getSpec();
        Configuration deployConfig = configManager.getDeployConfig(deployMeta, spec);

        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED.name());
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconciler
                .getReconciler()
                .deploy(getResourceContext(flinkApp), spec, deployConfig, Optional.empty(), false);

        String path1 = deployConfig.get(JobResultStoreOptions.STORAGE_PATH);
        Assertions.assertTrue(path1.startsWith(haStoragePath));

        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED.name());
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconciler
                .getReconciler()
                .deploy(getResourceContext(flinkApp), spec, deployConfig, Optional.empty(), false);
        String path2 = deployConfig.get(JobResultStoreOptions.STORAGE_PATH);
        Assertions.assertTrue(path2.startsWith(haStoragePath));
        assertNotEquals(path1, path2);
    }

    @Test
    public void testAlwaysSavepointOnFlinkVersionChange() throws Exception {
        var deployment = TestUtils.buildApplicationCluster(FlinkVersion.v1_14);
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);

        reconciler.reconcile(deployment, context);

        deployment.getSpec().setFlinkVersion(FlinkVersion.v1_15);

        var reconStatus = deployment.getStatus().getReconciliationStatus();

        // Do not trigger update until running
        reconciler.reconcile(deployment, context);
        assertEquals(ReconciliationState.DEPLOYED, reconStatus.getState());

        deployment.getStatus().getJobStatus().setState(JobState.RUNNING.name());
        deployment
                .getStatus()
                .getJobStatus()
                .setJobId(flinkService.listJobs().get(0).f1.getJobId().toHexString());

        reconciler.reconcile(deployment, context);
        assertEquals(ReconciliationState.UPGRADING, reconStatus.getState());
        assertEquals(
                UpgradeMode.SAVEPOINT,
                reconStatus.deserializeLastReconciledSpec().getJob().getUpgradeMode());
    }

    @Test
    public void testScaleWithReactiveModeDisabled() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());
        deployment.getSpec().getJob().setParallelism(100);
        reconciler.reconcile(deployment, context);
        assertEquals(
                JobState.SUSPENDED,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertFalse(deployment.getStatus().getReconciliationStatus().scalingInProgress());
    }

    @Test
    public void testScaleWithReactiveModeEnabled() throws Exception {

        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        JobManagerOptions.SCHEDULER_MODE.key(),
                        SchedulerExecutionMode.REACTIVE.name());

        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        // the default.parallelism is always ignored
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(CoreOptions.DEFAULT_PARALLELISM.key(), "100");

        assertFalse(deployment.getStatus().getReconciliationStatus().scalingInProgress());
        reconciler.reconcile(deployment, context);
        assertEquals(
                JobState.RUNNING,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertEquals(0, flinkService.getDesiredReplicas());

        deployment.getSpec().getJob().setParallelism(4);
        reconciler.reconcile(deployment, context);
        assertEquals(
                JobState.RUNNING,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertEquals(2, flinkService.getDesiredReplicas());
        assertTrue(deployment.getStatus().getReconciliationStatus().scalingInProgress());

        deployment.getSpec().getJob().setParallelism(8);
        reconciler.reconcile(deployment, context);
        assertEquals(
                JobState.RUNNING,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertEquals(4, flinkService.getDesiredReplicas());
    }

    @Test
    public void testScaleWithRescaleApi() throws Exception {
        var rescaleCounter = new AtomicInteger(0);
        var v1 = new JobVertexID();

        // We create a service mocking out some methods we don't want to call explicitly
        var nativeService =
                new NativeFlinkService(
                        kubernetesClient, null, executorService, operatorConfig, eventRecorder) {

                    Map<JobVertexID, JobVertexResourceRequirements> submitted =
                            Map.of(
                                    v1,
                                    new JobVertexResourceRequirements(
                                            new JobVertexResourceRequirements.Parallelism(1, 1)));

                    @Override
                    protected Map<JobVertexID, JobVertexResourceRequirements> getVertexResources(
                            RestClusterClient<String> c, AbstractFlinkResource<?, ?> r) {
                        return submitted;
                    }

                    @Override
                    protected void updateVertexResources(
                            RestClusterClient<String> c,
                            AbstractFlinkResource<?, ?> r,
                            Map<JobVertexID, JobVertexResourceRequirements> req) {
                        submitted = req;
                        rescaleCounter.incrementAndGet();
                    }

                    @Override
                    public void cancelJob(
                            FlinkDeployment deployment,
                            UpgradeMode upgradeMode,
                            Configuration conf) {}
                };

        var ctxFactory =
                new TestingFlinkResourceContextFactory(
                        getKubernetesClient(),
                        configManager,
                        operatorMetricGroup,
                        nativeService,
                        eventRecorder);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        // Set all the properties required by the rescale api
        deployment.getSpec().setFlinkVersion(FlinkVersion.v1_18);
        deployment.getSpec().setMode(KubernetesDeploymentMode.NATIVE);
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        JobManagerOptions.SCHEDULER.key(),
                        JobManagerOptions.SchedulerType.Adaptive.name());
        deployment.getMetadata().setGeneration(1L);

        // Deploy the job and update the status accordingly so we can proceed to rescaling it
        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        // Override parallelism for a vertex and trigger rescaling
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1.toHexString() + ":2");
        deployment.getMetadata().setGeneration(2L);
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(1, rescaleCounter.get());
        assertEquals(
                EventRecorder.Reason.Scaling.toString(),
                eventCollector.events.getLast().getReason());
        assertEquals(3, eventCollector.events.size());

        // Job should not be stopped, we simply call the rescale api
        assertEquals(
                JobState.RUNNING,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertTrue(deployment.getStatus().getReconciliationStatus().scalingInProgress());
        assertEquals(
                v1.toHexString() + ":2",
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getFlinkConfiguration()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES.key()));

        // Reconciler should not do anything while waiting for scaling completion
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                JobState.RUNNING,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertTrue(deployment.getStatus().getReconciliationStatus().scalingInProgress());
        assertEquals(
                v1.toHexString() + ":2",
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getFlinkConfiguration()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES.key()));
        assertEquals(1, rescaleCounter.get());
        assertEquals(3, eventCollector.events.size());

        var deploymentClone = ReconciliationUtils.clone(deployment);

        // Make sure to trigger regular upgrade on other spec changes
        deployment.getSpec().setRestartNonce(5L);
        deployment.getMetadata().setGeneration(3L);
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                JobState.SUSPENDED,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertEquals(1, rescaleCounter.get());
        assertEquals(
                EventRecorder.Reason.SpecChanged.toString(),
                eventCollector.events.get(eventCollector.events.size() - 2).getReason());

        // If the job failed while rescaling we fall back to the regular upgrade mechanism
        deployment = deploymentClone;
        deployment
                .getStatus()
                .getJobStatus()
                .setState(org.apache.flink.api.common.JobStatus.FAILED.name());
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                JobState.SUSPENDED,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        assertEquals(1, rescaleCounter.get());
    }

    @Test
    public void testApplyAutoscalerParallelism() throws Exception {
        var ctxFactory =
                new TestingFlinkResourceContextFactory(
                        getKubernetesClient(),
                        configManager,
                        operatorMetricGroup,
                        flinkService,
                        eventRecorder);
        var overrides = new HashMap<String, String>();
        JobAutoScalerFactory autoscalerFactory =
                (k, r) ->
                        new NoopJobAutoscalerFactory() {
                            @Override
                            public Map<String, String> getParallelismOverrides(
                                    FlinkResourceContext<?> ctx) {
                                return new HashMap<>(overrides);
                            }

                            @Override
                            public boolean scale(FlinkResourceContext<?> ctx) {
                                return true;
                            }
                        };

        appReconciler =
                new ApplicationReconciler(
                        kubernetesClient, eventRecorder, statusRecorder, autoscalerFactory);

        var deployment = TestUtils.buildApplicationCluster();
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());
        assertFalse(deployment.getStatus().isImmediateReconciliationNeeded());

        // Job running verify no upgrades if overrides are empty
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals("RUNNING", deployment.getStatus().getJobStatus().getState());
        assertTrue(deployment.getStatus().isImmediateReconciliationNeeded());

        // Test when there are only overrides by the autoscaler
        var v1 = new JobVertexID();
        overrides.put(v1.toHexString(), "2");

        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        assertEquals(
                Map.of(v1.toHexString(), "2"),
                ctxFactory
                        .getResourceContext(deployment, context)
                        .getObserveConfig()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES));

        // Test when there are also user overrides, autoscaler should take precedence

        // This should be ignored
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":1");
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(
                Map.of(v1.toHexString(), "2"),
                ctxFactory
                        .getResourceContext(deployment, context)
                        .getObserveConfig()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES));

        // Define partly overlapping overrides
        var v2 = new JobVertexID();
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":1," + v2 + ":4");
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        assertEquals(
                Map.of(v1.toString(), "2", v2.toString(), "4"),
                ctxFactory
                        .getResourceContext(deployment, context)
                        .getObserveConfig()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES));
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void verifyJobIdNotResetDuringLastStateRecovery(FlinkVersion flinkVersion) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster(flinkVersion);

        flinkService.setDeployFailure(true);

        try {
            reconciler.reconcile(deployment, context);
        } catch (Exception expected) {
        }
        statusRecorder.updateStatusFromCache(deployment);

        if (!flinkVersion.isNewerVersionThan(FlinkVersion.v1_15)) {
            assertFalse(StringUtils.isBlank(deployment.getStatus().getJobStatus().getJobId()));
        }
    }

    @Test
    public void testSetOwnerReference() throws Exception {
        FlinkDeployment flinkApp = TestUtils.buildApplicationCluster();
        ObjectMeta deployMeta = flinkApp.getMetadata();
        FlinkDeploymentStatus status = flinkApp.getStatus();
        FlinkDeploymentSpec spec = flinkApp.getSpec();
        Configuration deployConfig = configManager.getDeployConfig(deployMeta, spec);

        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED.name());
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconciler
                .getReconciler()
                .deploy(getResourceContext(flinkApp), spec, deployConfig, Optional.empty(), false);

        final List<Map<String, String>> expectedOwnerReferences =
                List.of(TestUtils.generateTestOwnerReferenceMap(flinkApp));
        List<Map<String, String>> or =
                deployConfig.get(KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE);
        Assertions.assertEquals(expectedOwnerReferences, or);
    }

    @Test
    public void testTerminalJmTtl() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        deployment.getSpec().getJob().setState(JobState.SUSPENDED);
        reconciler.reconcile(deployment, context);
        var status = deployment.getStatus();
        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED.toString(),
                status.getJobStatus().getState());
        assertEquals(JobManagerDeploymentStatus.READY, status.getJobManagerDeploymentStatus());

        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        KubernetesOperatorConfigOptions.OPERATOR_JM_SHUTDOWN_TTL.key(),
                        String.valueOf(Duration.ofMinutes(5).toMillis()));

        var now = Instant.now();
        status.getJobStatus().setUpdateTime(String.valueOf(now.toEpochMilli()));

        reconciler
                .getReconciler()
                .setClock(Clock.fixed(now.plus(Duration.ofMinutes(3)), ZoneId.systemDefault()));
        reconciler.reconcile(deployment, context);
        assertEquals(JobManagerDeploymentStatus.READY, status.getJobManagerDeploymentStatus());

        reconciler
                .getReconciler()
                .setClock(Clock.fixed(now.plus(Duration.ofMinutes(6)), ZoneId.systemDefault()));
        reconciler.reconcile(deployment, context);
        assertEquals(JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());
    }

    @Test
    public void testDeploymentRecoveryEvent() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        reconciler.reconcile(deployment, context);
        Assertions.assertEquals(MSG_SUBMIT, eventCollector.events.remove().getMessage());
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        flinkService.clear();
        FlinkDeploymentStatus deploymentStatus = deployment.getStatus();
        deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        deploymentStatus
                .getJobStatus()
                .setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
        reconciler.reconcile(deployment, context);
        Assertions.assertEquals(MSG_RECOVERY, eventCollector.events.remove().getMessage());
    }

    @Test
    public void testRestartUnhealthyEvent() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED.key(), "true");
        reconciler.reconcile(deployment, context);
        Assertions.assertEquals(MSG_SUBMIT, eventCollector.events.remove().getMessage());
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        var clusterHealthInfo = new ClusterHealthInfo();
        clusterHealthInfo.setTimeStamp(System.currentTimeMillis());
        clusterHealthInfo.setNumRestarts(2);
        clusterHealthInfo.setHealthy(false);
        ClusterHealthEvaluator.setLastValidClusterHealthInfo(
                deployment.getStatus().getClusterInfo(), clusterHealthInfo);
        reconciler.reconcile(deployment, context);
        Assertions.assertEquals(MSG_RESTART_UNHEALTHY, eventCollector.events.remove().getMessage());
    }

    @Test
    public void testReconcileIfUpgradeModeNotAvailable() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);

        // We disable last state fallback as we want to test that the deployment is properly
        // recovered before upgrade
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        KubernetesOperatorConfigOptions
                                .OPERATOR_JOB_UPGRADE_LAST_STATE_FALLBACK_ENABLED
                                .key(),
                        "false");

        // Initial deployment
        reconciler.reconcile(deployment, context);

        // Trigger upgrade but set jobmanager status to missing -> savepoint upgrade not available
        deployment.getSpec().setRestartNonce(123L);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        flinkService.clear();

        reconciler.reconcile(deployment, context);
        // We verify that deployment was recovered before upgrade
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());

        var lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        assertNotEquals(
                deployment.getSpec().getRestartNonce(), lastReconciledSpec.getRestartNonce());

        // Set to running to let savepoint upgrade proceed
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        reconciler.reconcile(deployment, context);
        // Make sure upgrade is properly triggered now
        lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        assertEquals(deployment.getSpec().getRestartNonce(), lastReconciledSpec.getRestartNonce());
        assertEquals(JobState.SUSPENDED, lastReconciledSpec.getJob().getState());
    }

    @Test
    public void testUpgradeReconciledGeneration() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getMetadata().setGeneration(1L);

        // Initial deployment
        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        assertEquals(
                1L,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpecWithMeta()
                        .getMeta()
                        .getMetadata()
                        .getGeneration());

        // Submit no-op upgrade
        deployment.getSpec().getFlinkConfiguration().put("kubernetes.operator.test", "value");
        deployment.getMetadata().setGeneration(2L);

        reconciler.reconcile(deployment, context);
        assertEquals(
                2L,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpecWithMeta()
                        .getMeta()
                        .getMetadata()
                        .getGeneration());
    }
}
