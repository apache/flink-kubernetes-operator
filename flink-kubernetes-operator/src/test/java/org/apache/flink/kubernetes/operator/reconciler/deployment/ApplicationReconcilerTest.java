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
import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.NoopJobAutoscaler;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigOption;
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
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.Checkpoint;
import org.apache.flink.kubernetes.operator.api.status.CheckpointInfo;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.api.status.SnapshotInfo;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthEvaluator;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;
import org.apache.flink.kubernetes.operator.reconciler.TestReconcilerAdapter;
import org.apache.flink.kubernetes.operator.service.NativeFlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.SnapshotStatus;
import org.apache.flink.kubernetes.operator.utils.SnapshotUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.commons.util.StringUtils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getCheckpointInfo;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getJobSpec;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getJobStatus;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getReconciledJobSpec;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getReconciledJobState;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getSavepointInfo;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractFlinkResourceReconciler.MSG_SUBMIT;
import static org.apache.flink.kubernetes.operator.reconciler.deployment.ApplicationReconciler.MSG_RECOVERY;
import static org.apache.flink.kubernetes.operator.reconciler.deployment.ApplicationReconciler.MSG_RESTART_UNHEALTHY;
import static org.apache.flink.kubernetes.operator.utils.SnapshotUtils.getLastSnapshotStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @link JobStatusObserver unit tests
 */
@EnableKubernetesMockClient(crud = true)
public class ApplicationReconcilerTest extends OperatorTestBase {

    private TestReconcilerAdapter<FlinkDeployment, FlinkDeploymentSpec, FlinkDeploymentStatus>
            reconciler;

    @Getter private KubernetesClient kubernetesClient;
    private ApplicationReconciler appReconciler;

    private FlinkOperatorConfiguration operatorConfig;
    private ExecutorService executorService;

    private Clock testClock = Clock.systemDefaultZone();

    @Override
    public void setup() {
        appReconciler =
                new ApplicationReconciler(eventRecorder, statusRecorder, new NoopJobAutoscaler<>());
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
        reconciler.reconcile(
                deployment, TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient));
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        // clean up
        assertEquals(
                null, deployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint());
        reconciler.cleanup(
                deployment, TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient));
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
    public void testSubmitAndCleanUpWithSavepointOnResource(FlinkVersion flinkVersion)
            throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster(flinkVersion);
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.SAVEPOINT_ON_DELETION.key(), "true");

        // session ready
        reconciler.reconcile(
                deployment, TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient));
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        // clean up
        assertEquals(
                null, deployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint());
        reconciler.cleanup(
                deployment, TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient));
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
        getJobSpec(statelessUpgrade).setUpgradeMode(UpgradeMode.STATELESS);
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
        assertEquals(null, getLastSnapshotStatus(statelessUpgrade, SAVEPOINT));
        runningJobs = flinkService.listJobs();
        assertEquals(1, flinkService.getRunningCount());
        assertNull(runningJobs.get(0).f0);

        assertNotEquals(
                runningJobs.get(0).f2.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID), jobId);
        jobId = runningJobs.get(0).f1.getJobId();

        getJobStatus(deployment).setJobId(jobId.toHexString());

        // Test stateful upgrade
        FlinkDeployment statefulUpgrade = ReconciliationUtils.clone(deployment);
        getJobSpec(statefulUpgrade).setUpgradeMode(UpgradeMode.SAVEPOINT);
        statefulUpgrade.getSpec().getFlinkConfiguration().put("new", "conf2");

        reconciler.reconcile(statefulUpgrade, context);

        assertEquals(0, flinkService.getRunningCount());

        reconciler.reconcile(statefulUpgrade, context);

        runningJobs = flinkService.listJobs();
        assertEquals(1, flinkService.getRunningCount());
        assertEquals("savepoint_0", runningJobs.get(0).f0);
        assertEquals(
                SnapshotTriggerType.UPGRADE,
                getSavepointInfo(statefulUpgrade).getLastSavepoint().getTriggerType());
        assertEquals(SnapshotStatus.SUCCEEDED, getLastSnapshotStatus(statefulUpgrade, SAVEPOINT));
        verifyJobId(deployment, runningJobs.get(0).f1, runningJobs.get(0).f2, jobId);

        getJobSpec(deployment).setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().setRestartNonce(100L);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastStableSpec(
                        deployment.getStatus().getReconciliationStatus().getLastReconciledSpec());
        flinkService.setHaDataAvailable(false);
        getJobStatus(deployment).setState("RECONCILING");

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
        getJobSpec(deployment).setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().setRestartNonce(200L);
        flinkService.setHaDataAvailable(false);
        getSavepointInfo(deployment)
                .setLastSavepoint(Savepoint.of("finished_sp", SnapshotTriggerType.UPGRADE));
        getJobStatus(deployment).setState("FINISHED");
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

    @NotNull
    private static Savepoint savepointFromSavepointInfo(
            SavepointInfo savepointInfo, Long savepointTriggerNonce) {
        return new Savepoint(
                savepointInfo.getTriggerTimestamp(),
                // To make sure the new savepoint has a new path
                savepointInfo.getTriggerId()
                        + savepointInfo.getTriggerTimestamp()
                        + savepointInfo.getTriggerId()
                        + savepointTriggerNonce,
                savepointInfo.getTriggerType(),
                savepointInfo.getFormatType(),
                SnapshotTriggerType.MANUAL == savepointInfo.getTriggerType()
                        ? savepointTriggerNonce
                        : null);
    }

    @Test
    public void triggerCheckpoint() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        testSnapshot(deployment, CHECKPOINT);
    }

    @Test
    public void triggerSavepoint() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        testSnapshot(deployment, SAVEPOINT);
    }

    @Test
    public void verifyStatusUpdatedBeforeDeploy() throws Exception {
        // Bootstrap running deployment status
        var deployment = TestUtils.buildApplicationCluster(FlinkVersion.v1_17);
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        // Suspend job and make sure deployment is not deleted (savepoint upgrade)
        deployment.getSpec().getJob().setState(JobState.SUSPENDED);
        reconciler.reconcile(deployment, context);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        // Resume but trigger deploy failure
        deployment.getSpec().getJob().setState(JobState.RUNNING);
        flinkService.setDeployFailure(true);

        try {
            reconciler.reconcile(deployment, context);
            fail();
        } catch (Exception expected) {
            // Make sure deployment deletion is already persisted in k8s
            deployment.getStatus().setJobManagerDeploymentStatus(null);
            statusRecorder.updateStatusFromCache(deployment);
            assertEquals(
                    JobManagerDeploymentStatus.MISSING,
                    deployment.getStatus().getJobManagerDeploymentStatus());
        }
    }

    private void testSnapshot(FlinkDeployment deployment, SnapshotType snapshotType)
            throws Exception {
        final Predicate<JobStatus> isSnapshotInProgress;
        final Function<FlinkDeployment, SnapshotInfo> getSnapshotInfo;
        final BiConsumer<JobSpec, Long> setTriggerNonce;
        final Function<JobSpec, Long> getTriggerNonce;
        final Consumer<FlinkDeployment> updateLastSnapshot;
        final BiConsumer<FlinkDeployment, Long> setLastSnapshotTime;
        final ConfigOption<String> triggerSnapshotExpression;
        final String triggerPrefix;

        switch (snapshotType) {
            case SAVEPOINT:
                isSnapshotInProgress = SnapshotUtils::savepointInProgress;
                getSnapshotInfo = FlinkResourceUtils::getSavepointInfo;
                setTriggerNonce = JobSpec::setSavepointTriggerNonce;
                getTriggerNonce = JobSpec::getSavepointTriggerNonce;
                updateLastSnapshot =
                        flinkDeployment -> {
                            var savepointInfo = getSavepointInfo(flinkDeployment);
                            var savepoint =
                                    savepointFromSavepointInfo(
                                            savepointInfo,
                                            getJobSpec(flinkDeployment).getSavepointTriggerNonce());
                            savepointInfo.updateLastSavepoint(savepoint);
                        };
                setLastSnapshotTime =
                        (flinkDeployment, timestamp) -> {
                            Savepoint lastSavepoint =
                                    Savepoint.of("", timestamp, SnapshotTriggerType.PERIODIC);
                            flinkDeployment
                                    .getStatus()
                                    .getJobStatus()
                                    .getSavepointInfo()
                                    .updateLastSavepoint(lastSavepoint);
                        };
                triggerSnapshotExpression =
                        KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL;
                triggerPrefix = "savepoint_";
                break;
            case CHECKPOINT:
                isSnapshotInProgress = SnapshotUtils::checkpointInProgress;
                getSnapshotInfo = FlinkResourceUtils::getCheckpointInfo;
                setTriggerNonce = JobSpec::setCheckpointTriggerNonce;
                getTriggerNonce = JobSpec::getCheckpointTriggerNonce;
                updateLastSnapshot =
                        flinkDeployment -> {
                            var checkpointInfo = getCheckpointInfo(flinkDeployment);
                            var checkpoint =
                                    checkpointFromCheckpointInfo(
                                            checkpointInfo,
                                            getJobSpec(flinkDeployment)
                                                    .getCheckpointTriggerNonce());
                            checkpointInfo.updateLastCheckpoint(checkpoint);
                        };
                setLastSnapshotTime =
                        (flinkDeployment, timestamp) -> {
                            Checkpoint lastCheckpoint =
                                    Checkpoint.of(timestamp, SnapshotTriggerType.PERIODIC);
                            flinkDeployment
                                    .getStatus()
                                    .getJobStatus()
                                    .getCheckpointInfo()
                                    .updateLastCheckpoint(lastCheckpoint);
                        };
                triggerSnapshotExpression =
                        KubernetesOperatorConfigOptions.PERIODIC_CHECKPOINT_INTERVAL;
                triggerPrefix = "checkpoint_";
                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);
        assertFalse(isSnapshotInProgress.test(getJobStatus(deployment)));
        assertNull(getSnapshotInfo.apply(deployment).getLastSnapshot());
        assertNull(getLastSnapshotStatus(deployment, snapshotType));

        FlinkDeployment snDeployment = ReconciliationUtils.clone(deployment);

        // don't trigger if nonce is missing
        reconciler.reconcile(snDeployment, context);
        assertFalse(isSnapshotInProgress.test((getJobStatus(snDeployment))));
        assertNull(getSnapshotInfo.apply(deployment).getLastSnapshot());
        assertNull(getLastSnapshotStatus(snDeployment, snapshotType));

        // trigger when nonce is defined
        setTriggerNonce.accept(getJobSpec(snDeployment), ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(snDeployment, context);
        assertNull(getTriggerNonce.apply(getReconciledJobSpec(snDeployment)));
        assertEquals(
                triggerPrefix + "trigger_0", getSnapshotInfo.apply(snDeployment).getTriggerId());
        assertTrue(isSnapshotInProgress.test(getJobStatus(snDeployment)));
        assertEquals(SnapshotStatus.PENDING, getLastSnapshotStatus(snDeployment, snapshotType));

        // don't trigger when snapshot is in progress
        reconciler.reconcile(snDeployment, context);
        assertEquals(
                triggerPrefix + "trigger_0", getSnapshotInfo.apply(snDeployment).getTriggerId());
        assertEquals(
                SnapshotTriggerType.MANUAL, getSnapshotInfo.apply(snDeployment).getTriggerType());

        ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                getSnapshotInfo.apply(snDeployment), snDeployment, snapshotType);
        getSnapshotInfo.apply(snDeployment).resetTrigger();

        // don't trigger when nonce is the same
        reconciler.reconcile(snDeployment, context);
        assertFalse(isSnapshotInProgress.test(getJobStatus(snDeployment)));
        assertNull(getLastSnapshotStatus(snDeployment, snapshotType));

        // trigger when new nonce is defined
        setTriggerNonce.accept(getJobSpec(snDeployment), ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(snDeployment, context);
        assertEquals(
                triggerPrefix + "trigger_1", getSnapshotInfo.apply(snDeployment).getTriggerId());
        assertEquals(
                SnapshotTriggerType.MANUAL, getSnapshotInfo.apply(snDeployment).getTriggerType());

        // re-trigger after reset
        getSnapshotInfo.apply(snDeployment).resetTrigger();
        reconciler.reconcile(snDeployment, context);
        assertEquals(
                triggerPrefix + "trigger_2", getSnapshotInfo.apply(snDeployment).getTriggerId());
        assertEquals(
                SnapshotTriggerType.MANUAL, getSnapshotInfo.apply(snDeployment).getTriggerType());

        //  reconciled and snapshot is updated
        ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                getSnapshotInfo.apply(snDeployment), snDeployment, snapshotType);
        updateLastSnapshot.accept(snDeployment);
        assertEquals(SnapshotStatus.SUCCEEDED, getLastSnapshotStatus(snDeployment, snapshotType));

        // re-trigger, reconciled but snapshot is not updated
        setTriggerNonce.accept(getJobSpec(snDeployment), ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(snDeployment, context);
        ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                getSnapshotInfo.apply(snDeployment), snDeployment, snapshotType);
        getSnapshotInfo.apply(snDeployment).resetTrigger();
        assertEquals(SnapshotStatus.ABANDONED, getLastSnapshotStatus(snDeployment, snapshotType));

        // don't trigger when nonce is cleared
        setTriggerNonce.accept(getJobSpec(snDeployment), null);
        reconciler.reconcile(snDeployment, context);
        assertFalse(isSnapshotInProgress.test(getJobStatus(snDeployment)));

        // trigger by periodic interval settings
        snDeployment.getSpec().getFlinkConfiguration().put(triggerSnapshotExpression.key(), "1");
        reconciler.reconcile(snDeployment, context);
        assertTrue(isSnapshotInProgress.test(getJobStatus(snDeployment)));
        assertEquals(SnapshotStatus.PENDING, getLastSnapshotStatus(snDeployment, snapshotType));
        snDeployment.getSpec().getFlinkConfiguration().put(triggerSnapshotExpression.key(), "0");

        // trigger by cron expression
        updateLastSnapshot.accept(snDeployment); // Ensures no snapshot is considered to be running
        assertFalse(isSnapshotInProgress.test(getJobStatus(snDeployment)));
        assertNotEquals(SnapshotStatus.PENDING, getLastSnapshotStatus(snDeployment, snapshotType));

        Calendar calendar = Calendar.getInstance();
        calendar.set(2022, Calendar.JUNE, 5, 11, 0);
        setLastSnapshotTime.accept(
                snDeployment, calendar.getTimeInMillis()); // Required for the cron to trigger

        snDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(triggerSnapshotExpression.key(), "0 0 12 5 6 ? 2022");
        reconciler.reconcile(snDeployment, context);
        assertTrue(isSnapshotInProgress.test(getJobStatus(snDeployment)));
        assertEquals(SnapshotStatus.PENDING, getLastSnapshotStatus(snDeployment, snapshotType));
        snDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(triggerSnapshotExpression.key(), triggerSnapshotExpression.defaultValue());
    }

    @NotNull
    private static Checkpoint checkpointFromCheckpointInfo(
            CheckpointInfo checkpointInfo, Long checkpointTriggerNonce) {
        return new Checkpoint(
                checkpointInfo.getTriggerTimestamp(),
                checkpointInfo.getTriggerType(),
                checkpointInfo.getFormatType(),
                SnapshotTriggerType.MANUAL == checkpointInfo.getTriggerType()
                        ? checkpointTriggerNonce
                        : null);
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
        assertEquals(JobState.SUSPENDED, getReconciledJobState(restartJob));
        assertEquals(0, flinkService.getRunningCount());

        reconciler.reconcile(restartJob, context);
        assertEquals(JobState.RUNNING, getReconciledJobState(restartJob));
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
        getJobSpec(spDeployment).setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(spDeployment, context);
        assertEquals("savepoint_trigger_0", getSavepointInfo(spDeployment).getTriggerId());
        assertEquals(JobState.RUNNING.name(), getJobStatus(spDeployment).getState());

        // Force upgrade when savepoint is in progress.
        spDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        KubernetesOperatorConfigOptions.JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT.key(),
                        "true");
        spDeployment.getSpec().setImage("flink:greatest");
        reconciler.reconcile(spDeployment, context);
        assertEquals("savepoint_trigger_0", getSavepointInfo(spDeployment).getTriggerId());
        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED.name(),
                getJobStatus(spDeployment).getState());
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
        getJobSpec(deployment).setUpgradeMode(UpgradeMode.LAST_STATE);

        reconciler.reconcile(deployment, context);

        deployment.getSpec().setFlinkVersion(FlinkVersion.v1_15);

        var reconStatus = deployment.getStatus().getReconciliationStatus();

        // Do not trigger update until running
        reconciler.reconcile(deployment, context);
        assertEquals(ReconciliationState.DEPLOYED, reconStatus.getState());

        getJobStatus(deployment).setState(JobState.RUNNING.name());
        getJobStatus(deployment)
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
        getJobSpec(deployment).setParallelism(100);
        reconciler.reconcile(deployment, context);
        assertEquals(JobState.SUSPENDED, getReconciledJobState(deployment));
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
        assertEquals(JobState.RUNNING, getReconciledJobState(deployment));
        assertEquals(0, flinkService.getDesiredReplicas());

        getJobSpec(deployment).setParallelism(4);
        reconciler.reconcile(deployment, context);
        assertEquals(JobState.RUNNING, getReconciledJobState(deployment));
        assertEquals(2, flinkService.getDesiredReplicas());
        assertTrue(deployment.getStatus().getReconciliationStatus().scalingInProgress());

        getJobSpec(deployment).setParallelism(8);
        reconciler.reconcile(deployment, context);
        assertEquals(JobState.RUNNING, getReconciledJobState(deployment));
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
                        configManager, operatorMetricGroup, nativeService, eventRecorder);
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
        assertEquals(JobState.RUNNING, getReconciledJobState(deployment));
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
        assertEquals(JobState.RUNNING, getReconciledJobState(deployment));
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
        assertEquals(JobState.SUSPENDED, getReconciledJobState(deployment));
        assertEquals(1, rescaleCounter.get());
        assertEquals(
                EventRecorder.Reason.SpecChanged.toString(),
                eventCollector.events.get(eventCollector.events.size() - 2).getReason());

        // If the job failed while rescaling we fall back to the regular upgrade mechanism
        deployment = deploymentClone;
        getJobStatus(deployment).setState(org.apache.flink.api.common.JobStatus.FAILED.name());
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(JobState.SUSPENDED, getReconciledJobState(deployment));
        assertEquals(1, rescaleCounter.get());
    }

    @Test
    public void testApplyAutoscalerParallelism() throws Exception {
        var ctxFactory =
                new TestingFlinkResourceContextFactory(
                        configManager, operatorMetricGroup, flinkService, eventRecorder);

        var overrideFunction = new AtomicReference<Consumer<AbstractFlinkSpec>>(s -> {});
        JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> autoscaler =
                new NoopJobAutoscaler<>() {
                    @Override
                    public void scale(KubernetesJobAutoScalerContext ctx) {
                        overrideFunction.get().accept(ctx.getResource().getSpec());
                    }
                };

        appReconciler = new ApplicationReconciler(eventRecorder, statusRecorder, autoscaler);

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

        // Test overrides are applied correctly
        var v1 = new JobVertexID();
        overrideFunction.set(
                s ->
                        s.getFlinkConfiguration()
                                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":2"));

        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(
                Map.of(v1.toHexString(), "2"),
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
            assertFalse(StringUtils.isBlank(getJobStatus(deployment).getJobId()));
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
        getJobSpec(deployment).setUpgradeMode(UpgradeMode.SAVEPOINT);
        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        getJobSpec(deployment).setState(JobState.SUSPENDED);
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testClusterCleanupBeforeDeploy(boolean requireMetadata) throws Exception {
        var flinkApp = TestUtils.buildApplicationCluster();
        var status = flinkApp.getStatus();
        var spec = flinkApp.getSpec();
        var deployConfig = configManager.getDeployConfig(flinkApp.getMetadata(), spec);

        status.getReconciliationStatus().serializeAndSetLastReconciledSpec(spec, flinkApp);
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED.name());

        var deleted = new AtomicBoolean(false);

        flinkService =
                new TestingFlinkService() {
                    @Override
                    protected void deleteClusterInternal(
                            ObjectMeta meta,
                            Configuration conf,
                            boolean deleteHaMeta,
                            DeletionPropagation deletionPropagation) {
                        deleted.set(deleteHaMeta);
                    }
                };

        reconciler
                .getReconciler()
                .deploy(
                        getResourceContext(flinkApp),
                        spec,
                        deployConfig,
                        Optional.empty(),
                        requireMetadata);
        assertEquals(deleted.get(), !requireMetadata);
        assertEquals(JobManagerDeploymentStatus.DEPLOYING, status.getJobManagerDeploymentStatus());
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
        getJobSpec(deployment).setUpgradeMode(UpgradeMode.SAVEPOINT);

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRollbackUpgradeModeHandling(boolean jmStarted) throws Exception {
        var deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        offsetReconcilerClock(deployment, Duration.ZERO);

        var flinkConfiguration = deployment.getSpec().getFlinkConfiguration();
        flinkConfiguration.put(
                KubernetesOperatorConfigOptions.DEPLOYMENT_ROLLBACK_ENABLED.key(), "true");
        flinkConfiguration.put(
                KubernetesOperatorConfigOptions.DEPLOYMENT_READINESS_TIMEOUT.key(), "10s");
        flinkConfiguration.put(
                KubernetesOperatorConfigOptions.OPERATOR_JOB_UPGRADE_LAST_STATE_FALLBACK_ENABLED
                        .key(),
                "false");

        // Initial deployment, mark as stable
        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());
        deployment.getStatus().getReconciliationStatus().markReconciledSpecAsStable();

        // Submit invalid change
        deployment.getSpec().getJob().setParallelism(9999);
        reconciler.reconcile(deployment, context);
        reconciler.reconcile(deployment, context);
        assertEquals(1, flinkService.listJobs().size());
        assertEquals(
                UpgradeMode.STATELESS,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastStableSpec()
                        .getJob()
                        .getUpgradeMode());
        assertEquals(
                UpgradeMode.SAVEPOINT,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getUpgradeMode());

        // Trigger rollback by delaying the recovery
        offsetReconcilerClock(deployment, Duration.ofSeconds(15));
        flinkService.setHaDataAvailable(jmStarted);
        flinkService.setJobManagerReady(jmStarted);
        reconciler.reconcile(deployment, context);

        assertEquals(
                ReconciliationState.ROLLING_BACK,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(0, flinkService.listJobs().size());
        assertEquals("FINISHED", deployment.getStatus().getJobStatus().getState());
        assertEquals(
                jmStarted ? UpgradeMode.LAST_STATE : UpgradeMode.SAVEPOINT,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getUpgradeMode());

        flinkService.setJobManagerReady(true);
        reconciler.reconcile(deployment, context);

        assertEquals(
                ReconciliationState.ROLLED_BACK,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(1, flinkService.listJobs().size());
        assertEquals("RECONCILING", deployment.getStatus().getJobStatus().getState());
    }

    private void offsetReconcilerClock(FlinkDeployment dep, Duration offset) {
        testClock = Clock.offset(testClock, offset);
        appReconciler.setClock(testClock);
    }
}
