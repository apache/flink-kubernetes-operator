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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
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
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
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
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.exception.UpgradeFailureException;
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthEvaluator;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthResult;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;
import org.apache.flink.kubernetes.operator.reconciler.TestReconcilerAdapter;
import org.apache.flink.kubernetes.operator.service.NativeFlinkService;
import org.apache.flink.kubernetes.operator.service.SuspendMode;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.SnapshotStatus;
import org.apache.flink.kubernetes.operator.utils.SnapshotUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.commons.util.StringUtils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedList;
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

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.flink.api.common.JobStatus.RECONCILING;
import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getCheckpointInfo;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getJobSpec;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getJobStatus;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getReconciledJobSpec;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getReconciledJobState;
import static org.apache.flink.kubernetes.operator.api.utils.FlinkResourceUtils.getSavepointInfo;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_JOB_SAVEPOINT_DISPOSE_ON_DELETE;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractFlinkResourceReconciler.MSG_SUBMIT;
import static org.apache.flink.kubernetes.operator.reconciler.deployment.ApplicationReconciler.MSG_RECOVERY;
import static org.apache.flink.kubernetes.operator.utils.SnapshotUtils.getLastSnapshotStatus;
import static org.assertj.core.api.Assertions.assertThat;
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
        assertNull(deployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint());
        assertNull(deployment.getStatus().getJobStatus().getUpgradeSavepointPath());
        reconciler.cleanup(
                deployment, TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient));

        assertThat(TestUtils.getFlinkStateSnapshotsForResource(kubernetesClient, deployment))
                .hasSize(1)
                .allSatisfy(
                        snapshot -> {
                            assertThat(snapshot.getSpec().getSavepoint().getPath())
                                    .isEqualTo("savepoint_0");
                            assertEquals(
                                    "savepoint_0",
                                    deployment
                                            .getStatus()
                                            .getJobStatus()
                                            .getUpgradeSavepointPath());
                        });
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
        assertNull(deployment.getStatus().getJobStatus().getUpgradeSavepointPath());
        reconciler.cleanup(
                deployment, TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient));

        assertThat(TestUtils.getFlinkStateSnapshotsForResource(kubernetesClient, deployment))
                .hasSize(1)
                .allSatisfy(
                        snapshot -> {
                            assertThat(snapshot.getSpec().getSavepoint().getPath())
                                    .isEqualTo("savepoint_0");
                            assertEquals(
                                    "savepoint_0",
                                    deployment
                                            .getStatus()
                                            .getJobStatus()
                                            .getUpgradeSavepointPath());
                        });
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

        // Last state upgrade
        FlinkDeployment lastStateUpgrade = ReconciliationUtils.clone(deployment);
        getJobSpec(lastStateUpgrade).setUpgradeMode(UpgradeMode.LAST_STATE);
        lastStateUpgrade.getSpec().setRestartNonce(1234L);
        reconciler.reconcile(deployment, context);
        reconciler.reconcile(deployment, context);
        // Make sure jobId is rotated on last-state startup
        verifyJobId(lastStateUpgrade, runningJobs.get(0).f1, runningJobs.get(0).f2, jobId);

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

        var spInfo = statefulUpgrade.getStatus().getJobStatus().getSavepointInfo();
        assertEquals("savepoint_0", spInfo.getLastSavepoint().getLocation());
        assertEquals(SnapshotTriggerType.UPGRADE, spInfo.getLastSavepoint().getTriggerType());
        assertEquals(
                spInfo.getLastSavepoint(),
                new LinkedList<>(spInfo.getSavepointHistory()).getLast());

        reconciler.reconcile(statefulUpgrade, context);

        runningJobs = flinkService.listJobs();
        assertEquals(1, flinkService.getRunningCount());
        var snapshots = TestUtils.getFlinkStateSnapshotsForResource(kubernetesClient, deployment);
        assertThat(snapshots).isNotEmpty();
        assertThat(snapshots.get(0).getSpec().getSavepoint().getPath()).isEqualTo("savepoint_0");
        assertEquals(
                SnapshotTriggerType.UPGRADE.name(),
                snapshots
                        .get(0)
                        .getMetadata()
                        .getLabels()
                        .get(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE));

        // Make sure jobId rotated on savepoint
        verifyNewJobId(runningJobs.get(0).f1, runningJobs.get(0).f2, jobId);
        jobId = runningJobs.get(0).f1.getJobId();

        getJobSpec(deployment).setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().setRestartNonce(100L);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastStableSpec(
                        deployment.getStatus().getReconciliationStatus().getLastReconciledSpec());
        flinkService.setHaDataAvailable(false);
        getJobStatus(deployment).setState(RECONCILING);

        try {
            deployment
                    .getStatus()
                    .setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
            reconciler.reconcile(deployment, context);
            fail();
        } catch (UpgradeFailureException expected) {
        }

        try {
            deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.ERROR);
            reconciler.reconcile(deployment, context);
            fail();
        } catch (UpgradeFailureException expected) {
        }

        flinkService.clear();
        getJobSpec(deployment).setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().setRestartNonce(200L);
        flinkService.setHaDataAvailable(false);
        getJobStatus(deployment).setState(FINISHED);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconciler.reconcile(deployment, context);
        reconciler.reconcile(deployment, context);

        assertEquals(1, flinkService.getRunningCount());
        // Make sure jobId rotated on savepoint
        verifyNewJobId(runningJobs.get(0).f1, runningJobs.get(0).f2, jobId);
    }

    private void verifyJobId(
            FlinkDeployment deployment, JobStatusMessage status, Configuration conf, JobID jobId) {
        // jobId set by operator
        assertEquals(jobId, status.getJobId());
        assertEquals(conf.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID), jobId.toHexString());
    }

    private void verifyNewJobId(JobStatusMessage status, Configuration conf, JobID jobId) {
        assertNotEquals(jobId.toHexString(), status.getJobId());
        assertEquals(
                conf.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID),
                status.getJobId().toHexString());
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
    public void triggerCheckpointLegacy() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().getFlinkConfiguration().put(SNAPSHOT_RESOURCE_ENABLED.key(), "false");
        testSnapshotLegacy(deployment, CHECKPOINT);
    }

    @Test
    public void triggerSavepointLegacy() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().getFlinkConfiguration().put(SNAPSHOT_RESOURCE_ENABLED.key(), "false");
        testSnapshotLegacy(deployment, SAVEPOINT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void triggerSavepointWithSnapshotResource(boolean disposeOnDelete) throws Exception {
        var deployment = TestUtils.buildApplicationCluster();
        if (disposeOnDelete) {
            deployment
                    .getSpec()
                    .getFlinkConfiguration()
                    .put(OPERATOR_JOB_SAVEPOINT_DISPOSE_ON_DELETE.key(), "true");
        }

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        var snDeployment = ReconciliationUtils.clone(deployment);

        // trigger when nonce is defined
        var triggerNonce = ThreadLocalRandom.current().nextLong();
        snDeployment.getSpec().getJob().setSavepointTriggerNonce(triggerNonce);
        reconciler.reconcile(snDeployment, context);

        assertEquals(triggerNonce, getReconciledJobSpec(snDeployment).getSavepointTriggerNonce());
        assertNull(snDeployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint());
        assertThat(TestUtils.getFlinkStateSnapshotsForResource(kubernetesClient, snDeployment))
                .hasSize(1)
                .allSatisfy(
                        snapshot -> {
                            assertTrue(snapshot.getSpec().isSavepoint());
                            assertFalse(snapshot.getSpec().getSavepoint().getAlreadyExists());
                            assertEquals(
                                    disposeOnDelete,
                                    snapshot.getSpec().getSavepoint().getDisposeOnDelete());
                            assertEquals(
                                    JobReference.fromFlinkResource(snDeployment),
                                    snapshot.getSpec().getJobReference());
                        });
    }

    @Test
    public void triggerCheckpointWithSnapshotResource() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        FlinkDeployment snDeployment = ReconciliationUtils.clone(deployment);

        // trigger when nonce is defined
        var triggerNonce = ThreadLocalRandom.current().nextLong();
        snDeployment.getSpec().getJob().setCheckpointTriggerNonce(triggerNonce);
        reconciler.reconcile(snDeployment, context);

        assertEquals(triggerNonce, getReconciledJobSpec(snDeployment).getCheckpointTriggerNonce());
        assertNull(snDeployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint());
        assertThat(TestUtils.getFlinkStateSnapshotsForResource(kubernetesClient, snDeployment))
                .hasSize(1)
                .allSatisfy(
                        snapshot -> {
                            assertTrue(snapshot.getSpec().isCheckpoint());
                            assertEquals(
                                    JobReference.fromFlinkResource(snDeployment),
                                    snapshot.getSpec().getJobReference());
                        });
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

    private void testSnapshotLegacy(FlinkDeployment deployment, SnapshotType snapshotType)
            throws Exception {
        final Predicate<JobStatus> isSnapshotInProgress;
        final Function<FlinkDeployment, SnapshotInfo> getSnapshotInfo;
        final BiConsumer<JobSpec, Long> setTriggerNonce;
        final Function<JobSpec, Long> getTriggerNonce;
        final Consumer<FlinkDeployment> updateLastSnapshot;
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
        assertNull(deployment.getStatus().getJobStatus().getUpgradeSavepointPath());
        assertNull(getLastSnapshotStatus(deployment, snapshotType));

        FlinkDeployment snDeployment = ReconciliationUtils.clone(deployment);

        // don't trigger if nonce is missing
        reconciler.reconcile(snDeployment, context);
        assertFalse(isSnapshotInProgress.test((getJobStatus(snDeployment))));
        assertNull(getSnapshotInfo.apply(deployment).getLastSnapshot());
        assertNull(deployment.getStatus().getJobStatus().getUpgradeSavepointPath());
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
                getSnapshotInfo.apply(snDeployment).getTriggerType(), snDeployment, snapshotType);
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
                getSnapshotInfo.apply(snDeployment).getTriggerType(), snDeployment, snapshotType);
        updateLastSnapshot.accept(snDeployment);
        assertEquals(SnapshotStatus.SUCCEEDED, getLastSnapshotStatus(snDeployment, snapshotType));

        // re-trigger, reconciled but snapshot is not updated
        setTriggerNonce.accept(getJobSpec(snDeployment), ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(snDeployment, context);
        ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                getSnapshotInfo.apply(snDeployment).getTriggerType(), snDeployment, snapshotType);
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
        verifyAndSetRunningJobsToStatus(deployment, runningJobs, null);
    }

    private void verifyAndSetRunningJobsToStatus(
            FlinkDeployment deployment,
            List<Tuple3<String, JobStatusMessage, Configuration>> runningJobs,
            String savepoint) {
        assertEquals(1, runningJobs.size());
        assertEquals(savepoint, runningJobs.get(0).f0);
        deployment
                .getStatus()
                .setJobStatus(
                        new JobStatus()
                                .toBuilder()
                                        .jobId(runningJobs.get(0).f1.getJobId().toHexString())
                                        .jobName(runningJobs.get(0).f1.getJobName())
                                        .updateTime(Long.toString(System.currentTimeMillis()))
                                        .state(RUNNING)
                                        .build());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
    }

    @Test
    public void testJobUpgradeIgnorePendingSavepointLegacy() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().getFlinkConfiguration().put(SNAPSHOT_RESOURCE_ENABLED.key(), "false");

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        FlinkDeployment spDeployment = ReconciliationUtils.clone(deployment);
        getJobSpec(spDeployment).setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
        reconciler.reconcile(spDeployment, context);
        assertEquals("savepoint_trigger_0", getSavepointInfo(spDeployment).getTriggerId());
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                getJobStatus(spDeployment).getState());

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
                org.apache.flink.api.common.JobStatus.FINISHED,
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

        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED);
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconciler
                .getReconciler()
                .deploy(getResourceContext(flinkApp), spec, deployConfig, Optional.empty(), false);

        String path1 = deployConfig.get(JobResultStoreOptions.STORAGE_PATH);
        Assertions.assertTrue(path1.startsWith(haStoragePath));

        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED);
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
        var deployment = TestUtils.buildApplicationCluster(FlinkVersion.v1_18);
        getJobSpec(deployment).setUpgradeMode(UpgradeMode.LAST_STATE);

        reconciler.reconcile(deployment, context);

        deployment.getSpec().setFlinkVersion(FlinkVersion.v1_19);

        var reconStatus = deployment.getStatus().getReconciliationStatus();

        // Do not trigger update until running
        reconciler.reconcile(deployment, context);
        assertEquals(ReconciliationState.DEPLOYED, reconStatus.getState());

        getJobStatus(deployment).setState(RUNNING);
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

        reconciler.reconcile(deployment, context);
        assertEquals(JobState.RUNNING, getReconciledJobState(deployment));
        assertEquals(0, flinkService.getDesiredReplicas());

        getJobSpec(deployment).setParallelism(4);
        reconciler.reconcile(deployment, context);
        assertEquals(JobState.RUNNING, getReconciledJobState(deployment));
        assertEquals(2, flinkService.getDesiredReplicas());

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
                    public CancelResult cancelJob(
                            FlinkDeployment deployment,
                            SuspendMode upgradeMode,
                            Configuration conf) {
                        return CancelResult.completed(null);
                    }
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
                flinkResourceEventCollector.events.getLast().getReason());
        assertEquals(3, flinkResourceEventCollector.events.size());

        // Job should not be stopped, we simply call the rescale api
        assertEquals(JobState.RUNNING, getReconciledJobState(deployment));
        var reconStatus = deployment.getStatus().getReconciliationStatus();
        assertEquals(
                v1.toHexString() + ":2",
                reconStatus
                        .deserializeLastReconciledSpec()
                        .getFlinkConfiguration()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES.key()));
        assertEquals(ReconciliationState.DEPLOYED, reconStatus.getState());
        assertFalse(reconStatus.isLastReconciledSpecStable());

        // Reconciler should not do anything after successful scaling
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        assertEquals(1, rescaleCounter.get());
        assertEquals(3, flinkResourceEventCollector.events.size());
        assertFalse(reconStatus.isLastReconciledSpecStable());
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

                    @Override
                    public void cleanup(KubernetesJobAutoScalerContext ctx) {
                        overrideFunction.set(s -> {});
                    }
                };
        var v1 = new JobVertexID();

        appReconciler = new ApplicationReconciler(eventRecorder, statusRecorder, autoscaler);

        var deployment = TestUtils.buildApplicationCluster();
        var config = deployment.getSpec().getFlinkConfiguration();
        config.put(AutoScalerOptions.AUTOSCALER_ENABLED.key(), "true");
        config.put(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":1");

        var specCopy = SpecUtils.clone(deployment.getSpec());

        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());
        deployment.setSpec(SpecUtils.clone(specCopy));

        // Job running verify no upgrades if overrides are empty
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        deployment.setSpec(SpecUtils.clone(specCopy));
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(RUNNING, deployment.getStatus().getJobStatus().getState());

        // Test overrides are applied correctly
        overrideFunction.set(
                s ->
                        s.getFlinkConfiguration()
                                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":2"));

        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        deployment.setSpec(SpecUtils.clone(specCopy));
        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(
                Map.of(v1.toHexString(), "2"),
                ctxFactory
                        .getResourceContext(deployment, context)
                        .getObserveConfig()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES));

        // Set the job into running state (scale up completed)
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        deployment.setSpec(SpecUtils.clone(specCopy));
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());
        deployment.setSpec(SpecUtils.clone(specCopy));

        // Make sure new reset nonce clears autoscaler
        deployment.getSpec().getJob().setAutoscalerResetNonce(1L);
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        deployment.setSpec(SpecUtils.clone(specCopy));
        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(
                Map.of(v1.toHexString(), "1"),
                ctxFactory
                        .getResourceContext(deployment, context)
                        .getObserveConfig()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES));
        assertEquals(
                1L,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getAutoscalerResetNonce());

        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());
        deployment.setSpec(SpecUtils.clone(specCopy));

        // Make sure autoscaler reset nonce properly updated even if no deployment happens

        deployment.getSpec().getJob().setAutoscalerResetNonce(2L);
        appReconciler.reconcile(ctxFactory.getResourceContext(deployment, context));
        deployment.setSpec(SpecUtils.clone(specCopy));
        assertEquals(
                2L,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getAutoscalerResetNonce());
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
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

        if (!flinkVersion.isEqualOrNewer(FlinkVersion.v1_16)) {
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

        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED);
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
    public void testTerminalJmTtlOnSuspend() throws Throwable {
        testTerminalJmTtl(
                dep -> {
                    getJobSpec(dep).setState(JobState.SUSPENDED);
                    reconciler.reconcile(dep, context);
                });
    }

    @Test
    public void testTerminalJmTtlOnFinished() throws Throwable {
        testTerminalJmTtl(
                dep ->
                        dep.getStatus()
                                .getJobStatus()
                                .setState(org.apache.flink.api.common.JobStatus.FINISHED));
    }

    @Test
    public void testTerminalJmTtlOnFailed() throws Throwable {
        testTerminalJmTtl(
                dep ->
                        dep.getStatus()
                                .getJobStatus()
                                .setState(org.apache.flink.api.common.JobStatus.FAILED));
    }

    public void testTerminalJmTtl(ThrowingConsumer<FlinkDeployment> deploymentSetup)
            throws Throwable {
        var deployment = TestUtils.buildApplicationCluster();
        getJobSpec(deployment).setUpgradeMode(UpgradeMode.SAVEPOINT);
        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());
        deploymentSetup.accept(deployment);
        var status = deployment.getStatus();
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
        // Make sure we don't resubmit
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
        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED);

        var deleted = new AtomicBoolean(false);

        flinkService =
                new TestingFlinkService() {
                    @Override
                    protected void deleteHAData(
                            String namespace, String clusterId, Configuration conf) {
                        deleted.set(true);
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
        Assertions.assertEquals(
                MSG_SUBMIT, flinkResourceEventCollector.events.remove().getMessage());
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        flinkService.clear();
        FlinkDeploymentStatus deploymentStatus = deployment.getStatus();
        deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        deploymentStatus.getJobStatus().setState(RECONCILING);
        reconciler.reconcile(deployment, context);
        Assertions.assertEquals(
                MSG_RECOVERY, flinkResourceEventCollector.events.remove().getMessage());
    }

    @Test
    public void testRestartUnhealthyEvent() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED.key(), "true");
        reconciler.reconcile(deployment, context);
        Assertions.assertEquals(
                MSG_SUBMIT, flinkResourceEventCollector.events.remove().getMessage());
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        var clusterHealthInfo = new ClusterHealthInfo();
        clusterHealthInfo.setTimeStamp(System.currentTimeMillis());
        clusterHealthInfo.setNumRestarts(2);
        clusterHealthInfo.setHealthResult(ClusterHealthResult.error("error"));
        ClusterHealthEvaluator.setLastValidClusterHealthInfo(
                deployment.getStatus().getClusterInfo(), clusterHealthInfo);
        reconciler.reconcile(deployment, context);
        Assertions.assertEquals("error", flinkResourceEventCollector.events.remove().getMessage());
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
        verifyAndSetRunningJobsToStatus(
                deployment,
                flinkService.listJobs(),
                ApplicationReconciler.LAST_STATE_DUMMY_SP_PATH);

        reconciler.reconcile(deployment, context);
        // Make sure upgrade is properly triggered now
        lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        assertEquals(deployment.getSpec().getRestartNonce(), lastReconciledSpec.getRestartNonce());
        assertEquals(JobState.SUSPENDED, lastReconciledSpec.getJob().getState());
        assertEquals(UpgradeMode.SAVEPOINT, lastReconciledSpec.getJob().getUpgradeMode());
        assertThat(TestUtils.getFlinkStateSnapshotsForResource(kubernetesClient, deployment))
                .hasSize(1)
                .allSatisfy(
                        snapshot -> {
                            assertThat(snapshot.getSpec().getSavepoint().getPath())
                                    .isEqualTo("savepoint_0");
                            assertEquals(
                                    "savepoint_0",
                                    deployment
                                            .getStatus()
                                            .getJobStatus()
                                            .getUpgradeSavepointPath());
                        });
    }

    @Test
    public void testUpgradeReconciledGeneration() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment.getMetadata().setGeneration(1L);

        // Initial deployment
        reconciler.reconcile(deployment, context);
        verifyAndSetRunningJobsToStatus(deployment, flinkService.listJobs());

        assertEquals(1L, deployment.getStatus().getObservedGeneration());

        // Submit no-op upgrade
        deployment.getSpec().getFlinkConfiguration().put("kubernetes.operator.test", "value");
        deployment.getMetadata().setGeneration(2L);

        reconciler.reconcile(deployment, context);
        assertEquals(2L, deployment.getStatus().getObservedGeneration());
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
                jmStarted ? ReconciliationState.ROLLING_BACK : ReconciliationState.ROLLED_BACK,
                deployment.getStatus().getReconciliationStatus().getState());
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
        assertEquals(RECONCILING, deployment.getStatus().getJobStatus().getState());
    }

    @ParameterizedTest
    @EnumSource(UpgradeMode.class)
    public void testSavepointRedeploy(UpgradeMode upgradeMode) throws Exception {
        var deployment = TestUtils.buildApplicationCluster();
        deployment.getSpec().getJob().setUpgradeMode(upgradeMode);

        reconciler.reconcile(deployment, context);
        var runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        // Test savepoint redeploy for running job
        verifySavepointRedeploy(deployment, runningJobs, "sp-t1");

        // Test savepoint redeploy for non-running job, we just deployed
        verifySavepointRedeploy(deployment, runningJobs, "sp-t2");

        // Test redeploy for to the same savepoint path
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        verifySavepointRedeploy(deployment, runningJobs, "sp-t2");

        // Test savepoint redeploy when jobstate is set to suspended
        deployment.getSpec().getJob().setState(JobState.SUSPENDED);
        verifySavepointRedeploy(deployment, runningJobs, "sp-t3");

        if (upgradeMode != UpgradeMode.STATELESS) {
            // When we suspended with a new initial savepoint path simple spec changes should use
            // the correct savepoint. This doesn't apply to stateless mode as that starts from empty
            // state after suspend
            deployment.getSpec().getJob().setParallelism(321);
            verifySavepointRedeploy(deployment, runningJobs, "sp-t3");

            deployment.getSpec().getJob().setState(JobState.SUSPENDED);
            reconciler.reconcile(deployment, context);
            assertEquals(
                    JobManagerDeploymentStatus.MISSING,
                    deployment.getStatus().getJobManagerDeploymentStatus());

            // Test suspend and a new initialSavepointPath
            deployment.getSpec().getJob().setState(JobState.RUNNING);
            verifySavepointRedeploy(deployment, runningJobs, "sp-t4");
        }
    }

    private void verifySavepointRedeploy(
            FlinkDeployment deployment,
            List<Tuple3<String, JobStatusMessage, Configuration>> runningJobs,
            String savepoint)
            throws Exception {
        var job = deployment.getSpec().getJob();
        job.setInitialSavepointPath(savepoint);
        job.setSavepointRedeployNonce(
                Optional.ofNullable(job.getSavepointRedeployNonce()).orElse(0L) + 1);
        reconciler.reconcile(deployment, context);
        boolean shouldRun = deployment.getSpec().getJob().getState() == JobState.RUNNING;

        if (shouldRun) {
            // Verify job is redeployed with sp
            assertEquals(1, runningJobs.size());
            assertEquals(savepoint, runningJobs.get(0).f0);
        } else {
            // Verify that job is stopped
            assertTrue(runningJobs.isEmpty());
        }

        var status = deployment.getStatus();
        assertEquals(
                shouldRun
                        ? JobManagerDeploymentStatus.DEPLOYING
                        : JobManagerDeploymentStatus.MISSING,
                status.getJobManagerDeploymentStatus());

        // Verify that savepoint and upgrade mode is recorded correctly in reconciled spec
        assertEquals(savepoint, status.getJobStatus().getUpgradeSavepointPath());
        assertEquals(
                UpgradeMode.SAVEPOINT,
                status.getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getUpgradeMode());

        assertTrue(status.getReconciliationStatus().isLastReconciledSpecStable());
    }

    private void offsetReconcilerClock(FlinkDeployment dep, Duration offset) {
        testClock = Clock.offset(testClock, offset);
        appReconciler.setClock(testClock);
    }
}
