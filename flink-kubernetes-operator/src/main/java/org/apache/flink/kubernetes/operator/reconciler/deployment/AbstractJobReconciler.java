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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotReference;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;
import org.apache.flink.kubernetes.operator.service.CheckpointHistoryWrapper;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotUtils;
import org.apache.flink.kubernetes.operator.utils.SnapshotUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.util.Preconditions;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Value;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_JOB_RESTART_FAILED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_JOB_UPGRADE_LAST_STATE_CHECKPOINT_MAX_AGE;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;

/**
 * Reconciler responsible for handling the job lifecycle according to the desired and current
 * states.
 */
public abstract class AbstractJobReconciler<
                CR extends AbstractFlinkResource<SPEC, STATUS>,
                SPEC extends AbstractFlinkSpec,
                STATUS extends CommonStatus<SPEC>>
        extends AbstractFlinkResourceReconciler<CR, SPEC, STATUS> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJobReconciler.class);

    public static final String LAST_STATE_DUMMY_SP_PATH = "KUBERNETES_OPERATOR_LAST_STATE";

    public AbstractJobReconciler(
            EventRecorder eventRecorder,
            StatusRecorder<CR, STATUS> statusRecorder,
            JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> autoscaler) {
        super(eventRecorder, statusRecorder, autoscaler);
    }

    @Override
    public boolean readyToReconcile(FlinkResourceContext<CR> ctx) {
        var status = ctx.getResource().getStatus();
        if (status.getReconciliationStatus().isBeforeFirstDeployment()) {
            return true;
        }
        if (shouldWaitForPendingSavepoint(status.getJobStatus(), ctx.getObserveConfig())) {
            LOG.info("Delaying job reconciliation until pending savepoint is completed.");
            return false;
        }
        return true;
    }

    private boolean shouldWaitForPendingSavepoint(JobStatus jobStatus, Configuration conf) {
        return !conf.getBoolean(
                        KubernetesOperatorConfigOptions.JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT)
                && SnapshotUtils.savepointInProgress(jobStatus);
    }

    @Override
    protected boolean reconcileSpecChange(
            DiffType diffType,
            FlinkResourceContext<CR> ctx,
            Configuration deployConfig,
            SPEC lastReconciledSpec)
            throws Exception {

        var resource = ctx.getResource();
        STATUS status = resource.getStatus();
        SPEC currentDeploySpec = resource.getSpec();

        JobState currentJobState = lastReconciledSpec.getJob().getState();
        JobState desiredJobState = currentDeploySpec.getJob().getState();

        if (diffType == DiffType.SAVEPOINT_REDEPLOY) {
            redeployWithSavepoint(
                    ctx, deployConfig, resource, status, currentDeploySpec, desiredJobState);
            return true;
        }

        if (currentJobState == JobState.RUNNING) {
            if (desiredJobState == JobState.RUNNING) {
                LOG.info("Upgrading/Restarting running job, suspending first...");
            }
            AvailableUpgradeMode availableUpgradeMode = getAvailableUpgradeMode(ctx, deployConfig);
            if (!availableUpgradeMode.isAvailable()) {
                return false;
            }

            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Normal,
                    EventRecorder.Reason.Suspended,
                    EventRecorder.Component.JobManagerDeployment,
                    MSG_SUSPENDED,
                    ctx.getKubernetesClient());

            UpgradeMode upgradeMode = availableUpgradeMode.getUpgradeMode().get();

            // We must record the upgrade mode used to the status later
            currentDeploySpec.getJob().setUpgradeMode(upgradeMode);

            cancelJob(ctx, upgradeMode);
            if (desiredJobState == JobState.RUNNING) {
                ReconciliationUtils.updateStatusBeforeDeploymentAttempt(
                        resource, deployConfig, clock);
            } else {
                ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConfig, clock);
            }
        }

        if (currentJobState == JobState.SUSPENDED && desiredJobState == JobState.RUNNING) {
            // We inherit the upgrade mode unless stateless upgrade requested
            if (currentDeploySpec.getJob().getUpgradeMode() != UpgradeMode.STATELESS) {
                currentDeploySpec
                        .getJob()
                        .setUpgradeMode(lastReconciledSpec.getJob().getUpgradeMode());
            }
            // We record the target spec into an upgrading state before deploying
            ReconciliationUtils.updateStatusBeforeDeploymentAttempt(resource, deployConfig, clock);
            statusRecorder.patchAndCacheStatus(resource, ctx.getKubernetesClient());

            restoreJob(
                    ctx,
                    currentDeploySpec,
                    deployConfig,
                    // We decide to enforce HA based on how job was previously suspended
                    lastReconciledSpec.getJob().getUpgradeMode() == UpgradeMode.LAST_STATE);

            ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConfig, clock);
        }
        return true;
    }

    protected AvailableUpgradeMode getAvailableUpgradeMode(
            FlinkResourceContext<CR> ctx, Configuration deployConfig) throws Exception {
        var resource = ctx.getResource();
        var status = resource.getStatus();
        var upgradeMode = resource.getSpec().getJob().getUpgradeMode();

        if (upgradeMode == UpgradeMode.STATELESS) {
            LOG.info("Stateless job, ready for upgrade");
            return AvailableUpgradeMode.of(UpgradeMode.STATELESS);
        }

        var flinkService = ctx.getFlinkService();
        if (ReconciliationUtils.isJobInTerminalState(status)
                && !flinkService.isHaMetadataAvailable(ctx.getObserveConfig())) {

            if (!SnapshotUtils.lastSavepointKnown(status)) {
                throw new RecoveryFailureException(
                        "Job is in terminal state but last checkpoint is unknown, possibly due to an unrecoverable restore error. Manual restore required.",
                        "UpgradeFailed");
            }
            LOG.info(
                    "Job is in terminal state, ready for upgrade from observed latest checkpoint/savepoint");
            return AvailableUpgradeMode.of(UpgradeMode.SAVEPOINT);
        }

        if (ReconciliationUtils.isJobRunning(status)) {
            LOG.info("Job is in running state, ready for upgrade with {}", upgradeMode);
            var changedToLastStateWithoutHa =
                    ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                            resource, ctx.getObserveConfig());
            if (changedToLastStateWithoutHa) {
                LOG.info(
                        "Using savepoint upgrade mode when switching to last-state without HA previously enabled");
                return AvailableUpgradeMode.of(UpgradeMode.SAVEPOINT);
            }

            if (flinkVersionChanged(
                    ReconciliationUtils.getDeployedSpec(resource), resource.getSpec())) {
                LOG.info("Using savepoint upgrade mode when upgrading Flink version");
                return AvailableUpgradeMode.of(UpgradeMode.SAVEPOINT);
            }

            if (upgradeMode == UpgradeMode.LAST_STATE) {
                return changeLastStateIfCheckpointTooOld(ctx, deployConfig);
            }

            return AvailableUpgradeMode.of(UpgradeMode.SAVEPOINT);
        }

        return AvailableUpgradeMode.unavailable();
    }

    @VisibleForTesting
    protected AvailableUpgradeMode changeLastStateIfCheckpointTooOld(
            FlinkResourceContext<CR> ctx, Configuration deployConfig) throws Exception {

        var maxAge = deployConfig.get(OPERATOR_JOB_UPGRADE_LAST_STATE_CHECKPOINT_MAX_AGE);
        if (maxAge == null) {
            return AvailableUpgradeMode.of(UpgradeMode.LAST_STATE);
        }

        var jobStatus = ctx.getResource().getStatus().getJobStatus();
        var jobId = JobID.fromHexString(jobStatus.getJobId());
        var startTime = Instant.ofEpochMilli(Long.parseLong(jobStatus.getStartTime()));
        var now = clock.instant();

        Predicate<Instant> withinMaxAge = ts -> now.minus(maxAge).isBefore(ts);

        // If job started recently, no need to query checkpoint
        if (withinMaxAge.test(startTime)) {
            return AvailableUpgradeMode.of(UpgradeMode.LAST_STATE);
        }

        var chkInfo = ctx.getFlinkService().getCheckpointInfo(jobId, ctx.getObserveConfig());
        var completedTs =
                chkInfo.f0
                        .map(CheckpointHistoryWrapper.CompletedCheckpointInfo::getTimestamp)
                        .map(Instant::ofEpochMilli)
                        .orElse(Instant.MIN);
        var pendingTs =
                chkInfo.f1
                        .map(CheckpointHistoryWrapper.PendingCheckpointInfo::getTimestamp)
                        .map(Instant::ofEpochMilli)
                        .orElse(Instant.MIN);

        if (withinMaxAge.test(completedTs)) {
            // We have a recent enough checkpoint
            return AvailableUpgradeMode.of(UpgradeMode.LAST_STATE);
        } else if (withinMaxAge.test(pendingTs)) {
            LOG.info("Waiting for pending checkpoint to complete before upgrading.");
            return AvailableUpgradeMode.pendingUpgrade();
        } else {
            LOG.info(
                    "Using savepoint upgrade mode because latest checkpoint is too old for last-state upgrade");
            return AvailableUpgradeMode.of(UpgradeMode.SAVEPOINT);
        }
    }

    protected void restoreJob(
            FlinkResourceContext<CR> ctx,
            SPEC spec,
            Configuration deployConfig,
            boolean requireHaMetadata)
            throws Exception {
        Optional<String> savepointOpt = Optional.empty();

        if (spec.getJob().getUpgradeMode() == UpgradeMode.SAVEPOINT) {
            savepointOpt =
                    Optional.ofNullable(
                                    ctx.getResource()
                                            .getStatus()
                                            .getJobStatus()
                                            .getUpgradeSnapshotReference())
                            .map(
                                    ref ->
                                            FlinkStateSnapshotUtils
                                                    .getValidatedFlinkStateSnapshotPath(
                                                            ctx.getKubernetesClient(), ref));
            if (savepointOpt.isEmpty()) {
                savepointOpt =
                        Optional.ofNullable(
                                        ctx.getResource()
                                                .getStatus()
                                                .getJobStatus()
                                                .getSavepointInfo()
                                                .getLastSavepoint())
                                .flatMap(s -> Optional.ofNullable(s.getLocation()));
            }
        }

        deploy(ctx, spec, deployConfig, savepointOpt, requireHaMetadata);
    }

    /**
     * Updates the upgrade snapshot reference field in the JobSpec of the current Flink resource. If
     * snapshot resources are enabled, a new FlinkStateSnapshot will be created, else it will only
     * set the path field of the snapshot reference.
     *
     * @param ctx context
     * @param savepointLocation location of savepoint taken
     */
    protected void setUpgradeSnapshotReferenceFromSavepoint(
            FlinkResourceContext<?> ctx, String savepointLocation) {
        var conf = ObjectUtils.firstNonNull(ctx.getObserveConfig(), new Configuration());
        var savepointFormatType =
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);

        var snapshotRef =
                FlinkStateSnapshotUtils.createReferenceForUpgradeSavepoint(
                        conf,
                        ctx.getOperatorConfig(),
                        ctx.getKubernetesClient(),
                        ctx.getResource(),
                        SavepointFormatType.valueOf(savepointFormatType.name()),
                        savepointLocation);
        ctx.getResource().getStatus().getJobStatus().setUpgradeSnapshotReference(snapshotRef);
    }

    /**
     * Triggers any pending manual or periodic snapshots and updates the status accordingly.
     *
     * @param ctx Reconciliation context.
     * @return True if a snapshot was triggered.
     * @throws Exception An error during snapshot triggering.
     */
    @Override
    public boolean reconcileOtherChanges(FlinkResourceContext<CR> ctx) throws Exception {
        var status = ctx.getResource().getStatus();
        var jobStatus =
                org.apache.flink.api.common.JobStatus.valueOf(status.getJobStatus().getState());
        if (jobStatus == org.apache.flink.api.common.JobStatus.FAILED
                && ctx.getObserveConfig().getBoolean(OPERATOR_JOB_RESTART_FAILED)) {
            LOG.info("Stopping failed Flink job...");
            cleanupAfterFailedJob(ctx);
            status.setError(null);
            resubmitJob(ctx, false);
            return true;
        } else {
            boolean savepointTriggered = triggerSnapshotIfNeeded(ctx, SAVEPOINT);
            boolean checkpointTriggered = triggerSnapshotIfNeeded(ctx, CHECKPOINT);

            return savepointTriggered || checkpointTriggered;
        }
    }

    /**
     * Triggers specified snapshot type if needed. When using FlinkStateSnapshot resources this can
     * only be periodic snapshot. If using the legacy snapshot system, this can be manual as well.
     *
     * @param ctx Flink resource context
     * @param snapshotType type of snapshot
     * @return true if a snapshot was triggered
     * @throws Exception snapshot error
     */
    private boolean triggerSnapshotIfNeeded(FlinkResourceContext<CR> ctx, SnapshotType snapshotType)
            throws Exception {
        var resource = ctx.getResource();
        var conf = ctx.getObserveConfig();

        Optional<SnapshotTriggerType> triggerOpt =
                SnapshotUtils.shouldTriggerSnapshot(resource, conf, snapshotType);
        if (triggerOpt.isEmpty()) {
            return false;
        }
        var triggerType = triggerOpt.get();

        var createSnapshotResource =
                FlinkStateSnapshotUtils.isSnapshotResourceEnabled(ctx.getOperatorConfig(), conf);

        String jobId = resource.getStatus().getJobStatus().getJobId();
        switch (snapshotType) {
            case SAVEPOINT:
                var savepointFormatType =
                        conf.get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);
                var savepointDirectory =
                        Preconditions.checkNotNull(
                                conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));

                if (createSnapshotResource) {
                    FlinkStateSnapshotUtils.createSavepointResource(
                            ctx.getKubernetesClient(),
                            resource,
                            savepointDirectory,
                            triggerType,
                            SavepointFormatType.valueOf(savepointFormatType.name()),
                            conf.getBoolean(
                                    KubernetesOperatorConfigOptions
                                            .OPERATOR_JOB_SAVEPOINT_DISPOSE_ON_DELETE));

                    ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                            triggerType, resource, SAVEPOINT);
                } else {
                    var triggerId =
                            ctx.getFlinkService()
                                    .triggerSavepoint(
                                            jobId, savepointFormatType, savepointDirectory, conf);
                    resource.getStatus()
                            .getJobStatus()
                            .getSavepointInfo()
                            .setTrigger(
                                    triggerId,
                                    triggerType,
                                    SavepointFormatType.valueOf(savepointFormatType.name()));
                }

                break;
            case CHECKPOINT:
                if (createSnapshotResource) {
                    FlinkStateSnapshotUtils.createCheckpointResource(
                            ctx.getKubernetesClient(), resource, triggerType);

                    ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                            triggerType, resource, CHECKPOINT);
                } else {
                    var checkpointType =
                            conf.get(KubernetesOperatorConfigOptions.OPERATOR_CHECKPOINT_TYPE);
                    var triggerId =
                            ctx.getFlinkService()
                                    .triggerCheckpoint(
                                            jobId,
                                            org.apache.flink.core.execution.CheckpointType.valueOf(
                                                    checkpointType.name()),
                                            conf);
                    resource.getStatus()
                            .getJobStatus()
                            .getCheckpointInfo()
                            .setTrigger(triggerId, triggerType, checkpointType);
                }

                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }
        return true;
    }

    protected void resubmitJob(FlinkResourceContext<CR> ctx, boolean requireHaMetadata)
            throws Exception {
        LOG.info("Resubmitting Flink job...");
        SPEC specToRecover = ReconciliationUtils.getDeployedSpec(ctx.getResource());

        var upgradeSnapshotRef =
                ctx.getResource().getStatus().getJobStatus().getUpgradeSnapshotReference();
        var savepointLegacy =
                ctx.getResource().getStatus().getJobStatus().getSavepointInfo().getLastSavepoint();
        var lastSavepointKnown = upgradeSnapshotRef != null || savepointLegacy != null;

        if (requireHaMetadata) {
            specToRecover.getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        } else if (ctx.getResource().getSpec().getJob().getUpgradeMode() != UpgradeMode.STATELESS
                && lastSavepointKnown) {
            specToRecover.getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        }
        restoreJob(ctx, specToRecover, ctx.getObserveConfig(), requireHaMetadata);
    }

    private void redeployWithSavepoint(
            FlinkResourceContext<CR> ctx,
            Configuration deployConfig,
            CR resource,
            STATUS status,
            SPEC currentDeploySpec,
            JobState desiredJobState)
            throws Exception {
        LOG.info("Redeploying from savepoint");
        cancelJob(ctx, UpgradeMode.STATELESS);
        currentDeploySpec.getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);

        var snapshotRef = currentDeploySpec.getJob().getFlinkStateSnapshotReference();
        var initialSavepointPath = currentDeploySpec.getJob().getInitialSavepointPath();

        if (snapshotRef == null && initialSavepointPath != null) {
            snapshotRef = FlinkStateSnapshotReference.fromPath(initialSavepointPath);
        }

        Optional<String> savepointPath = Optional.empty();
        if (snapshotRef != null) {
            savepointPath =
                    Optional.of(
                            FlinkStateSnapshotUtils.getValidatedFlinkStateSnapshotPath(
                                    ctx.getKubernetesClient(), snapshotRef));
            status.getJobStatus().setUpgradeSnapshotReference(snapshotRef);
        }

        if (desiredJobState == JobState.RUNNING) {
            deploy(
                    ctx,
                    currentDeploySpec,
                    ctx.getDeployConfig(currentDeploySpec),
                    savepointPath,
                    false);
        }
        ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConfig, clock);
        status.getReconciliationStatus().markReconciledSpecAsStable();
    }

    /**
     * Cancel the job for the given resource using the specified upgrade mode.
     *
     * @param ctx Reconciler context.
     * @param upgradeMode Upgrade mode used during cancel.
     * @throws Exception Error during cancellation.
     */
    protected abstract void cancelJob(FlinkResourceContext<CR> ctx, UpgradeMode upgradeMode)
            throws Exception;

    /**
     * Removes a failed job.
     *
     * @param ctx Reconciler context.
     * @throws Exception Error during cancellation.
     */
    protected abstract void cleanupAfterFailedJob(FlinkResourceContext<CR> ctx) throws Exception;

    /** Object to capture available upgrade mode. */
    @Value
    public static class AvailableUpgradeMode {
        Optional<UpgradeMode> upgradeMode;
        boolean allowFallback;

        public boolean isAvailable() {
            return upgradeMode.isPresent();
        }

        static AvailableUpgradeMode of(UpgradeMode upgradeMode) {
            return new AvailableUpgradeMode(Optional.of(upgradeMode), false);
        }

        static AvailableUpgradeMode unavailable() {
            return new AvailableUpgradeMode(Optional.empty(), true);
        }

        static AvailableUpgradeMode pendingUpgrade() {
            return new AvailableUpgradeMode(Optional.empty(), false);
        }
    }
}
