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
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.UpgradeFailureException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotTriggerTimestampStore;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;
import org.apache.flink.kubernetes.operator.service.CheckpointHistoryWrapper;
import org.apache.flink.kubernetes.operator.service.SuspendMode;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotUtils;
import org.apache.flink.kubernetes.operator.utils.SnapshotUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Value;
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

    private final SnapshotTriggerTimestampStore snapshotTriggerTimestampStore =
            new SnapshotTriggerTimestampStore();

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
            var jobUpgrade = getJobUpgrade(ctx, deployConfig);
            if (!jobUpgrade.isAvailable()) {
                // If job upgrade is currently not available for some reason we must still check if
                // other reconciliation action may be taken while we wait...
                LOG.info(
                        "Job is not running and checkpoint information is not available for executing the upgrade, waiting for upgradeable state");
                return !jobUpgrade.allowOtherReconcileActions;
            }
            LOG.debug("Job upgrade available: {}", jobUpgrade);

            var suspendMode = jobUpgrade.getSuspendMode();
            if (suspendMode != SuspendMode.NOOP) {
                eventRecorder.triggerEvent(
                        resource,
                        EventRecorder.Type.Normal,
                        EventRecorder.Reason.Suspended,
                        EventRecorder.Component.JobManagerDeployment,
                        MSG_SUSPENDED,
                        ctx.getKubernetesClient());
            }

            boolean async = cancelJob(ctx, suspendMode);
            if (async) {
                // Async cancellation will be completed in the background, so we must exit
                // reconciliation early and wait until it completes to finish the upgrade.
                resource.getStatus()
                        .getReconciliationStatus()
                        .setState(ReconciliationState.UPGRADING);
                ReconciliationUtils.updateLastReconciledSpec(
                        resource,
                        (s, m) -> {
                            s.getJob().setUpgradeMode(jobUpgrade.getRestoreMode());
                            m.setFirstDeployment(false);
                        });
                return true;
            }

            // We must record the upgrade mode used to the status later
            currentDeploySpec.getJob().setUpgradeMode(jobUpgrade.getRestoreMode());

            if (desiredJobState == JobState.RUNNING) {
                ReconciliationUtils.updateStatusBeforeDeploymentAttempt(
                        resource, deployConfig, clock);
            } else {
                ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConfig, clock);
            }

            if (suspendMode == SuspendMode.NOOP) {
                // If already cancelled we want to restore immediately so we modify the current
                // state
                // We don't do this when we actually performed a potentially lengthy cancel action
                // to allow reconciling the spec
                lastReconciledSpec.getJob().setUpgradeMode(jobUpgrade.getRestoreMode());
                currentJobState = JobState.SUSPENDED;
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

    protected JobUpgrade getJobUpgrade(FlinkResourceContext<CR> ctx, Configuration deployConfig)
            throws Exception {
        var resource = ctx.getResource();
        var status = resource.getStatus();
        var upgradeMode = resource.getSpec().getJob().getUpgradeMode();
        boolean terminal = ReconciliationUtils.isJobInTerminalState(status);

        if (upgradeMode == UpgradeMode.STATELESS) {
            LOG.info("Stateless job, ready for upgrade");
            return JobUpgrade.stateless(terminal);
        }

        var flinkService = ctx.getFlinkService();
        if (ReconciliationUtils.isJobCancelled(status)
                || (terminal && !flinkService.isHaMetadataAvailable(ctx.getObserveConfig()))) {

            if (!SnapshotUtils.lastSavepointKnown(status)) {
                throw new UpgradeFailureException(
                        "Job is in terminal state but last checkpoint is unknown, possibly due to an unrecoverable restore error. Manual restore required.",
                        "UpgradeFailed");
            }
            LOG.info("Job is in terminal state, ready for upgrade from observed latest state");
            return JobUpgrade.savepoint(true);
        }

        if (ReconciliationUtils.isJobCancelling(status)) {
            LOG.info("Cancellation is in progress. Waiting for cancelled state.");
            return JobUpgrade.pendingCancellation();
        }

        boolean running = ReconciliationUtils.isJobRunning(status);
        boolean versionChanged =
                flinkVersionChanged(
                        ReconciliationUtils.getDeployedSpec(resource), resource.getSpec());

        if (upgradeMode == UpgradeMode.SAVEPOINT) {
            if (running) {
                LOG.info("Job is in running state, ready for upgrade with savepoint");
                return JobUpgrade.savepoint(false);
            } else if (versionChanged
                    || deployConfig.get(
                            KubernetesOperatorConfigOptions
                                    .OPERATOR_JOB_UPGRADE_LAST_STATE_FALLBACK_ENABLED)) {
                LOG.info("Falling back to last-state upgrade mode from savepoint");
                ctx.getResource()
                        .getSpec()
                        .getJob()
                        .setUpgradeMode(upgradeMode = UpgradeMode.LAST_STATE);
            } else {
                LOG.info("Last-state fallback is disabled, waiting for upgradable state");
                return JobUpgrade.pendingUpgrade();
            }
        }

        if (upgradeMode == UpgradeMode.LAST_STATE) {
            if (versionChanged) {
                // We need some special handling in case of version upgrades where HA based
                // last-state upgrade is not possible
                boolean savepointPossible =
                        !StringUtils.isNullOrWhitespaceOnly(
                                ctx.getObserveConfig()
                                        .getString(CheckpointingOptions.SAVEPOINT_DIRECTORY));
                if (running && savepointPossible) {
                    LOG.info("Using savepoint to upgrade Flink version");
                    return JobUpgrade.savepoint(false);
                } else if (ReconciliationUtils.isJobCancellable(resource.getStatus())) {
                    LOG.info("Using last-state upgrade with cancellation to upgrade Flink version");
                    return JobUpgrade.lastStateUsingCancel();
                } else {
                    LOG.info(
                            "Neither savepoint nor cancellation is possible, cannot perform stateful version upgrade");
                    return JobUpgrade.pendingUpgrade();
                }
            }

            boolean cancellable = allowLastStateCancel(ctx);
            if (running) {
                var mode = getUpgradeModeBasedOnStateAge(ctx, deployConfig, cancellable);
                LOG.info("Job is running, using {} for last-state upgrade", mode);
                return mode;
            }

            if (cancellable) {
                LOG.info("Job is not running, using cancel to perform last-state upgrade");
                return JobUpgrade.lastStateUsingCancel();
            }
        }

        return JobUpgrade.unavailable();
    }

    @VisibleForTesting
    protected JobUpgrade getUpgradeModeBasedOnStateAge(
            FlinkResourceContext<CR> ctx, Configuration deployConfig, boolean cancellable)
            throws Exception {

        var defaultMode =
                cancellable ? JobUpgrade.lastStateUsingCancel() : JobUpgrade.lastStateUsingHaMeta();
        var maxAge = deployConfig.get(OPERATOR_JOB_UPGRADE_LAST_STATE_CHECKPOINT_MAX_AGE);
        if (maxAge == null) {
            return defaultMode;
        }

        var jobStatus = ctx.getResource().getStatus().getJobStatus();
        var jobId = JobID.fromHexString(jobStatus.getJobId());
        var startTime = Instant.ofEpochMilli(Long.parseLong(jobStatus.getStartTime()));
        var now = clock.instant();

        Predicate<Instant> withinMaxAge = ts -> now.minus(maxAge).isBefore(ts);

        // If job started recently, no need to query checkpoint
        if (withinMaxAge.test(startTime)) {
            return defaultMode;
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
            return defaultMode;
        } else if (withinMaxAge.test(pendingTs)) {
            LOG.info("Waiting for pending checkpoint to complete before upgrading.");
            return JobUpgrade.pendingUpgrade();
        } else {
            LOG.info(
                    "Using savepoint upgrade mode because latest checkpoint is too old for last-state upgrade");
            return JobUpgrade.savepoint(false);
        }
    }

    private boolean allowLastStateCancel(FlinkResourceContext<CR> ctx) {
        var resource = ctx.getResource();
        if (!ReconciliationUtils.isJobCancellable(resource.getStatus())) {
            return false;
        }
        if (resource instanceof FlinkSessionJob) {
            return true;
        }

        var conf = ctx.getObserveConfig();
        if (!ctx.getFlinkService().isHaMetadataAvailable(conf)) {
            LOG.info("HA metadata not available, cancel will be used instead of last-state");
            return true;
        }
        return conf.get(KubernetesOperatorConfigOptions.OPERATOR_JOB_UPGRADE_LAST_STATE_CANCEL_JOB);
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
                            ctx.getResource().getStatus().getJobStatus().getUpgradeSavepointPath());
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
     * Updates the upgrade savepoint field in the JobSpec of the current Flink resource and if
     * snapshot resources are enabled, a new FlinkStateSnapshot will be created.
     *
     * @param ctx context
     * @param savepointLocation location of savepoint taken
     * @param cancelTs Timestamp when upgrade/cancel was triggered
     */
    protected void setUpgradeSavepointPath(
            FlinkResourceContext<?> ctx, String savepointLocation, Instant cancelTs) {
        var conf = ctx.getObserveConfig();
        var savepointFormatType =
                SavepointFormatType.valueOf(
                        conf.get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE)
                                .name());

        FlinkStateSnapshotUtils.createUpgradeSnapshotResource(
                conf,
                ctx.getOperatorConfig(),
                ctx.getKubernetesClient(),
                ctx.getResource(),
                savepointFormatType,
                savepointLocation);
        var jobStatus = ctx.getResource().getStatus().getJobStatus();
        jobStatus.setUpgradeSavepointPath(savepointLocation);

        // Register created savepoint in the now deprecated savepoint info and history
        var savepoint =
                new Savepoint(
                        cancelTs.toEpochMilli(),
                        savepointLocation,
                        SnapshotTriggerType.UPGRADE,
                        savepointFormatType,
                        null);
        jobStatus.getSavepointInfo().updateLastSavepoint(savepoint);
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
        var jobStatus = status.getJobStatus().getState();
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

        var lastTrigger =
                snapshotTriggerTimestampStore.getLastPeriodicTriggerInstant(
                        resource,
                        snapshotType,
                        FlinkStateSnapshotUtils.getFlinkStateSnapshotsSupplier(ctx));

        var triggerOpt =
                SnapshotUtils.shouldTriggerSnapshot(resource, conf, snapshotType, lastTrigger);
        if (triggerOpt.isEmpty()) {
            return false;
        }
        var triggerType = triggerOpt.get();

        if (SnapshotTriggerType.PERIODIC.equals(triggerType)) {
            snapshotTriggerTimestampStore.updateLastPeriodicTriggerTimestamp(
                    resource, snapshotType, Instant.now());
        }

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

        var upgradeStatePath =
                ctx.getResource().getStatus().getJobStatus().getUpgradeSavepointPath();
        var savepointLegacy =
                ctx.getResource().getStatus().getJobStatus().getSavepointInfo().getLastSavepoint();
        var lastSavepointKnown = upgradeStatePath != null || savepointLegacy != null;

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
        cancelJob(ctx, SuspendMode.STATELESS);
        currentDeploySpec.getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);

        Optional<String> savepointPath =
                Optional.ofNullable(currentDeploySpec.getJob().getInitialSavepointPath());
        status.getJobStatus().setUpgradeSavepointPath(savepointPath.orElse(null));

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
     * @param suspendMode Suspend mode used during cancel.
     * @throws Exception Error during cancellation.
     * @return True if this is an async cancellation
     */
    protected abstract boolean cancelJob(FlinkResourceContext<CR> ctx, SuspendMode suspendMode)
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
    public static class JobUpgrade {
        SuspendMode suspendMode;
        UpgradeMode restoreMode;
        boolean available;
        boolean allowFallback;
        boolean allowOtherReconcileActions;

        static JobUpgrade stateless(boolean terminal) {
            return new JobUpgrade(
                    terminal ? SuspendMode.NOOP : SuspendMode.STATELESS,
                    UpgradeMode.STATELESS,
                    true,
                    false,
                    false);
        }

        static JobUpgrade savepoint(boolean terminal) {
            return new JobUpgrade(
                    terminal ? SuspendMode.NOOP : SuspendMode.SAVEPOINT,
                    UpgradeMode.SAVEPOINT,
                    true,
                    false,
                    false);
        }

        static JobUpgrade lastStateUsingHaMeta() {
            return new JobUpgrade(
                    SuspendMode.LAST_STATE, UpgradeMode.LAST_STATE, true, false, false);
        }

        static JobUpgrade lastStateUsingCancel() {
            return new JobUpgrade(SuspendMode.CANCEL, UpgradeMode.SAVEPOINT, true, false, false);
        }

        static JobUpgrade pendingCancellation() {
            return new JobUpgrade(null, null, false, false, false);
        }

        static JobUpgrade pendingUpgrade() {
            return new JobUpgrade(null, null, false, false, true);
        }

        static JobUpgrade unavailable() {
            return new JobUpgrade(null, null, false, true, true);
        }
    }
}
