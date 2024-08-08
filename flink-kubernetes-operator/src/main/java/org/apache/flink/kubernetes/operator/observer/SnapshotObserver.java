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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.utils.DateTimeUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotReference;
import org.apache.flink.kubernetes.operator.api.status.Checkpoint;
import org.apache.flink.kubernetes.operator.api.status.CheckpointInfo;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.ConfigOptionUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotUtils;
import org.apache.flink.kubernetes.operator.utils.SnapshotUtils;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.COMPLETED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CHECKPOINT_HISTORY_MAX_AGE;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CHECKPOINT_HISTORY_MAX_COUNT;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_CLEANUP_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;
import static org.apache.flink.kubernetes.operator.utils.SnapshotUtils.isSnapshotTriggeringSupported;

/** An observer of savepoint progress. */
public class SnapshotObserver<
        CR extends AbstractFlinkResource<?, STATUS>, STATUS extends CommonStatus<?>> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotObserver.class);

    private final EventRecorder eventRecorder;

    public SnapshotObserver(EventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
    }

    public void observeSavepointStatus(FlinkResourceContext<CR> ctx) {

        var resource = ctx.getResource();
        var jobStatus = resource.getStatus().getJobStatus();
        var jobId = jobStatus.getJobId();

        // If any manual or periodic savepoint is in progress, observe it
        if (SnapshotUtils.savepointInProgress(jobStatus)) {
            observeTriggeredSavepoint(ctx, jobId);
        }

        // If job is in globally terminal state, observe last savepoint
        if (ReconciliationUtils.isJobInTerminalState(resource.getStatus())) {
            observeLatestCheckpoint(
                    ctx.getFlinkService(), jobStatus, jobId, ctx.getObserveConfig());
        }

        cleanupSavepointHistory(ctx);
    }

    public void observeCheckpointStatus(FlinkResourceContext<CR> ctx) {
        if (!isSnapshotTriggeringSupported(ctx.getObserveConfig())) {
            return;
        }
        var resource = ctx.getResource();
        var jobStatus = resource.getStatus().getJobStatus();
        var jobId = jobStatus.getJobId();

        // If any manual or periodic checkpoint is in progress, observe it
        if (SnapshotUtils.checkpointInProgress(jobStatus)) {
            observeTriggeredCheckpoint(ctx, jobId);
        }
    }

    private void observeTriggeredSavepoint(FlinkResourceContext<CR> ctx, String jobID) {
        var resource = (AbstractFlinkResource<?, ?>) ctx.getResource();

        var savepointInfo = resource.getStatus().getJobStatus().getSavepointInfo();

        LOG.info("Observing savepoint status.");
        var savepointFetchResult =
                ctx.getFlinkService()
                        .fetchSavepointInfo(
                                savepointInfo.getTriggerId(), jobID, ctx.getObserveConfig());

        if (savepointFetchResult.isPending()) {
            LOG.info("Savepoint operation not finished yet...");
            return;
        }

        if (savepointFetchResult.getError() != null) {
            var err = savepointFetchResult.getError();
            Duration gracePeriod =
                    ctx.getObserveConfig()
                            .get(
                                    KubernetesOperatorConfigOptions
                                            .OPERATOR_SAVEPOINT_TRIGGER_GRACE_PERIOD);
            if (SnapshotUtils.gracePeriodEnded(gracePeriod, savepointInfo)) {
                LOG.error(
                        "Savepoint attempt failed after grace period. Won't be retried again: "
                                + err);
                ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                        savepointInfo.getTriggerType(),
                        (AbstractFlinkResource) resource,
                        SAVEPOINT);
            } else {
                LOG.warn("Savepoint failed within grace period, retrying: " + err);
            }
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.SavepointError,
                    EventRecorder.Component.Operator,
                    savepointInfo.formatErrorMessage(
                            resource.getSpec().getJob().getSavepointTriggerNonce()),
                    ctx.getKubernetesClient());
            savepointInfo.resetTrigger();
            return;
        }

        var savepoint =
                new Savepoint(
                        savepointInfo.getTriggerTimestamp(),
                        savepointFetchResult.getLocation(),
                        savepointInfo.getTriggerType(),
                        savepointInfo.getFormatType(),
                        SnapshotTriggerType.MANUAL == savepointInfo.getTriggerType()
                                ? resource.getSpec().getJob().getSavepointTriggerNonce()
                                : null);

        ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                savepointInfo.getTriggerType(), resource, SAVEPOINT);

        // In case of periodic and manual savepoint, we still use lastSavepoint
        savepointInfo.updateLastSavepoint(savepoint);
    }

    private void observeTriggeredCheckpoint(FlinkResourceContext<CR> ctx, String jobID) {
        var resource = (AbstractFlinkResource<?, ?>) ctx.getResource();

        CheckpointInfo checkpointInfo = resource.getStatus().getJobStatus().getCheckpointInfo();

        LOG.info("Observing checkpoint status.");
        var checkpointFetchResult =
                ctx.getFlinkService()
                        .fetchCheckpointInfo(
                                checkpointInfo.getTriggerId(), jobID, ctx.getObserveConfig());

        if (checkpointFetchResult.isPending()) {
            LOG.info("Checkpoint operation not finished yet...");
            return;
        }

        if (checkpointFetchResult.getError() != null) {
            var err = checkpointFetchResult.getError();
            Duration gracePeriod =
                    ctx.getObserveConfig()
                            .get(
                                    KubernetesOperatorConfigOptions
                                            .OPERATOR_CHECKPOINT_TRIGGER_GRACE_PERIOD);
            if (SnapshotUtils.gracePeriodEnded(gracePeriod, checkpointInfo)) {
                LOG.error(
                        "Checkpoint attempt failed after grace period. Won't be retried again: "
                                + err);
                ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                        checkpointInfo.getTriggerType(),
                        (AbstractFlinkResource) resource,
                        CHECKPOINT);
            } else {
                LOG.warn("Checkpoint failed within grace period, retrying: " + err);
            }
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.CheckpointError,
                    EventRecorder.Component.Operator,
                    checkpointInfo.formatErrorMessage(
                            resource.getSpec().getJob().getCheckpointTriggerNonce()),
                    ctx.getKubernetesClient());
            checkpointInfo.resetTrigger();
            return;
        }

        var checkpoint =
                new Checkpoint(
                        checkpointInfo.getTriggerTimestamp(),
                        checkpointInfo.getTriggerType(),
                        checkpointInfo.getFormatType(),
                        SnapshotTriggerType.MANUAL == checkpointInfo.getTriggerType()
                                ? resource.getSpec().getJob().getCheckpointTriggerNonce()
                                : null);

        ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                checkpointInfo.getTriggerType(), resource, CHECKPOINT);
        checkpointInfo.updateLastCheckpoint(checkpoint);
    }

    /** Clean up and dispose savepoints according to the configured max size/age. */
    @VisibleForTesting
    void cleanupSavepointHistory(FlinkResourceContext<CR> ctx) {
        if (!FlinkStateSnapshotUtils.isSnapshotResourceEnabled(
                ctx.getOperatorConfig(), ctx.getObserveConfig())) {
            cleanupSavepointHistoryLegacy(ctx);
            return;
        }

        var snapshots = ctx.getJosdkContext().getSecondaryResources(FlinkStateSnapshot.class);
        if (CollectionUtil.isNullOrEmpty(snapshots)) {
            return;
        }
        if (ctx.getObserveConfig().get(OPERATOR_SAVEPOINT_CLEANUP_ENABLED)) {
            var savepointsToDelete =
                    getFlinkStateSnapshotsToCleanUp(
                            snapshots, ctx.getObserveConfig(), ctx.getOperatorConfig(), SAVEPOINT);
            var checkpointsToDelete =
                    getFlinkStateSnapshotsToCleanUp(
                            snapshots, ctx.getObserveConfig(), ctx.getOperatorConfig(), CHECKPOINT);
            Stream.concat(savepointsToDelete.stream(), checkpointsToDelete.stream())
                    .forEach(
                            snapshot ->
                                    ctx.getKubernetesClient()
                                            .resource(snapshot)
                                            .withTimeoutInMillis(0L)
                                            .delete());
        }
    }

    @VisibleForTesting
    Set<FlinkStateSnapshot> getFlinkStateSnapshotsToCleanUp(
            Collection<FlinkStateSnapshot> snapshots,
            Configuration observeConfig,
            FlinkOperatorConfiguration operatorConfig,
            SnapshotType snapshotType) {
        var snapshotList =
                snapshots.stream()
                        .filter(s -> s.getStatus() != null)
                        .filter(s -> COMPLETED.equals(s.getStatus().getState()))
                        .filter(
                                s ->
                                        SnapshotTriggerType.PERIODIC.equals(
                                                FlinkStateSnapshotUtils.getSnapshotTriggerType(s)))
                        .filter(s -> (s.getSpec().isSavepoint() == (snapshotType == SAVEPOINT)))
                        .sorted(
                                Comparator.comparing(
                                        s ->
                                                DateTimeUtils.parseKubernetes(
                                                        s.getStatus().getResultTimestamp())))
                        .collect(Collectors.toList());

        var maxCount = getMaxCountForSnapshotType(observeConfig, operatorConfig, snapshotType);
        var maxTms = getMaxAgeForSnapshotType(observeConfig, operatorConfig, snapshotType);
        var result = new HashSet<FlinkStateSnapshot>();

        if (snapshotList.size() < 2) {
            return result;
        }
        var lastSnapshot = snapshotList.get(snapshotList.size() - 1);

        while (snapshotList.size() > maxCount) {
            var snapshot = snapshotList.remove(0);
            result.add(snapshot);
        }
        for (var snapshot : snapshotList) {
            var ts =
                    DateTimeUtils.parseKubernetes(snapshot.getStatus().getResultTimestamp())
                            .toEpochMilli();
            if (ts < maxTms && snapshot != lastSnapshot) {
                result.add(snapshot);
            }
        }

        return result;
    }

    private void cleanupSavepointHistoryLegacy(FlinkResourceContext<CR> ctx) {
        var maxTms =
                getMaxAgeForSnapshotType(
                        ctx.getObserveConfig(), ctx.getOperatorConfig(), SAVEPOINT);
        var maxCount =
                getMaxCountForSnapshotType(
                        ctx.getObserveConfig(), ctx.getOperatorConfig(), SAVEPOINT);

        var savepointHistory =
                ctx.getResource()
                        .getStatus()
                        .getJobStatus()
                        .getSavepointInfo()
                        .getSavepointHistory();

        var savepointCleanupEnabled =
                ctx.getObserveConfig().getBoolean(OPERATOR_SAVEPOINT_CLEANUP_ENABLED);

        if (savepointHistory.size() < 2) {
            return;
        }
        var lastSavepoint = savepointHistory.get(savepointHistory.size() - 1);

        while (savepointHistory.size() > maxCount) {
            // remove oldest entries
            var sp = savepointHistory.remove(0);
            if (savepointCleanupEnabled) {
                disposeSavepointQuietly(ctx, sp.getLocation());
            }
        }

        var it = savepointHistory.iterator();
        while (it.hasNext()) {
            var sp = it.next();
            if (sp.getTimeStamp() < maxTms && sp != lastSavepoint) {
                it.remove();
                if (savepointCleanupEnabled) {
                    disposeSavepointQuietly(ctx, sp.getLocation());
                }
            }
        }
    }

    private void disposeSavepointQuietly(FlinkResourceContext<CR> ctx, String path) {
        try {
            LOG.info("Disposing savepoint {}", path);
            ctx.getFlinkService().disposeSavepoint(path, ctx.getObserveConfig());
        } catch (Exception e) {
            // savepoint dispose error should not affect the deployment
            LOG.error("Exception while disposing savepoint {}", path, e);
        }
    }

    private long getMaxAgeForSnapshotType(
            Configuration observeConfig,
            FlinkOperatorConfiguration operatorConfig,
            SnapshotType snapshotType) {
        Duration maxAge;
        switch (snapshotType) {
            case CHECKPOINT:
                maxAge =
                        ConfigOptionUtils.getValueWithThreshold(
                                observeConfig,
                                OPERATOR_CHECKPOINT_HISTORY_MAX_AGE,
                                operatorConfig.getCheckpointHistoryAgeThreshold());
                break;
            case SAVEPOINT:
                maxAge =
                        ConfigOptionUtils.getValueWithThreshold(
                                observeConfig,
                                OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                                operatorConfig.getSavepointHistoryAgeThreshold());
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Unknown snapshot type %s", snapshotType.name()));
        }
        return System.currentTimeMillis() - maxAge.toMillis();
    }

    private int getMaxCountForSnapshotType(
            Configuration observeConfig,
            FlinkOperatorConfiguration operatorConfig,
            SnapshotType snapshotType) {
        switch (snapshotType) {
            case CHECKPOINT:
                return ConfigOptionUtils.getValueWithThreshold(
                        observeConfig,
                        OPERATOR_CHECKPOINT_HISTORY_MAX_COUNT,
                        operatorConfig.getCheckpointHistoryCountThreshold());
            case SAVEPOINT:
                return ConfigOptionUtils.getValueWithThreshold(
                        observeConfig,
                        OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT,
                        operatorConfig.getSavepointHistoryCountThreshold());
            default:
                throw new IllegalArgumentException(
                        String.format("Unknown snapshot type %s", snapshotType.name()));
        }
    }

    private void observeLatestCheckpoint(
            FlinkService flinkService,
            JobStatus jobStatus,
            String jobID,
            Configuration observeConfig) {
        try {
            flinkService
                    .getLastCheckpoint(JobID.fromHexString(jobID), observeConfig)
                    .ifPresent(
                            snapshot ->
                                    jobStatus.setUpgradeSnapshotReference(
                                            FlinkStateSnapshotReference.fromPath(
                                                    snapshot.getLocation())));
        } catch (Exception e) {
            LOG.error("Could not observe latest checkpoint information.", e);
            throw new ReconciliationException(e);
        }
    }
}
