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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SnapshotInfo;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;
import org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractJobReconciler;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.util.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;

/** Savepoint utilities. */
public class SnapshotUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotUtils.class);

    public static boolean savepointInProgress(JobStatus jobStatus) {
        return StringUtils.isNotEmpty(jobStatus.getSavepointInfo().getTriggerId());
    }

    public static boolean checkpointInProgress(JobStatus jobStatus) {
        return StringUtils.isNotEmpty(jobStatus.getCheckpointInfo().getTriggerId());
    }

    @VisibleForTesting
    public static SnapshotStatus getLastSnapshotStatus(
            AbstractFlinkResource<?, ?> resource, SnapshotType snapshotType) {

        var status = resource.getStatus();
        var jobStatus = status.getJobStatus();
        var jobSpec = resource.getSpec().getJob();
        var reconciledJobSpec =
                status.getReconciliationStatus().deserializeLastReconciledSpec().getJob();

        // Values that are specific to the snapshot type
        Long triggerNonce;
        Long reconciledTriggerNonce;
        SnapshotInfo snapshotInfo;

        switch (snapshotType) {
            case SAVEPOINT:
                triggerNonce = jobSpec.getSavepointTriggerNonce();
                reconciledTriggerNonce = reconciledJobSpec.getSavepointTriggerNonce();
                snapshotInfo = jobStatus.getSavepointInfo();
                break;
            case CHECKPOINT:
                triggerNonce = jobSpec.getCheckpointTriggerNonce();
                reconciledTriggerNonce = reconciledJobSpec.getCheckpointTriggerNonce();
                snapshotInfo = jobStatus.getCheckpointInfo();
                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }

        if (snapshotInfo.getTriggerId() != null) {
            return SnapshotStatus.PENDING;
        }

        // if triggerNonce is cleared, the snapshot is not triggered.
        // For manual snapshots, we report pending status
        // during retries while the triggerId gets reset between retries.
        if (triggerNonce != null && !Objects.equals(triggerNonce, reconciledTriggerNonce)) {
            return SnapshotStatus.PENDING;
        }

        Long lastTriggerNonce = snapshotInfo.getLastTriggerNonce();
        SnapshotTriggerType lastSnapshotTriggerType = snapshotInfo.getLastTriggerType();

        if (lastSnapshotTriggerType == null) {
            // Indicates that no snapshot of snapshotType was ever taken
            return null;
        }

        // Last snapshot was manual and triggerNonce matches
        if (Objects.equals(reconciledTriggerNonce, lastTriggerNonce)) {
            return SnapshotStatus.SUCCEEDED;
        }

        // Last snapshot was not manual
        if (lastSnapshotTriggerType != SnapshotTriggerType.MANUAL) {
            return SnapshotStatus.SUCCEEDED;
        }

        return SnapshotStatus.ABANDONED;
    }

    /**
     * Checks whether a snapshot should be triggered based on the current status and spec, and if
     * yes, returns the correct {@link SnapshotTriggerType}.
     *
     * <p>This logic is responsible for both manual and periodic snapshots triggering.
     *
     * @param resource The resource to be snapshotted.
     * @param conf The observe configuration of the resource.
     * @param snapshotType The type of the snapshot.
     * @param lastTrigger the last time the snapshot was triggered.
     * @return An optional {@link SnapshotTriggerType}.
     */
    @VisibleForTesting
    public static Optional<SnapshotTriggerType> shouldTriggerSnapshot(
            AbstractFlinkResource<?, ?> resource,
            Configuration conf,
            SnapshotType snapshotType,
            Instant lastTrigger) {

        var status = resource.getStatus();
        var jobStatus = status.getJobStatus();
        var jobSpec = resource.getSpec().getJob();

        if (!ReconciliationUtils.isJobRunning(status)) {
            return Optional.empty();
        }

        var reconciledJobSpec =
                status.getReconciliationStatus().deserializeLastReconciledSpec().getJob();

        // Values that are specific to the snapshot type
        Long triggerNonce;
        Long reconciledTriggerNonce;
        boolean inProgress;
        String automaticTriggerExpression;

        switch (snapshotType) {
            case SAVEPOINT:
                triggerNonce = jobSpec.getSavepointTriggerNonce();
                reconciledTriggerNonce = reconciledJobSpec.getSavepointTriggerNonce();
                inProgress = savepointInProgress(jobStatus);
                automaticTriggerExpression =
                        conf.get(KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL);
                break;
            case CHECKPOINT:
                triggerNonce = jobSpec.getCheckpointTriggerNonce();
                reconciledTriggerNonce = reconciledJobSpec.getCheckpointTriggerNonce();
                inProgress = checkpointInProgress(jobStatus);
                automaticTriggerExpression =
                        conf.get(KubernetesOperatorConfigOptions.PERIODIC_CHECKPOINT_INTERVAL);
                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }

        if (inProgress) {
            return Optional.empty();
        }

        var triggerNonceChanged =
                triggerNonce != null && !triggerNonce.equals(reconciledTriggerNonce);
        if (triggerNonceChanged) {
            if (snapshotType == CHECKPOINT && !isSnapshotTriggeringSupported(conf)) {
                LOG.warn(
                        "Manual checkpoint triggering is attempted, but is not supported (requires Flink 1.17+)");
                return Optional.empty();
            } else {
                return Optional.of(SnapshotTriggerType.MANUAL);
            }
        }

        if (shouldTriggerAutomaticSnapshot(snapshotType, automaticTriggerExpression, lastTrigger)) {
            if (snapshotType == CHECKPOINT && !isSnapshotTriggeringSupported(conf)) {
                LOG.warn(
                        "Automatic checkpoints triggering is configured but is not supported (requires Flink 1.17+)");
                return Optional.empty();
            } else {
                return Optional.of(SnapshotTriggerType.PERIODIC);
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    static boolean shouldTriggerAutomaticSnapshot(
            SnapshotType snapshotType, String automaticTriggerExpression, Instant lastTrigger) {
        if (StringUtils.isBlank(automaticTriggerExpression)) {
            return false;
        } // automaticTriggerExpression was configured by the user

        Optional<Duration> interval = interpretAsInterval(automaticTriggerExpression);
        Optional<CronExpression> cron = interpretAsCron(automaticTriggerExpression);

        // This should never happen. The string cannot be both a valid Duration and a cron
        // expression at the same time.
        if (interval.isPresent() && cron.isPresent()) {
            LOG.error(
                    "Something went wrong with the automatic {} trigger expression {}. This setting cannot be simultaneously a valid Duration and a cron expression.",
                    snapshotType,
                    automaticTriggerExpression);
            return false;
        }

        if (interval.isPresent()) {
            return shouldTriggerIntervalBasedSnapshot(snapshotType, interval.get(), lastTrigger);
        } else if (cron.isPresent()) {
            return shouldTriggerCronBasedSnapshot(
                    snapshotType, cron.get(), lastTrigger, Instant.now());
        } else {
            LOG.warn(
                    "Automatic {} triggering is configured, but the trigger expression '{}' is neither a valid Duration, nor a cron expression.",
                    snapshotType,
                    automaticTriggerExpression);
            return false;
        }
    }

    @VisibleForTesting
    static boolean shouldTriggerCronBasedSnapshot(
            SnapshotType snapshotType,
            CronExpression cronExpression,
            Instant lastTriggerDateInstant,
            Instant nowInstant) {
        Date now = Date.from(nowInstant);
        Date lastTrigger = Date.from(lastTriggerDateInstant);

        Date nextValidTimeAfterLastTrigger = cronExpression.getNextValidTimeAfter(lastTrigger);

        if (nextValidTimeAfterLastTrigger != null && nextValidTimeAfterLastTrigger.before(now)) {
            LOG.info(
                    "Triggering new automatic {} based on cron schedule '{}' due at {}",
                    snapshotType.toString().toLowerCase(),
                    cronExpression.toString(),
                    nextValidTimeAfterLastTrigger);
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    static boolean shouldTriggerIntervalBasedSnapshot(
            SnapshotType snapshotType, Duration interval, Instant lastTrigger) {
        if (interval.isZero()) {
            return false;
        }
        var now = Instant.now();
        if (lastTrigger.plus(interval).isBefore(Instant.now())) {
            LOG.info(
                    "Triggering new automatic {} after {}",
                    snapshotType.toString().toLowerCase(),
                    Duration.between(lastTrigger, now));
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    static Optional<Duration> interpretAsInterval(String triggerExpression) {
        try {
            return Optional.of(ConfigurationUtils.convertValue(triggerExpression, Duration.class));
        } catch (Exception exception) {
            return Optional.empty();
        }
    }

    @VisibleForTesting
    static Optional<CronExpression> interpretAsCron(String triggerExpression) {
        try {
            return Optional.of(new CronExpression(triggerExpression));
        } catch (ParseException e) {
            return Optional.empty();
        }
    }

    public static boolean isSnapshotTriggeringSupported(Configuration conf) {
        // Flink REST API supports triggering checkpoints externally starting with 1.17
        return conf.get(FLINK_VERSION) != null
                && conf.get(FLINK_VERSION).isEqualOrNewer(FlinkVersion.v1_17);
    }

    public static boolean gracePeriodEnded(Duration gracePeriod, SnapshotInfo snapshotInfo) {
        var endOfGracePeriod =
                Instant.ofEpochMilli(snapshotInfo.getTriggerTimestamp()).plus(gracePeriod);
        return endOfGracePeriod.isBefore(Instant.now());
    }

    public static void resetSnapshotTriggers(
            AbstractFlinkResource<?, ?> resource,
            EventRecorder eventRecorder,
            KubernetesClient client) {
        var status = resource.getStatus();
        var jobStatus = status.getJobStatus();

        if (!ReconciliationUtils.isJobRunning(status)) {
            if (SnapshotUtils.savepointInProgress(jobStatus)) {
                var savepointInfo = jobStatus.getSavepointInfo();
                ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                        savepointInfo.getTriggerType(), resource, SAVEPOINT);
                savepointInfo.resetTrigger();
                LOG.error("Job is not running, cancelling savepoint operation");
                eventRecorder.triggerEvent(
                        resource,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.SavepointError,
                        EventRecorder.Component.Operator,
                        savepointInfo.formatErrorMessage(
                                resource.getSpec().getJob().getSavepointTriggerNonce()),
                        client);
            }
            if (SnapshotUtils.checkpointInProgress(jobStatus)) {
                var checkpointInfo = jobStatus.getCheckpointInfo();
                ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                        checkpointInfo.getTriggerType(), resource, CHECKPOINT);
                checkpointInfo.resetTrigger();
                LOG.error("Job is not running, cancelling checkpoint operation");
                eventRecorder.triggerEvent(
                        resource,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.CheckpointError,
                        EventRecorder.Component.Operator,
                        checkpointInfo.formatErrorMessage(
                                resource.getSpec().getJob().getCheckpointTriggerNonce()),
                        client);
            }
        }
    }

    /**
     * Check if the last snapshot information is known. True if the snapshot location is known
     * explicitly (not implicitly through a last-state upgrade) or if the savepoint is known to be
     * empty.
     *
     * @param status Flink resource status
     * @return True if last savepoint is known
     */
    public static boolean lastSavepointKnown(CommonStatus<?> status) {
        var location =
                ObjectUtils.firstNonNull(
                        status.getJobStatus().getUpgradeSavepointPath(),
                        Optional.ofNullable(
                                        status.getJobStatus().getSavepointInfo().getLastSavepoint())
                                .map(Savepoint::getLocation)
                                .orElse(null));

        if (location == null) {
            return true;
        }
        return !location.equals(AbstractJobReconciler.LAST_STATE_DUMMY_SP_PATH);
    }
}
