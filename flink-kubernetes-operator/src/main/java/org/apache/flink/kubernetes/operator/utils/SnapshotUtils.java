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
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.SnapshotInfo;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;
import org.apache.flink.kubernetes.operator.service.FlinkService;

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
     * Triggers any pending manual or periodic snapshots and updates the status accordingly.
     *
     * @param flinkService The {@link FlinkService} used to trigger snapshots.
     * @param resource The resource that should be snapshotted.
     * @param conf The observe config of the resource.
     * @return True if a snapshot was triggered.
     * @throws Exception An error during snapshot triggering.
     */
    public static boolean triggerSnapshotIfNeeded(
            FlinkService flinkService,
            AbstractFlinkResource<?, ?> resource,
            Configuration conf,
            SnapshotType snapshotType)
            throws Exception {

        Optional<SnapshotTriggerType> triggerOpt =
                shouldTriggerSnapshot(resource, conf, snapshotType);
        if (triggerOpt.isEmpty()) {
            return false;
        }

        var triggerType = triggerOpt.get();
        String jobId = resource.getStatus().getJobStatus().getJobId();
        switch (snapshotType) {
            case SAVEPOINT:
                flinkService.triggerSavepoint(
                        jobId,
                        triggerType,
                        resource.getStatus().getJobStatus().getSavepointInfo(),
                        conf);
                break;
            case CHECKPOINT:
                flinkService.triggerCheckpoint(
                        jobId,
                        triggerType,
                        resource.getStatus().getJobStatus().getCheckpointInfo(),
                        conf);
                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }
        return true;
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
     * @return An optional {@link SnapshotTriggerType}.
     */
    @VisibleForTesting
    protected static Optional<SnapshotTriggerType> shouldTriggerSnapshot(
            AbstractFlinkResource<?, ?> resource, Configuration conf, SnapshotType snapshotType) {

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
        SnapshotInfo snapshotInfo;
        String automaticTriggerExpression;

        switch (snapshotType) {
            case SAVEPOINT:
                triggerNonce = jobSpec.getSavepointTriggerNonce();
                reconciledTriggerNonce = reconciledJobSpec.getSavepointTriggerNonce();
                inProgress = savepointInProgress(jobStatus);
                snapshotInfo = jobStatus.getSavepointInfo();
                automaticTriggerExpression =
                        conf.get(KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL);
                break;
            case CHECKPOINT:
                triggerNonce = jobSpec.getCheckpointTriggerNonce();
                reconciledTriggerNonce = reconciledJobSpec.getCheckpointTriggerNonce();
                inProgress = checkpointInProgress(jobStatus);
                snapshotInfo = jobStatus.getCheckpointInfo();
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

        var lastTriggerTs = snapshotInfo.getLastPeriodicTriggerTimestamp();
        // When the resource is first created/periodic snapshotting enabled we have to compare
        // against the creation timestamp for triggering the first periodic savepoint
        var lastTrigger =
                lastTriggerTs == 0
                        ? Instant.parse(resource.getMetadata().getCreationTimestamp())
                        : Instant.ofEpochMilli(lastTriggerTs);

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

        // Try to interpret as an interval (Duration).
        boolean expressionIsAnInterval = true;
        boolean shouldTriggerInterval = false;
        try {
            shouldTriggerInterval =
                    shouldTriggerIntervalBasedSnapshot(
                            snapshotType, automaticTriggerExpression, lastTrigger);
        } catch (ParseException e) {
            expressionIsAnInterval = false;
        }

        // Try to interpret as a cron expression.
        boolean expressionIsACron = true;
        boolean shouldTriggerCron = false;
        try {
            shouldTriggerCron =
                    shouldTriggerCronBasedSnapshot(
                            snapshotType, automaticTriggerExpression, lastTrigger, Instant.now());
        } catch (ParseException e) {
            expressionIsACron = false;
        }

        if (!expressionIsAnInterval && !expressionIsACron) {
            LOG.warn(
                    "Automatic {} triggering is configured, but the trigger expression '{}' is neither a valid Duration, nor a cron expression.",
                    snapshotType,
                    automaticTriggerExpression);
            return false;
        }

        // This should never happen. The string cannot be both a valid Duration and a cron
        // expression at the same time.
        if (expressionIsAnInterval && expressionIsACron) {
            LOG.error(
                    "Something went wrong with the automatic {} trigger expression {}. This setting cannot be simultaneously a valid Duration and a cron expression.",
                    snapshotType,
                    automaticTriggerExpression);
            return false;
        }

        return shouldTriggerInterval || shouldTriggerCron;
    }

    @VisibleForTesting
    static boolean shouldTriggerCronBasedSnapshot(
            SnapshotType snapshotType,
            String cronExpressionString,
            Instant lastTriggerDateInstant,
            Instant nowInstant)
            throws ParseException {
        CronExpression cronExpression = new CronExpression(cronExpressionString);
        Date now = Date.from(nowInstant);
        Date lastTrigger = Date.from(lastTriggerDateInstant);

        Date nextValidTimeAfterLastTrigger = cronExpression.getNextValidTimeAfter(lastTrigger);

        if (nextValidTimeAfterLastTrigger != null && nextValidTimeAfterLastTrigger.before(now)) {
            LOG.info(
                    "Triggering new automatic {} based on cron schedule '{}' due at {}",
                    snapshotType.toString().toLowerCase(),
                    cronExpressionString,
                    nextValidTimeAfterLastTrigger);
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    static boolean shouldTriggerIntervalBasedSnapshot(
            SnapshotType snapshotType, String triggerExpression, Instant lastTrigger)
            throws ParseException {

        Duration interval;
        try {
            interval = ConfigurationUtils.convertValue(triggerExpression, Duration.class);
        } catch (Exception exception) {
            throw new ParseException(
                    "Trigger expression " + triggerExpression + " cannot be parsed as Duration.",
                    -1);
        }

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

    public static boolean isSnapshotTriggeringSupported(Configuration conf) {
        // Flink REST API supports triggering checkpoints externally starting with 1.17
        return conf.get(FLINK_VERSION) != null
                && conf.get(FLINK_VERSION).isNewerVersionThan(FlinkVersion.v1_16);
    }

    public static boolean gracePeriodEnded(Duration gracePeriod, SnapshotInfo snapshotInfo) {
        var endOfGracePeriod =
                Instant.ofEpochMilli(snapshotInfo.getTriggerTimestamp()).plus(gracePeriod);
        return endOfGracePeriod.isBefore(Instant.now());
    }

    public static void resetSnapshotTriggers(
            AbstractFlinkResource<?, ?> resource, EventRecorder eventRecorder) {
        var status = resource.getStatus();
        var jobStatus = status.getJobStatus();

        if (!ReconciliationUtils.isJobRunning(status)) {
            if (SnapshotUtils.savepointInProgress(jobStatus)) {
                var savepointInfo = jobStatus.getSavepointInfo();
                ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                        savepointInfo, resource, SAVEPOINT);
                savepointInfo.resetTrigger();
                LOG.error("Job is not running, cancelling savepoint operation");
                eventRecorder.triggerEvent(
                        resource,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.SavepointError,
                        EventRecorder.Component.Operator,
                        savepointInfo.formatErrorMessage(
                                resource.getSpec().getJob().getSavepointTriggerNonce()));
            }
            if (SnapshotUtils.checkpointInProgress(jobStatus)) {
                var checkpointInfo = jobStatus.getCheckpointInfo();
                ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                        checkpointInfo, resource, CHECKPOINT);
                checkpointInfo.resetTrigger();
                LOG.error("Job is not running, cancelling checkpoint operation");
                eventRecorder.triggerEvent(
                        resource,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.CheckpointError,
                        EventRecorder.Component.Operator,
                        checkpointInfo.formatErrorMessage(
                                resource.getSpec().getJob().getCheckpointTriggerNonce()));
            }
        }
    }

    public static SavepointFormatType getSavepointFormatType(Configuration configuration) {
        var savepointFormatType = org.apache.flink.core.execution.SavepointFormatType.CANONICAL;
        if (configuration.get(FLINK_VERSION) != null
                && configuration.get(FLINK_VERSION).isNewerVersionThan(FlinkVersion.v1_14)) {
            savepointFormatType =
                    configuration.get(
                            KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);
        }
        return savepointFormatType;
    }
}
