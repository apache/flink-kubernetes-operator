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
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.api.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;

/** Savepoint utilities. */
public class SavepointUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointUtils.class);

    public static boolean savepointInProgress(JobStatus jobStatus) {
        return StringUtils.isNotEmpty(jobStatus.getSavepointInfo().getTriggerId());
    }

    /**
     * @param deployment
     * @return the current status of last attempted and/or completed savepoint in three states:
     *     PENDING, SUCCEEDED and FAILED. If no savepoints was ever completed or attempted,
     *     Optional.empty() is returned.
     */
    public static Optional<SavepointStatus> getLastSavepointStatus(
            AbstractFlinkResource<?, ?> deployment) {
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        var savepointInfo = jobStatus.getSavepointInfo();
        var lastSavepoint = savepointInfo.getLastSavepoint();

        // The first condition is for backward compatibility if the CR is old
        // where lastSavepoint is not created for pending savepoint
        if (savepointInfo.getTriggerType() == null && lastSavepoint == null) {
            return Optional.empty();
        }

        // The first condition is for backward compatibility if the CR is old
        // where lastSavepoint is not created for pending savepoint
        if ((savepointInfo.getTriggerType() != null
                        && savepointInfo.getTriggerType() != SavepointTriggerType.MANUAL)
                || lastSavepoint != null
                        && lastSavepoint.getTriggerType() != SavepointTriggerType.MANUAL) {
            return SavepointUtils.savepointInProgress(jobStatus)
                    ? Optional.of(SavepointStatus.PENDING)
                    : Optional.of(SavepointStatus.SUCCEEDED);
        }

        var targetSavepointTriggerNonce = deployment.getSpec().getJob().getSavepointTriggerNonce();
        var reconcileSavepointTriggerNonce =
                status.getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getSavepointTriggerNonce();

        // if savepointTriggerNonce is cleared, savepoint is not triggered.
        // For manual savepoints, we report pending status
        // during retries while the triggerId gets reset between retries.
        if (targetSavepointTriggerNonce != null
                && !Objects.equals(targetSavepointTriggerNonce, reconcileSavepointTriggerNonce)) {
            return Optional.of(SavepointStatus.PENDING);
        }

        // Last savepoint was manual and triggerNonce matches
        var lastCompletedSavepoint = savepointInfo.retrieveLastCompletedSavepoint();
        if (lastCompletedSavepoint.isEmpty()) {
            return Optional.of(SavepointStatus.FAILED);
        }

        return Objects.equals(
                        reconcileSavepointTriggerNonce,
                        lastCompletedSavepoint.get().getTriggerNonce())
                ? Optional.of(SavepointStatus.SUCCEEDED)
                : Optional.of(SavepointStatus.FAILED);
    }

    /**
     * Triggers any pending manual or periodic savepoints and updates the status accordingly.
     *
     * @param flinkService {@link FlinkService} used to trigger savepoints
     * @param resource Resource that should be savepointed
     * @param conf Observe config of the resource
     * @return True if a savepoint was triggered
     * @throws Exception Error during savepoint triggering.
     */
    public static boolean triggerSavepointIfNeeded(
            FlinkService flinkService, AbstractFlinkResource<?, ?> resource, Configuration conf)
            throws Exception {

        Optional<SavepointTriggerType> triggerOpt = shouldTriggerSavepoint(resource, conf);
        if (triggerOpt.isEmpty()) {
            return false;
        }

        var triggerType = triggerOpt.get();
        flinkService.triggerSavepoint(
                resource.getStatus().getJobStatus().getJobId(),
                triggerType,
                resource.getStatus().getJobStatus().getSavepointInfo(),
                triggerType == SavepointTriggerType.MANUAL
                        ? resource.getSpec().getJob().getSavepointTriggerNonce()
                        : null,
                conf);

        return true;
    }

    /**
     * Checks whether savepoint should be triggered based on the current status and spec and if yes,
     * returns the correct {@link SavepointTriggerType}.
     *
     * <p>This logic is responsible for both manual and periodic savepoint triggering.
     *
     * @param resource Resource to be savepointed
     * @param conf Observe configuration of the resource
     * @return Optional @{@link SavepointTriggerType}
     */
    @VisibleForTesting
    protected static Optional<SavepointTriggerType> shouldTriggerSavepoint(
            AbstractFlinkResource<?, ?> resource, Configuration conf) {

        var status = resource.getStatus();
        var jobSpec = resource.getSpec().getJob();
        var jobStatus = status.getJobStatus();

        if (!ReconciliationUtils.isJobRunning(status) || savepointInProgress(jobStatus)) {
            return Optional.empty();
        }

        var triggerNonceChanged =
                jobSpec.getSavepointTriggerNonce() != null
                        && !jobSpec.getSavepointTriggerNonce()
                                .equals(
                                        status.getReconciliationStatus()
                                                .deserializeLastReconciledSpec()
                                                .getJob()
                                                .getSavepointTriggerNonce());
        if (triggerNonceChanged) {
            return Optional.of(SavepointTriggerType.MANUAL);
        }

        var savepointInterval =
                conf.get(KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL);

        if (savepointInterval.isZero()) {
            return Optional.empty();
        }

        var lastTriggerTs = jobStatus.getSavepointInfo().getLastPeriodicSavepointTimestamp();

        // When the resource is first created/periodic savepointing enabled we have to compare
        // against the creation timestamp for triggering the first periodic savepoint
        var lastTrigger =
                lastTriggerTs == 0
                        ? Instant.parse(resource.getMetadata().getCreationTimestamp())
                        : Instant.ofEpochMilli(lastTriggerTs);
        var now = Instant.now();
        if (lastTrigger.plus(savepointInterval).isBefore(Instant.now())) {
            LOG.info(
                    "Triggering new periodic savepoint after {}",
                    Duration.between(lastTrigger, now));
            return Optional.of(SavepointTriggerType.PERIODIC);
        }
        return Optional.empty();
    }

    public static boolean gracePeriodEnded(Configuration conf, SavepointInfo savepointInfo) {
        Duration gracePeriod =
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_TRIGGER_GRACE_PERIOD);
        var endOfGracePeriod =
                Instant.ofEpochMilli(savepointInfo.getLastSavepoint().getTimeStamp())
                        .plus(gracePeriod);
        return endOfGracePeriod.isBefore(Instant.now());
    }

    public static void resetTriggerIfJobNotRunning(
            AbstractFlinkResource<?, ?> resource, EventRecorder eventRecorder) {
        var status = resource.getStatus();
        var jobStatus = status.getJobStatus();
        if (!ReconciliationUtils.isJobRunning(status)
                && SavepointUtils.savepointInProgress(jobStatus)) {
            var savepointInfo = jobStatus.getSavepointInfo();
            ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(savepointInfo, resource);
            savepointInfo.resetTrigger();
            LOG.error("Job is not running, cancelling savepoint operation");
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.SavepointError,
                    EventRecorder.Component.Operator,
                    createSavepointError(
                            savepointInfo, resource.getSpec().getJob().getSavepointTriggerNonce()));
        }
    }

    public static String createSavepointError(SavepointInfo savepointInfo, Long triggerNonce) {
        if (savepointInfo.getLastSavepoint() == null) {
            return "";
        }
        return SavepointTriggerType.PERIODIC == savepointInfo.getLastSavepoint().getTriggerType()
                ? "Periodic savepoint failed"
                : "Savepoint failed for savepointTriggerNonce: " + triggerNonce;
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

    /**
     * Check if the CR has lingering deprecated fields from older version and migrate them.
     *
     * @param savepointInfo
     * @param triggerNonce
     */
    public static void checkAndMigrateDeprecatedTriggerFields(
            SavepointInfo savepointInfo, Long triggerNonce) {
        if (savepointInfo.getTriggerTimestamp() == null
                && savepointInfo.getTriggerType() == null
                && savepointInfo.getFormatType() == null) {
            return;
        }
        var lastSavepoint =
                new Savepoint(
                        savepointInfo.getTriggerTimestamp(),
                        null,
                        savepointInfo.getTriggerType(),
                        savepointInfo.getFormatType(),
                        savepointInfo.getTriggerType() == SavepointTriggerType.MANUAL
                                ? triggerNonce
                                : null);
        savepointInfo.setFormatType(null);
        savepointInfo.setTriggerTimestamp(null);
        savepointInfo.setTriggerType(null);
        savepointInfo.setLastSavepoint(lastSavepoint);
    }
}
