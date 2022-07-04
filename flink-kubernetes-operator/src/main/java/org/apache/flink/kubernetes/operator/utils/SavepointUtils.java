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
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/** Savepoint utilities. */
public class SavepointUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointUtils.class);

    public static boolean savepointInProgress(JobStatus jobStatus) {
        return StringUtils.isNotEmpty(jobStatus.getSavepointInfo().getTriggerId());
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
                Instant.ofEpochMilli(savepointInfo.getTriggerTimestamp()).plus(gracePeriod);
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
        return SavepointTriggerType.PERIODIC == savepointInfo.getTriggerType()
                ? "Periodic savepoint failed"
                : "Savepoint failed for savepointTriggerNonce: " + triggerNonce;
    }
}
