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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.api.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.ConfigOptionUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/** An observer of savepoint progress. */
public class SavepointObserver<
        CR extends AbstractFlinkResource<?, STATUS>, STATUS extends CommonStatus<?>> {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointObserver.class);

    private final EventRecorder eventRecorder;

    public SavepointObserver(EventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
    }

    public void observeSavepointStatus(FlinkResourceContext<CR> ctx) {

        var resource = ctx.getResource();
        var jobStatus = resource.getStatus().getJobStatus();
        var savepointInfo = jobStatus.getSavepointInfo();
        var jobId = jobStatus.getJobId();

        // If any manual or periodic savepoint is in progress, observe it
        if (SavepointUtils.savepointInProgress(jobStatus)) {
            observeTriggeredSavepoint(ctx, jobId);
        }

        // If job is in globally terminal state, observe last savepoint
        if (ReconciliationUtils.isJobInTerminalState(resource.getStatus())) {
            observeLatestSavepoint(
                    ctx.getFlinkService(), savepointInfo, jobId, ctx.getObserveConfig());
        }

        cleanupSavepointHistory(ctx, savepointInfo);
    }

    /**
     * Observe the status of manually triggered savepoints.
     *
     * @param ctx Resource context.
     * @param jobID the jobID of the observed job.
     */
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
            if (SavepointUtils.gracePeriodEnded(ctx.getObserveConfig(), savepointInfo)) {
                LOG.error(
                        "Savepoint attempt failed after grace period. Won't be retried again: "
                                + err);
                ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(
                        savepointInfo, (AbstractFlinkResource) resource);
            } else {
                LOG.warn("Savepoint failed within grace period, retrying: " + err);
            }
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.SavepointError,
                    EventRecorder.Component.Operator,
                    SavepointUtils.createSavepointError(
                            savepointInfo, resource.getSpec().getJob().getSavepointTriggerNonce()));
            savepointInfo.resetTrigger();
            return;
        }

        var savepoint =
                new Savepoint(
                        savepointInfo.getTriggerTimestamp(),
                        savepointFetchResult.getLocation(),
                        savepointInfo.getTriggerType(),
                        savepointInfo.getFormatType(),
                        SavepointTriggerType.MANUAL == savepointInfo.getTriggerType()
                                ? resource.getSpec().getJob().getSavepointTriggerNonce()
                                : null);

        ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(savepointInfo, resource);
        savepointInfo.updateLastSavepoint(savepoint);
    }

    /** Clean up and dispose savepoints according to the configured max size/age. */
    @VisibleForTesting
    void cleanupSavepointHistory(FlinkResourceContext<CR> ctx, SavepointInfo currentSavepointInfo) {

        var observeConfig = ctx.getObserveConfig();
        var flinkService = ctx.getFlinkService();
        boolean savepointCleanupEnabled =
                observeConfig.getBoolean(
                        KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_CLEANUP_ENABLED);

        // maintain history
        List<Savepoint> savepointHistory = currentSavepointInfo.getSavepointHistory();
        if (savepointHistory.size() < 2) {
            return;
        }
        var lastSavepoint = savepointHistory.get(savepointHistory.size() - 1);

        int maxCount =
                Math.max(
                        1,
                        ConfigOptionUtils.getValueWithThreshold(
                                observeConfig,
                                KubernetesOperatorConfigOptions
                                        .OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT,
                                ctx.getOperatorConfig().getSavepointHistoryCountThreshold()));
        while (savepointHistory.size() > maxCount) {
            // remove oldest entries
            Savepoint sp = savepointHistory.remove(0);
            if (savepointCleanupEnabled) {
                disposeSavepointQuietly(flinkService, sp, observeConfig);
            }
        }

        Duration maxAge =
                ConfigOptionUtils.getValueWithThreshold(
                        observeConfig,
                        KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                        ctx.getOperatorConfig().getSavepointHistoryAgeThreshold());
        long maxTms = System.currentTimeMillis() - maxAge.toMillis();
        Iterator<Savepoint> it = savepointHistory.iterator();
        while (it.hasNext()) {
            Savepoint sp = it.next();
            if (sp.getTimeStamp() < maxTms && sp != lastSavepoint) {
                it.remove();
                if (savepointCleanupEnabled) {
                    disposeSavepointQuietly(flinkService, sp, observeConfig);
                }
            }
        }
    }

    private void disposeSavepointQuietly(
            FlinkService flinkService, Savepoint sp, Configuration conf) {
        try {
            LOG.info("Disposing savepoint {}", sp);
            flinkService.disposeSavepoint(sp.getLocation(), conf);
        } catch (Exception e) {
            // savepoint dispose error should not affect the deployment
            LOG.error("Exception while disposing savepoint {}", sp.getLocation(), e);
        }
    }

    private void observeLatestSavepoint(
            FlinkService flinkService,
            SavepointInfo savepointInfo,
            String jobID,
            Configuration observeConfig) {
        try {
            flinkService
                    .getLastCheckpoint(JobID.fromHexString(jobID), observeConfig)
                    .ifPresent(savepointInfo::updateLastSavepoint);
        } catch (Exception e) {
            LOG.error("Could not observe latest savepoint information.", e);
            throw new ReconciliationException(e);
        }
    }
}
