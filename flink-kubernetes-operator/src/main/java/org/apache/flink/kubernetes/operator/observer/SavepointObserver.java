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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.ConfigOptionUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/** An observer of savepoint progress. */
public class SavepointObserver<STATUS extends CommonStatus<?>> {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointObserver.class);

    private final FlinkService flinkService;
    private final FlinkConfigManager configManager;
    private final StatusRecorder<STATUS> statusRecorder;
    private final EventRecorder eventRecorder;

    public SavepointObserver(
            FlinkService flinkService,
            FlinkConfigManager configManager,
            StatusRecorder<STATUS> statusRecorder,
            EventRecorder eventRecorder) {
        this.flinkService = flinkService;
        this.configManager = configManager;
        this.statusRecorder = statusRecorder;
        this.eventRecorder = eventRecorder;
    }

    public void observeSavepointStatus(
            AbstractFlinkResource<?, STATUS> resource, Configuration deployedConfig) {

        var jobStatus = resource.getStatus().getJobStatus();
        var savepointInfo = jobStatus.getSavepointInfo();
        var jobId = jobStatus.getJobId();

        // If any manual or periodic savepoint is in progress, observe it
        if (SavepointUtils.savepointInProgress(jobStatus)) {
            observeTriggeredSavepoint(resource, jobId, deployedConfig);
        }

        // If job is in globally terminal state, observe last savepoint
        if (ReconciliationUtils.isJobInTerminalState(resource.getStatus())) {
            observeLatestSavepoint(savepointInfo, jobId, deployedConfig);
        }
    }

    /**
     * Observe the status of manually triggered savepoints.
     *
     * @param resource the resource being observed
     * @param jobID the jobID of the observed job.
     * @param deployedConfig Deployed job config.
     */
    private void observeTriggeredSavepoint(
            AbstractFlinkResource<?, ?> resource, String jobID, Configuration deployedConfig) {

        var savepointInfo = resource.getStatus().getJobStatus().getSavepointInfo();

        LOG.info("Observing savepoint status.");
        var savepointFetchResult =
                flinkService.fetchSavepointInfo(
                        savepointInfo.getTriggerId(), jobID, deployedConfig);

        if (savepointFetchResult.isPending()) {
            LOG.info("Savepoint operation not finished yet...");
            return;
        }

        if (savepointFetchResult.getError() != null) {
            var err = savepointFetchResult.getError();
            if (SavepointUtils.gracePeriodEnded(deployedConfig, savepointInfo)) {
                LOG.error(
                        "Savepoint attempt failed after grace period. Won't be retried again: "
                                + err);
                ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(
                        savepointInfo, resource);
                eventRecorder.triggerEvent(
                        resource,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.SavepointError,
                        EventRecorder.Component.Operator,
                        SavepointUtils.createSavepointError(
                                savepointInfo,
                                resource.getSpec().getJob().getSavepointTriggerNonce()));
            } else {
                LOG.warn("Savepoint failed within grace period, retrying: " + err);
            }
            savepointInfo.resetTrigger();
            return;
        }

        var savepoint =
                new Savepoint(
                        savepointInfo.getTriggerTimestamp(),
                        savepointFetchResult.getLocation(),
                        savepointInfo.getTriggerType());

        ReconciliationUtils.updateLastReconciledSavepointTriggerNonce(savepointInfo, resource);
        savepointInfo.updateLastSavepoint(savepoint);
        cleanupSavepointHistory(savepointInfo, savepoint, deployedConfig);
    }

    /** Clean up and dispose savepoints according to the configured max size/age. */
    @VisibleForTesting
    void cleanupSavepointHistory(
            SavepointInfo currentSavepointInfo,
            Savepoint newSavepoint,
            Configuration deployedConfig) {

        // maintain history
        List<Savepoint> savepointHistory = currentSavepointInfo.getSavepointHistory();
        int maxCount =
                ConfigOptionUtils.getValueWithThreshold(
                        deployedConfig,
                        KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT,
                        configManager
                                .getOperatorConfiguration()
                                .getSavepointHistoryCountThreshold());
        while (savepointHistory.size() > maxCount) {
            // remove oldest entries
            disposeSavepointQuietly(savepointHistory.remove(0), deployedConfig);
        }

        Duration maxAge =
                ConfigOptionUtils.getValueWithThreshold(
                        deployedConfig,
                        KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                        configManager.getOperatorConfiguration().getSavepointHistoryAgeThreshold());
        long maxTms = System.currentTimeMillis() - maxAge.toMillis();
        Iterator<Savepoint> it = savepointHistory.iterator();
        while (it.hasNext()) {
            Savepoint sp = it.next();
            if (sp.getTimeStamp() < maxTms && sp != newSavepoint) {
                it.remove();
                disposeSavepointQuietly(sp, deployedConfig);
            }
        }
    }

    private void disposeSavepointQuietly(Savepoint sp, Configuration conf) {
        try {
            LOG.info("Disposing savepoint {}", sp);
            flinkService.disposeSavepoint(sp.getLocation(), conf);
        } catch (Exception e) {
            // savepoint dispose error should not affect the deployment
            LOG.error("Exception while disposing savepoint {}", sp.getLocation(), e);
        }
    }

    private void observeLatestSavepoint(
            SavepointInfo savepointInfo, String jobID, Configuration deployedConfig) {
        try {
            flinkService
                    .getLastCheckpoint(JobID.fromHexString(jobID), deployedConfig)
                    .ifPresent(savepointInfo::updateLastSavepoint);
        } catch (Exception e) {
            LOG.error("Could not observe latest savepoint information.", e);
            throw new ReconciliationException(e);
        }
    }
}
