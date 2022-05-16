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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.kubernetes.operator.utils.StatusHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/** An observer of savepoint progress. */
public class SavepointObserver<STATUS extends CommonStatus<?>> {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointObserver.class);

    private final FlinkService flinkService;
    private final FlinkConfigManager configManager;
    private final StatusHelper<STATUS> statusHelper;

    public SavepointObserver(
            FlinkService flinkService,
            FlinkConfigManager configManager,
            StatusHelper<STATUS> statusHelper) {
        this.flinkService = flinkService;
        this.configManager = configManager;
        this.statusHelper = statusHelper;
    }

    public void observeSavepointStatus(
            AbstractFlinkResource<?, STATUS> resource, Configuration deployedConfig) {
        var jobStatus = resource.getStatus().getJobStatus();
        var savepointInfo = jobStatus.getSavepointInfo();
        var jobId = jobStatus.getJobId();
        var previousLastSpPath =
                Optional.ofNullable(savepointInfo.getLastSavepoint())
                        .map(Savepoint::getLocation)
                        .orElse(null);

        observeTriggeredSavepointProgress(savepointInfo, jobId, deployedConfig)
                .ifPresent(err -> ReconciliationUtils.updateForReconciliationError(resource, err));

        // We only need to observe latest checkpoint/savepoint for terminal jobs
        if (JobStatus.valueOf(jobStatus.getState()).isGloballyTerminalState()) {
            observeLatestSavepoint(savepointInfo, jobId, deployedConfig);
        }

        var currentLastSpPath =
                Optional.ofNullable(savepointInfo.getLastSavepoint())
                        .map(Savepoint::getLocation)
                        .orElse(null);

        // If the last savepoint information changes we need to patch the status
        // to avoid losing this in case of an operator failure after the cluster was shut down
        if (currentLastSpPath != null && !currentLastSpPath.equals(previousLastSpPath)) {
            LOG.info(
                    "Updating resource status after observing new last savepoint {}",
                    currentLastSpPath);
            statusHelper.patchAndCacheStatus(resource);
        }
    }

    /**
     * Observe the savepoint result based on the current savepoint info.
     *
     * @param currentSavepointInfo the current savepoint info.
     * @param jobID the jobID of the observed job.
     * @param deployedConfig Deployed job config.
     * @return The observed error, if no error observed, {@code Optional.empty()} will be returned.
     */
    private Optional<String> observeTriggeredSavepointProgress(
            SavepointInfo currentSavepointInfo, String jobID, Configuration deployedConfig) {
        if (currentSavepointInfo.getTriggerId() == null) {
            LOG.debug("Savepoint not in progress");
            return Optional.empty();
        }
        LOG.info("Observing savepoint status");
        SavepointFetchResult savepointFetchResult;
        try {
            savepointFetchResult =
                    flinkService.fetchSavepointInfo(
                            currentSavepointInfo.getTriggerId(), jobID, deployedConfig);
        } catch (Exception e) {
            LOG.error("Exception while fetching savepoint info", e);
            return Optional.empty();
        }

        if (!savepointFetchResult.isTriggered()) {
            String error = savepointFetchResult.getError();
            if (error != null
                    || SavepointUtils.gracePeriodEnded(
                            configManager.getOperatorConfiguration(), currentSavepointInfo)) {
                String errorMsg = error != null ? error : "Savepoint status unknown";
                LOG.error(errorMsg);
                currentSavepointInfo.resetTrigger();
                return Optional.of(errorMsg);
            }
            LOG.info("Savepoint operation not running, waiting within grace period...");
        }
        if (savepointFetchResult.getSavepoint() == null) {
            LOG.info("Savepoint is still in progress...");
            return Optional.empty();
        }

        LOG.info("Savepoint status updated with latest completed savepoint info");
        currentSavepointInfo.updateLastSavepoint(savepointFetchResult.getSavepoint());
        updateSavepointHistory(
                currentSavepointInfo, savepointFetchResult.getSavepoint(), deployedConfig);
        return Optional.empty();
    }

    @VisibleForTesting
    void updateSavepointHistory(
            SavepointInfo currentSavepointInfo,
            Savepoint newSavepoint,
            Configuration deployedConfig) {

        currentSavepointInfo.addSavepointToHistory(newSavepoint);

        // maintain history
        List<Savepoint> savepointHistory = currentSavepointInfo.getSavepointHistory();
        int maxCount = configManager.getOperatorConfiguration().getSavepointHistoryMaxCount();
        while (savepointHistory.size() > maxCount) {
            // remove oldest entries
            disposeSavepointQuietly(savepointHistory.remove(0), deployedConfig);
        }

        Duration maxAge = configManager.getOperatorConfiguration().getSavepointHistoryMaxAge();
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
                    .ifPresent(
                            sp -> {
                                savepointInfo.updateLastSavepoint(sp);
                                savepointInfo.addSavepointToHistory(sp);
                            });
        } catch (Exception e) {
            LOG.error("Could not observe latest savepoint information.", e);
            throw new ReconciliationException(e);
        }
    }
}
