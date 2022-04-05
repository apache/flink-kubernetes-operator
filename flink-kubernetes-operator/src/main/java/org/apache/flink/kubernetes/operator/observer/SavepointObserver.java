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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** An observer of savepoint progress. */
public class SavepointObserver {

    private static final Logger LOG = LoggerFactory.getLogger(SavepointObserver.class);

    private final FlinkService flinkService;
    private final FlinkOperatorConfiguration operatorConfiguration;

    public SavepointObserver(
            FlinkService flinkService, FlinkOperatorConfiguration operatorConfiguration) {
        this.flinkService = flinkService;
        this.operatorConfiguration = operatorConfiguration;
    }

    /**
     * Observe the savepoint result based on the current savepoint info.
     *
     * @param currentSavepointInfo the current savepoint info.
     * @param jobID the jobID of the observed job.
     * @param deployedConfig Deployed job config.
     * @return The observed error, if no error observed, {@code Optional.empty()} will be returned.
     */
    public Optional<String> observe(
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
                            operatorConfiguration, currentSavepointInfo)) {
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
        return Optional.empty();
    }
}
