/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

/** The observer of {@link org.apache.flink.kubernetes.operator.config.Mode#APPLICATION} cluster. */
public class JobObserver extends BaseObserver {

    public JobObserver(
            FlinkService flinkService, FlinkOperatorConfiguration operatorConfiguration) {
        super(flinkService, operatorConfiguration);
    }

    @Override
    public void observe(FlinkDeployment flinkApp, Context context, Configuration effectiveConfig) {
        if (!isClusterReady(flinkApp)) {
            observeJmDeployment(flinkApp, context, effectiveConfig);
        }
        if (isClusterReady(flinkApp)) {
            boolean jobFound = observeFlinkJobStatus(flinkApp, context, effectiveConfig);
            if (jobFound) {
                observeSavepointStatus(flinkApp, effectiveConfig);
            }
        }
        clearErrorsIfJobManagerDeploymentNotInErrorStatus(flinkApp);
    }

    private boolean observeFlinkJobStatus(
            FlinkDeployment flinkApp, Context context, Configuration effectiveConfig) {
        logger.info("Observing job status");
        FlinkDeploymentStatus flinkAppStatus = flinkApp.getStatus();
        String previousJobStatus = flinkAppStatus.getJobStatus().getState();
        Collection<JobStatusMessage> clusterJobStatuses;
        try {
            clusterJobStatuses = flinkService.listJobs(effectiveConfig);
        } catch (Exception e) {
            logger.error("Exception while listing jobs", e);
            flinkAppStatus.getJobStatus().setState(JOB_STATE_UNKNOWN);
            if (e instanceof TimeoutException) {
                // check for problems with the underlying deployment
                observeJmDeployment(flinkApp, context, effectiveConfig);
            }
            return false;
        }
        if (clusterJobStatuses.isEmpty()) {
            logger.info("No job found on cluster yet");
            flinkAppStatus.getJobStatus().setState(JOB_STATE_UNKNOWN);
            return false;
        }
        String targetJobStatus =
                updateJobStatus(flinkAppStatus.getJobStatus(), new ArrayList<>(clusterJobStatuses));
        if (targetJobStatus.equals(previousJobStatus)) {
            logger.info("Job status ({}) unchanged", previousJobStatus);
        } else {
            logger.info(
                    "Job status successfully updated from {} to {}",
                    previousJobStatus,
                    targetJobStatus);
        }
        return true;
    }

    /**
     * Update previous job status based on the job list from the cluster and return the target
     * status.
     */
    private String updateJobStatus(JobStatus status, List<JobStatusMessage> clusterJobStatuses) {
        Collections.sort(
                clusterJobStatuses, (j1, j2) -> Long.compare(j2.getStartTime(), j1.getStartTime()));
        JobStatusMessage newJob = clusterJobStatuses.get(0);

        status.setState(newJob.getJobState().name());
        status.setJobName(newJob.getJobName());
        status.setJobId(newJob.getJobId().toHexString());
        status.setStartTime(String.valueOf(newJob.getStartTime()));
        status.setUpdateTime(String.valueOf(System.currentTimeMillis()));
        return status.getState();
    }

    private void observeSavepointStatus(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        SavepointInfo savepointInfo = flinkApp.getStatus().getJobStatus().getSavepointInfo();
        if (!SavepointUtils.savepointInProgress(flinkApp)) {
            logger.debug("Savepoint not in progress");
            return;
        }
        logger.info("Observing savepoint status");

        SavepointFetchResult savepointFetchResult;
        try {
            savepointFetchResult = flinkService.fetchSavepointInfo(flinkApp, effectiveConfig);
        } catch (Exception e) {
            logger.error("Exception while fetching savepoint info", e);
            return;
        }

        if (!savepointFetchResult.isTriggered()) {
            String error = savepointFetchResult.getError();
            if (error != null
                    || SavepointUtils.gracePeriodEnded(operatorConfiguration, savepointInfo)) {
                String errorMsg = error != null ? error : "Savepoint status unknown";
                logger.error(errorMsg);
                savepointInfo.resetTrigger();
                ReconciliationUtils.updateForReconciliationError(flinkApp, errorMsg);
                return;
            }
            logger.info("Savepoint operation not running, waiting within grace period...");
        }
        if (savepointFetchResult.getSavepoint() == null) {
            logger.info("Savepoint is still in progress...");
            return;
        }
        logger.info("Savepoint status updated with latest completed savepoint info");
        savepointInfo.updateLastSavepoint(savepointFetchResult.getSavepoint());
    }
}
