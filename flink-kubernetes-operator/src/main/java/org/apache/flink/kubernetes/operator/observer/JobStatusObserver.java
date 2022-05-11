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
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/** An observer to observe the job status. */
public abstract class JobStatusObserver<CTX> {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatusObserver.class);
    private final FlinkService flinkService;

    public JobStatusObserver(FlinkService flinkService) {
        this.flinkService = flinkService;
    }

    /**
     * Observe the status of the flink job.
     *
     * @param jobStatus The job status to be observed.
     * @param deployedConfig Deployed job config.
     * @return If job found return true, otherwise return false.
     */
    public boolean observe(JobStatus jobStatus, Configuration deployedConfig, CTX ctx) {
        LOG.info("Observing job status");
        var previousJobStatus = jobStatus.getState();
        List<JobStatusMessage> clusterJobStatuses;
        try {
            clusterJobStatuses = new ArrayList<>(flinkService.listJobs(deployedConfig));
        } catch (Exception e) {
            LOG.error("Exception while listing jobs", e);
            if (e instanceof TimeoutException) {
                onTimeout(ctx);
            }
            ifRunningMoveToReconciling(jobStatus, previousJobStatus);
            return false;
        }

        if (!clusterJobStatuses.isEmpty()) {
            Optional<String> targetJobStatus = updateJobStatus(jobStatus, clusterJobStatuses);
            if (targetJobStatus.isEmpty()) {
                jobStatus.setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
                return false;
            } else {
                if (targetJobStatus.get().equals(previousJobStatus)) {
                    LOG.info("Job status ({}) unchanged", previousJobStatus);
                } else {
                    LOG.info(
                            "Job status successfully updated from {} to {}",
                            previousJobStatus,
                            targetJobStatus.get());
                }
            }
            return true;
        } else {
            ifRunningMoveToReconciling(jobStatus, previousJobStatus);
            return false;
        }
    }

    private void ifRunningMoveToReconciling(JobStatus jobStatus, String previousJobStatus) {
        if (org.apache.flink.api.common.JobStatus.RUNNING.name().equals(previousJobStatus)) {
            jobStatus.setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
        }
    }

    /** Callback when list jobs timeout. */
    protected abstract void onTimeout(CTX ctx);

    /**
     * Find and update previous job status based on the job list from the cluster and return the
     * target status.
     *
     * @param status the target job status to be updated.
     * @param clusterJobStatuses the candidate cluster jobs.
     * @return The target status of the job. If no matched job found, {@code Optional.empty()} will
     *     be returned.
     */
    protected abstract Optional<String> updateJobStatus(
            JobStatus status, List<JobStatusMessage> clusterJobStatuses);
}
