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
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/** An observer to observe the job status. */
public abstract class JobStatusObserver<R extends AbstractFlinkResource<?, ?>, CTX> {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatusObserver.class);

    private static final int MAX_ERROR_STRING_LENGTH = 512;
    public static final String MISSING_SESSION_JOB_ERR = "Missing Session Job";

    private final FlinkService flinkService;
    private final EventRecorder eventRecorder;

    public JobStatusObserver(FlinkService flinkService, EventRecorder eventRecorder) {
        this.flinkService = flinkService;
        this.eventRecorder = eventRecorder;
    }

    /**
     * Observe the status of the flink job.
     *
     * @param resource The custom resource to be observed.
     * @param deployedConfig Deployed job config.
     * @param ctx Observe context.
     * @return If job found return true, otherwise return false.
     */
    public boolean observe(R resource, Configuration deployedConfig, CTX ctx) {
        var jobStatus = resource.getStatus().getJobStatus();
        LOG.info("Observing job status");
        var previousJobStatus = jobStatus.getState();

        List<JobStatusMessage> clusterJobStatuses;
        try {
            // Query job list from the cluster
            clusterJobStatuses = new ArrayList<>(flinkService.listJobs(deployedConfig));
        } catch (Exception e) {
            // Error while accessing the rest api, will try again later...
            LOG.error("Exception while listing jobs", e);
            ifRunningMoveToReconciling(jobStatus, previousJobStatus);
            if (e instanceof TimeoutException) {
                onTimeout(ctx);
            }
            return false;
        }

        if (!clusterJobStatuses.isEmpty()) {
            // There are jobs on the cluster, we filter the ones for this resource
            Optional<JobStatusMessage> targetJobStatusMessage =
                    filterTargetJob(jobStatus, clusterJobStatuses);

            if (targetJobStatusMessage.isEmpty()) {
                LOG.warn("No matching jobs found on the cluster");
                ifRunningMoveToReconciling(jobStatus, previousJobStatus);
                onTargetJobNotFound(resource, deployedConfig);
                return false;
            } else {
                updateJobStatus(resource, targetJobStatusMessage.get(), deployedConfig);
            }
            ReconciliationUtils.checkAndUpdateStableSpec(resource.getStatus());
            return true;
        } else {
            LOG.debug("No jobs found on the cluster");
            // No jobs found on the cluster, it is possible that the jobmanager is still starting up
            ifRunningMoveToReconciling(jobStatus, previousJobStatus);
            onNoJobsFound(resource, deployedConfig);
            return false;
        }
    }

    /**
     * Callback when no matching target job was found on a cluster where jobs were found.
     *
     * @param resource The Flink resource.
     * @param config Deployed/observe configuration.
     */
    protected abstract void onTargetJobNotFound(R resource, Configuration config);

    /**
     * Callback when no jobs were found on the cluster.
     *
     * @param resource The Flink resource.
     * @param config Deployed/observe configuration.
     */
    protected void onNoJobsFound(R resource, Configuration config) {}

    /**
     * If we observed the job previously in RUNNING state we move to RECONCILING instead as we are
     * not sure anymore.
     *
     * @param jobStatus JobStatus object.
     * @param previousJobStatus Last observed job state.
     */
    private void ifRunningMoveToReconciling(JobStatus jobStatus, String previousJobStatus) {
        if (org.apache.flink.api.common.JobStatus.RUNNING.name().equals(previousJobStatus)) {
            jobStatus.setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
        }
    }

    /**
     * Callback when list jobs timeout.
     *
     * @param ctx Observe context.
     */
    protected abstract void onTimeout(CTX ctx);

    /**
     * Filter the target job status message by the job list from the cluster.
     *
     * @param status the target job status.
     * @param clusterJobStatuses the candidate cluster jobs.
     * @return The target job status message. If no matched job found, {@code Optional.empty()} will
     *     be returned.
     */
    protected abstract Optional<JobStatusMessage> filterTargetJob(
            JobStatus status, List<JobStatusMessage> clusterJobStatuses);

    /**
     * Update the status in CR according to the cluster job status.
     *
     * @param resource the target custom resource.
     * @param clusterJobStatus the status fetch from the cluster.
     * @param deployedConfig Deployed job config.
     */
    private void updateJobStatus(
            R resource, JobStatusMessage clusterJobStatus, Configuration deployedConfig) {
        var jobStatus = resource.getStatus().getJobStatus();
        var previousJobStatus = jobStatus.getState();

        jobStatus.setState(clusterJobStatus.getJobState().name());
        jobStatus.setJobName(clusterJobStatus.getJobName());
        jobStatus.setJobId(clusterJobStatus.getJobId().toHexString());
        jobStatus.setStartTime(String.valueOf(clusterJobStatus.getStartTime()));

        if (jobStatus.getState().equals(previousJobStatus)) {
            LOG.info("Job status ({}) unchanged", previousJobStatus);
        } else {
            jobStatus.setUpdateTime(String.valueOf(System.currentTimeMillis()));
            var message =
                    previousJobStatus == null
                            ? String.format("Job status changed to %s", jobStatus.getState())
                            : String.format(
                                    "Job status changed from %s to %s",
                                    previousJobStatus, jobStatus.getState());
            LOG.info(message);

            setErrorIfPresent(resource, clusterJobStatus, deployedConfig);
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Normal,
                    EventRecorder.Reason.JobStatusChanged,
                    EventRecorder.Component.Job,
                    message);
        }
    }

    private void setErrorIfPresent(
            R resource, JobStatusMessage clusterJobStatus, Configuration deployedConfig) {
        if (clusterJobStatus.getJobState() == org.apache.flink.api.common.JobStatus.FAILED) {
            try {
                var result =
                        flinkService.requestJobResult(deployedConfig, clusterJobStatus.getJobId());
                result.getSerializedThrowable()
                        .ifPresent(
                                t -> {
                                    var error = t.getFullStringifiedStackTrace();
                                    var trimmedError = getErrorWithMaxLength(error);
                                    trimmedError.ifPresent(
                                            value -> {
                                                if (!value.equals(
                                                        resource.getStatus().getError())) {
                                                    resource.getStatus().setError(value);
                                                    LOG.error(
                                                            "Job {} failed with error: {}",
                                                            clusterJobStatus.getJobId(),
                                                            error);
                                                }
                                            });
                                });
            } catch (Exception e) {
                LOG.warn("Failed to request the job result", e);
            }
        }
    }

    private Optional<String> getErrorWithMaxLength(String error) {
        if (error == null) {
            return Optional.empty();
        } else {
            return Optional.of(
                    error.substring(0, Math.min(error.length(), MAX_ERROR_STRING_LENGTH)));
        }
    }
}
