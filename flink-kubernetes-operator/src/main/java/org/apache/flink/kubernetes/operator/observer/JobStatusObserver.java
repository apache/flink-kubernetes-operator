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
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.kubernetes.operator.utils.FlinkResourceExceptionUtils.updateFlinkResourceException;

/** An observer to observe the job status. */
public abstract class JobStatusObserver<R extends AbstractFlinkResource<?, ?>> {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatusObserver.class);

    public static final String MISSING_SESSION_JOB_ERR = "Missing Session Job";

    protected final EventRecorder eventRecorder;
    protected final FlinkConfigManager configManager;

    public JobStatusObserver(FlinkConfigManager configManager, EventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
        this.configManager = configManager;
    }

    /**
     * Observe the status of the flink job.
     *
     * @param ctx Resource context.
     * @return If job found return true, otherwise return false.
     */
    public boolean observe(FlinkResourceContext<R> ctx) {
        var resource = ctx.getResource();
        var jobStatus = resource.getStatus().getJobStatus();
        LOG.info("Observing job status");
        var previousJobStatus = jobStatus.getState();

        List<JobStatusMessage> clusterJobStatuses;
        try {
            // Query job list from the cluster
            clusterJobStatuses =
                    new ArrayList<>(ctx.getFlinkService().listJobs(ctx.getObserveConfig()));
        } catch (Exception e) {
            // Error while accessing the rest api, will try again later...
            LOG.warn("Exception while listing jobs", e);
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
                onTargetJobNotFound(resource, ctx.getObserveConfig());
                return false;
            } else {
                updateJobStatus(ctx, targetJobStatusMessage.get());
            }
            ReconciliationUtils.checkAndUpdateStableSpec(resource.getStatus());
            return true;
        } else {
            LOG.debug("No jobs found on the cluster");
            // No jobs found on the cluster, it is possible that the jobmanager is still starting up
            ifRunningMoveToReconciling(jobStatus, previousJobStatus);
            onNoJobsFound(resource, ctx.getObserveConfig());
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
     * @param ctx Resource context.
     */
    protected abstract void onTimeout(FlinkResourceContext<R> ctx);

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
     * @param ctx Resource context.
     * @param clusterJobStatus the status fetch from the cluster.
     */
    private void updateJobStatus(FlinkResourceContext<R> ctx, JobStatusMessage clusterJobStatus) {
        var resource = ctx.getResource();
        var jobStatus = resource.getStatus().getJobStatus();
        var previousJobId = jobStatus.getJobId();
        var previousJobStatus = jobStatus.getState();

        jobStatus.setState(clusterJobStatus.getJobState().name());
        jobStatus.setJobName(clusterJobStatus.getJobName());
        jobStatus.setJobId(clusterJobStatus.getJobId().toHexString());
        jobStatus.setStartTime(String.valueOf(clusterJobStatus.getStartTime()));

        if (jobStatus.getJobId().equals(previousJobId)
                && jobStatus.getState().equals(previousJobStatus)) {
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

            setErrorIfPresent(ctx, clusterJobStatus);
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Normal,
                    EventRecorder.Reason.JobStatusChanged,
                    EventRecorder.Component.Job,
                    message);
        }
    }

    private void setErrorIfPresent(FlinkResourceContext<R> ctx, JobStatusMessage clusterJobStatus) {
        if (clusterJobStatus.getJobState() == org.apache.flink.api.common.JobStatus.FAILED) {
            try {
                var result =
                        ctx.getFlinkService()
                                .requestJobResult(
                                        ctx.getObserveConfig(), clusterJobStatus.getJobId());
                result.getSerializedThrowable()
                        .ifPresent(
                                t -> {
                                    updateFlinkResourceException(
                                            t,
                                            ctx.getResource(),
                                            configManager.getOperatorConfiguration());
                                    LOG.error(
                                            "Job {} failed with error: {}",
                                            clusterJobStatus.getJobId(),
                                            t.getFullStringifiedStackTrace());
                                });
            } catch (Exception e) {
                LOG.warn("Failed to request the job result", e);
            }
        }
    }
}
