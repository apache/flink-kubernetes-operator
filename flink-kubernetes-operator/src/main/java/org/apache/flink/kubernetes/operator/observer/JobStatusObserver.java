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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static org.apache.flink.kubernetes.operator.utils.FlinkResourceExceptionUtils.updateFlinkResourceException;

/** An observer to observe the job status. */
public class JobStatusObserver<R extends AbstractFlinkResource<?, ?>> {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatusObserver.class);

    public static final String JOB_NOT_FOUND_ERR = "Job Not Found";

    protected final EventRecorder eventRecorder;

    public JobStatusObserver(EventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
    }

    /**
     * Observe the status of the flink job.
     *
     * @param ctx Resource context.
     * @return If job found return true, otherwise return false.
     */
    public boolean observe(FlinkResourceContext<R> ctx) {
        var resource = ctx.getResource();
        if (resource.getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState()
                == JobState.SUSPENDED) {
            return false;
        }
        var jobStatus = resource.getStatus().getJobStatus();
        LOG.debug("Observing job status");
        var previousJobStatus = jobStatus.getState();

        try {
            var newJobStatusOpt =
                    ctx.getFlinkService()
                            .getJobStatus(
                                    ctx.getObserveConfig(),
                                    JobID.fromHexString(jobStatus.getJobId()));

            if (newJobStatusOpt.isPresent()) {
                updateJobStatus(ctx, newJobStatusOpt.get());
                ReconciliationUtils.checkAndUpdateStableSpec(resource.getStatus());
                return true;
            } else {
                onTargetJobNotFound(ctx);
            }
        } catch (Exception e) {
            // Error while accessing the rest api, will try again later...
            LOG.warn("Exception while getting job status", e);
            ifRunningMoveToReconciling(jobStatus, previousJobStatus);
            if (e instanceof TimeoutException) {
                onTimeout(ctx);
            }
        }
        return false;
    }

    /**
     * Callback when no matching target job was found on a cluster where jobs were found.
     *
     * @param ctx The Flink resource context.
     */
    protected void onTargetJobNotFound(FlinkResourceContext<R> ctx) {
        var resource = ctx.getResource();
        var jobStatus = resource.getStatus().getJobStatus();

        eventRecorder.triggerEvent(
                resource,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.Missing,
                EventRecorder.Component.Job,
                JOB_NOT_FOUND_ERR,
                ctx.getKubernetesClient());

        if (resource instanceof FlinkSessionJob
                && !ReconciliationUtils.isJobInTerminalState(resource.getStatus())
                && resource.getSpec().getJob().getUpgradeMode() == UpgradeMode.STATELESS) {
            // We also mark jobs that were previously not terminated as suspended if
            // stateless upgrade mode is used. In these cases we want to simply restart the job.
            markSuspended(resource);
        } else {
            // We could not suspend the job as it was lost during a stateless upgrade, cancel
            // upgrading state and retry the upgrade (if possible)
            resource.getStatus().getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
        }
        jobStatus.setState(org.apache.flink.api.common.JobStatus.RECONCILING);
        resource.getStatus().setError(JOB_NOT_FOUND_ERR);
    }

    /**
     * If we observed the job previously in RUNNING state we move to RECONCILING instead as we are
     * not sure anymore.
     *
     * @param jobStatus JobStatus object.
     * @param previousJobStatus Last observed job state.
     */
    private void ifRunningMoveToReconciling(
            org.apache.flink.kubernetes.operator.api.status.JobStatus jobStatus,
            JobStatus previousJobStatus) {
        if (JobStatus.RUNNING == previousJobStatus) {
            jobStatus.setState(JobStatus.RECONCILING);
        }
    }

    /**
     * Callback when list jobs timeout.
     *
     * @param ctx Resource context.
     */
    protected void onTimeout(FlinkResourceContext<R> ctx) {}

    /**
     * Update the status in CR according to the cluster job status.
     *
     * @param ctx Resource context.
     * @param clusterJobStatus the status fetch from the cluster.
     */
    private void updateJobStatus(FlinkResourceContext<R> ctx, JobStatusMessage clusterJobStatus) {
        var resource = ctx.getResource();
        var jobStatus = resource.getStatus().getJobStatus();
        var previousJobStatus = jobStatus.getState();
        var currentJobStatus = clusterJobStatus.getJobState();

        jobStatus.setState(currentJobStatus);
        jobStatus.setJobName(clusterJobStatus.getJobName());
        jobStatus.setStartTime(String.valueOf(clusterJobStatus.getStartTime()));

        if (jobStatus.getState().equals(previousJobStatus)) {
            LOG.debug("Job status ({}) unchanged", previousJobStatus);
        } else {
            jobStatus.setUpdateTime(String.valueOf(System.currentTimeMillis()));
            var message =
                    previousJobStatus == null
                            ? String.format("Job status changed to %s", jobStatus.getState())
                            : String.format(
                                    "Job status changed from %s to %s",
                                    previousJobStatus, jobStatus.getState());

            if (JobStatus.CANCELED == currentJobStatus
                    || (currentJobStatus.isGloballyTerminalState()
                            && JobStatus.CANCELLING.equals(previousJobStatus))) {
                // The job was cancelled
                markSuspended(resource);
            }

            setErrorIfPresent(ctx, clusterJobStatus);
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Normal,
                    EventRecorder.Reason.JobStatusChanged,
                    EventRecorder.Component.Job,
                    message,
                    ctx.getKubernetesClient());
        }
    }

    private static void markSuspended(AbstractFlinkResource<?, ?> resource) {
        LOG.info("Marking suspended");
        ReconciliationUtils.updateLastReconciledSpec(
                resource,
                (s, m) -> {
                    s.getJob().setState(JobState.SUSPENDED);
                    m.setFirstDeployment(false);
                });
    }

    private void setErrorIfPresent(FlinkResourceContext<R> ctx, JobStatusMessage clusterJobStatus) {
        if (clusterJobStatus.getJobState() == JobStatus.FAILED) {
            try {
                var result =
                        ctx.getFlinkService()
                                .requestJobResult(
                                        ctx.getObserveConfig(), clusterJobStatus.getJobId());
                result.getSerializedThrowable()
                        .ifPresent(
                                t -> {
                                    updateFlinkResourceException(
                                            t, ctx.getResource(), ctx.getOperatorConfig());
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
