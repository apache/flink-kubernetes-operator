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
import org.apache.flink.kubernetes.operator.utils.EventUtils;
import org.apache.flink.kubernetes.operator.utils.ExceptionUtils;
import org.apache.flink.kubernetes.operator.utils.K8sAnnotationsSanitizer;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.kubernetes.operator.utils.FlinkResourceExceptionUtils.updateFlinkResourceException;

/** An observer to observe the job status. */
public class JobStatusObserver<R extends AbstractFlinkResource<?, ?>> {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatusObserver.class);

    public static final String JOB_NOT_FOUND_ERR = "Job Not Found";
    public static final String EXCEPTION_TIMESTAMP = "exception-timestamp";
    public static final Duration MAX_K8S_EVENT_AGE = Duration.ofMinutes(30);

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
        var jobId = jobStatus.getJobId();
        try {
            var newJobStatusOpt =
                    ctx.getFlinkService()
                            .getJobStatus(ctx.getObserveConfig(), JobID.fromHexString(jobId));

            if (newJobStatusOpt.isPresent()) {
                var newJobStatus = newJobStatusOpt.get();
                updateJobStatus(ctx, newJobStatus);
                ReconciliationUtils.checkAndUpdateStableSpec(resource.getStatus());
                // see if the JM server is up, try to get the exceptions
                if (!previousJobStatus.isGloballyTerminalState()) {
                    observeJobManagerExceptions(ctx);
                }
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
     * Observe the exceptions raised in the job manager and take appropriate action.
     *
     * @param ctx the context with which the operation is executed
     */
    protected void observeJobManagerExceptions(FlinkResourceContext<R> ctx) {
        var resource = ctx.getResource();
        var operatorConfig = ctx.getOperatorConfig();
        var jobStatus = resource.getStatus().getJobStatus();

        try {
            var jobId = JobID.fromHexString(jobStatus.getJobId());
            // TODO: Ideally the best way to restrict the number of events is to use the query param
            // `maxExceptions`
            //  but the JobExceptionsMessageParameters does not expose the parameters and nor does
            // it have setters.
            var history =
                    ctx.getFlinkService().getJobExceptions(resource, jobId, ctx.getObserveConfig());

            if (history == null || history.getExceptionHistory() == null) {
                return;
            }

            var exceptionHistory = history.getExceptionHistory();
            var exceptions = exceptionHistory.getEntries();
            if (exceptions != null) {
                exceptions = new ArrayList<>(exceptions);
                exceptions.sort(
                        Comparator.comparingLong(
                                        JobExceptionsInfoWithHistory.RootExceptionInfo
                                                ::getTimestamp)
                                .reversed());
            } else {
                exceptions = Collections.emptyList();
            }

            String currentJobId = jobStatus.getJobId();
            var cacheEntry = ctx.getExceptionCacheEntry();

            if (!cacheEntry.isInitialized()) {
                Instant lastExceptionTs;
                if (exceptions.isEmpty()) {
                    // If the job doesn't have any exceptions set to MIN as we always have to record
                    // the next
                    lastExceptionTs = Instant.MIN;
                } else {
                    var k8sExpirationTs = Instant.now().minus(MAX_K8S_EVENT_AGE);
                    var maxJobExceptionTs = Instant.ofEpochMilli(exceptions.get(0).getTimestamp());
                    if (maxJobExceptionTs.isBefore(k8sExpirationTs)) {
                        // If the last job exception was a long time ago, then there is no point in
                        // checking in k8s. We won't report this as exception
                        lastExceptionTs = maxJobExceptionTs;
                    } else {
                        // If there were recent exceptions, we check the triggered events from kube
                        // to make sure we don't double trigger
                        lastExceptionTs =
                                EventUtils.findLastJobExceptionTsFromK8s(
                                                ctx.getKubernetesClient(), resource)
                                        .orElse(k8sExpirationTs);
                    }
                }

                cacheEntry.setLastTimestamp(lastExceptionTs);
                cacheEntry.setInitialized(true);
                cacheEntry.setJobId(currentJobId);
            }

            var lastRecorded =
                    currentJobId.equals(cacheEntry.getJobId())
                            ? cacheEntry.getLastTimestamp()
                            : Instant.MIN;

            if (exceptions.isEmpty()) {
                return;
            }

            int maxEvents = operatorConfig.getReportedExceptionEventsMaxCount();
            int maxStackTraceLines = operatorConfig.getReportedExceptionEventsMaxStackTraceLength();

            int count = 0;
            for (var exception : exceptions) {
                var exceptionTime = Instant.ofEpochMilli(exception.getTimestamp());
                // Skip already recorded exceptions and after max count
                if (!exceptionTime.isAfter(lastRecorded) || count++ >= maxEvents) {
                    break;
                }
                emitJobManagerExceptionEvent(ctx, exception, exceptionTime, maxStackTraceLines);
            }

            if (count > maxEvents) {
                LOG.warn("Job exception history is truncated. Some exceptions may be missing.");
            }

            cacheEntry.setJobId(currentJobId);
            cacheEntry.setLastTimestamp(Instant.ofEpochMilli(exceptions.get(0).getTimestamp()));
        } catch (Exception e) {
            LOG.warn("Failed to fetch JobManager exception info.", e);
        }
    }

    private void emitJobManagerExceptionEvent(
            FlinkResourceContext<R> ctx,
            JobExceptionsInfoWithHistory.RootExceptionInfo exception,
            Instant exceptionTime,
            int maxStackTraceLines) {
        Map<String, String> annotations = new HashMap<>();
        if (exceptionTime != null) {
            annotations.put(EXCEPTION_TIMESTAMP, exceptionTime.toString());
        }
        if (exception.getTaskName() != null) {
            annotations.put("task-name", exception.getTaskName());
        }
        if (exception.getEndpoint() != null) {
            annotations.put("endpoint", exception.getEndpoint());
        }
        if (exception.getTaskManagerId() != null) {
            annotations.put("tm-id", exception.getTaskManagerId());
        }
        if (exception.getFailureLabels() != null) {
            exception
                    .getFailureLabels()
                    .forEach((k, v) -> annotations.put("failure-label-" + k, v));
        }

        StringBuilder eventMessage = new StringBuilder();
        String stacktrace = exception.getStacktrace();
        if (stacktrace != null && !stacktrace.isBlank()) {
            String[] lines = stacktrace.split("\n");
            for (int i = 0; i < Math.min(maxStackTraceLines, lines.length); i++) {
                eventMessage.append(lines[i]).append("\n");
            }
            if (lines.length > maxStackTraceLines) {
                eventMessage
                        .append("... (")
                        .append(lines.length - maxStackTraceLines)
                        .append(" more lines)");
            }
        }

        String identityKey =
                "jobmanager-exception-"
                        + Integer.toHexString(Objects.hash(eventMessage.toString()));
        eventRecorder.triggerEventWithAnnotations(
                ctx.getResource(),
                EventRecorder.Type.Warning,
                EventRecorder.Reason.JobException,
                eventMessage.toString().trim(),
                EventRecorder.Component.Job,
                identityKey,
                ctx.getKubernetesClient(),
                K8sAnnotationsSanitizer.sanitizeAnnotations(annotations));
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

            recordJobErrorIfPresent(ctx, clusterJobStatus);
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

    private void recordJobErrorIfPresent(
            FlinkResourceContext<R> ctx, JobStatusMessage clusterJobStatus) {
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

                                    eventRecorder.triggerEvent(
                                            ctx.getResource(),
                                            EventRecorder.Type.Warning,
                                            EventRecorder.Reason.Error,
                                            EventRecorder.Component.Job,
                                            ExceptionUtils.getExceptionMessage(t),
                                            ctx.getKubernetesClient());
                                });
            } catch (Exception e) {
                LOG.warn("Failed to request the job result", e);
            }
        }
    }
}
