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

package org.apache.flink.kubernetes.operator.observer.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.utils.DateTimeUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthObserver;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.SnapshotObserver;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;

/** The observer of {@link org.apache.flink.kubernetes.operator.config.Mode#APPLICATION} cluster. */
public class ApplicationObserver extends AbstractFlinkDeploymentObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationObserver.class);

    private final SnapshotObserver<FlinkDeployment, FlinkDeploymentStatus> savepointObserver;
    private final JobStatusObserver<FlinkDeployment> jobStatusObserver;
    private final ClusterHealthObserver clusterHealthObserver;

    public ApplicationObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
        this.savepointObserver = new SnapshotObserver<>(eventRecorder);
        this.jobStatusObserver = new ApplicationJobObserver(eventRecorder);
        this.clusterHealthObserver = new ClusterHealthObserver();
    }

    @Override
    protected void observeFlinkCluster(FlinkResourceContext<FlinkDeployment> ctx) {
        logger.debug("Observing application cluster");
        boolean jobFound = jobStatusObserver.observe(ctx);
        if (jobFound) {
            var observeConfig = ctx.getObserveConfig();
            savepointObserver.observeSavepointStatus(ctx);
            savepointObserver.observeCheckpointStatus(ctx);
            if (observeConfig.getBoolean(OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED)) {
                clusterHealthObserver.observe(ctx);
            }
        }
    }

    @Override
    protected void observeJobManagerExceptions(FlinkResourceContext<FlinkDeployment> ctx) {
        var deployment = ctx.getResource();
        var operatorConfig = ctx.getOperatorConfig();
        var jobStatus = deployment.getStatus().getJobStatus();
        // Ideally should not happen
        if (jobStatus == null || jobStatus.getJobId() == null) {
            LOG.warn(
                    "No jobId found for deployment '{}', skipping exception observation.",
                    deployment.getMetadata().getName());
            return;
        }

        try {
            JobID jobId = JobID.fromHexString(jobStatus.getJobId());
            // TODO: Ideally the best way to restrict the number of events is to use the query param
            // `maxExceptions`
            //  but the JobExceptionsMessageParameters does not expose the parameters and nor does
            // it have setters.
            var history =
                    ctx.getFlinkService()
                            .getJobExceptions(
                                    deployment, jobId, ctx.getDeployConfig(deployment.getSpec()));

            if (history == null || history.getExceptionHistory() == null) {
                return;
            }

            var exceptionHistory = history.getExceptionHistory();
            var exceptions = exceptionHistory.getEntries();
            if (exceptions.isEmpty()) {
                return;
            }

            if (exceptionHistory.isTruncated()) {
                LOG.warn(
                        "Job exception history is truncated for jobId '{}'. Some exceptions may be missing.",
                        jobId);
            }

            Instant lastRecorded = null; // first reconciliation
            if (deployment.getStatus().getLastRecordedExceptionTimestamp() != null) {
                try {
                    lastRecorded =
                            Instant.parse(
                                    deployment.getStatus().getLastRecordedExceptionTimestamp());
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to parse last recorded exception timestamp for deployment '{}'.",
                            deployment.getMetadata().getName(),
                            e);
                    return;
                }
            }
            Instant now = Instant.now();

            int maxEvents = operatorConfig.getReportedExceptionEventsMaxCount();
            int maxStackTraceLines = operatorConfig.getReportedExceptionEventsMaxStackTraceLength();

            int count = 0;
            for (var exception : exceptions) {
                Instant exceptionTime = Instant.ofEpochMilli(exception.getTimestamp());
                if (lastRecorded != null && exceptionTime.isBefore(lastRecorded)) {
                    continue;
                }

                emitJobManagerExceptionEvent(ctx, exception, exceptionTime, maxStackTraceLines);
                if (++count >= maxEvents) {
                    break;
                }
            }
            // only update the last recorded exception timestamp if we emitted an event
            deployment.getStatus().setLastRecordedExceptionTimestamp(now.toString());

        } catch (Exception e) {
            LOG.warn(
                    "Failed to fetch JobManager exception info for deployment '{}'.",
                    deployment.getMetadata().getName(),
                    e);
        }
    }

    private void emitJobManagerExceptionEvent(
            FlinkResourceContext<FlinkDeployment> ctx,
            JobExceptionsInfoWithHistory.RootExceptionInfo exception,
            Instant exceptionTime,
            int maxStackTraceLines) {

        String exceptionName = exception.getExceptionName();
        if (exceptionName == null || exceptionName.isBlank()) {
            return;
        }

        Map<String, String> annotations = new HashMap<>();
        annotations.put(
                "event-time-readable",
                DateTimeUtils.readable(exceptionTime, ZoneId.systemDefault()));
        annotations.put("event-timestamp-millis", String.valueOf(exceptionTime.toEpochMilli()));

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

        StringBuilder eventMessage = new StringBuilder(exceptionName);
        String stacktrace = exception.getStacktrace();
        if (stacktrace != null && !stacktrace.isBlank()) {
            String[] lines = stacktrace.split("\n");
            eventMessage.append("\n\nStacktrace (truncated):\n");
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

        String keyMessage =
                exceptionName.length() > 128 ? exceptionName.substring(0, 128) : exceptionName;

        eventRecorder.triggerEventOnceWithAnnotations(
                exceptionTime.toEpochMilli(),
                ctx.getResource(),
                EventRecorder.Type.Warning,
                EventRecorder.Reason.JobException,
                eventMessage.toString().trim(),
                EventRecorder.Component.JobManagerDeployment,
                "jobmanager-exception-" + keyMessage.hashCode(),
                ctx.getKubernetesClient(),
                annotations);
    }

    private class ApplicationJobObserver extends JobStatusObserver<FlinkDeployment> {
        public ApplicationJobObserver(EventRecorder eventRecorder) {
            super(eventRecorder);
        }

        @Override
        public void onTimeout(FlinkResourceContext<FlinkDeployment> ctx) {
            observeJmDeployment(ctx);
        }
    }
}
