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

package org.apache.flink.kubernetes.operator.observer.sessionjob;

import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.MissingSessionJobException;
import org.apache.flink.kubernetes.operator.observer.AbstractFlinkResourceObserver;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.SnapshotObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** The observer of {@link FlinkSessionJob}. */
public class FlinkSessionJobObserver extends AbstractFlinkResourceObserver<FlinkSessionJob> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSessionJobObserver.class);

    private final SessionJobStatusObserver jobStatusObserver;
    private final SnapshotObserver<FlinkSessionJob, FlinkSessionJobStatus> savepointObserver;

    public FlinkSessionJobObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
        this.jobStatusObserver = new SessionJobStatusObserver(eventRecorder);
        this.savepointObserver = new SnapshotObserver<>(eventRecorder);
    }

    @Override
    protected boolean isResourceReadyToBeObserved(FlinkResourceContext<FlinkSessionJob> ctx) {
        return super.isResourceReadyToBeObserved(ctx) && ctx.getFlinkService() != null;
    }

    @Override
    protected void observeInternal(FlinkResourceContext<FlinkSessionJob> ctx) {
        var jobFound = jobStatusObserver.observe(ctx);
        if (jobFound) {
            savepointObserver.observeSavepointStatus(ctx);
            savepointObserver.observeCheckpointStatus(ctx);
        }
    }

    @Override
    protected boolean checkIfAlreadyUpgraded(FlinkResourceContext<FlinkSessionJob> ctx) {
        var flinkSessionJob = ctx.getResource();
        Collection<JobStatusMessage> jobStatusMessages;
        try {
            jobStatusMessages = ctx.getFlinkService().listJobs(ctx.getObserveConfig());
        } catch (Exception e) {
            throw new RuntimeException("Failed to list jobs", e);
        }
        var submittedJobId = flinkSessionJob.getStatus().getJobStatus().getJobId();
        for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
            if (jobStatusMessage.getJobId().toHexString().equals(submittedJobId)) {
                LOG.info("Job with id {} is already deployed.", submittedJobId);
                return true;
            }
        }
        return false;
    }

    private static class SessionJobStatusObserver extends JobStatusObserver<FlinkSessionJob> {

        public SessionJobStatusObserver(EventRecorder eventRecorder) {
            super(eventRecorder);
        }

        @Override
        protected void onTimeout(FlinkResourceContext<FlinkSessionJob> ctx) {}

        @Override
        protected Optional<JobStatusMessage> filterTargetJob(
                JobStatus status, List<JobStatusMessage> clusterJobStatuses) {
            var jobId =
                    Preconditions.checkNotNull(
                            status.getJobId(), "The jobID to be observed should not be null");
            var matchedList =
                    clusterJobStatuses.stream()
                            .filter(job -> job.getJobId().toHexString().equals(jobId))
                            .collect(Collectors.toList());
            Preconditions.checkArgument(
                    matchedList.size() <= 1,
                    String.format(
                            "Expected one job for JobID: %s, but found %d",
                            status.getJobId(), matchedList.size()));

            if (matchedList.size() == 0) {
                LOG.warn("No job found for JobID: {}", jobId);
                return Optional.empty();
            } else {
                return Optional.of(matchedList.get(0));
            }
        }

        @Override
        protected void onTargetJobNotFound(FlinkResourceContext<FlinkSessionJob> ctx) {
            ifHaDisabledMarkSessionJobMissing(ctx);
        }

        @Override
        protected void onNoJobsFound(FlinkResourceContext<FlinkSessionJob> ctx) {
            ifHaDisabledMarkSessionJobMissing(ctx);
        }

        /**
         * When HA is disabled the session job will not recover on JM restarts. If the JM goes down
         * / restarted the session job should be marked missing.
         *
         * @param ctx Flink session job context.
         */
        private void ifHaDisabledMarkSessionJobMissing(FlinkResourceContext<FlinkSessionJob> ctx) {
            var sessionJob = ctx.getResource();
            if (HighAvailabilityMode.isHighAvailabilityModeActivated(ctx.getObserveConfig())) {
                return;
            }
            sessionJob
                    .getStatus()
                    .getJobStatus()
                    .setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
            LOG.error(MISSING_SESSION_JOB_ERR);
            ReconciliationUtils.updateForReconciliationError(
                    ctx, new MissingSessionJobException(MISSING_SESSION_JOB_ERR));
            eventRecorder.triggerEvent(
                    sessionJob,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.Missing,
                    EventRecorder.Component.Job,
                    MISSING_SESSION_JOB_ERR,
                    ctx.getKubernetesClient());
        }
    }
}
