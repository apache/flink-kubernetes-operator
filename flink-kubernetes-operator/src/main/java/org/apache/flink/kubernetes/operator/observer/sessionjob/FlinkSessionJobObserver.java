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

import org.apache.flink.api.common.JobID;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.MissingSessionJobException;
import org.apache.flink.kubernetes.operator.observer.AbstractFlinkResourceObserver;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.SavepointObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.utils.FlinkUtils.generateSessionJobFixedJobID;

/** The observer of {@link FlinkSessionJob}. */
public class FlinkSessionJobObserver extends AbstractFlinkResourceObserver<FlinkSessionJob> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSessionJobObserver.class);

    private final SessionJobStatusObserver jobStatusObserver;
    private final SavepointObserver<FlinkSessionJob, FlinkSessionJobStatus> savepointObserver;

    public FlinkSessionJobObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
        this.jobStatusObserver = new SessionJobStatusObserver(eventRecorder);
        this.savepointObserver = new SavepointObserver<>(eventRecorder);
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
        }
    }

    @Override
    protected boolean checkIfAlreadyUpgraded(FlinkResourceContext<FlinkSessionJob> ctx) {
        var flinkSessionJob = ctx.getResource();
        var uid = flinkSessionJob.getMetadata().getUid();
        Collection<JobStatusMessage> jobStatusMessages;
        try {
            jobStatusMessages = ctx.getFlinkService().listJobs(ctx.getObserveConfig());
        } catch (Exception e) {
            throw new RuntimeException("Failed to list jobs", e);
        }
        var matchedJobs = new ArrayList<JobID>();
        for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
            var jobId = jobStatusMessage.getJobId();
            if (jobId.getLowerPart()
                            == generateSessionJobFixedJobID(uid, jobId.getUpperPart() + 1L)
                                    .getLowerPart()
                    && !jobStatusMessage.getJobState().isGloballyTerminalState()) {
                matchedJobs.add(jobId);
            }
        }

        if (matchedJobs.isEmpty()) {
            return false;
        } else if (matchedJobs.size() > 1) {
            // this indicates a bug, which means we have more than one running job for a single
            // SessionJob CR.
            throw new RuntimeException(
                    String.format(
                            "Unexpected case: %d job found for the resource with uid: %s",
                            matchedJobs.size(), flinkSessionJob.getMetadata().getUid()));
        } else {
            var matchedJobID = matchedJobs.get(0);
            Long upgradeTargetGeneration =
                    ReconciliationUtils.getUpgradeTargetGeneration(flinkSessionJob);
            long deployedGeneration = matchedJobID.getUpperPart();
            var oldJobID = flinkSessionJob.getStatus().getJobStatus().getJobId();

            if (upgradeTargetGeneration == deployedGeneration) {
                LOG.info(
                        "Pending upgrade is already deployed, updating status. Old jobID:{}, new jobID:{}",
                        oldJobID,
                        matchedJobID.toHexString());
                flinkSessionJob.getStatus().getJobStatus().setJobId(matchedJobID.toHexString());
                return true;
            } else {
                var msg =
                        String.format(
                                "Running job %s's generation %s doesn't match upgrade target generation %s.",
                                matchedJobID.toHexString(),
                                deployedGeneration,
                                upgradeTargetGeneration);
                throw new RuntimeException(msg);
            }
        }
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
                    MISSING_SESSION_JOB_ERR);
        }
    }
}
