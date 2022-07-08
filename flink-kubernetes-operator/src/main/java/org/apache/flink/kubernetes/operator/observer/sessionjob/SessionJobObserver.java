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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.observer.SavepointObserver;
import org.apache.flink.kubernetes.operator.observer.context.VoidObserverContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.service.FlinkServiceFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.util.Preconditions;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** The observer of {@link FlinkSessionJob}. */
public class SessionJobObserver implements Observer<FlinkSessionJob> {

    private static final Logger LOG = LoggerFactory.getLogger(SessionJobObserver.class);
    private final FlinkServiceFactory flinkServiceFactory;
    private final FlinkConfigManager configManager;
    private final EventRecorder eventRecorder;
    private final StatusRecorder<FlinkSessionJobStatus> statusRecorder;

    public SessionJobObserver(
            FlinkServiceFactory flinkServiceFactory,
            FlinkConfigManager configManager,
            StatusRecorder<FlinkSessionJobStatus> statusRecorder,
            EventRecorder eventRecorder) {
        this.flinkServiceFactory = flinkServiceFactory;
        this.configManager = configManager;
        this.eventRecorder = eventRecorder;
        this.statusRecorder = statusRecorder;
    }

    private JobStatusObserver<VoidObserverContext> getJobStatusObserver(FlinkService flinkService) {
        return new JobStatusObserver<>(flinkService, eventRecorder) {
            @Override
            protected void onTimeout(VoidObserverContext sessionJobObserverContext) {}

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
                                "Expected one job for JobID: %s, but %d founded",
                                status.getJobId(), matchedList.size()));

                if (matchedList.size() == 0) {
                    LOG.info("No job found for JobID: {}", jobId);
                    return Optional.empty();
                } else {
                    return Optional.of(matchedList.get(0));
                }
            }
        };
    }

    @Override
    public void observe(FlinkSessionJob flinkSessionJob, Context context) {
        if (flinkSessionJob.getStatus().getReconciliationStatus().isFirstDeployment()) {
            return;
        }

        Optional<FlinkDeployment> flinkDepOpt = context.getSecondaryResource(FlinkDeployment.class);

        if (!SessionJobReconciler.sessionClusterReady(flinkDepOpt)) {
            return;
        }

        FlinkService flinkService = flinkServiceFactory.getOrCreate(flinkDepOpt.get());
        var jobStatusObserver = getJobStatusObserver(flinkService);
        var deployedConfig =
                configManager.getSessionJobConfig(flinkDepOpt.get(), flinkSessionJob.getSpec());
        var reconciliationStatus = flinkSessionJob.getStatus().getReconciliationStatus();
        if (reconciliationStatus.getState() == ReconciliationState.UPGRADING) {
            checkIfAlreadyUpgraded(flinkSessionJob, deployedConfig, flinkService);
            if (reconciliationStatus.getState() == ReconciliationState.UPGRADING) {
                return;
            }
        }

        var jobFound =
                jobStatusObserver.observe(
                        flinkSessionJob, deployedConfig, VoidObserverContext.INSTANCE);

        if (jobFound) {
            SavepointObserver savepointObserver =
                    new SavepointObserver<>(
                            flinkService, configManager, statusRecorder, eventRecorder);
            savepointObserver.observeSavepointStatus(flinkSessionJob, deployedConfig);
        }
        SavepointUtils.resetTriggerIfJobNotRunning(flinkSessionJob, eventRecorder);
    }

    private void checkIfAlreadyUpgraded(
            FlinkSessionJob flinkSessionJob,
            Configuration deployedConfig,
            FlinkService flinkService) {
        var uid = flinkSessionJob.getMetadata().getUid();
        Collection<JobStatusMessage> jobStatusMessages;
        try {
            jobStatusMessages = flinkService.listJobs(deployedConfig);
        } catch (Exception e) {
            throw new RuntimeException("Failed to list jobs", e);
        }
        var matchedJobs = new ArrayList<JobID>();
        for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
            var jobId = jobStatusMessage.getJobId();
            if (jobId.getLowerPart() == uid.hashCode()
                    && !jobStatusMessage.getJobState().isGloballyTerminalState()) {
                matchedJobs.add(jobId);
            }
        }

        if (matchedJobs.isEmpty()) {
            return;
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
                ReconciliationUtils.updateStatusForAlreadyUpgraded(flinkSessionJob);
                flinkSessionJob
                        .getStatus()
                        .getJobStatus()
                        .setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
                flinkSessionJob.getStatus().getJobStatus().setJobId(matchedJobID.toHexString());
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
}
