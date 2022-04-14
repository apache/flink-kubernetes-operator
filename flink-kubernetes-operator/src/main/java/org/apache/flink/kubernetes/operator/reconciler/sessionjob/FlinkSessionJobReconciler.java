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

package org.apache.flink.kubernetes.operator.reconciler.sessionjob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.kubernetes.operator.observer.deployment.AbstractDeploymentObserver.JOB_STATE_UNKNOWN;

/** The reconciler for the {@link FlinkSessionJob}. */
public class FlinkSessionJobReconciler implements Reconciler<FlinkSessionJob> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSessionJobReconciler.class);

    private final FlinkOperatorConfiguration operatorConfiguration;
    private final Configuration defaultConfig;
    private final KubernetesClient kubernetesClient;
    private final FlinkService flinkService;

    public FlinkSessionJobReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkOperatorConfiguration operatorConfiguration,
            Configuration defaultConfig) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
        this.operatorConfiguration = operatorConfiguration;
        this.defaultConfig = defaultConfig;
    }

    @Override
    public void reconcile(FlinkSessionJob flinkSessionJob, Context context) throws Exception {

        SessionJobHelper helper = new SessionJobHelper(flinkSessionJob, LOG);
        FlinkSessionJobSpec lastReconciledSpec =
                flinkSessionJob
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec();

        Optional<FlinkDeployment> flinkDepOptional =
                OperatorUtils.getSecondaryResource(flinkSessionJob, context, operatorConfiguration);

        // if session cluster is not ready, we can't do reconcile for the job.
        if (!helper.sessionClusterReady(flinkDepOptional)) {
            return;
        }

        if (lastReconciledSpec == null) {
            Configuration effectiveConfig =
                    FlinkUtils.getEffectiveConfig(flinkDepOptional.get(), defaultConfig);
            submitAndInitStatus(
                    flinkSessionJob,
                    effectiveConfig,
                    Optional.ofNullable(
                                    flinkSessionJob.getSpec().getJob().getInitialSavepointPath())
                            .orElse(null));
            ReconciliationUtils.updateForSpecReconciliationSuccess(
                    flinkSessionJob, JobState.RUNNING);
            return;
        }

        if (helper.savepointInProgress()) {
            LOG.info("Delaying job reconciliation until pending savepoint is completed");
            return;
        }

        boolean specChanged = helper.specChanged(lastReconciledSpec);

        var effectiveConfig = FlinkUtils.getEffectiveConfig(flinkDepOptional.get(), defaultConfig);
        if (specChanged) {
            var jobSpec = flinkSessionJob.getSpec().getJob();
            JobState currentJobState = lastReconciledSpec.getJob().getState();
            JobState desiredJobState = jobSpec.getState();
            UpgradeMode upgradeMode = jobSpec.getUpgradeMode();
            JobState stateAfterReconcile = currentJobState;
            if (currentJobState == JobState.RUNNING) {
                if (desiredJobState == JobState.RUNNING) {
                    LOG.info("Upgrading/Restarting running job, suspending first...");
                }
                stateAfterReconcile = suspendJob(flinkSessionJob, upgradeMode, effectiveConfig);
            }
            if (currentJobState == JobState.SUSPENDED && desiredJobState == JobState.RUNNING) {
                if (upgradeMode == UpgradeMode.STATELESS) {
                    submitAndInitStatus(flinkSessionJob, effectiveConfig, null);
                } else if (upgradeMode == UpgradeMode.LAST_STATE
                        || upgradeMode == UpgradeMode.SAVEPOINT) {
                    restoreFromLastSavepoint(flinkSessionJob, effectiveConfig);
                }
                stateAfterReconcile = JobState.RUNNING;
            }
            ReconciliationUtils.updateForSpecReconciliationSuccess(
                    flinkSessionJob, stateAfterReconcile);
        } else if (helper.shouldTriggerSavepoint() && helper.isJobRunning(flinkDepOptional.get())) {
            triggerSavepoint(flinkSessionJob, effectiveConfig);
            ReconciliationUtils.updateSavepointReconciliationSuccess(flinkSessionJob);
        }
    }

    @Override
    public DeleteControl cleanup(FlinkSessionJob sessionJob, Context context) {
        Optional<FlinkDeployment> flinkDepOptional =
                OperatorUtils.getSecondaryResource(sessionJob, context, operatorConfiguration);

        if (flinkDepOptional.isPresent()) {
            Configuration effectiveConfig =
                    FlinkUtils.getEffectiveConfig(flinkDepOptional.get(), defaultConfig);
            String jobID = sessionJob.getStatus().getJobStatus().getJobId();
            if (jobID != null) {
                try {
                    flinkService.cancelSessionJob(
                            JobID.fromHexString(jobID), UpgradeMode.STATELESS, effectiveConfig);
                } catch (Exception e) {
                    LOG.error("Failed to cancel job.", e);
                }
            }
        } else {
            LOG.info("Session cluster deployment not available");
        }
        return DeleteControl.defaultDelete();
    }

    private void submitAndInitStatus(
            FlinkSessionJob sessionJob, Configuration effectiveConfig, @Nullable String savepoint)
            throws Exception {
        var jobID = flinkService.submitJobToSessionCluster(sessionJob, effectiveConfig, savepoint);
        sessionJob
                .getStatus()
                .setJobStatus(
                        new JobStatus()
                                .toBuilder()
                                .jobId(jobID.toHexString())
                                .state(JOB_STATE_UNKNOWN)
                                .build());
    }

    private void restoreFromLastSavepoint(
            FlinkSessionJob flinkSessionJob, Configuration effectiveConfig) throws Exception {
        JobStatus jobStatus = flinkSessionJob.getStatus().getJobStatus();
        Optional<String> savepointOpt =
                Optional.ofNullable(jobStatus.getSavepointInfo().getLastSavepoint())
                        .flatMap(s -> Optional.ofNullable(s.getLocation()));

        submitAndInitStatus(flinkSessionJob, effectiveConfig, savepointOpt.orElse(null));
    }

    private Optional<String> internalSuspendJob(
            FlinkSessionJob sessionJob, UpgradeMode upgradeMode, Configuration effectiveConfig)
            throws Exception {
        final String jobIdString = sessionJob.getStatus().getJobStatus().getJobId();
        Preconditions.checkNotNull(jobIdString, "The job to be suspend should not be null");
        return flinkService.cancelSessionJob(
                JobID.fromHexString(jobIdString), upgradeMode, effectiveConfig);
    }

    private JobState suspendJob(
            FlinkSessionJob sessionJob, UpgradeMode upgradeMode, Configuration effectiveConfig)
            throws Exception {
        final Optional<String> savepointOpt =
                internalSuspendJob(sessionJob, upgradeMode, effectiveConfig);

        JobStatus jobStatus = sessionJob.getStatus().getJobStatus();
        JobState stateAfterReconcile = JobState.SUSPENDED;
        jobStatus.setState(stateAfterReconcile.name());
        savepointOpt.ifPresent(
                location -> jobStatus.getSavepointInfo().setLastSavepoint(Savepoint.of(location)));
        return stateAfterReconcile;
    }

    private void triggerSavepoint(FlinkSessionJob sessionJob, Configuration effectiveConfig)
            throws Exception {
        flinkService.triggerSavepoint(
                sessionJob.getStatus().getJobStatus().getJobId(),
                sessionJob.getStatus().getJobStatus().getSavepointInfo(),
                effectiveConfig);
    }
}
