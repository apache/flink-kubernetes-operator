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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
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
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Optional;

/** The reconciler for the {@link FlinkSessionJob}. */
public class FlinkSessionJobReconciler implements Reconciler<FlinkSessionJob> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSessionJobReconciler.class);

    private final FlinkConfigManager configManager;
    private final KubernetesClient kubernetesClient;
    private final FlinkService flinkService;

    public FlinkSessionJobReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
        this.configManager = configManager;
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
                OperatorUtils.getSecondaryResource(
                        flinkSessionJob, context, configManager.getOperatorConfiguration());

        // if session cluster is not ready, we can't do reconcile for the job.
        if (!helper.sessionClusterReady(flinkDepOptional)) {
            return;
        }

        Configuration deployedConfig =
                configManager.getSessionJobConfig(flinkDepOptional.get(), flinkSessionJob);

        if (lastReconciledSpec == null) {
            submitAndInitStatus(
                    flinkSessionJob,
                    deployedConfig,
                    Optional.ofNullable(
                                    flinkSessionJob.getSpec().getJob().getInitialSavepointPath())
                            .orElse(null));
            ReconciliationUtils.updateForSpecReconciliationSuccess(
                    flinkSessionJob, JobState.RUNNING);
            return;
        }

        if (!deployedConfig.getBoolean(
                        KubernetesOperatorConfigOptions.JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT)
                && helper.savepointInProgress()) {
            LOG.info("Delaying job reconciliation until pending savepoint is completed");
            return;
        }

        boolean specChanged = helper.specChanged(lastReconciledSpec);

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
                stateAfterReconcile = suspendJob(flinkSessionJob, upgradeMode, deployedConfig);
            }
            if (currentJobState == JobState.SUSPENDED && desiredJobState == JobState.RUNNING) {
                if (upgradeMode == UpgradeMode.STATELESS) {
                    submitAndInitStatus(flinkSessionJob, deployedConfig, null);
                } else if (upgradeMode == UpgradeMode.LAST_STATE
                        || upgradeMode == UpgradeMode.SAVEPOINT) {
                    restoreFromLastSavepoint(flinkSessionJob, deployedConfig);
                }
                stateAfterReconcile = JobState.RUNNING;
            }
            ReconciliationUtils.updateForSpecReconciliationSuccess(
                    flinkSessionJob, stateAfterReconcile);
        } else if (helper.shouldTriggerSavepoint() && helper.isJobRunning(flinkDepOptional.get())) {
            triggerSavepoint(flinkSessionJob, deployedConfig);
            ReconciliationUtils.updateSavepointReconciliationSuccess(flinkSessionJob);
        }
    }

    @Override
    public DeleteControl cleanup(FlinkSessionJob sessionJob, Context context) {
        Optional<FlinkDeployment> flinkDepOptional =
                OperatorUtils.getSecondaryResource(
                        sessionJob, context, configManager.getOperatorConfiguration());

        if (flinkDepOptional.isPresent()) {
            String jobID = sessionJob.getStatus().getJobStatus().getJobId();
            if (jobID != null) {
                try {
                    flinkService.cancelSessionJob(
                            JobID.fromHexString(jobID),
                            UpgradeMode.STATELESS,
                            configManager.getSessionJobConfig(flinkDepOptional.get(), sessionJob));
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
                                .state(org.apache.flink.api.common.JobStatus.RECONCILING.name())
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
                location -> {
                    Savepoint sp = Savepoint.of(location);
                    jobStatus.getSavepointInfo().setLastSavepoint(sp);
                    jobStatus.getSavepointInfo().addSavepointToHistory(sp);
                });
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
