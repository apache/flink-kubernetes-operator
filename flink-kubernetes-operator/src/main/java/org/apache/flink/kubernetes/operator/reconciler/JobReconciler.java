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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.flink.kubernetes.operator.observer.BaseObserver.JOB_STATE_UNKNOWN;

/**
 * Reconciler responsible for handling the job lifecycle according to the desired and current
 * states.
 */
public class JobReconciler extends BaseReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(JobReconciler.class);

    public JobReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkOperatorConfiguration operatorConfiguration) {
        super(kubernetesClient, flinkService, operatorConfiguration);
    }

    @Override
    public void reconcile(FlinkDeployment flinkApp, Context context, Configuration effectiveConfig)
            throws Exception {

        FlinkDeploymentSpec lastReconciledSpec =
                flinkApp.getStatus().getReconciliationStatus().getLastReconciledSpec();
        JobSpec jobSpec = flinkApp.getSpec().getJob();
        if (lastReconciledSpec == null) {
            deployFlinkJob(
                    flinkApp,
                    effectiveConfig,
                    Optional.ofNullable(jobSpec.getInitialSavepointPath()));
            IngressUtils.updateIngressRules(flinkApp, effectiveConfig, kubernetesClient);
            ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, JobState.RUNNING);
            return;
        }

        if (SavepointUtils.savepointInProgress(flinkApp)) {
            LOG.info("Delaying job reconciliation until pending savepoint is completed");
            return;
        }

        boolean specChanged = !flinkApp.getSpec().equals(lastReconciledSpec);
        if (specChanged) {
            if (!inUpgradeableState(flinkApp)) {
                LOG.info("Waiting for upgradeable state");
                return;
            }
            JobState currentJobState = lastReconciledSpec.getJob().getState();
            JobState desiredJobState = jobSpec.getState();
            UpgradeMode upgradeMode = jobSpec.getUpgradeMode();
            JobState stateAfterReconcile = currentJobState;
            if (currentJobState == JobState.RUNNING) {
                if (desiredJobState == JobState.RUNNING) {
                    LOG.info("Upgrading/Restarting running job, suspending first...");
                }
                printCancelLogs(upgradeMode);
                stateAfterReconcile = suspendJob(flinkApp, upgradeMode, effectiveConfig);
            }
            if (currentJobState == JobState.SUSPENDED && desiredJobState == JobState.RUNNING) {
                if (upgradeMode == UpgradeMode.STATELESS) {
                    deployFlinkJob(flinkApp, effectiveConfig, Optional.empty());
                } else if (upgradeMode == UpgradeMode.LAST_STATE
                        || upgradeMode == UpgradeMode.SAVEPOINT) {
                    restoreFromLastSavepoint(flinkApp, effectiveConfig);
                }
                stateAfterReconcile = JobState.RUNNING;
            }
            IngressUtils.updateIngressRules(flinkApp, effectiveConfig, kubernetesClient);
            ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, stateAfterReconcile);
        } else if (SavepointUtils.shouldTriggerSavepoint(flinkApp) && isJobRunning(flinkApp)) {
            triggerSavepoint(flinkApp, effectiveConfig);
            ReconciliationUtils.updateSavepointReconciliationSuccess(flinkApp);
        }
    }

    private boolean inUpgradeableState(FlinkDeployment deployment) {
        if (deployment.getSpec().getJob().getUpgradeMode() != UpgradeMode.SAVEPOINT
                && !ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                        deployment)) {
            // Only savepoint upgrade mode or changed from stateless/savepoint to last-state while
            // HA disabled previously need a running job
            return true;
        }
        return deployment.getStatus().getJobManagerDeploymentStatus()
                        == JobManagerDeploymentStatus.MISSING
                || isJobRunning(deployment);
    }

    private boolean isJobRunning(FlinkDeployment deployment) {
        FlinkDeploymentStatus status = deployment.getStatus();
        JobManagerDeploymentStatus deploymentStatus = status.getJobManagerDeploymentStatus();
        return deploymentStatus == JobManagerDeploymentStatus.READY
                && org.apache.flink.api.common.JobStatus.RUNNING
                        .name()
                        .equals(status.getJobStatus().getState());
    }

    private void deployFlinkJob(
            FlinkDeployment flinkApp, Configuration effectiveConfig, Optional<String> savepoint)
            throws Exception {
        if (savepoint.isPresent()) {
            effectiveConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, savepoint.get());
        } else {
            effectiveConfig.removeConfig(SavepointConfigOptions.SAVEPOINT_PATH);
        }
        flinkService.submitApplicationCluster(flinkApp, effectiveConfig);
        flinkApp.getStatus().getJobStatus().setState(JOB_STATE_UNKNOWN);
        flinkApp.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
    }

    private void restoreFromLastSavepoint(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        JobStatus jobStatus = flinkApp.getStatus().getJobStatus();
        Optional<String> savepointOpt =
                Optional.ofNullable(jobStatus.getSavepointInfo().getLastSavepoint())
                        .flatMap(s -> Optional.ofNullable(s.getLocation()));

        deployFlinkJob(flinkApp, effectiveConfig, savepointOpt);
    }

    private void printCancelLogs(UpgradeMode upgradeMode) {
        switch (upgradeMode) {
            case STATELESS:
                LOG.info("Cancelling job");
                break;
            case SAVEPOINT:
                LOG.info("Suspending job");
                break;
            case LAST_STATE:
                LOG.info("Cancelling job with last state retained");
                break;
            default:
                throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
        }
    }

    private Optional<String> internalSuspendJob(
            FlinkDeployment flinkApp, UpgradeMode upgradeMode, Configuration effectiveConfig)
            throws Exception {
        final String jobIdString = flinkApp.getStatus().getJobStatus().getJobId();
        // Always trigger a savepoint when upgrade mode changes from stateless/savepoint to
        // last-state and HA is disabled previously. This is a safeguard to ensure the state is
        // never lost.
        if (ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(flinkApp)) {
            return flinkService.cancelJob(
                    JobID.fromHexString(Preconditions.checkNotNull(jobIdString)),
                    UpgradeMode.SAVEPOINT,
                    effectiveConfig);
        }
        if (upgradeMode == UpgradeMode.STATELESS) {
            shutdown(flinkApp, effectiveConfig);
            return Optional.empty();
        }
        return flinkService.cancelJob(
                jobIdString != null ? JobID.fromHexString(jobIdString) : null,
                upgradeMode,
                effectiveConfig);
    }

    private JobState suspendJob(
            FlinkDeployment flinkApp, UpgradeMode upgradeMode, Configuration effectiveConfig)
            throws Exception {
        final Optional<String> savepointOpt =
                internalSuspendJob(flinkApp, upgradeMode, effectiveConfig);

        JobStatus jobStatus = flinkApp.getStatus().getJobStatus();
        JobState stateAfterReconcile = JobState.SUSPENDED;
        jobStatus.setState(stateAfterReconcile.name());
        flinkApp.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        savepointOpt.ifPresent(
                location -> jobStatus.getSavepointInfo().setLastSavepoint(Savepoint.of(location)));
        return stateAfterReconcile;
    }

    @Override
    protected void shutdown(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        if (isJobRunning(flinkApp)) {
            LOG.info("Job is running, attempting graceful shutdown.");
            try {
                flinkService.cancelJob(
                        JobID.fromHexString(flinkApp.getStatus().getJobStatus().getJobId()),
                        UpgradeMode.STATELESS,
                        effectiveConfig);
                return;
            } catch (Exception e) {
                LOG.error("Could not shut down cluster gracefully, deleting...", e);
            }
        }

        FlinkUtils.deleteCluster(flinkApp, kubernetesClient, true);
    }

    private void triggerSavepoint(FlinkDeployment deployment, Configuration effectiveConfig)
            throws Exception {
        flinkService.triggerSavepoint(deployment, effectiveConfig);
    }
}
