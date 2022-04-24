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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.flink.kubernetes.operator.observer.deployment.AbstractDeploymentObserver.JOB_STATE_UNKNOWN;

/**
 * Reconciler responsible for handling the job lifecycle according to the desired and current
 * states.
 */
public class ApplicationReconciler extends AbstractDeploymentReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationReconciler.class);

    public ApplicationReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkOperatorConfiguration operatorConfiguration,
            Configuration defaultConfig) {
        super(kubernetesClient, flinkService, operatorConfiguration, defaultConfig);
    }

    @Override
    public void reconcile(FlinkDeployment flinkApp, Context context) throws Exception {
        ObjectMeta deployMeta = flinkApp.getMetadata();
        Configuration effectiveConfig = FlinkUtils.getEffectiveConfig(flinkApp, defaultConfig);
        FlinkDeploymentStatus status = flinkApp.getStatus();
        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                status.getReconciliationStatus();
        FlinkDeploymentSpec lastReconciledSpec =
                reconciliationStatus.deserializeLastReconciledSpec();
        FlinkDeploymentSpec currentDeploySpec = flinkApp.getSpec();

        JobSpec desiredJobSpec = currentDeploySpec.getJob();
        if (lastReconciledSpec == null) {
            deployFlinkJob(
                    desiredJobSpec,
                    status,
                    effectiveConfig,
                    Optional.ofNullable(desiredJobSpec.getInitialSavepointPath()));
            IngressUtils.updateIngressRules(
                    deployMeta, currentDeploySpec, effectiveConfig, kubernetesClient);
            ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, JobState.RUNNING);
            return;
        }

        if (!effectiveConfig.getBoolean(
                        KubernetesOperatorConfigOptions.JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT)
                && SavepointUtils.savepointInProgress(status.getJobStatus())) {
            LOG.info("Delaying job reconciliation until pending savepoint is completed.");
            return;
        }

        boolean specChanged = !currentDeploySpec.equals(lastReconciledSpec);
        if (specChanged) {
            if (!inUpgradeableState(flinkApp)) {
                LOG.info("Waiting for upgradeable state");
                return;
            }
            JobState currentJobState = lastReconciledSpec.getJob().getState();
            JobState desiredJobState = desiredJobSpec.getState();
            UpgradeMode upgradeMode = desiredJobSpec.getUpgradeMode();
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
                    deployFlinkJob(desiredJobSpec, status, effectiveConfig, Optional.empty());
                } else {
                    restoreFromLastSavepoint(desiredJobSpec, status, effectiveConfig);
                }
                stateAfterReconcile = JobState.RUNNING;
            }
            IngressUtils.updateIngressRules(
                    deployMeta, currentDeploySpec, effectiveConfig, kubernetesClient);
            ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, stateAfterReconcile);
        } else if (ReconciliationUtils.shouldRollBack(reconciliationStatus, effectiveConfig)) {
            rollbackApplication(flinkApp);
        } else if (SavepointUtils.shouldTriggerSavepoint(desiredJobSpec, status)
                && isJobRunning(status)) {
            triggerSavepoint(flinkApp, effectiveConfig);
            ReconciliationUtils.updateSavepointReconciliationSuccess(flinkApp);
        } else {
            LOG.info("Deployment is fully reconciled, nothing to do.");
        }
    }

    private void rollbackApplication(FlinkDeployment flinkApp) throws Exception {
        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                flinkApp.getStatus().getReconciliationStatus();

        if (initiateRollBack(flinkApp.getStatus())) {
            return;
        }

        LOG.warn("Executing rollback operation");
        FlinkDeploymentSpec rollbackSpec = reconciliationStatus.deserializeLastStableSpec();
        Configuration rollbackConfig =
                FlinkUtils.getEffectiveConfig(flinkApp.getMetadata(), rollbackSpec, defaultConfig);

        UpgradeMode upgradeMode = flinkApp.getSpec().getJob().getUpgradeMode();

        suspendJob(
                flinkApp,
                upgradeMode == UpgradeMode.STATELESS
                        ? UpgradeMode.STATELESS
                        : UpgradeMode.LAST_STATE,
                rollbackConfig);
        deployFlinkJob(
                rollbackSpec.getJob(),
                flinkApp.getStatus(),
                rollbackConfig,
                upgradeMode == UpgradeMode.STATELESS
                        ? Optional.empty()
                        : Optional.ofNullable(
                                        flinkApp.getStatus()
                                                .getJobStatus()
                                                .getSavepointInfo()
                                                .getLastSavepoint())
                                .map(Savepoint::getLocation));
        IngressUtils.updateIngressRules(
                flinkApp.getMetadata(), rollbackSpec, rollbackConfig, kubernetesClient);
        reconciliationStatus.setState(ReconciliationState.ROLLED_BACK);
    }

    private boolean inUpgradeableState(FlinkDeployment deployment) {
        if (deployment.getSpec().getJob().getUpgradeMode() != UpgradeMode.SAVEPOINT
                && !ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                        deployment, defaultConfig)) {
            // Only savepoint upgrade mode or changed from stateless/savepoint to last-state while
            // HA disabled previously need a running job
            return true;
        }

        FlinkDeploymentStatus status = deployment.getStatus();
        return status.getJobManagerDeploymentStatus() == JobManagerDeploymentStatus.MISSING
                || isJobRunning(status);
    }

    private boolean isJobRunning(FlinkDeploymentStatus status) {
        JobManagerDeploymentStatus deploymentStatus = status.getJobManagerDeploymentStatus();
        return deploymentStatus == JobManagerDeploymentStatus.READY
                && org.apache.flink.api.common.JobStatus.RUNNING
                        .name()
                        .equals(status.getJobStatus().getState());
    }

    private void deployFlinkJob(
            JobSpec jobSpec,
            FlinkDeploymentStatus status,
            Configuration effectiveConfig,
            Optional<String> savepoint)
            throws Exception {
        if (savepoint.isPresent()) {
            effectiveConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, savepoint.get());
        } else {
            effectiveConfig.removeConfig(SavepointConfigOptions.SAVEPOINT_PATH);
        }
        flinkService.submitApplicationCluster(jobSpec, effectiveConfig);
        status.getJobStatus().setState(JOB_STATE_UNKNOWN);
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
    }

    private void restoreFromLastSavepoint(
            JobSpec jobSpec, FlinkDeploymentStatus status, Configuration effectiveConfig)
            throws Exception {
        JobStatus jobStatus = status.getJobStatus();
        Optional<String> savepointOpt =
                Optional.ofNullable(jobStatus.getSavepointInfo().getLastSavepoint())
                        .flatMap(s -> Optional.ofNullable(s.getLocation()));

        deployFlinkJob(jobSpec, status, effectiveConfig, savepointOpt);
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
        if (ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                flinkApp, defaultConfig)) {
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
        if (isJobRunning(flinkApp.getStatus())) {
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

        FlinkUtils.deleteCluster(
                flinkApp.getMetadata(),
                kubernetesClient,
                true,
                operatorConfiguration.getFlinkShutdownClusterTimeout().toSeconds());
    }

    private void triggerSavepoint(FlinkDeployment deployment, Configuration effectiveConfig)
            throws Exception {
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                effectiveConfig);
    }
}
