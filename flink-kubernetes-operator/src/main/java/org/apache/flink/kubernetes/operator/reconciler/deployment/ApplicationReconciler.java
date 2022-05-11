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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
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
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

/**
 * Reconciler responsible for handling the job lifecycle according to the desired and current
 * states.
 */
public class ApplicationReconciler extends AbstractDeploymentReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationReconciler.class);

    public ApplicationReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager) {
        super(kubernetesClient, flinkService, configManager);
    }

    @Override
    public void reconcile(FlinkDeployment flinkApp, Context context) throws Exception {
        ObjectMeta deployMeta = flinkApp.getMetadata();
        FlinkDeploymentStatus status = flinkApp.getStatus();
        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                status.getReconciliationStatus();
        FlinkDeploymentSpec lastReconciledSpec =
                reconciliationStatus.deserializeLastReconciledSpec();
        FlinkDeploymentSpec currentDeploySpec = flinkApp.getSpec();

        JobSpec desiredJobSpec = currentDeploySpec.getJob();
        Configuration deployConfig = configManager.getDeployConfig(deployMeta, currentDeploySpec);
        if (lastReconciledSpec == null) {
            deployFlinkJob(
                    deployMeta,
                    desiredJobSpec,
                    status,
                    deployConfig,
                    Optional.ofNullable(desiredJobSpec.getInitialSavepointPath()));
            IngressUtils.updateIngressRules(
                    deployMeta, currentDeploySpec, deployConfig, kubernetesClient);
            ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, JobState.RUNNING);
            return;
        }

        if (!deployConfig.getBoolean(
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
                stateAfterReconcile = suspendJob(flinkApp, upgradeMode);
            }
            if (currentJobState == JobState.SUSPENDED && desiredJobState == JobState.RUNNING) {
                if (upgradeMode == UpgradeMode.STATELESS) {
                    deployFlinkJob(
                            deployMeta, desiredJobSpec, status, deployConfig, Optional.empty());
                } else {
                    restoreFromLastSavepoint(deployMeta, desiredJobSpec, status, deployConfig);
                }
                stateAfterReconcile = JobState.RUNNING;
            }
            ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, stateAfterReconcile);
            IngressUtils.updateIngressRules(
                    deployMeta, currentDeploySpec, deployConfig, kubernetesClient);
        } else if (ReconciliationUtils.shouldRollBack(reconciliationStatus, deployConfig)) {
            rollbackApplication(flinkApp);
        } else if (ReconciliationUtils.deploymentRecoveryEnabled(deployConfig)
                && ReconciliationUtils.jmMissingForRunningDeployment(status)) {
            recoverJmDeployment(flinkApp);
        } else if (SavepointUtils.shouldTriggerSavepoint(desiredJobSpec, status)
                && ReconciliationUtils.isJobRunning(status)) {
            triggerSavepoint(flinkApp);
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
                configManager.getDeployConfig(flinkApp.getMetadata(), rollbackSpec);
        UpgradeMode upgradeMode = flinkApp.getSpec().getJob().getUpgradeMode();

        suspendJob(
                flinkApp,
                upgradeMode == UpgradeMode.STATELESS
                        ? UpgradeMode.STATELESS
                        : UpgradeMode.LAST_STATE);
        deployFlinkJob(
                flinkApp.getMetadata(),
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
        reconciliationStatus.setState(ReconciliationState.ROLLED_BACK);
        IngressUtils.updateIngressRules(
                flinkApp.getMetadata(), rollbackSpec, rollbackConfig, kubernetesClient);
    }

    private void recoverJmDeployment(FlinkDeployment deployment) throws Exception {
        LOG.info("Missing Flink Cluster deployment, trying to recover...");
        FlinkDeploymentSpec specToRecover = ReconciliationUtils.getDeployedSpec(deployment);
        restoreFromLastSavepoint(
                deployment.getMetadata(),
                specToRecover.getJob(),
                deployment.getStatus(),
                configManager.getDeployConfig(deployment.getMetadata(), specToRecover));
    }

    private boolean inUpgradeableState(FlinkDeployment deployment) {
        if (deployment.getSpec().getJob().getUpgradeMode() != UpgradeMode.SAVEPOINT
                && !ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                        deployment, configManager)) {
            // Only savepoint upgrade mode or changed from stateless/savepoint to last-state while
            // HA disabled previously need a running job
            return true;
        }

        FlinkDeploymentStatus status = deployment.getStatus();

        if (ReconciliationUtils.jmMissingOrErrorForRunningDep(status)) {
            // JobManager is missing for savepoint upgrade, we cannot roll back
            throw new DeploymentFailedException(
                    "Cannot perform savepoint upgrade on missing/failed JobManager deployment",
                    DeploymentFailedException.COMPONENT_JOBMANAGER,
                    "Error",
                    "UpgradeFailed");
        }

        return ReconciliationUtils.isJobInTerminalState(status)
                || ReconciliationUtils.isJobRunning(status);
    }

    @VisibleForTesting
    protected void deployFlinkJob(
            ObjectMeta meta,
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

        setRandomJobResultStorePath(effectiveConfig);

        if (status.getJobManagerDeploymentStatus() != JobManagerDeploymentStatus.MISSING) {
            if (!ReconciliationUtils.isJobInTerminalState(status)) {
                throw new RuntimeException("This indicates a bug...");
            }
            LOG.info("Deleting deployment with terminated application before new deployment");
            flinkService.deleteClusterDeployment(meta, status, false);
            FlinkUtils.waitForClusterShutdown(
                    kubernetesClient,
                    effectiveConfig,
                    configManager
                            .getOperatorConfiguration()
                            .getFlinkShutdownClusterTimeout()
                            .toSeconds());
        }
        flinkService.submitApplicationCluster(jobSpec, effectiveConfig);
        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
    }

    private void restoreFromLastSavepoint(
            ObjectMeta meta,
            JobSpec jobSpec,
            FlinkDeploymentStatus status,
            Configuration effectiveConfig)
            throws Exception {
        JobStatus jobStatus = status.getJobStatus();
        Optional<String> savepointOpt =
                Optional.ofNullable(jobStatus.getSavepointInfo().getLastSavepoint())
                        .flatMap(s -> Optional.ofNullable(s.getLocation()));

        deployFlinkJob(meta, jobSpec, status, effectiveConfig, savepointOpt);
    }

    // Workaround for https://issues.apache.org/jira/browse/FLINK-27569
    public static void setRandomJobResultStorePath(Configuration effectiveConfig) {
        if (effectiveConfig.contains(HighAvailabilityOptions.HA_STORAGE_PATH)) {

            if (!effectiveConfig.contains(JobResultStoreOptions.DELETE_ON_COMMIT)) {
                effectiveConfig.set(JobResultStoreOptions.DELETE_ON_COMMIT, false);
            }

            effectiveConfig.set(
                    JobResultStoreOptions.STORAGE_PATH,
                    effectiveConfig.getString(HighAvailabilityOptions.HA_STORAGE_PATH)
                            + "/job-result-store/"
                            + effectiveConfig.getString(KubernetesConfigOptions.CLUSTER_ID)
                            + "/"
                            + UUID.randomUUID());
        }
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

    private JobState suspendJob(FlinkDeployment deployment, UpgradeMode upgradeMode)
            throws Exception {

        // Always trigger a savepoint when upgrade mode changes from stateless/savepoint to
        // last-state and HA is disabled previously. This is a safeguard to ensure the state is
        // never lost.
        if (ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                deployment, configManager)) {
            flinkService.cancelJob(deployment, UpgradeMode.SAVEPOINT);
        } else if (upgradeMode == UpgradeMode.STATELESS) {
            shutdown(deployment);
        } else {
            flinkService.cancelJob(deployment, upgradeMode);
        }

        return JobState.SUSPENDED;
    }

    @Override
    protected void shutdown(FlinkDeployment flinkApp) {
        if (ReconciliationUtils.isJobRunning(flinkApp.getStatus())) {
            LOG.info("Job is running, attempting graceful shutdown.");
            try {
                flinkService.cancelJob(flinkApp, UpgradeMode.STATELESS);
            } catch (Exception e) {
                LOG.error("Could not shut down cluster gracefully, deleting...", e);
            }
        }

        flinkService.deleteClusterDeployment(flinkApp.getMetadata(), flinkApp.getStatus(), true);
    }

    private void triggerSavepoint(FlinkDeployment deployment) throws Exception {
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                configManager.getObserveConfig(deployment));
    }
}
