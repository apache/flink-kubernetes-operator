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
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
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
import lombok.SneakyThrows;
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
            LOG.debug("Deploying application for the first time");
            deployFlinkJob(
                    deployMeta,
                    desiredJobSpec,
                    status,
                    deployConfig,
                    Optional.ofNullable(desiredJobSpec.getInitialSavepointPath()),
                    false);
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

        Configuration observeConfig = configManager.getObserveConfig(flinkApp);
        boolean specChanged = !currentDeploySpec.equals(lastReconciledSpec);
        if (specChanged) {
            if (newSpecIsAlreadyDeployed(flinkApp)) {
                return;
            }
            LOG.debug("Detected spec change, starting upgrade process.");
            JobState currentJobState = lastReconciledSpec.getJob().getState();
            JobState desiredJobState = desiredJobSpec.getState();
            JobState stateAfterReconcile = currentJobState;
            if (currentJobState == JobState.RUNNING) {
                if (desiredJobState == JobState.RUNNING) {
                    LOG.info("Upgrading/Restarting running job, suspending first...");
                }
                Optional<UpgradeMode> availableUpgradeMode =
                        getAvailableUpgradeMode(flinkApp, deployConfig);
                if (availableUpgradeMode.isEmpty()) {
                    return;
                }
                // We must record the upgrade mode used to the status later
                desiredJobSpec.setUpgradeMode(availableUpgradeMode.get());
                flinkService.cancelJob(flinkApp, availableUpgradeMode.get());
                stateAfterReconcile = JobState.SUSPENDED;
            }
            if (currentJobState == JobState.SUSPENDED && desiredJobState == JobState.RUNNING) {
                restoreJob(
                        deployMeta,
                        desiredJobSpec,
                        status,
                        deployConfig,
                        // We decide to enforce HA based on how job was previously suspended
                        lastReconciledSpec.getJob().getUpgradeMode() == UpgradeMode.LAST_STATE);
                stateAfterReconcile = JobState.RUNNING;
            }
            ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, stateAfterReconcile);
            IngressUtils.updateIngressRules(
                    deployMeta, currentDeploySpec, deployConfig, kubernetesClient);
        } else if (ReconciliationUtils.shouldRollBack(
                flinkService, reconciliationStatus, observeConfig)) {
            rollbackApplication(flinkApp);
        } else if (ReconciliationUtils.shouldRecoverDeployment(observeConfig, flinkApp)) {
            recoverJmDeployment(flinkApp, observeConfig);
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

        flinkService.cancelJob(
                flinkApp,
                upgradeMode == UpgradeMode.STATELESS
                        ? UpgradeMode.STATELESS
                        : UpgradeMode.LAST_STATE);

        restoreJob(
                flinkApp.getMetadata(),
                rollbackSpec.getJob(),
                flinkApp.getStatus(),
                rollbackConfig,
                upgradeMode != UpgradeMode.STATELESS);

        reconciliationStatus.setState(ReconciliationState.ROLLED_BACK);
        IngressUtils.updateIngressRules(
                flinkApp.getMetadata(), rollbackSpec, rollbackConfig, kubernetesClient);
    }

    private void recoverJmDeployment(FlinkDeployment deployment, Configuration observeConfig)
            throws Exception {
        LOG.info("Missing Flink Cluster deployment, trying to recover...");
        FlinkDeploymentSpec specToRecover = ReconciliationUtils.getDeployedSpec(deployment);
        restoreJob(
                deployment.getMetadata(),
                specToRecover.getJob(),
                deployment.getStatus(),
                observeConfig,
                true);
    }

    private Optional<UpgradeMode> getAvailableUpgradeMode(
            FlinkDeployment deployment, Configuration deployConfig) {
        var status = deployment.getStatus();
        var upgradeMode = deployment.getSpec().getJob().getUpgradeMode();
        var changedToLastStateWithoutHa =
                ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                        deployment, configManager);

        if (upgradeMode == UpgradeMode.STATELESS) {
            LOG.info("Stateless job, ready for upgrade");
            return Optional.of(upgradeMode);
        }

        if (ReconciliationUtils.isJobInTerminalState(status)) {
            LOG.info(
                    "Job is in terminal state, ready for upgrade from observed latest checkpoint/savepoint");
            return Optional.of(UpgradeMode.SAVEPOINT);
        }

        if (ReconciliationUtils.isJobRunning(status)) {
            LOG.info("Job is in running state, ready for upgrade with {}", upgradeMode);
            if (changedToLastStateWithoutHa) {
                LOG.info(
                        "Using savepoint upgrade mode when switching to last-state without HA previously enabled");
                return Optional.of(UpgradeMode.SAVEPOINT);
            } else {
                return Optional.of(upgradeMode);
            }
        }

        if (FlinkUtils.isKubernetesHAActivated(deployConfig)
                && FlinkUtils.isKubernetesHAActivated(configManager.getObserveConfig(deployment))
                && flinkService.isHaMetadataAvailable(deployConfig)) {
            LOG.info(
                    "Job is not running but HA metadata is available for last state restore, ready for upgrade");
            return Optional.of(UpgradeMode.LAST_STATE);
        }

        if (status.getJobManagerDeploymentStatus() == JobManagerDeploymentStatus.MISSING
                || status.getJobManagerDeploymentStatus() == JobManagerDeploymentStatus.ERROR) {
            throw new DeploymentFailedException(
                    "JobManager deployment is missing and HA data is not available to make stateful upgrades. "
                            + "It is possible that the job has finished or terminally failed, or the configmaps have been deleted. "
                            + "Manual restore required.",
                    "UpgradeFailed");
        } else {
            LOG.info(
                    "Job is not running yet and HA metadata is not available, waiting for upgradeable state");
            return Optional.empty();
        }
    }

    @VisibleForTesting
    protected void deployFlinkJob(
            ObjectMeta meta,
            JobSpec jobSpec,
            FlinkDeploymentStatus status,
            Configuration deployConfig,
            Optional<String> savepoint,
            boolean requireHaMetadata)
            throws Exception {

        if (savepoint.isPresent()) {
            deployConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, savepoint.get());
        } else {
            deployConfig.removeConfig(SavepointConfigOptions.SAVEPOINT_PATH);
        }

        setRandomJobResultStorePath(deployConfig);

        if (status.getJobManagerDeploymentStatus() != JobManagerDeploymentStatus.MISSING) {
            if (!ReconciliationUtils.isJobInTerminalState(status)) {
                LOG.error("Invalid status for deployment: {}", status);
                throw new RuntimeException("This indicates a bug...");
            }
            LOG.info("Deleting deployment with terminated application before new deployment");
            flinkService.deleteClusterDeployment(meta, status, true);
            FlinkUtils.waitForClusterShutdown(
                    kubernetesClient,
                    deployConfig,
                    configManager
                            .getOperatorConfiguration()
                            .getFlinkShutdownClusterTimeout()
                            .toSeconds());
        }
        flinkService.submitApplicationCluster(jobSpec, deployConfig, requireHaMetadata);
        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
    }

    private void restoreJob(
            ObjectMeta meta,
            JobSpec jobSpec,
            FlinkDeploymentStatus status,
            Configuration deployConfig,
            boolean requireHaMetadata)
            throws Exception {
        Optional<String> savepointOpt = Optional.empty();

        if (jobSpec.getUpgradeMode() != UpgradeMode.STATELESS) {
            savepointOpt =
                    Optional.ofNullable(status.getJobStatus().getSavepointInfo().getLastSavepoint())
                            .flatMap(s -> Optional.ofNullable(s.getLocation()));
        }

        deployFlinkJob(meta, jobSpec, status, deployConfig, savepointOpt, requireHaMetadata);
    }

    // Workaround for https://issues.apache.org/jira/browse/FLINK-27569
    private static void setRandomJobResultStorePath(Configuration effectiveConfig) {
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

    @Override
    @SneakyThrows
    protected void shutdown(FlinkDeployment flinkApp) {
        flinkService.cancelJob(flinkApp, UpgradeMode.STATELESS);
    }

    private void triggerSavepoint(FlinkDeployment deployment) throws Exception {
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                configManager.getObserveConfig(deployment));
    }
}
