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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_JOB_RESTART_FAILED;

/**
 * Reconciler responsible for handling the job lifecycle according to the desired and current
 * states.
 */
public abstract class AbstractJobReconciler<
                CR extends AbstractFlinkResource<SPEC, STATUS>,
                SPEC extends AbstractFlinkSpec,
                STATUS extends CommonStatus<SPEC>>
        extends AbstractFlinkResourceReconciler<CR, SPEC, STATUS> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJobReconciler.class);

    public AbstractJobReconciler(
            KubernetesClient kubernetesClient,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder,
            StatusRecorder<CR, STATUS> statusRecorder) {
        super(kubernetesClient, configManager, eventRecorder, statusRecorder);
    }

    @Override
    public boolean readyToReconcile(CR resource, Context<?> context, Configuration deployConfig) {
        if (shouldWaitForPendingSavepoint(
                resource.getStatus().getJobStatus(),
                getDeployConfig(resource.getMetadata(), resource.getSpec(), context))) {
            LOG.info("Delaying job reconciliation until pending savepoint is completed.");
            return false;
        }
        return true;
    }

    private boolean shouldWaitForPendingSavepoint(JobStatus jobStatus, Configuration conf) {
        return !conf.getBoolean(
                        KubernetesOperatorConfigOptions.JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT)
                && SavepointUtils.savepointInProgress(jobStatus);
    }

    @Override
    protected void reconcileSpecChange(
            CR resource,
            Context<?> ctx,
            Configuration observeConfig,
            Configuration deployConfig,
            DiffType diffType)
            throws Exception {

        STATUS status = resource.getStatus();
        var reconciliationStatus = status.getReconciliationStatus();
        SPEC lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();
        SPEC currentDeploySpec = resource.getSpec();

        if (diffType == DiffType.SCALE) {
            boolean scaled =
                    getFlinkService(resource, ctx)
                            .scale(
                                    resource.getMetadata(),
                                    resource.getSpec().getJob(),
                                    deployConfig);
            if (scaled) {
                LOG.info("Reactive scaling succeeded");
                ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConfig);
                return;
            }
        }

        JobState currentJobState = lastReconciledSpec.getJob().getState();
        JobState desiredJobState = currentDeploySpec.getJob().getState();
        if (currentJobState == JobState.RUNNING) {
            if (desiredJobState == JobState.RUNNING) {
                LOG.info("Upgrading/Restarting running job, suspending first...");
            }
            Optional<UpgradeMode> availableUpgradeMode =
                    getAvailableUpgradeMode(resource, deployConfig, observeConfig);
            if (availableUpgradeMode.isEmpty()) {
                return;
            }

            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Normal,
                    EventRecorder.Reason.Suspended,
                    EventRecorder.Component.JobManagerDeployment,
                    MSG_SUSPENDED);
            // We must record the upgrade mode used to the status later
            currentDeploySpec.getJob().setUpgradeMode(availableUpgradeMode.get());
            cancelJob(resource, ctx, availableUpgradeMode.get(), observeConfig);
            if (desiredJobState == JobState.RUNNING) {
                ReconciliationUtils.updateStatusBeforeDeploymentAttempt(resource, deployConfig);
            } else {
                ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConfig);
            }
        }
        if (currentJobState == JobState.SUSPENDED && desiredJobState == JobState.RUNNING) {
            // We record the target spec into an upgrading state before deploying
            ReconciliationUtils.updateStatusBeforeDeploymentAttempt(resource, deployConfig);
            statusRecorder.patchAndCacheStatus(resource);

            restoreJob(
                    resource,
                    currentDeploySpec,
                    status,
                    ctx,
                    deployConfig,
                    // We decide to enforce HA based on how job was previously suspended
                    lastReconciledSpec.getJob().getUpgradeMode() == UpgradeMode.LAST_STATE);

            ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConfig);
        }
    }

    protected Optional<UpgradeMode> getAvailableUpgradeMode(
            CR resource, Configuration deployConfig, Configuration observeConfig) {
        var status = resource.getStatus();
        var upgradeMode = resource.getSpec().getJob().getUpgradeMode();

        if (upgradeMode == UpgradeMode.STATELESS) {
            LOG.info("Stateless job, ready for upgrade");
            return Optional.of(UpgradeMode.STATELESS);
        }

        if (ReconciliationUtils.isJobInTerminalState(status)) {
            LOG.info(
                    "Job is in terminal state, ready for upgrade from observed latest checkpoint/savepoint");
            return Optional.of(UpgradeMode.SAVEPOINT);
        }

        if (ReconciliationUtils.isJobRunning(status)) {
            LOG.info("Job is in running state, ready for upgrade with {}", upgradeMode);
            var changedToLastStateWithoutHa =
                    ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                            resource, observeConfig);
            if (changedToLastStateWithoutHa) {
                LOG.info(
                        "Using savepoint upgrade mode when switching to last-state without HA previously enabled");
                return Optional.of(UpgradeMode.SAVEPOINT);
            }

            if (flinkVersionChanged(
                    ReconciliationUtils.getDeployedSpec(resource), resource.getSpec())) {
                LOG.info("Using savepoint upgrade mode when upgrading Flink version");
                return Optional.of(UpgradeMode.SAVEPOINT);
            }

            return Optional.of(upgradeMode);
        }

        return Optional.empty();
    }

    protected void restoreJob(
            CR resource,
            SPEC spec,
            STATUS status,
            Context<?> ctx,
            Configuration deployConfig,
            boolean requireHaMetadata)
            throws Exception {
        Optional<String> savepointOpt = Optional.empty();

        if (spec.getJob().getUpgradeMode() != UpgradeMode.STATELESS) {
            savepointOpt =
                    status.getJobStatus()
                            .getSavepointInfo()
                            .retrieveLastCompletedSavepoint()
                            .flatMap(s -> Optional.ofNullable(s.getLocation()));
        }

        deploy(resource, spec, status, ctx, deployConfig, savepointOpt, requireHaMetadata);
    }

    @Override
    protected void rollback(CR resource, Context<?> ctx, Configuration observeConfig)
            throws Exception {
        var reconciliationStatus = resource.getStatus().getReconciliationStatus();
        var rollbackSpec = reconciliationStatus.deserializeLastStableSpec();

        UpgradeMode upgradeMode = resource.getSpec().getJob().getUpgradeMode();

        cancelJob(
                resource,
                ctx,
                upgradeMode == UpgradeMode.STATELESS
                        ? UpgradeMode.STATELESS
                        : UpgradeMode.LAST_STATE,
                observeConfig);

        restoreJob(
                resource,
                rollbackSpec,
                resource.getStatus(),
                ctx,
                getDeployConfig(resource.getMetadata(), rollbackSpec, ctx),
                upgradeMode != UpgradeMode.STATELESS);

        reconciliationStatus.setState(ReconciliationState.ROLLED_BACK);
    }

    @Override
    public boolean reconcileOtherChanges(
            CR resource, Context<?> context, Configuration observeConfig) throws Exception {
        var jobStatus =
                org.apache.flink.api.common.JobStatus.valueOf(
                        resource.getStatus().getJobStatus().getState());
        if (jobStatus == org.apache.flink.api.common.JobStatus.FAILED
                && observeConfig.getBoolean(OPERATOR_JOB_RESTART_FAILED)) {
            LOG.info("Stopping failed Flink job...");
            cleanupAfterFailedJob(resource, context, observeConfig);
            resource.getStatus().setError(null);
            resubmitJob(resource, context, observeConfig, false);
            return true;
        } else {
            return SavepointUtils.triggerSavepointIfNeeded(
                    getFlinkService(resource, context), resource, observeConfig);
        }
    }

    protected void resubmitJob(
            CR deployment, Context<?> ctx, Configuration observeConfig, boolean requireHaMetadata)
            throws Exception {
        LOG.info("Resubmitting Flink job...");
        SPEC specToRecover = ReconciliationUtils.getDeployedSpec(deployment);
        restoreJob(
                deployment,
                specToRecover,
                deployment.getStatus(),
                ctx,
                observeConfig,
                requireHaMetadata);
    }

    /**
     * Cancel the job for the given resource using the specified upgrade mode.
     *
     * @param resource Related Flink resource.
     * @param upgradeMode Upgrade mode used during cancel.
     * @param observeConfig Observe configuration.
     * @throws Exception Error during cancellation.
     */
    protected abstract void cancelJob(
            CR resource, Context<?> ctx, UpgradeMode upgradeMode, Configuration observeConfig)
            throws Exception;

    /**
     * Removes a failed job.
     *
     * @param resource The failed job.
     * @param observeConfig Observe configuration.
     * @throws Exception Error during cancellation.
     */
    protected abstract void cleanupAfterFailedJob(
            CR resource, Context<?> ctx, Configuration observeConfig) throws Exception;
}
