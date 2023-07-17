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
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.diff.DiffResult;
import org.apache.flink.kubernetes.operator.reconciler.diff.ReflectiveDiffBuilder;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for all Flink resource reconcilers. It contains the general flow of reconciling Flink
 * related resources including initial deployments, upgrades, rollbacks etc.
 */
public abstract class AbstractFlinkResourceReconciler<
                CR extends AbstractFlinkResource<SPEC, STATUS>,
                SPEC extends AbstractFlinkSpec,
                STATUS extends CommonStatus<SPEC>>
        implements Reconciler<CR> {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractFlinkResourceReconciler.class);

    protected final EventRecorder eventRecorder;
    protected final StatusRecorder<CR, STATUS> statusRecorder;
    protected final KubernetesClient kubernetesClient;
    protected final JobAutoScaler resourceScaler;

    public static final String MSG_SUSPENDED = "Suspending existing deployment.";
    public static final String MSG_SPEC_CHANGED =
            "%s change(s) detected (%s), starting reconciliation.";
    public static final String MSG_ROLLBACK = "Rolling back failed deployment.";
    public static final String MSG_SUBMIT = "Starting deployment";

    protected Clock clock = Clock.systemDefaultZone();

    public AbstractFlinkResourceReconciler(
            KubernetesClient kubernetesClient,
            EventRecorder eventRecorder,
            StatusRecorder<CR, STATUS> statusRecorder,
            JobAutoScalerFactory autoscalerFactory) {
        this.kubernetesClient = kubernetesClient;
        this.eventRecorder = eventRecorder;
        this.statusRecorder = statusRecorder;
        this.resourceScaler = autoscalerFactory.create(kubernetesClient, eventRecorder);
    }

    @Override
    public void reconcile(FlinkResourceContext<CR> ctx) throws Exception {
        var cr = ctx.getResource();
        var status = cr.getStatus();
        var reconciliationStatus = cr.getStatus().getReconciliationStatus();

        // If the resource is not ready for reconciliation we simply return
        if (!readyToReconcile(ctx)) {
            LOG.info("Not ready for reconciliation yet...");
            return;
        }

        // If this is the first deployment for the resource we simply submit the job and return.
        // No further logic is required at this point.
        if (reconciliationStatus.isBeforeFirstDeployment()) {
            LOG.info("Deploying for the first time");
            var spec = cr.getSpec();
            var deployConfig = ctx.getDeployConfig(spec);
            updateStatusBeforeFirstDeployment(cr, spec, deployConfig, status);
            deploy(
                    ctx,
                    spec,
                    deployConfig,
                    Optional.ofNullable(spec.getJob()).map(JobSpec::getInitialSavepointPath),
                    false);

            ReconciliationUtils.updateStatusForDeployedSpec(cr, deployConfig, clock);
            return;
        }

        SPEC lastReconciledSpec =
                cr.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        SPEC currentDeploySpec = cr.getSpec();
        applyAutoscalerParallelismOverrides(
                resourceScaler.getParallelismOverrides(ctx), currentDeploySpec);

        var specDiff =
                new ReflectiveDiffBuilder<>(
                                ctx.getDeploymentMode(), lastReconciledSpec, currentDeploySpec)
                        .build();
        var diffType = specDiff.getType();

        boolean specChanged =
                DiffType.IGNORE != diffType
                        || reconciliationStatus.getState() == ReconciliationState.UPGRADING;

        if (reconciliationStatus.getState() == ReconciliationState.ROLLING_BACK) {
            specChanged = prepareCrForRollback(ctx, currentDeploySpec, lastReconciledSpec);
        }

        var observeConfig = ctx.getObserveConfig();
        if (specChanged) {
            var deployConfig = ctx.getDeployConfig(cr.getSpec());
            if (checkNewSpecAlreadyDeployed(cr, deployConfig)) {
                return;
            }
            triggerSpecChangeEvent(cr, specDiff);

            // Try scaling if this is not an upgrade change
            boolean scaled = diffType != DiffType.UPGRADE && scale(ctx, deployConfig);

            // Reconcile spec change unless scaling was enough
            if (scaled || reconcileSpecChange(ctx, deployConfig)) {
                // If we executed a scale or spec upgrade action we return, otherwise we
                // continue to reconcile other changes
                return;
            }
        } else {
            ReconciliationUtils.updateReconciliationMetadata(cr);
        }

        if (shouldRollBack(ctx, observeConfig)) {
            // Rollbacks are executed in two steps, we initiate it first then return
            if (initiateRollBack(status)) {
                return;
            }
            LOG.warn(MSG_ROLLBACK);
            eventRecorder.triggerEvent(
                    cr,
                    EventRecorder.Type.Normal,
                    EventRecorder.Reason.Rollback,
                    EventRecorder.Component.JobManagerDeployment,
                    MSG_ROLLBACK);
        } else if (!reconcileOtherChanges(ctx)) {
            if (resourceScaler.scale(ctx)) {
                LOG.info(
                        "Rescheduling new reconciliation immediately to execute scaling operation.");
                status.setImmediateReconciliationNeeded(true);
            } else {
                LOG.info("Resource fully reconciled, nothing to do...");
            }
        }
    }

    private void triggerSpecChangeEvent(CR cr, DiffResult<SPEC> specDiff) {
        eventRecorder.triggerEventOnce(
                cr,
                EventRecorder.Type.Normal,
                EventRecorder.Reason.SpecChanged,
                EventRecorder.Component.JobManagerDeployment,
                String.format(MSG_SPEC_CHANGED, specDiff.getType(), specDiff),
                "SpecChange: " + cr.getMetadata().getGeneration());
    }

    /**
     * Update the status before the first deployment. We have to record the upgrade mode based on
     * the initial savepoint path provided, and record the to-be-deployed spec in the status.
     *
     * @param cr Related flink resource
     * @param spec Spec to be deployed
     * @param deployConfig Deploy configuration
     * @param status Resource status
     */
    private void updateStatusBeforeFirstDeployment(
            CR cr, SPEC spec, Configuration deployConfig, STATUS status) {
        if (spec.getJob() != null) {
            var initialUpgradeMode = UpgradeMode.STATELESS;
            var initialSp = spec.getJob().getInitialSavepointPath();

            if (initialSp != null) {
                status.getJobStatus()
                        .getSavepointInfo()
                        .setLastSavepoint(Savepoint.of(initialSp, SavepointTriggerType.UNKNOWN));
                initialUpgradeMode = UpgradeMode.SAVEPOINT;
            }

            spec.getJob().setUpgradeMode(initialUpgradeMode);
        }
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(cr, deployConfig, clock);
        // Before we try to submit the job we record the current spec in the status so we can
        // handle subsequent deployment and status update errors
        statusRecorder.patchAndCacheStatus(cr);
    }

    /**
     * Check whether the given Flink resource is ready to be reconciled or we are still waiting for
     * any pending operation or condition first.
     *
     * @param ctx Reconciliation context.
     * @return True if the resource is ready to be reconciled.
     */
    protected abstract boolean readyToReconcile(FlinkResourceContext<CR> ctx);

    /**
     * Reconcile spec upgrade on the currently deployed/suspended Flink resource and update the
     * status accordingly.
     *
     * @param ctx Reconciliation context.
     * @param deployConfig Deployment configuration.
     * @throws Exception Error during spec upgrade.
     * @return True if spec change reconciliation was executed
     */
    protected abstract boolean reconcileSpecChange(
            FlinkResourceContext<CR> ctx, Configuration deployConfig) throws Exception;

    /**
     * Reconcile any other changes required for this resource that are specific to the reconciler
     * implementation.
     *
     * @param ctx Reconciliation context.
     * @return True if any further reconciliation action was taken.
     * @throws Exception Error during reconciliation.
     */
    protected abstract boolean reconcileOtherChanges(FlinkResourceContext<CR> ctx) throws Exception;

    @Override
    public DeleteControl cleanup(FlinkResourceContext<CR> ctx) {
        resourceScaler.cleanup(ctx);
        return cleanupInternal(ctx);
    }

    /**
     * Deploys the target resource spec to Kubernetes.
     *
     * @param ctx Reconciliation context.
     * @param spec Spec that should be deployed to Kubernetes.
     * @param deployConfig Flink conf for the deployment.
     * @param savepoint Optional savepoint path for applications and session jobs.
     * @param requireHaMetadata Flag used by application deployments to validate HA metadata
     * @throws Exception Error during deployment.
     */
    @VisibleForTesting
    public abstract void deploy(
            FlinkResourceContext<CR> ctx,
            SPEC spec,
            Configuration deployConfig,
            Optional<String> savepoint,
            boolean requireHaMetadata)
            throws Exception;

    /**
     * Shut down and clean up all Flink job/cluster resources.
     *
     * @param ctx Current context.
     * @return DeleteControl object.
     */
    protected abstract DeleteControl cleanupInternal(FlinkResourceContext<CR> ctx);

    /**
     * Checks whether the desired spec already matches the currently deployed spec. If they match
     * the resource status is updated to reflect successful reconciliation.
     *
     * @param resource Resource being reconciled.
     * @param deployConf Deploy configuration for the Flink resource.
     * @return True if desired spec was already deployed.
     */
    private boolean checkNewSpecAlreadyDeployed(CR resource, Configuration deployConf) {
        if (resource.getStatus().getReconciliationStatus().getState()
                        == ReconciliationState.UPGRADING
                || resource.getStatus().getReconciliationStatus().getState()
                        == ReconciliationState.ROLLING_BACK) {
            return false;
        }
        AbstractFlinkSpec deployedSpec = ReconciliationUtils.getDeployedSpec(resource);
        if (resource.getSpec().equals(deployedSpec)) {
            LOG.info(
                    "The new spec matches the currently deployed last stable spec. No upgrade needed.");
            ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConf, clock);
            return true;
        }
        return false;
    }

    /**
     * If there are any parallelism overrides by the {@link JobAutoScaler} apply them to the spec.
     *
     * @param autoscalerOverrides Parallelism overrides initiated by the autoscaler
     * @param spec Current user spec
     */
    private void applyAutoscalerParallelismOverrides(
            Map<String, String> autoscalerOverrides, SPEC spec) {

        if (autoscalerOverrides.isEmpty()) {
            return;
        }

        LOG.debug("Applying autoscaler parallelism overrides: {}", autoscalerOverrides);

        var configMap = spec.getFlinkConfiguration();
        var userOverridesStr =
                configMap.getOrDefault(PipelineOptions.PARALLELISM_OVERRIDES.key(), "");
        var userOverrides =
                new HashMap<>(
                        ConfigurationUtils.<Map<String, String>>convertValue(
                                userOverridesStr, Map.class));

        autoscalerOverrides.forEach(userOverrides::put);
        configMap.put(
                PipelineOptions.PARALLELISM_OVERRIDES.key(),
                ConfigurationUtils.convertValue(userOverrides, String.class));
    }

    /**
     * Scale the cluster in-place if possible, either through reactive scaling or declarative
     * resources.
     *
     * @param ctx Resource context.
     * @param deployConfig Configuration to be deployed.
     * @return True if the scaling is successful
     * @throws Exception
     */
    private boolean scale(FlinkResourceContext<CR> ctx, Configuration deployConfig)
            throws Exception {

        var scalingResult = ctx.getFlinkService().scale(ctx, deployConfig);
        if (scalingResult == FlinkService.ScalingResult.CANNOT_SCALE) {
            return false;
        }

        ReconciliationUtils.updateAfterScaleUp(
                ctx.getResource(), deployConfig, clock, scalingResult);
        return true;
    }

    /**
     * Checks whether the currently deployed Flink resource spec should be rolled back to the stable
     * spec. This includes validating the current deployment status, config and checking if the last
     * reconciled spec did not become stable within the configured grace period.
     *
     * <p>Rollbacks are only supported to previously running resource specs with HA enabled.
     *
     * @param ctx Reconciliation context.
     * @param configuration Flink cluster configuration.
     * @return True if the resource should be rolled back.
     */
    private boolean shouldRollBack(FlinkResourceContext<CR> ctx, Configuration configuration) {

        var resource = ctx.getResource();
        var reconciliationStatus = resource.getStatus().getReconciliationStatus();
        if (reconciliationStatus.getState() == ReconciliationState.ROLLING_BACK) {
            return true;
        }

        if (!configuration.get(KubernetesOperatorConfigOptions.DEPLOYMENT_ROLLBACK_ENABLED)
                || reconciliationStatus.getState() == ReconciliationState.ROLLED_BACK
                || reconciliationStatus.isLastReconciledSpecStable()) {
            return false;
        }

        var lastStableSpec = reconciliationStatus.deserializeLastStableSpec();
        if (lastStableSpec == null) {
            // Nothing to roll back to yet
            return false;
        }

        if (lastStableSpec.getJob() != null
                && lastStableSpec.getJob().getState() == JobState.SUSPENDED) {
            // Should not roll back to suspended state
            return false;
        }

        if (flinkVersionChanged(resource.getSpec(), lastStableSpec)) {
            // Should not roll back Flink version changes
            return false;
        }

        Duration readinessTimeout =
                configuration.get(KubernetesOperatorConfigOptions.DEPLOYMENT_READINESS_TIMEOUT);
        if (!clock.instant()
                .minus(readinessTimeout)
                .isAfter(Instant.ofEpochMilli(reconciliationStatus.getReconciliationTimestamp()))) {
            return false;
        }

        if (resource.getSpec().getJob() != null
                && resource.getSpec().getJob().getUpgradeMode() == UpgradeMode.SAVEPOINT
                && FlinkUtils.jmPodNeverStarted(ctx.getJosdkContext())) {
            // HA data not available as JM never start and relying on SAVEPOINT upgrade mode
            // Safe to rollback relying on savepoint
            return true;
        }

        var haDataAvailable = ctx.getFlinkService().isHaMetadataAvailable(configuration);
        if (!haDataAvailable) {
            LOG.warn("Rollback is not possible due to missing HA metadata");
        }
        return haDataAvailable;
    }

    /**
     * Initiate rollback process by changing the {@link ReconciliationState} in the status.
     *
     * @param status Resource status.
     * @return True if a new rollback was initiated.
     */
    private boolean initiateRollBack(STATUS status) {
        var reconciliationStatus = status.getReconciliationStatus();
        if (reconciliationStatus.getState() != ReconciliationState.ROLLING_BACK) {
            LOG.warn("Preparing to roll back to last stable spec.");
            if (StringUtils.isEmpty(status.getError())) {
                status.setError(
                        "Deployment is not ready within the configured timeout, rolling back.");
            }
            reconciliationStatus.setState(ReconciliationState.ROLLING_BACK);
            return true;
        }
        return false;
    }

    private boolean prepareCrForRollback(
            FlinkResourceContext<CR> ctx, SPEC currentDeploySpec, SPEC lastReconciledSpec) {
        var cr = ctx.getResource();
        var reconciliationStatus = cr.getStatus().getReconciliationStatus();
        // Spec has changed while rolling back we should apply new spec and move to upgrading
        // state. Don't take in account changes on job.state as it could be overriden to running if
        // the current spec is not valid
        if (lastReconciledSpec.getJob() != null) {
            lastReconciledSpec.getJob().setState(currentDeploySpec.getJob().getState());
        }
        var specDiffRollingBack =
                new ReflectiveDiffBuilder<>(
                                ctx.getDeploymentMode(), lastReconciledSpec, currentDeploySpec)
                        .build();
        if (DiffType.IGNORE != specDiffRollingBack.getType()) {
            reconciliationStatus.setState(ReconciliationState.UPGRADING);
        } else {
            // Rely on the last stable spec if rolling back and no change in the spec
            cr.setSpec(cr.getStatus().getReconciliationStatus().deserializeLastStableSpec());
        }
        return true;
    }

    /**
     * Checks whether the JobManager Kubernetes Deployment recovery logic should be initiated. This
     * is triggered only if, jm deployment missing, recovery config and HA enabled. This logic is
     * only used by the Session and Application reconcilers.
     *
     * @param conf Flink cluster configuration.
     * @param deployment FlinkDeployment object.
     * @return True if recovery should be executed.
     */
    protected boolean shouldRecoverDeployment(Configuration conf, FlinkDeployment deployment) {
        boolean result = false;

        if (conf.get(KubernetesOperatorConfigOptions.OPERATOR_JM_DEPLOYMENT_RECOVERY_ENABLED)) {
            LOG.debug("Checking whether jobmanager deployment needs recovery");

            if (jmMissingForRunningDeployment(deployment)) {
                LOG.debug("Jobmanager deployment is missing, trying to recover");
                var jobSpec = deployment.getSpec().getJob();
                boolean stateless =
                        jobSpec != null && jobSpec.getUpgradeMode() == UpgradeMode.STATELESS;
                if (stateless || HighAvailabilityMode.isHighAvailabilityModeActivated(conf)) {
                    LOG.debug("HA is enabled, recovering lost jobmanager deployment");
                    result = true;
                } else {
                    LOG.warn("Could not recover lost jobmanager deployment without HA enabled");
                }
            }
        }

        return result;
    }

    private boolean jmMissingForRunningDeployment(FlinkDeployment deployment) {
        var deployedJob = ReconciliationUtils.getDeployedSpec(deployment).getJob();
        return (deployedJob == null || deployedJob.getState() == JobState.RUNNING)
                && (deployment.getStatus().getJobManagerDeploymentStatus()
                        == JobManagerDeploymentStatus.MISSING);
    }

    protected boolean flinkVersionChanged(SPEC oldSpec, SPEC newSpec) {
        if (oldSpec instanceof FlinkDeploymentSpec) {
            return ((FlinkDeploymentSpec) oldSpec).getFlinkVersion()
                    != ((FlinkDeploymentSpec) newSpec).getFlinkVersion();
        }
        return false;
    }

    protected void setOwnerReference(CR owner, Configuration deployConfig) {
        final Map<String, String> ownerReference =
                Map.of(
                        "apiVersion", owner.getApiVersion(),
                        "kind", owner.getKind(),
                        "name", owner.getMetadata().getName(),
                        "uid", owner.getMetadata().getUid(),
                        "blockOwnerDeletion", "true",
                        "controller", "false");
        deployConfig.set(
                KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE, List.of(ownerReference));
    }

    @VisibleForTesting
    public void setClock(Clock clock) {
        this.clock = clock;
    }
}
