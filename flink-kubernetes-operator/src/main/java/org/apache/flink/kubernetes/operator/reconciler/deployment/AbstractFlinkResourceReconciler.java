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
import org.apache.flink.kubernetes.operator.reconciler.diff.ReflectiveDiffBuilder;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
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
        var spec = cr.getSpec();
        var deployConfig = ctx.getDeployConfig(spec);
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

        var specDiff = new ReflectiveDiffBuilder<>(lastReconciledSpec, currentDeploySpec).build();

        boolean specChanged =
                DiffType.IGNORE != specDiff.getType()
                        || reconciliationStatus.getState() == ReconciliationState.UPGRADING;

        var observeConfig = ctx.getObserveConfig();
        if (specChanged) {
            if (checkNewSpecAlreadyDeployed(cr, deployConfig)) {
                return;
            }

            var specChangeMessage = String.format(MSG_SPEC_CHANGED, specDiff.getType(), specDiff);
            LOG.info(specChangeMessage);
            if (reconciliationStatus.getState() != ReconciliationState.UPGRADING) {
                eventRecorder.triggerEvent(
                        cr,
                        EventRecorder.Type.Normal,
                        EventRecorder.Reason.SpecChanged,
                        EventRecorder.Component.JobManagerDeployment,
                        specChangeMessage);
            }
            boolean reconciled =
                    scaleCluster(cr, ctx.getFlinkService(), deployConfig, specDiff.getType())
                            || reconcileSpecChange(ctx, deployConfig);
            if (reconciled) {
                // If we executed a scale or spec upgrade action we return, otherwise we continue to
                // reconcile other changes
                return;
            }
        } else {
            ReconciliationUtils.updateReconciliationMetadata(cr);
        }

        if (shouldRollBack(cr, observeConfig, ctx.getFlinkService())) {
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
            rollback(ctx);
        } else if (!reconcileOtherChanges(ctx)) {
            if (!resourceScaler.scale(ctx)) {
                LOG.info("Resource fully reconciled, nothing to do...");
            }
        }
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
     * Rollback deployed resource to the last stable spec.
     *
     * @param ctx Reconciliation context.
     * @throws Exception Error during rollback.
     */
    protected abstract void rollback(FlinkResourceContext<CR> ctx) throws Exception;

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
        resourceScaler.cleanup(ctx.getResource());
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
                == ReconciliationState.UPGRADING) {
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
     * Scale the cluster whenever there is a scaling change, based on the task manager replica
     * update or the parallelism in case of scheduler mode.
     *
     * @param cr Resource being reconciled.
     * @param flinkService Flink service.
     * @param deployConfig Configuration to be deployed.
     * @param diffType Spec change type.
     * @return True if the scaling is successful
     * @throws Exception
     */
    private boolean scaleCluster(
            CR cr, FlinkService flinkService, Configuration deployConfig, DiffType diffType)
            throws Exception {
        if (diffType != DiffType.SCALE) {
            return false;
        }
        boolean scaled = flinkService.scale(cr.getMetadata(), cr.getSpec().getJob(), deployConfig);
        if (scaled) {
            LOG.info("Scaling succeeded");
            ReconciliationUtils.updateStatusForDeployedSpec(cr, deployConfig, clock);
            return true;
        }
        return false;
    }

    /**
     * Checks whether the currently deployed Flink resource spec should be rolled back to the stable
     * spec. This includes validating the current deployment status, config and checking if the last
     * reconciled spec did not become stable within the configured grace period.
     *
     * <p>Rollbacks are only supported to previously running resource specs with HA enabled.
     *
     * @param resource Resource being reconciled.
     * @param configuration Flink cluster configuration.
     * @return True if the resource should be rolled back.
     */
    private boolean shouldRollBack(
            AbstractFlinkResource<SPEC, STATUS> resource,
            Configuration configuration,
            FlinkService flinkService) {

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
                && resource.getSpec().getJob().getUpgradeMode() == UpgradeMode.SAVEPOINT) {
            // HA data is not available during rollback for savepoint upgrade mode
            return true;
        }

        var haDataAvailable = flinkService.isHaMetadataAvailable(configuration);
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
