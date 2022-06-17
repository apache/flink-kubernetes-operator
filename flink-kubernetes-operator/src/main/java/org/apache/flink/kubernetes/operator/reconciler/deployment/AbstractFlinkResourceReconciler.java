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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
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

    protected final FlinkConfigManager configManager;
    protected final EventRecorder eventRecorder;
    protected final StatusRecorder<STATUS> statusRecorder;
    protected final KubernetesClient kubernetesClient;

    public static final String MSG_SUSPENDED = "Suspending existing deployment.";
    public static final String MSG_SPEC_CHANGED = "Detected spec change, starting reconciliation.";
    public static final String MSG_ROLLBACK = "Rolling back failed deployment.";
    public static final String MSG_SUBMIT = "Starting deployment";

    public AbstractFlinkResourceReconciler(
            KubernetesClient kubernetesClient,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder,
            StatusRecorder<STATUS> statusRecorder) {
        this.kubernetesClient = kubernetesClient;
        this.configManager = configManager;
        this.eventRecorder = eventRecorder;
        this.statusRecorder = statusRecorder;
    }

    @Override
    public final void reconcile(CR cr, Context ctx) throws Exception {
        var spec = cr.getSpec();
        var deployConfig = getDeployConfig(cr.getMetadata(), spec, ctx);
        var status = cr.getStatus();
        var reconciliationStatus = cr.getStatus().getReconciliationStatus();

        // If the resource is not ready for reconciliation we simply return
        if (!readyToReconcile(cr, ctx, deployConfig)) {
            LOG.info("Not ready for reconciliation yet...");
            return;
        }

        // If this is the first deployment for the resource we simply submit the job and return.
        // No further logic is required at this point.
        if (reconciliationStatus.isFirstDeployment()) {
            LOG.info("Deploying for the first time");

            // Before we try to submit the job we record the current spec in the status so we can
            // handle subsequent deployment and status update errors
            ReconciliationUtils.updateStatusBeforeDeploymentAttempt(cr, deployConfig);
            statusRecorder.patchAndCacheStatus(cr);

            deploy(
                    cr,
                    spec,
                    status,
                    ctx,
                    deployConfig,
                    Optional.ofNullable(spec.getJob()).map(JobSpec::getInitialSavepointPath),
                    false);

            ReconciliationUtils.updateStatusForDeployedSpec(cr, deployConfig);
            return;
        }

        SPEC lastReconciledSpec =
                cr.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        SPEC currentDeploySpec = cr.getSpec();

        boolean specChanged =
                reconciliationStatus.getState() == ReconciliationState.UPGRADING
                        || !currentDeploySpec.equals(lastReconciledSpec);
        var observeConfig = getObserveConfig(cr, ctx);
        var flinkService = getFlinkService(cr, ctx);
        if (specChanged) {
            if (checkNewSpecAlreadyDeployed(cr, deployConfig)) {
                return;
            }
            LOG.info(MSG_SPEC_CHANGED);
            if (reconciliationStatus.getState() != ReconciliationState.UPGRADING) {
                eventRecorder.triggerEvent(
                        cr,
                        EventRecorder.Type.Normal,
                        EventRecorder.Reason.SpecChanged,
                        EventRecorder.Component.JobManagerDeployment,
                        MSG_SPEC_CHANGED);
            }
            reconcileSpecChange(cr, ctx, observeConfig, deployConfig);
        } else if (shouldRollBack(cr, observeConfig, flinkService)) {
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
            rollback(cr, ctx, observeConfig);
        } else if (!reconcileOtherChanges(cr, ctx, observeConfig)) {
            LOG.info("Resource fully reconciled, nothing to do...");
        }
    }

    /**
     * Get Flink configuration object for deploying the given spec using {@link #deploy}.
     *
     * @param meta ObjectMeta of the related resource.
     * @param spec Spec for which the config should be created.
     * @param ctx Reconciliation context.
     * @return Deployment configuration.
     */
    protected abstract Configuration getDeployConfig(ObjectMeta meta, SPEC spec, Context ctx);

    /**
     * Get Flink configuration for client interactions with the running Flink deployment/session
     * job.
     *
     * @param resource Related Flink resource.
     * @param context Reconciliation context.
     * @return Observe configuration.
     */
    protected abstract Configuration getObserveConfig(CR resource, Context context);

    /**
     * Check whether the given Flink resource is ready to be reconciled or we are still waiting for
     * any pending operation or condition first.
     *
     * @param cr Related Flink resource.
     * @param ctx Reconciliation context.
     * @param deployConfig Deployment configuration.
     * @return True if the resource is ready to be reconciled.
     */
    protected abstract boolean readyToReconcile(CR cr, Context ctx, Configuration deployConfig);

    /**
     * Reconcile spec upgrade on the currently deployed/suspended Flink resource and update the
     * status accordingly.
     *
     * @param cr Related Flink resource.
     * @param observeConfig Observe configuration.
     * @param deployConfig Deployment configuration.
     * @throws Exception Error during spec upgrade.
     */
    protected abstract void reconcileSpecChange(
            CR cr, Context ctx, Configuration observeConfig, Configuration deployConfig)
            throws Exception;

    /**
     * Rollback deployed resource to the last stable spec.
     *
     * @param cr Related Flink resource.
     * @param ctx Reconciliation context.
     * @param observeConfig Observe configuration.
     * @throws Exception Error during rollback.
     */
    protected abstract void rollback(CR cr, Context ctx, Configuration observeConfig)
            throws Exception;

    /**
     * Reconcile any other changes required for this resource that are specific to the reconciler
     * implementation.
     *
     * @param cr Related Flink resource.
     * @param observeConfig Observe configuration.
     * @return True if any further reconciliation action was taken.
     * @throws Exception Error during reconciliation.
     */
    protected abstract boolean reconcileOtherChanges(
            CR cr, Context context, Configuration observeConfig) throws Exception;

    @Override
    public final DeleteControl cleanup(CR resource, Context context) {
        return cleanupInternal(resource, context);
    }

    /**
     * Deploys the target resource spec to Kubernetes.
     *
     * @param relatedResource Related resource.
     * @param spec Spec that should be deployed to Kubernetes.
     * @param status Status object of the resource
     * @param deployConfig Flink conf for the deployment.
     * @param ctx Reconciliation context.
     * @param savepoint Optional savepoint path for applications and session jobs.
     * @param requireHaMetadata Flag used by application deployments to validate HA metadata
     * @throws Exception Error during deployment.
     */
    protected abstract void deploy(
            CR relatedResource,
            SPEC spec,
            STATUS status,
            Context ctx,
            Configuration deployConfig,
            Optional<String> savepoint,
            boolean requireHaMetadata)
            throws Exception;

    /**
     * Shut down and clean up all Flink job/cluster resources.
     *
     * @param resource Resource being reconciled.
     * @param context Current context.
     * @return DeleteControl object.
     */
    protected abstract DeleteControl cleanupInternal(CR resource, Context context);

    /**
     * Get the Flink service related to the resource and context.
     *
     * @param resource
     * @param context
     * @return
     */
    protected abstract FlinkService getFlinkService(CR resource, Context context);

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
            ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConf);
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
        if (!Instant.now()
                .minus(readinessTimeout)
                .isAfter(Instant.ofEpochMilli(reconciliationStatus.getReconciliationTimestamp()))) {
            return false;
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
    protected static boolean shouldRecoverDeployment(
            Configuration conf, FlinkDeployment deployment) {

        if (!jmMissingForRunningDeployment(deployment)
                || !conf.get(
                        KubernetesOperatorConfigOptions.OPERATOR_JM_DEPLOYMENT_RECOVERY_ENABLED)) {
            return false;
        }

        if (!FlinkUtils.isKubernetesHAActivated(conf)) {
            LOG.warn("Could not recover lost deployment without HA enabled");
            return false;
        }
        return true;
    }

    private static boolean jmMissingForRunningDeployment(FlinkDeployment deployment) {
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
}
