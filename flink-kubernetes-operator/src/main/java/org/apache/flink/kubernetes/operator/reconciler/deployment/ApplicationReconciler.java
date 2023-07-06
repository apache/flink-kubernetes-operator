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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthEvaluator;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;

/** Reconciler Flink Application deployments. */
public class ApplicationReconciler
        extends AbstractJobReconciler<FlinkDeployment, FlinkDeploymentSpec, FlinkDeploymentStatus> {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationReconciler.class);
    static final String MSG_RECOVERY = "Recovering lost deployment";
    static final String MSG_RESTART_UNHEALTHY = "Restarting unhealthy job";

    public ApplicationReconciler(
            KubernetesClient kubernetesClient,
            EventRecorder eventRecorder,
            StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder,
            JobAutoScalerFactory autoscalerFactory) {
        super(kubernetesClient, eventRecorder, statusRecorder, autoscalerFactory);
    }

    @Override
    protected AvailableUpgradeMode getAvailableUpgradeMode(
            FlinkResourceContext<FlinkDeployment> ctx, Configuration deployConfig)
            throws Exception {

        var deployment = ctx.getResource();
        var status = deployment.getStatus();
        var availableUpgradeMode = super.getAvailableUpgradeMode(ctx, deployConfig);

        if (availableUpgradeMode.isAvailable() || !availableUpgradeMode.isAllowFallback()) {
            return availableUpgradeMode;
        }
        var flinkService = ctx.getFlinkService();

        if (deployConfig.getBoolean(
                        KubernetesOperatorConfigOptions
                                .OPERATOR_JOB_UPGRADE_LAST_STATE_FALLBACK_ENABLED)
                && HighAvailabilityMode.isHighAvailabilityModeActivated(deployConfig)
                && HighAvailabilityMode.isHighAvailabilityModeActivated(ctx.getObserveConfig())
                && !flinkVersionChanged(
                        ReconciliationUtils.getDeployedSpec(deployment), deployment.getSpec())) {

            if (flinkService.isHaMetadataAvailable(deployConfig)) {
                LOG.info(
                        "Job is not running but HA metadata is available for last state restore, ready for upgrade");
                return AvailableUpgradeMode.of(UpgradeMode.LAST_STATE);
            }
        }

        var jmDeployStatus = status.getJobManagerDeploymentStatus();
        if (jmDeployStatus != JobManagerDeploymentStatus.MISSING
                && status.getReconciliationStatus()
                                .deserializeLastReconciledSpec()
                                .getJob()
                                .getUpgradeMode()
                        != UpgradeMode.LAST_STATE
                && FlinkUtils.jmPodNeverStarted(ctx.getJosdkContext())) {
            deleteJmThatNeverStarted(flinkService, deployment, deployConfig);
            return getAvailableUpgradeMode(ctx, deployConfig);
        }

        if ((jmDeployStatus == JobManagerDeploymentStatus.MISSING
                        || jmDeployStatus == JobManagerDeploymentStatus.ERROR)
                && !flinkService.isHaMetadataAvailable(deployConfig)) {
            throw new RecoveryFailureException(
                    "JobManager deployment is missing and HA data is not available to make stateful upgrades. "
                            + "It is possible that the job has finished or terminally failed, or the configmaps have been deleted. "
                            + "Manual restore required.",
                    "UpgradeFailed");
        }

        LOG.info(
                "Job is not running and HA metadata is not available or usable for executing the upgrade, waiting for upgradeable state");
        return AvailableUpgradeMode.unavailable();
    }

    private void deleteJmThatNeverStarted(
            FlinkService flinkService, FlinkDeployment deployment, Configuration deployConfig) {
        deployment.getStatus().getJobStatus().setState(JobStatus.FAILED.name());
        flinkService.deleteClusterDeployment(
                deployment.getMetadata(), deployment.getStatus(), deployConfig, false);
        flinkService.waitForClusterShutdown(deployConfig);
        LOG.info("Deleted jobmanager deployment that never started.");
    }

    @Override
    public void deploy(
            FlinkResourceContext<FlinkDeployment> ctx,
            FlinkDeploymentSpec spec,
            Configuration deployConfig,
            Optional<String> savepoint,
            boolean requireHaMetadata)
            throws Exception {

        var relatedResource = ctx.getResource();
        var status = relatedResource.getStatus();
        var flinkService = ctx.getFlinkService();

        ClusterHealthEvaluator.removeLastValidClusterHealthInfo(
                relatedResource.getStatus().getClusterInfo());

        if (savepoint.isPresent()) {
            deployConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, savepoint.get());
        } else {
            deployConfig.removeConfig(SavepointConfigOptions.SAVEPOINT_PATH);
        }

        setOwnerReference(relatedResource, deployConfig);
        setRandomJobResultStorePath(deployConfig);

        if (status.getJobManagerDeploymentStatus() != JobManagerDeploymentStatus.MISSING) {
            if (!ReconciliationUtils.isJobInTerminalState(status)) {
                LOG.error("Invalid status for deployment: {}", status);
                throw new RuntimeException("This indicates a bug...");
            }
            LOG.info("Deleting deployment with terminated application before new deployment");
            flinkService.deleteClusterDeployment(
                    relatedResource.getMetadata(), status, deployConfig, true);
            flinkService.waitForClusterShutdown(deployConfig);
        }

        setJobIdIfNecessary(spec, relatedResource, deployConfig);

        eventRecorder.triggerEvent(
                relatedResource,
                EventRecorder.Type.Normal,
                EventRecorder.Reason.Submit,
                EventRecorder.Component.JobManagerDeployment,
                MSG_SUBMIT);
        flinkService.submitApplicationCluster(spec.getJob(), deployConfig, requireHaMetadata);
        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);

        IngressUtils.updateIngressRules(
                relatedResource.getMetadata(), spec, deployConfig, kubernetesClient);
    }

    private void setJobIdIfNecessary(
            FlinkDeploymentSpec spec, FlinkDeployment resource, Configuration deployConfig) {
        // The jobId assigned by Flink would be constant,
        // overwrite to avoid checkpoint path conflicts.
        // https://issues.apache.org/jira/browse/FLINK-19358
        // https://issues.apache.org/jira/browse/FLINK-29109

        if (deployConfig.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID) != null) {
            // user managed, don't touch
            return;
        }

        var status = resource.getStatus();
        // generate jobId initially or rotate on every deployment when mode is stateless
        if (status.getJobStatus().getJobId() == null
                || spec.getJob().getUpgradeMode() == UpgradeMode.STATELESS) {
            String jobId = JobID.generate().toHexString();
            // record before first deployment to ensure we use it on any retry
            status.getJobStatus().setJobId(jobId);
            LOG.info("Assigning JobId override to {}", jobId);
            statusRecorder.patchAndCacheStatus(resource);
        }

        String jobId = status.getJobStatus().getJobId();
        LOG.debug("Setting {} to {}", PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId);
        deployConfig.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId);
    }

    @Override
    protected void cancelJob(FlinkResourceContext<FlinkDeployment> ctx, UpgradeMode upgradeMode)
            throws Exception {
        ctx.getFlinkService().cancelJob(ctx.getResource(), upgradeMode, ctx.getObserveConfig());
    }

    @Override
    protected void cleanupAfterFailedJob(FlinkResourceContext<FlinkDeployment> ctx) {
        // The job has already stopped. Delete the deployment and we are ready.
        var flinkService = ctx.getFlinkService();
        var conf = ctx.getDeployConfig(ctx.getResource().getSpec());
        flinkService.deleteClusterDeployment(
                ctx.getResource().getMetadata(), ctx.getResource().getStatus(), conf, false);
        flinkService.waitForClusterShutdown(conf);
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
    public boolean reconcileOtherChanges(FlinkResourceContext<FlinkDeployment> ctx)
            throws Exception {
        if (super.reconcileOtherChanges(ctx)) {
            return true;
        }

        var deployment = ctx.getResource();
        var observeConfig = ctx.getObserveConfig();
        boolean shouldRestartJobBecauseUnhealthy =
                shouldRestartJobBecauseUnhealthy(deployment, observeConfig);
        boolean shouldRecoverDeployment = shouldRecoverDeployment(observeConfig, deployment);
        if (shouldRestartJobBecauseUnhealthy || shouldRecoverDeployment) {
            if (shouldRecoverDeployment) {
                eventRecorder.triggerEvent(
                        deployment,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.RecoverDeployment,
                        EventRecorder.Component.Job,
                        MSG_RECOVERY);
            }

            if (shouldRestartJobBecauseUnhealthy) {
                eventRecorder.triggerEvent(
                        deployment,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.RestartUnhealthyJob,
                        EventRecorder.Component.Job,
                        MSG_RESTART_UNHEALTHY);
                cleanupAfterFailedJob(ctx);
            }

            resubmitJob(
                    ctx,
                    HighAvailabilityMode.isHighAvailabilityModeActivated(ctx.getObserveConfig()));
            return true;
        }

        return cleanupTerminalJmAfterTtl(ctx.getFlinkService(), deployment, observeConfig);
    }

    private boolean shouldRestartJobBecauseUnhealthy(
            FlinkDeployment deployment, Configuration observeConfig) {
        boolean restartNeeded = false;

        if (observeConfig.getBoolean(OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED)) {
            var clusterInfo = deployment.getStatus().getClusterInfo();
            ClusterHealthInfo clusterHealthInfo =
                    ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo);
            if (clusterHealthInfo != null) {
                LOG.debug("Cluster info contains job health info");
                if (!clusterHealthInfo.isHealthy()) {
                    if (deployment.getSpec().getJob().getUpgradeMode() == UpgradeMode.STATELESS) {
                        LOG.debug("Stateless job, recovering unhealthy jobmanager deployment");
                        restartNeeded = true;
                    } else if (HighAvailabilityMode.isHighAvailabilityModeActivated(
                            observeConfig)) {
                        LOG.debug("HA is enabled, recovering unhealthy jobmanager deployment");
                        restartNeeded = true;
                    } else {
                        LOG.warn(
                                "Could not recover unhealthy jobmanager deployment without HA enabled");
                    }

                    if (restartNeeded) {
                        ClusterHealthEvaluator.removeLastValidClusterHealthInfo(clusterInfo);
                    }
                }
            } else {
                LOG.debug("Cluster info not contains job health info, skipping health check");
            }
        }

        return restartNeeded;
    }

    private boolean cleanupTerminalJmAfterTtl(
            FlinkService flinkService, FlinkDeployment deployment, Configuration observeConfig) {
        var status = deployment.getStatus();
        boolean terminal = ReconciliationUtils.isJobInTerminalState(status);
        boolean jmStillRunning =
                status.getJobManagerDeploymentStatus() != JobManagerDeploymentStatus.MISSING;

        if (terminal && jmStillRunning) {
            var ttl = observeConfig.get(KubernetesOperatorConfigOptions.OPERATOR_JM_SHUTDOWN_TTL);
            boolean ttlPassed =
                    clock.instant()
                            .isAfter(
                                    Instant.ofEpochMilli(
                                                    Long.parseLong(
                                                            status.getJobStatus().getUpdateTime()))
                                            .plus(ttl));
            if (ttlPassed) {
                LOG.info("Removing JobManager deployment for terminal application.");
                flinkService.deleteClusterDeployment(
                        deployment.getMetadata(), status, observeConfig, false);
                return true;
            }
        }
        return false;
    }

    @Override
    @SneakyThrows
    protected DeleteControl cleanupInternal(FlinkResourceContext<FlinkDeployment> ctx) {
        var deployment = ctx.getResource();
        var status = deployment.getStatus();
        var conf = ctx.getDeployConfig(ctx.getResource().getSpec());
        var cleanupUpgradeMode =
                deployment.getSpec().getJob().isSavepointOnDeletion()
                        ? UpgradeMode.SAVEPOINT
                        : UpgradeMode.STATELESS;
        if (status.getReconciliationStatus().isBeforeFirstDeployment()) {
            ctx.getFlinkService()
                    .deleteClusterDeployment(deployment.getMetadata(), status, conf, true);
        } else {
            ctx.getFlinkService().cancelJob(deployment, cleanupUpgradeMode, ctx.getObserveConfig());
        }
        return DeleteControl.defaultDelete();
    }
}
