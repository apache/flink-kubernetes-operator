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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
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

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;

/** Reconciler Flink Application deployments. */
public class ApplicationReconciler
        extends AbstractJobReconciler<FlinkDeployment, FlinkDeploymentSpec, FlinkDeploymentStatus> {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationReconciler.class);
    protected final FlinkService flinkService;

    public ApplicationReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder,
            StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder) {
        super(kubernetesClient, configManager, eventRecorder, statusRecorder);
        this.flinkService = flinkService;
    }

    @Override
    protected FlinkService getFlinkService(FlinkDeployment resource, Context<?> context) {
        return flinkService;
    }

    @Override
    protected Configuration getObserveConfig(FlinkDeployment deployment, Context<?> context) {
        return configManager.getObserveConfig(deployment);
    }

    @Override
    protected Configuration getDeployConfig(
            ObjectMeta deployMeta, FlinkDeploymentSpec currentDeploySpec, Context<?> context) {
        return configManager.getDeployConfig(deployMeta, currentDeploySpec);
    }

    @Override
    protected Optional<UpgradeMode> getAvailableUpgradeMode(
            FlinkDeployment deployment, Configuration deployConfig, Configuration observeConfig) {

        var status = deployment.getStatus();
        var availableUpgradeMode =
                super.getAvailableUpgradeMode(deployment, deployConfig, observeConfig);

        if (availableUpgradeMode.isPresent()) {
            return availableUpgradeMode;
        }

        if (deployConfig.getBoolean(
                        KubernetesOperatorConfigOptions
                                .OPERATOR_JOB_UPGRADE_LAST_STATE_FALLBACK_ENABLED)
                && FlinkUtils.isKubernetesHAActivated(deployConfig)
                && FlinkUtils.isKubernetesHAActivated(observeConfig)
                && !flinkVersionChanged(
                        ReconciliationUtils.getDeployedSpec(deployment), deployment.getSpec())) {

            if (!flinkService.isHaMetadataAvailable(deployConfig)) {
                if (deployment.getStatus().getReconciliationStatus().getLastStableSpec() == null) {
                    // initial deployment failure, reset to allow for spec change to proceed
                    return resetOnMissingStableSpec(deployment, deployConfig);
                }
            } else {
                LOG.info(
                        "Job is not running but HA metadata is available for last state restore, ready for upgrade");
                return Optional.of(UpgradeMode.LAST_STATE);
            }
        }

        if (status.getJobManagerDeploymentStatus() == JobManagerDeploymentStatus.MISSING
                || status.getJobManagerDeploymentStatus() == JobManagerDeploymentStatus.ERROR) {
            throw new DeploymentFailedException(
                    "JobManager deployment is missing and HA data is not available to make stateful upgrades. "
                            + "It is possible that the job has finished or terminally failed, or the configmaps have been deleted. "
                            + "Manual restore required.",
                    "UpgradeFailed");
        }

        LOG.info(
                "Job is not running yet and HA metadata is not available, waiting for upgradeable state");
        return Optional.empty();
    }

    private Optional<UpgradeMode> resetOnMissingStableSpec(
            FlinkDeployment deployment, Configuration deployConfig) {
        // initial deployment failure, reset to allow for spec change to proceed
        flinkService.deleteClusterDeployment(
                deployment.getMetadata(), deployment.getStatus(), false);
        flinkService.waitForClusterShutdown(deployConfig);
        if (!flinkService.isHaMetadataAvailable(deployConfig)) {
            LOG.info("Job never entered stable state. Resetting status for initial deploy");
            ReconciliationUtils.clearLastReconciledSpecIfFirstDeploy(deployment);
            return Optional.empty();
        } else {
            // proceed with upgrade if deployment succeeded between check and delete
            LOG.info("Found HA state after deployment deletion, falling back to stateful upgrade");
            return Optional.of(UpgradeMode.LAST_STATE);
        }
    }

    @Override
    protected void deploy(
            FlinkDeployment relatedResource,
            FlinkDeploymentSpec spec,
            FlinkDeploymentStatus status,
            Context<?> ctx,
            Configuration deployConfig,
            Optional<String> savepoint,
            boolean requireHaMetadata)
            throws Exception {

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
            flinkService.deleteClusterDeployment(relatedResource.getMetadata(), status, true);
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
        // https://issues.apache.org/jira/browse/FLINK-19358
        // https://issues.apache.org/jira/browse/FLINK-29109
        if (spec.getFlinkVersion().isNewerVersionThan(FlinkVersion.v1_15)) {
            return;
        }

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
    protected void cancelJob(
            FlinkDeployment deployment,
            Context<?> ctx,
            UpgradeMode upgradeMode,
            Configuration observeConfig)
            throws Exception {
        flinkService.cancelJob(deployment, upgradeMode, observeConfig);
    }

    @Override
    protected void cleanupAfterFailedJob(
            FlinkDeployment deployment, Context<?> ctx, Configuration observeConfig) {
        // The job has already stopped. Delete the deployment and we are ready.
        flinkService.deleteClusterDeployment(
                deployment.getMetadata(), deployment.getStatus(), false);
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
    public boolean reconcileOtherChanges(
            FlinkDeployment deployment, Context<?> ctx, Configuration observeConfig)
            throws Exception {
        if (super.reconcileOtherChanges(deployment, ctx, observeConfig)) {
            return true;
        }

        boolean shouldRestartJobBecauseUnhealthy =
                shouldRestartJobBecauseUnhealthy(deployment, observeConfig);
        boolean shouldRecoverDeployment = shouldRecoverDeployment(observeConfig, deployment);
        if (shouldRestartJobBecauseUnhealthy || shouldRecoverDeployment) {
            if (shouldRestartJobBecauseUnhealthy) {
                cleanupAfterFailedJob(deployment, ctx, observeConfig);
            }
            resubmitJob(deployment, ctx, observeConfig, true);
            return true;
        }

        return false;
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
                    } else if (FlinkUtils.isKubernetesHAActivated(observeConfig)) {
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

    @Override
    @SneakyThrows
    protected DeleteControl cleanupInternal(FlinkDeployment deployment, Context<?> context) {
        var status = deployment.getStatus();
        if (status.getReconciliationStatus().isBeforeFirstDeployment()) {
            flinkService.deleteClusterDeployment(deployment.getMetadata(), status, true);
        } else {
            flinkService.cancelJob(
                    deployment, UpgradeMode.STATELESS, configManager.getObserveConfig(deployment));
        }
        return DeleteControl.defaultDelete();
    }
}
