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

package org.apache.flink.kubernetes.operator.observer.deployment;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.MissingJobManagerException;
import org.apache.flink.kubernetes.operator.observer.AbstractFlinkResourceObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Base observer for session and application clusters. */
public abstract class AbstractFlinkDeploymentObserver
        extends AbstractFlinkResourceObserver<FlinkDeployment, FlinkDeploymentObserverContext> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final FlinkService flinkService;

    public AbstractFlinkDeploymentObserver(
            FlinkService flinkService,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder) {
        super(configManager, eventRecorder);
        this.flinkService = flinkService;
    }

    @Override
    protected FlinkDeploymentObserverContext getObserverContext(
            FlinkDeployment resource, Context<?> context) {
        return new FlinkDeploymentObserverContext(resource, configManager);
    }

    @Override
    public void observeInternal(
            FlinkDeployment flinkDep,
            Context<?> context,
            FlinkDeploymentObserverContext observerContext) {

        if (!isJmDeploymentReady(flinkDep)) {
            // Only observe the JM if we think it's in bad state
            observeJmDeployment(flinkDep, context, observerContext.getDeployedConfig());
        }

        if (isJmDeploymentReady(flinkDep)) {
            // Only observe session/application if JM is ready
            observeFlinkCluster(flinkDep, context, observerContext);
        }

        if (isJmDeploymentReady(flinkDep)) {
            observeClusterInfo(flinkDep, observerContext.getDeployedConfig());
        }

        clearErrorsIfDeploymentIsHealthy(flinkDep);
    }

    private void observeClusterInfo(FlinkDeployment flinkApp, Configuration configuration) {
        try {
            Map<String, String> clusterInfo = flinkService.getClusterInfo(configuration);
            flinkApp.getStatus().getClusterInfo().putAll(clusterInfo);
            logger.debug("ClusterInfo: {}", flinkApp.getStatus().getClusterInfo());
        } catch (Exception e) {
            logger.error("Exception while fetching cluster info", e);
        }
    }

    protected void observeJmDeployment(
            FlinkDeployment flinkApp, Context<?> context, Configuration effectiveConfig) {
        FlinkDeploymentStatus deploymentStatus = flinkApp.getStatus();
        JobManagerDeploymentStatus previousJmStatus =
                deploymentStatus.getJobManagerDeploymentStatus();

        if (isSuspendedJob(flinkApp)) {
            logger.debug("Skipping observe step for suspended application deployments");
            return;
        }

        logger.info(
                "Observing JobManager deployment. Previous status: {}", previousJmStatus.name());

        if (JobManagerDeploymentStatus.DEPLOYED_NOT_READY == previousJmStatus) {
            logger.info("JobManager deployment is ready");
            deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
            return;
        }

        Optional<Deployment> deployment = context.getSecondaryResource(Deployment.class);
        if (deployment.isPresent()) {
            DeploymentStatus status = deployment.get().getStatus();
            DeploymentSpec spec = deployment.get().getSpec();
            if (status != null
                    && status.getAvailableReplicas() != null
                    && spec.getReplicas().intValue() == status.getReplicas()
                    && spec.getReplicas().intValue() == status.getAvailableReplicas()
                    && flinkService.isJobManagerPortReady(effectiveConfig)) {

                // typically it takes a few seconds for the REST server to be ready
                logger.info(
                        "JobManager deployment port is ready, waiting for the Flink REST API...");
                deploymentStatus.setJobManagerDeploymentStatus(
                        JobManagerDeploymentStatus.DEPLOYED_NOT_READY);
                return;
            }

            try {
                checkFailedCreate(status);
                // checking the pod is expensive; only do it when the deployment isn't ready
                checkContainerBackoff(flinkApp, effectiveConfig);
            } catch (DeploymentFailedException dfe) {
                // throw only when not already in error status to allow for spec update
                deploymentStatus.getJobStatus().setState(JobStatus.RECONCILING.name());
                if (!JobManagerDeploymentStatus.ERROR.equals(
                        deploymentStatus.getJobManagerDeploymentStatus())) {
                    throw dfe;
                }
                return;
            }

            logger.info("JobManager is being deployed");
            deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
            return;
        }

        deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        deploymentStatus.getJobStatus().setState(JobStatus.RECONCILING.name());

        if (previousJmStatus != JobManagerDeploymentStatus.MISSING
                && previousJmStatus != JobManagerDeploymentStatus.ERROR) {
            onMissingDeployment(flinkApp);
        }
    }

    private void checkFailedCreate(DeploymentStatus status) {
        List<DeploymentCondition> conditions = status.getConditions();
        for (DeploymentCondition dc : conditions) {
            if ("FailedCreate".equals(dc.getReason()) && "ReplicaFailure".equals(dc.getType())) {
                throw new DeploymentFailedException(dc);
            }
        }
    }

    private void checkContainerBackoff(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        PodList jmPods = flinkService.getJmPodList(flinkApp, effectiveConfig);
        for (Pod pod : jmPods.getItems()) {
            for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
                ContainerStateWaiting csw = cs.getState().getWaiting();
                if (csw != null
                        && Set.of(
                                        DeploymentFailedException.REASON_CRASH_LOOP_BACKOFF,
                                        DeploymentFailedException.REASON_IMAGE_PULL_BACKOFF)
                                .contains(csw.getReason())) {
                    throw new DeploymentFailedException(csw);
                }
            }
        }
    }

    protected boolean isJmDeploymentReady(FlinkDeployment dep) {
        return dep.getStatus().getJobManagerDeploymentStatus() == JobManagerDeploymentStatus.READY;
    }

    protected void clearErrorsIfDeploymentIsHealthy(FlinkDeployment dep) {
        FlinkDeploymentStatus status = dep.getStatus();
        var reconciliationStatus = status.getReconciliationStatus();
        if (status.getJobManagerDeploymentStatus() != JobManagerDeploymentStatus.ERROR
                && !JobStatus.FAILED.name().equals(dep.getStatus().getJobStatus().getState())
                && reconciliationStatus.isLastReconciledSpecStable()) {
            status.setError("");
        }
    }

    protected boolean isSuspendedJob(FlinkDeployment deployment) {
        JobSpec jobSpec = deployment.getSpec().getJob();
        if (jobSpec == null) {
            return false;
        }

        FlinkDeploymentStatus deploymentStatus = deployment.getStatus();
        FlinkDeploymentSpec lastReconciledSpec =
                deploymentStatus.getReconciliationStatus().deserializeLastReconciledSpec();

        return deploymentStatus.getJobManagerDeploymentStatus()
                        == JobManagerDeploymentStatus.MISSING
                && jobSpec.getState() == JobState.SUSPENDED
                && lastReconciledSpec != null
                && lastReconciledSpec.getJob().getState() == JobState.SUSPENDED;
    }

    private void onMissingDeployment(FlinkDeployment deployment) {
        String err = "Missing JobManager deployment";
        logger.error(err);
        ReconciliationUtils.updateForReconciliationError(
                deployment,
                new MissingJobManagerException(err),
                configManager.getOperatorConfiguration());
        eventRecorder.triggerEvent(
                deployment,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.Missing,
                EventRecorder.Component.JobManagerDeployment,
                err);
    }

    @Override
    protected void updateStatusToDeployedIfAlreadyUpgraded(
            FlinkDeployment flinkDep,
            Context<?> context,
            FlinkDeploymentObserverContext observerContext) {
        var status = flinkDep.getStatus();
        Optional<Deployment> depOpt = context.getSecondaryResource(Deployment.class);
        depOpt.ifPresent(
                deployment -> {
                    Map<String, String> annotations = deployment.getMetadata().getAnnotations();
                    if (annotations == null) {
                        return;
                    }
                    Long deployedGeneration =
                            Optional.ofNullable(annotations.get(FlinkUtils.CR_GENERATION_LABEL))
                                    .map(Long::valueOf)
                                    .orElse(-1L);

                    Long upgradeTargetGeneration =
                            ReconciliationUtils.getUpgradeTargetGeneration(flinkDep);

                    if (deployedGeneration.equals(upgradeTargetGeneration)) {
                        logger.info("Pending upgrade is already deployed, updating status.");
                        ReconciliationUtils.updateStatusForAlreadyUpgraded(flinkDep);
                        if (flinkDep.getSpec().getJob() != null) {
                            status.getJobStatus()
                                    .setState(
                                            org.apache.flink.api.common.JobStatus.RECONCILING
                                                    .name());
                        }
                        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
                    } else {
                        logger.warn(
                                "Running deployment generation {} doesn't match upgrade target generation {}.",
                                deployedGeneration,
                                upgradeTargetGeneration);
                    }
                });
    }

    /**
     * Observe the flinkApp status when the cluster is ready. It will be implemented by child class
     * to reflect the changed status on the flinkApp resource.
     *
     * @param flinkApp the target flinkDeployment resource
     * @param context the context with which the operation is executed
     * @param observerContext Observer context
     */
    protected abstract void observeFlinkCluster(
            FlinkDeployment flinkApp,
            Context<?> context,
            FlinkDeploymentObserverContext observerContext);
}
