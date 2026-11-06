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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.TaskManagerInfo;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.MissingJobManagerException;
import org.apache.flink.kubernetes.operator.observer.AbstractFlinkResourceObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.EventUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.util.TimeUtils;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * Base observer for session and application clusters.
 *
 * <p>The observation is layered by source, Kubernetes, the Flink cluster and the job, and each
 * layer owns one status transition:
 *
 * <ul>
 *   <li>{@link #observeJmDeployment}: the Kubernetes view, the JobManager Deployment and its pods.
 *       It can conclude {@code MISSING}, {@code ERROR}, {@code DEPLOYING} or, once the REST port
 *       accepts connections, {@code DEPLOYED_NOT_READY}. It never sets {@code READY}.
 *   <li>{@link #observeFlinkCluster}: the JobManager process behind its REST API. A successful call
 *       is the only way to {@code READY} and stores the cluster info. A failed call hands back to
 *       {@link #observeJmDeployment} to find out what happened to the JobManager pod.
 *   <li>{@link #observeJob}: what the cluster runs, implemented per mode and consulted only while
 *       {@code READY}.
 * </ul>
 *
 * <p>NOTE: The Kubernetes step and the REST step never run in the same loop. The loop that reaches
 * {@code DEPLOYED_NOT_READY} returns and the next one probes. This keeps the time to {@code READY}
 * at the port check plus one reschedule, and keeps the job step away from a JobManager that has not
 * finished starting.
 */
public abstract class AbstractFlinkDeploymentObserver
        extends AbstractFlinkResourceObserver<FlinkDeployment> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public AbstractFlinkDeploymentObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
    }

    @Override
    public void observeInternal(FlinkResourceContext<FlinkDeployment> ctx) {
        var flinkDep = ctx.getResource();
        if (!isJmDeployed(flinkDep)) {
            observeJmDeployment(ctx);
        } else {
            observeFlinkCluster(ctx);
        }

        if (isJmReady(flinkDep)) {
            observeJob(ctx);
        }

        clearErrorsIfDeploymentIsHealthy(flinkDep);
    }

    protected void observeFlinkCluster(FlinkResourceContext<FlinkDeployment> ctx) {
        var flinkApp = ctx.getResource();
        var status = flinkApp.getStatus();
        try {
            var clusterInfo = ctx.getFlinkService().getClusterInfo(ctx.getObserveConfig());
            status.getClusterInfo().putAll(clusterInfo);
        } catch (Exception e) {
            if (isJmReady(flinkApp)) {
                logger.warn("JobManager REST API stopped answering", e);
                if (JobStatus.RUNNING == status.getJobStatus().getState()) {
                    // The job was last observed RUNNING, its state is unknown until the REST API
                    // answers again
                    status.getJobStatus().setState(JobStatus.RECONCILING);
                }
            } else if (e instanceof TimeoutException) {
                logger.info(
                        "JobManager REST API is not ready yet, no answer within {}",
                        TimeUtils.formatWithHighestUnit(
                                ctx.getOperatorConfig().getFlinkClientTimeout()));
            } else {
                logger.info("JobManager REST API is not ready yet: {}", e.toString());
            }
            // Either way, find out what happened to the JobManager pod
            observeJmDeployment(ctx);
            return;
        }

        if (!isJmReady(flinkApp)) {
            logger.info("JobManager REST API is ready");
            status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        }
        observeTaskManagers(ctx);
    }

    private void observeTaskManagers(FlinkResourceContext<FlinkDeployment> ctx) {
        var flinkApp = ctx.getResource();
        var conf = ctx.getObserveConfig();
        try {
            int replicas = ctx.getFlinkService().getTaskManagerReplicas(conf);
            var clusterInfo = flinkApp.getStatus().getClusterInfo();
            clusterInfo.put(
                    AbstractFlinkService.FIELD_NAME_TOTAL_CPU,
                    String.valueOf(FlinkUtils.calculateClusterCpuUsage(conf, replicas)));
            clusterInfo.put(
                    AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY,
                    String.valueOf(FlinkUtils.calculateClusterMemoryUsage(conf, replicas)));
            logger.debug("ClusterInfo: {}", clusterInfo);

            var labelSelector =
                    FlinkUtils.getTaskManagerLabelSelector(flinkApp.getMetadata().getName());
            flinkApp.getStatus().setTaskManager(new TaskManagerInfo(labelSelector, replicas));
        } catch (Exception e) {
            logger.warn("Exception while fetching TaskManager info", e);
        }
    }

    protected void observeJmDeployment(FlinkResourceContext<FlinkDeployment> ctx) {
        var flinkApp = ctx.getResource();
        FlinkDeploymentStatus deploymentStatus = flinkApp.getStatus();
        JobManagerDeploymentStatus previousJmStatus =
                deploymentStatus.getJobManagerDeploymentStatus();

        if (isSuspendedJob(flinkApp)) {
            logger.debug("Skipping observe step for suspended application deployments");
            return;
        }

        logger.info(
                "Observing JobManager deployment. Previous status: {}", previousJmStatus.name());

        Optional<Deployment> deployment =
                ctx.getJosdkContext().getSecondaryResource(Deployment.class);
        if (deployment.isPresent()) {
            DeploymentStatus status = deployment.get().getStatus();
            if (status != null
                    && status.getAvailableReplicas() != null
                    // One available JM is enough to run the job correctly
                    && status.getReplicas() > 0
                    && status.getAvailableReplicas() > 0
                    && ctx.getFlinkService().isJobManagerPortReady(ctx.getObserveConfig())) {

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
                checkContainerErrors(ctx);
            } catch (DeploymentFailedException dfe) {
                // throw only when not already in error status to allow for spec update
                deploymentStatus.getJobStatus().setState(JobStatus.RECONCILING);
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

        if (!ReconciliationUtils.isJobInTerminalState(deploymentStatus)) {
            deploymentStatus.getJobStatus().setState(JobStatus.RECONCILING);
        }

        if (previousJmStatus != JobManagerDeploymentStatus.MISSING
                && previousJmStatus != JobManagerDeploymentStatus.ERROR) {
            onMissingDeployment(ctx);
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

    private void checkContainerErrors(FlinkResourceContext<FlinkDeployment> ctx) {
        PodList jmPods =
                ctx.getFlinkService().getJmPodList(ctx.getResource(), ctx.getObserveConfig());
        for (Pod pod : jmPods.getItems()) {
            var podStatus = pod.getStatus();
            Stream.concat(
                            podStatus.getContainerStatuses().stream(),
                            podStatus.getInitContainerStatuses().stream())
                    .forEach(AbstractFlinkDeploymentObserver::checkContainerError);

            // No obvious errors were found, check for volume mount issues
            EventUtils.checkForVolumeMountErrors(ctx.getKubernetesClient(), pod);
        }
    }

    private static void checkContainerError(ContainerStatus cs) {
        if (cs.getState() == null || cs.getState().getWaiting() == null) {
            return;
        }
        if (DeploymentFailedException.CONTAINER_ERROR_REASONS.contains(
                cs.getState().getWaiting().getReason())) {
            throw DeploymentFailedException.forContainerStatus(cs);
        }
    }

    protected boolean isJmDeployed(FlinkDeployment dep) {
        var jmStatus = dep.getStatus().getJobManagerDeploymentStatus();
        return jmStatus == JobManagerDeploymentStatus.DEPLOYED_NOT_READY
                || jmStatus == JobManagerDeploymentStatus.READY;
    }

    protected boolean isJmReady(FlinkDeployment dep) {
        return dep.getStatus().getJobManagerDeploymentStatus() == JobManagerDeploymentStatus.READY;
    }

    protected void clearErrorsIfDeploymentIsHealthy(FlinkDeployment dep) {
        FlinkDeploymentStatus status = dep.getStatus();
        var reconciliationStatus = status.getReconciliationStatus();
        if (status.getJobManagerDeploymentStatus() != JobManagerDeploymentStatus.ERROR
                && !JobStatus.FAILED.equals(dep.getStatus().getJobStatus().getState())
                && reconciliationStatus.isLastReconciledSpecStable()) {
            status.setError(null);
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

    private void onMissingDeployment(FlinkResourceContext<FlinkDeployment> ctx) {
        String err = "Missing JobManager deployment";
        logger.error(err);
        ReconciliationUtils.updateForReconciliationError(ctx, new MissingJobManagerException(err));
        eventRecorder.triggerEvent(
                ctx.getResource(),
                EventRecorder.Type.Warning,
                EventRecorder.Reason.Missing,
                EventRecorder.Component.JobManagerDeployment,
                err,
                ctx.getKubernetesClient());
    }

    @Override
    protected boolean checkIfAlreadyUpgraded(FlinkResourceContext<FlinkDeployment> ctx) {
        var flinkDep = ctx.getResource();
        var status = flinkDep.getStatus();

        if (status.getJobManagerDeploymentStatus() != JobManagerDeploymentStatus.MISSING) {
            // We know that the current deployment is not missing, nothing to check
            return false;
        }

        // We are performing a full upgrade
        Optional<Deployment> depOpt = ctx.getJosdkContext().getSecondaryResource(Deployment.class);

        if (!depOpt.isPresent()) {
            // Nothing is deployed, so definitely not upgraded
            return false;
        }

        var deployment = depOpt.get();
        if (deployment.isMarkedForDeletion()) {
            logger.debug("Deployment already marked for deletion, ignoring...");
            return false;
        }

        Map<String, String> annotations = deployment.getMetadata().getAnnotations();
        if (annotations == null) {
            logger.warn(
                    "Running deployment doesn't have any annotations. This could indicate a deployment error.");
            return false;
        }
        Long deployedGeneration =
                Optional.ofNullable(annotations.get(FlinkUtils.CR_GENERATION_LABEL))
                        .map(Long::valueOf)
                        .orElse(-1L);

        Long upgradeTargetGeneration = ReconciliationUtils.getUpgradeTargetGeneration(flinkDep);

        if (deployedGeneration.equals(upgradeTargetGeneration)) {
            logger.info("Pending upgrade is already deployed, updating status.");
            status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
            return true;
        } else {
            logger.warn(
                    "Running deployment generation {} doesn't match upgrade target generation {}.",
                    deployedGeneration,
                    upgradeTargetGeneration);
            return false;
        }
    }

    /** Observe what the cluster runs, called only while the JobManager is {@code READY}. */
    protected abstract void observeJob(FlinkResourceContext<FlinkDeployment> ctx);
}
