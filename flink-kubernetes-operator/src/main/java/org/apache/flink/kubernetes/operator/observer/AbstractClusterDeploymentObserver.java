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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.MissingJobManagerException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.EventUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;

import java.util.Map;
import java.util.Optional;

/**
 * Base observer for resources that run their Flink cluster from a JobManager Deployment that the
 * operator creates.
 *
 * <p>The Deployment is observed into the JobManager deployment status of the resource. Once it is
 * ready, the Flink cluster and its cluster info are observed through the REST API of the cluster.
 *
 * @param <CR> Flink resource type
 */
public abstract class AbstractClusterDeploymentObserver<CR extends AbstractFlinkResource<?, ?>>
        extends AbstractFlinkResourceObserver<CR> {

    public AbstractClusterDeploymentObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
    }

    @Override
    public void observeInternal(FlinkResourceContext<CR> ctx) {
        var resource = ctx.getResource();
        if (!isJmDeploymentReady(resource)) {
            // Only observe the JM if we think it's in bad state
            observeJmDeployment(ctx);
        }

        if (isJmDeploymentReady(resource)) {
            // Only observe the cluster if JM is ready
            observeFlinkCluster(ctx);
        }

        if (isJmDeploymentReady(resource)) {
            observeClusterInfo(ctx);
        }

        clearErrorsIfDeploymentIsHealthy(resource);
    }

    protected void observeJmDeployment(FlinkResourceContext<CR> ctx) {
        var resource = ctx.getResource();
        var resourceStatus = resource.getStatus();
        JobManagerDeploymentStatus previousJmStatus =
                resource.getStatus().getKubernetesDeploymentStatus();
        var jmName = getJmDescription();

        if (isSuspendedJob(resource)) {
            logger.debug("Skipping observe step for suspended {} deployments", jmName);
            return;
        }

        logger.info(
                "Observing {} deployment. Previous status: {}", jmName, previousJmStatus.name());

        if (JobManagerDeploymentStatus.DEPLOYED_NOT_READY == previousJmStatus) {
            logger.info("{} deployment is ready", jmName);
            resource.getStatus().setKubernetesDeploymentStatus(JobManagerDeploymentStatus.READY);
            return;
        }

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
                        "{} deployment port is ready, waiting for the Flink REST API...", jmName);
                resource.getStatus()
                        .setKubernetesDeploymentStatus(
                                JobManagerDeploymentStatus.DEPLOYED_NOT_READY);
                return;
            }

            try {
                EventUtils.checkFailedCreate(status);
                // checking the pod is expensive; only do it when the deployment isn't ready
                EventUtils.checkContainerErrors(ctx.getKubernetesClient(), getJmPodList(ctx));
            } catch (DeploymentFailedException dfe) {
                // throw only when not already in error status to allow for spec update
                resourceStatus.getJobStatus().setState(JobStatus.RECONCILING);
                if (!JobManagerDeploymentStatus.ERROR.equals(
                        resource.getStatus().getKubernetesDeploymentStatus())) {
                    throw dfe;
                }
                return;
            }

            logger.info("{} is being deployed", jmName);
            resource.getStatus()
                    .setKubernetesDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
            return;
        }

        resource.getStatus().setKubernetesDeploymentStatus(JobManagerDeploymentStatus.MISSING);

        if (!ReconciliationUtils.isJobInTerminalState(resourceStatus)) {
            resourceStatus.getJobStatus().setState(JobStatus.RECONCILING);
        }

        if (previousJmStatus != JobManagerDeploymentStatus.MISSING
                && previousJmStatus != JobManagerDeploymentStatus.ERROR) {
            onMissingDeployment(ctx);
        }
    }

    protected boolean isJmDeploymentReady(CR resource) {
        return resource.getStatus().getKubernetesDeploymentStatus()
                == JobManagerDeploymentStatus.READY;
    }

    protected void clearErrorsIfDeploymentIsHealthy(CR resource) {
        var status = resource.getStatus();
        var reconciliationStatus = status.getReconciliationStatus();
        if (resource.getStatus().getKubernetesDeploymentStatus() == JobManagerDeploymentStatus.READY
                && !JobStatus.FAILED.equals(status.getJobStatus().getState())
                && reconciliationStatus.isLastReconciledSpecStable()) {
            status.setError(null);
        }
    }

    protected boolean isSuspendedJob(CR resource) {
        var jobSpec = resource.getSpec().getJob();
        if (jobSpec == null) {
            return false;
        }

        var lastReconciledSpec =
                resource.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();

        return resource.getStatus().getKubernetesDeploymentStatus()
                        == JobManagerDeploymentStatus.MISSING
                && jobSpec.getState() == JobState.SUSPENDED
                && lastReconciledSpec != null
                && lastReconciledSpec.getJob().getState() == JobState.SUSPENDED;
    }

    private void onMissingDeployment(FlinkResourceContext<CR> ctx) {
        String err = "Missing " + getJmDescription() + " deployment";
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
    protected boolean isPendingUpgradeDeployed(FlinkResourceContext<CR> ctx) {
        var resource = ctx.getResource();

        if (resource.getStatus().getKubernetesDeploymentStatus()
                != JobManagerDeploymentStatus.MISSING) {
            // Every deploy persists MISSING before creating the new JobManager Deployment, so a
            // non-MISSING JobManager is the old one and the pending upgrade was not submitted yet
            return false;
        }

        // We are performing a full upgrade
        Optional<Deployment> depOpt = ctx.getJosdkContext().getSecondaryResource(Deployment.class);

        if (depOpt.isEmpty()) {
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

        Long upgradeTargetGeneration = ReconciliationUtils.getUpgradeTargetGeneration(resource);

        if (deployedGeneration.equals(upgradeTargetGeneration)) {
            logger.info("Pending upgrade is already deploying, updating status.");
            resource.getStatus()
                    .setKubernetesDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
            return true;
        } else {
            logger.warn(
                    "Running deployment generation {} doesn't match upgrade target generation {}.",
                    deployedGeneration,
                    upgradeTargetGeneration);
            return false;
        }
    }

    /**
     * @param ctx the context with which the operation is executed
     * @return the pods of the JobManager Deployment
     */
    protected abstract PodList getJmPodList(FlinkResourceContext<CR> ctx);

    /**
     * @return how the JobManager is described in logs, events and errors
     */
    protected abstract String getJmDescription();

    /**
     * Observe the resource when the cluster is ready, to reflect the state of the cluster in the
     * status of the resource.
     *
     * @param ctx the context with which the operation is executed
     */
    protected abstract void observeFlinkCluster(FlinkResourceContext<CR> ctx);

    /**
     * Observe the cluster info when the cluster is ready.
     *
     * @param ctx the context with which the operation is executed
     */
    protected abstract void observeClusterInfo(FlinkResourceContext<CR> ctx);
}
