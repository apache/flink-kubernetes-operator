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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventUtils;

import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The base observer. */
public abstract class AbstractDeploymentObserver implements Observer<FlinkDeployment> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final KubernetesClient kubernetesClient;
    protected final FlinkService flinkService;
    protected final FlinkConfigManager configManager;

    public AbstractDeploymentObserver(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
        this.configManager = configManager;
    }

    @Override
    public void observe(FlinkDeployment flinkApp, Context context) {
        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                flinkApp.getStatus().getReconciliationStatus();
        FlinkDeploymentSpec lastReconciledSpec =
                reconciliationStatus.deserializeLastReconciledSpec();

        // Nothing has been launched so skip observing
        if (lastReconciledSpec == null
                || reconciliationStatus.getState() == ReconciliationState.ROLLING_BACK
                || reconciliationStatus.getState() == ReconciliationState.UPGRADING) {
            return;
        }

        Configuration observeConfig = configManager.getObserveConfig(flinkApp);
        if (!isJmDeploymentReady(flinkApp)) {
            observeJmDeployment(flinkApp, context, observeConfig);
        }

        if (isJmDeploymentReady(flinkApp)) {
            if (observeFlinkCluster(flinkApp, context, observeConfig)) {
                if (reconciliationStatus.getState() != ReconciliationState.ROLLED_BACK) {
                    reconciliationStatus.markReconciledSpecAsStable();
                }
            }
        }

        if (isJmDeploymentReady(flinkApp)) {
            observeClusterInfo(flinkApp, observeConfig);
        }

        if (!ReconciliationUtils.isJobRunning(flinkApp.getStatus())) {
            if (flinkApp.getStatus().getJobStatus().getSavepointInfo().resetTrigger()) {
                logger.error("Job is not running, cancelling savepoint operation");
                EventUtils.createOrUpdateEvent(
                        flinkService.getKubernetesClient(),
                        flinkApp,
                        EventUtils.Type.Warning,
                        "SavepointError",
                        "Savepoint failed for savepointTriggerNonce: "
                                + flinkApp.getSpec().getJob().getSavepointTriggerNonce(),
                        EventUtils.Component.Operator);
            }
        }

        clearErrorsIfDeploymentIsHealthy(flinkApp);
    }

    private void observeClusterInfo(FlinkDeployment flinkApp, Configuration configuration) {
        if (!flinkApp.getStatus().getClusterInfo().isEmpty()) {
            return;
        }
        try {
            Map<String, String> clusterInfo = flinkService.getClusterInfo(configuration);
            flinkApp.getStatus().setClusterInfo(clusterInfo);
            logger.debug("ClusterInfo: {}", clusterInfo);
        } catch (Exception e) {
            logger.error("Exception while fetching cluster info", e);
        }
    }

    protected void observeJmDeployment(
            FlinkDeployment flinkApp, Context context, Configuration effectiveConfig) {
        FlinkDeploymentStatus deploymentStatus = flinkApp.getStatus();
        JobManagerDeploymentStatus previousJmStatus =
                deploymentStatus.getJobManagerDeploymentStatus();

        if (isSuspendedJob(flinkApp)) {
            logger.debug("Skipping observe step for suspended application deployments.");
            return;
        }

        flinkApp.getStatus().setClusterInfo(new HashMap<>());

        logger.info(
                "Observing JobManager deployment. Previous status: {}", previousJmStatus.name());

        if (JobManagerDeploymentStatus.DEPLOYED_NOT_READY == previousJmStatus) {
            logger.info("JobManager deployment is ready");
            deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
            return;
        }

        Optional<Deployment> deployment = getSecondaryResource(flinkApp, context);
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
                checkCrashLoopBackoff(flinkApp, effectiveConfig);
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

    private Optional<Deployment> getSecondaryResource(FlinkDeployment flinkApp, Context context) {
        return context.getSecondaryResource(
                Deployment.class,
                configManager.getOperatorConfiguration().getWatchedNamespaces().size() > 1
                        ? flinkApp.getMetadata().getNamespace()
                        : null);
    }

    private void checkFailedCreate(DeploymentStatus status) {
        List<DeploymentCondition> conditions = status.getConditions();
        for (DeploymentCondition dc : conditions) {
            if ("FailedCreate".equals(dc.getReason()) && "ReplicaFailure".equals(dc.getType())) {
                throw new DeploymentFailedException(dc);
            }
        }
    }

    private void checkCrashLoopBackoff(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        PodList jmPods = flinkService.getJmPodList(flinkApp, effectiveConfig);
        for (Pod pod : jmPods.getItems()) {
            for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
                ContainerStateWaiting csw = cs.getState().getWaiting();
                if (csw != null
                        && DeploymentFailedException.REASON_CRASH_LOOP_BACKOFF.equals(
                                csw.getReason())) {
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
        ReconciliationStatus reconciliationStatus = status.getReconciliationStatus();
        if (status.getJobManagerDeploymentStatus() != JobManagerDeploymentStatus.ERROR
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

    private void onMissingDeployment(FlinkDeployment deployment) {
        String err = "Missing JobManager deployment";
        logger.error(err);
        ReconciliationUtils.updateForReconciliationError(deployment, err);
        EventUtils.createOrUpdateEvent(
                kubernetesClient,
                deployment,
                EventUtils.Type.Warning,
                "Missing",
                err,
                EventUtils.Component.JobManagerDeployment);
    }

    /**
     * Observe the flinkApp status when the cluster is ready. It will be implemented by child class
     * to reflect the changed status on the flinkApp resource.
     *
     * @param flinkApp the target flinkDeployment resource
     * @param context the context with which the operation is executed
     * @param deployedConfig config that is deployed on the Flink cluster
     * @return true if cluster state is stable
     */
    protected abstract boolean observeFlinkCluster(
            FlinkDeployment flinkApp, Context context, Configuration deployedConfig);
}
