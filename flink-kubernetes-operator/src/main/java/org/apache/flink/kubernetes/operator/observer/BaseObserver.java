/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.service.FlinkService;

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
import java.util.Optional;

/** The base observer. */
public abstract class BaseObserver implements Observer {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final String JOB_STATE_UNKNOWN = "UNKNOWN";

    protected final FlinkService flinkService;
    protected final FlinkOperatorConfiguration operatorConfiguration;

    public BaseObserver(
            FlinkService flinkService, FlinkOperatorConfiguration operatorConfiguration) {
        this.flinkService = flinkService;
        this.operatorConfiguration = operatorConfiguration;
    }

    protected void observeJmDeployment(
            FlinkDeployment flinkApp, Context context, Configuration effectiveConfig) {
        FlinkDeploymentStatus deploymentStatus = flinkApp.getStatus();
        JobManagerDeploymentStatus previousJmStatus =
                deploymentStatus.getJobManagerDeploymentStatus();

        if (JobManagerDeploymentStatus.DEPLOYED_NOT_READY == previousJmStatus) {
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
                        "JobManager deployment {} in namespace {} port ready, waiting for the REST API...",
                        flinkApp.getMetadata().getName(),
                        flinkApp.getMetadata().getNamespace());
                deploymentStatus.setJobManagerDeploymentStatus(
                        JobManagerDeploymentStatus.DEPLOYED_NOT_READY);
                return;
            }
            logger.info(
                    "JobManager deployment {} in namespace {} exists but not ready, status {}",
                    flinkApp.getMetadata().getName(),
                    flinkApp.getMetadata().getNamespace(),
                    status);

            List<DeploymentCondition> conditions = status.getConditions();
            for (DeploymentCondition dc : conditions) {
                if ("FailedCreate".equals(dc.getReason())
                        && "ReplicaFailure".equals(dc.getType())) {
                    // throw only when not already in error status to allow for spec update
                    if (!JobManagerDeploymentStatus.ERROR.equals(
                            deploymentStatus.getJobManagerDeploymentStatus())) {
                        throw new DeploymentFailedException(
                                DeploymentFailedException.COMPONENT_JOBMANAGER, dc);
                    }
                    return;
                }
            }

            // checking the pod is expensive; only do it when the deployment isn't ready
            try {
                checkCrashLoopBackoff(flinkApp, effectiveConfig);
            } catch (DeploymentFailedException dfe) {
                if (!JobManagerDeploymentStatus.ERROR.equals(
                        deploymentStatus.getJobManagerDeploymentStatus())) {
                    throw dfe;
                }
                return;
            }

            deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
            return;
        }

        deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
    }

    private Optional<Deployment> getSecondaryResource(FlinkDeployment flinkApp, Context context) {
        return context.getSecondaryResource(
                Deployment.class,
                operatorConfiguration.getWatchedNamespaces().size() > 1
                        ? flinkApp.getMetadata().getNamespace()
                        : null);
    }

    private void checkCrashLoopBackoff(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        PodList jmPods = flinkService.getJmPodList(flinkApp, effectiveConfig);
        for (Pod pod : jmPods.getItems()) {
            for (ContainerStatus cs : pod.getStatus().getContainerStatuses()) {
                ContainerStateWaiting csw = cs.getState().getWaiting();
                if (DeploymentFailedException.REASON_CRASH_LOOP_BACKOFF.equals(csw.getReason())) {
                    logger.warn(
                            "JobManager pod from deployment {} in namespace {} fails: {} {}",
                            flinkApp.getMetadata().getName(),
                            flinkApp.getMetadata().getNamespace(),
                            csw.getReason(),
                            csw.getMessage());
                    throw new DeploymentFailedException(
                            DeploymentFailedException.COMPONENT_JOBMANAGER, "Warning", csw);
                }
            }
        }
    }

    protected boolean isClusterReady(FlinkDeployment dep) {
        return dep.getStatus().getJobManagerDeploymentStatus() == JobManagerDeploymentStatus.READY;
    }
}
