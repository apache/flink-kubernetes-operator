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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;

/** BaseReconciler with functionality that is common to job and session modes. */
public abstract class BaseReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(BaseReconciler.class);

    public static final int REFRESH_SECONDS = 60;
    public static final int PORT_READY_DELAY_SECONDS = 10;

    private final HashSet<String> jobManagerDeployments = new HashSet<>();

    protected final KubernetesClient kubernetesClient;
    protected final FlinkService flinkService;

    public BaseReconciler(KubernetesClient kubernetesClient, FlinkService flinkService) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
    }

    public boolean removeDeployment(FlinkDeployment flinkApp) {
        return jobManagerDeployments.remove(flinkApp.getMetadata().getUid());
    }

    public abstract UpdateControl<FlinkDeployment> reconcile(
            String operatorNamespace,
            FlinkDeployment flinkApp,
            Context context,
            Configuration effectiveConfig)
            throws Exception;

    protected JobManagerDeploymentStatus checkJobManagerDeployment(
            FlinkDeployment flinkApp, Context context, Configuration effectiveConfig) {
        if (!jobManagerDeployments.contains(flinkApp.getMetadata().getUid())) {
            Optional<Deployment> deployment = context.getSecondaryResource(Deployment.class);
            if (deployment.isPresent()) {
                DeploymentStatus status = deployment.get().getStatus();
                DeploymentSpec spec = deployment.get().getSpec();
                if (status != null
                        && status.getAvailableReplicas() != null
                        && spec.getReplicas().intValue() == status.getReplicas()
                        && spec.getReplicas().intValue() == status.getAvailableReplicas()) {
                    // typically it takes a few seconds for the REST server to be ready
                    if (flinkService.isJobManagerPortReady(effectiveConfig)) {
                        LOG.info(
                                "JobManager deployment {} in namespace {} is ready",
                                flinkApp.getMetadata().getName(),
                                flinkApp.getMetadata().getNamespace());
                        jobManagerDeployments.add(flinkApp.getMetadata().getUid());
                        if (flinkApp.getStatus().getJobStatus() != null) {
                            // pre-existing deployments on operator restart - proceed with
                            // reconciliation
                            return JobManagerDeploymentStatus.READY;
                        }
                    }
                    LOG.info(
                            "JobManager deployment {} in namespace {} port not ready",
                            flinkApp.getMetadata().getName(),
                            flinkApp.getMetadata().getNamespace());
                    return JobManagerDeploymentStatus.DEPLOYED_NOT_READY;
                }
                LOG.info(
                        "JobManager deployment {} in namespace {} not yet ready, status {}",
                        flinkApp.getMetadata().getName(),
                        flinkApp.getMetadata().getNamespace(),
                        status);

                return JobManagerDeploymentStatus.DEPLOYING;
            }
            return JobManagerDeploymentStatus.MISSING;
        }
        return JobManagerDeploymentStatus.READY;
    }

    /**
     * Shuts down the job and deletes all kubernetes resources including k8s HA resources. It will
     * first perform a graceful shutdown (cancel) before deleting the data if that is unsuccessful
     * in order to avoid leaking HA metadata to durable storage.
     *
     * <p>This feature is limited at the moment to cleaning up native kubernetes HA resources, other
     * HA providers like ZK need to be cleaned up manually after deletion.
     */
    public DeleteControl shutdownAndDelete(
            String operatorNamespace,
            FlinkDeployment flinkApp,
            Context context,
            Configuration effectiveConfig) {

        if (checkJobManagerDeployment(flinkApp, context, effectiveConfig)
                == JobManagerDeploymentStatus.READY) {
            shutdown(flinkApp, effectiveConfig);
        } else {
            FlinkUtils.deleteCluster(flinkApp, kubernetesClient, true);
        }
        removeDeployment(flinkApp);
        IngressUtils.updateIngressRules(
                flinkApp, effectiveConfig, operatorNamespace, kubernetesClient, true);

        return DeleteControl.defaultDelete();
    }

    protected abstract void shutdown(FlinkDeployment flinkApp, Configuration effectiveConfig);
}
