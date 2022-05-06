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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconciler responsible for handling the session cluster lifecycle according to the desired and
 * current states.
 */
public class SessionReconciler extends AbstractDeploymentReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(SessionReconciler.class);

    public SessionReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager) {
        super(kubernetesClient, flinkService, configManager);
    }

    @Override
    public void reconcile(FlinkDeployment flinkApp, Context context) throws Exception {
        FlinkDeploymentStatus status = flinkApp.getStatus();
        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                status.getReconciliationStatus();
        FlinkDeploymentSpec lastReconciledSpec =
                reconciliationStatus.deserializeLastReconciledSpec();
        FlinkDeploymentSpec currentDeploySpec = flinkApp.getSpec();

        if (lastReconciledSpec == null) {
            Configuration conf =
                    configManager.getDeployConfig(flinkApp.getMetadata(), currentDeploySpec);
            flinkService.submitSessionCluster(conf);
            status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
            IngressUtils.updateIngressRules(
                    flinkApp.getMetadata(), currentDeploySpec, conf, kubernetesClient);
            ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, null);
            return;
        }

        Configuration observeConfig = configManager.getObserveConfig(flinkApp);
        boolean specChanged = !currentDeploySpec.equals(lastReconciledSpec);
        if (specChanged) {
            upgradeSessionCluster(
                    flinkApp,
                    currentDeploySpec,
                    configManager.getDeployConfig(flinkApp.getMetadata(), currentDeploySpec));
            ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, null);
        } else if (ReconciliationUtils.shouldRollBack(reconciliationStatus, observeConfig)) {
            rollbackSessionCluster(flinkApp);
        } else if (ReconciliationUtils.deploymentRecoveryEnabled(observeConfig)
                && status.getJobManagerDeploymentStatus() == JobManagerDeploymentStatus.MISSING) {
            recoverSession(flinkApp, observeConfig);
        }
    }

    private void upgradeSessionCluster(
            FlinkDeployment deployment,
            FlinkDeploymentSpec deploySpec,
            Configuration effectiveConfig)
            throws Exception {
        LOG.info("Upgrading session cluster");
        flinkService.deleteClusterDeployment(
                deployment.getMetadata(), deployment.getStatus(), false);
        FlinkUtils.waitForClusterShutdown(
                kubernetesClient,
                effectiveConfig,
                configManager
                        .getOperatorConfiguration()
                        .getFlinkShutdownClusterTimeout()
                        .toSeconds());
        flinkService.submitSessionCluster(effectiveConfig);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
        IngressUtils.updateIngressRules(
                deployment.getMetadata(), deploySpec, effectiveConfig, kubernetesClient);
    }

    private void recoverSession(FlinkDeployment deployment, Configuration effectiveConfig)
            throws Exception {
        flinkService.submitSessionCluster(effectiveConfig);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
    }

    private void rollbackSessionCluster(FlinkDeployment deployment) throws Exception {
        FlinkDeploymentStatus status = deployment.getStatus();
        if (initiateRollBack(status)) {
            return;
        }

        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                status.getReconciliationStatus();
        FlinkDeploymentSpec rollbackSpec = reconciliationStatus.deserializeLastStableSpec();
        Configuration rollbackConfig =
                configManager.getDeployConfig(deployment.getMetadata(), rollbackSpec);
        upgradeSessionCluster(deployment, rollbackSpec, rollbackConfig);
        reconciliationStatus.setState(ReconciliationState.ROLLED_BACK);
    }

    @Override
    protected void shutdown(FlinkDeployment deployment) {
        LOG.info("Stopping session cluster");
        flinkService.deleteClusterDeployment(
                deployment.getMetadata(), deployment.getStatus(), true);
    }
}
