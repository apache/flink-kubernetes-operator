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

import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** BaseReconciler with functionality that is common to job and session modes. */
public abstract class AbstractDeploymentReconciler implements Reconciler<FlinkDeployment> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDeploymentReconciler.class);

    protected final FlinkConfigManager configManager;
    protected final KubernetesClient kubernetesClient;
    protected final FlinkService flinkService;

    public AbstractDeploymentReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
        this.configManager = configManager;
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkApp, Context context) {
        shutdown(flinkApp);
        return DeleteControl.defaultDelete();
    }

    protected boolean initiateRollBack(FlinkDeploymentStatus status) {
        ReconciliationStatus<?> reconciliationStatus = status.getReconciliationStatus();
        if (reconciliationStatus.getState() != ReconciliationState.ROLLING_BACK) {
            LOG.warn("Preparing to roll back to last stable spec.");
            if (status.getError() == null) {
                status.setError(
                        "Deployment is not ready within the configured timeout, rolling back.");
            }
            reconciliationStatus.setState(ReconciliationState.ROLLING_BACK);
            return true;
        }
        return false;
    }

    protected boolean newSpecIsAlreadyDeployed(FlinkDeployment flinkApp) {
        FlinkDeploymentSpec deployedSpec = ReconciliationUtils.getDeployedSpec(flinkApp);
        if (flinkApp.getSpec().equals(deployedSpec)) {
            LOG.info(
                    "The new spec matches the currently deployed last stable spec. No upgrade needed.");
            ReconciliationUtils.updateForSpecReconciliationSuccess(
                    flinkApp,
                    deployedSpec.getJob() != null ? deployedSpec.getJob().getState() : null);
            return true;
        }
        return false;
    }

    protected abstract void shutdown(FlinkDeployment flinkApp);
}
