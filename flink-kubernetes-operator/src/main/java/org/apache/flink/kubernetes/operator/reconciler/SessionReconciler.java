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
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconciler responsible for handling the session cluster lifecycle according to the desired and
 * current states.
 */
public class SessionReconciler extends BaseReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(SessionReconciler.class);

    public SessionReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkOperatorConfiguration operatorConfiguration) {
        super(kubernetesClient, flinkService, operatorConfiguration);
    }

    @Override
    public void reconcile(FlinkDeployment flinkApp, Context context, Configuration effectiveConfig)
            throws Exception {

        FlinkDeploymentSpec lastReconciledSpec =
                flinkApp.getStatus().getReconciliationStatus().getLastReconciledSpec();

        if (lastReconciledSpec == null) {
            flinkService.submitSessionCluster(flinkApp, effectiveConfig);
            flinkApp.getStatus()
                    .setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
            IngressUtils.updateIngressRules(flinkApp, effectiveConfig, kubernetesClient);
        }

        boolean specChanged = !flinkApp.getSpec().equals(lastReconciledSpec);
        if (specChanged) {
            upgradeSessionCluster(flinkApp, effectiveConfig);
            IngressUtils.updateIngressRules(flinkApp, effectiveConfig, kubernetesClient);
        }
        ReconciliationUtils.updateForSpecReconciliationSuccess(flinkApp, null);
    }

    private void upgradeSessionCluster(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        LOG.info("Upgrading session cluster");
        flinkService.stopSessionCluster(flinkApp, effectiveConfig, false);
        flinkService.submitSessionCluster(flinkApp, effectiveConfig);
        flinkApp.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
    }

    @Override
    protected void shutdown(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        LOG.info("Stopping session cluster");
        flinkService.stopSessionCluster(flinkApp, effectiveConfig, true);
    }
}
