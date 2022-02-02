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

package org.apache.flink.kubernetes.operator.controller.reconciler;

import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.KubernetesUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconciler responsible for handling the session cluster lifecycle according to the desired and
 * current states.
 */
public class SessionReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(SessionReconciler.class);

    private final KubernetesClient kubernetesClient;

    public SessionReconciler(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    public boolean reconcile(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        if (flinkApp.getStatus() == null) {
            flinkApp.setStatus(new FlinkDeploymentStatus());
            try {
                deployFlinkSession(flinkApp, effectiveConfig);
                KubernetesUtils.deployIngress(flinkApp, effectiveConfig, kubernetesClient);
                return true;
            } catch (Exception e) {
                LOG.error("Error while deploying " + flinkApp.getMetadata().getName());
                return false;
            }
        }

        boolean specChanged = !flinkApp.getSpec().equals(flinkApp.getStatus().getSpec());

        if (specChanged) {
            if (flinkApp.getStatus().getSpec().getJob() != null) {
                throw new RuntimeException("Cannot switch from job to session cluster");
            }
            upgradeSessionCluster(flinkApp, effectiveConfig);
        }
        return true;
    }

    private void deployFlinkSession(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        LOG.info("Deploying session cluster {}", flinkApp.getMetadata().getName());
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ClusterClientFactory<String> kubernetesClusterClientFactory =
                clusterClientServiceLoader.getClusterClientFactory(effectiveConfig);
        try (final ClusterDescriptor<String> kubernetesClusterDescriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(effectiveConfig)) {
            kubernetesClusterDescriptor.deploySessionCluster(
                    kubernetesClusterClientFactory.getClusterSpecification(effectiveConfig));
        }
        LOG.info("Session cluster {} deployed", flinkApp.getMetadata().getName());
    }

    private void upgradeSessionCluster(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        FlinkUtils.deleteCluster(flinkApp, kubernetesClient);
        FlinkUtils.waitForClusterShutdown(kubernetesClient, effectiveConfig);
        deployFlinkSession(flinkApp, effectiveConfig);
    }
}
