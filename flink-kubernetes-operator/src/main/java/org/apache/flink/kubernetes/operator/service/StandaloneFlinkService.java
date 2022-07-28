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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.Mode;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.kubeclient.Fabric8FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.standalone.KubernetesStandaloneClusterDescriptor;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;

/**
 * Implementation of {@link FlinkService} submitting and interacting with Standalone Kubernetes
 * Flink clusters and jobs.
 */
public class StandaloneFlinkService extends AbstractFlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(StandaloneFlinkService.class);

    public StandaloneFlinkService(
            KubernetesClient kubernetesClient, FlinkConfigManager configManager) {
        super(kubernetesClient, configManager);
    }

    @Override
    protected void deployApplicationCluster(JobSpec jobSpec, Configuration conf) throws Exception {
        LOG.info("Deploying application cluster");
        submitClusterInternal(conf, Mode.APPLICATION);
        LOG.info("Application cluster successfully deployed");
    }

    @Override
    public void submitSessionCluster(Configuration conf) throws Exception {
        LOG.info("Deploying session cluster");
        submitClusterInternal(conf, Mode.SESSION);
        LOG.info("Session cluster successfully deployed");
    }

    @Override
    public void cancelJob(FlinkDeployment deployment, UpgradeMode upgradeMode, Configuration conf)
            throws Exception {
        cancelJob(deployment, upgradeMode, conf, true);
    }

    @Override
    public void deleteClusterDeployment(
            ObjectMeta meta, FlinkDeploymentStatus status, boolean deleteHaData) {
        deleteClusterInternal(meta, deleteHaData);
    }

    @Override
    protected PodList getJmPodList(String namespace, String clusterId) {
        return kubernetesClient
                .pods()
                .inNamespace(namespace)
                .withLabels(StandaloneKubernetesUtils.getJobManagerSelectors(clusterId))
                .list();
    }

    @VisibleForTesting
    protected FlinkStandaloneKubeClient createNamespacedKubeClient(
            Configuration configuration, String namespace) {
        final int poolSize =
                configuration.get(KubernetesConfigOptions.KUBERNETES_CLIENT_IO_EXECUTOR_POOL_SIZE);

        ExecutorService executorService =
                Executors.newFixedThreadPool(
                        poolSize,
                        new ExecutorThreadFactory("flink-kubeclient-io-for-standalone-service"));

        return new Fabric8FlinkStandaloneKubeClient(
                configuration,
                Fabric8FlinkStandaloneKubeClient.createNamespacedKubeClient(namespace),
                executorService);
    }

    private void submitClusterInternal(Configuration conf, Mode mode)
            throws ClusterDeploymentException {
        final String namespace = conf.get(KubernetesConfigOptions.NAMESPACE);

        FlinkStandaloneKubeClient client = createNamespacedKubeClient(conf, namespace);
        try (final KubernetesStandaloneClusterDescriptor kubernetesClusterDescriptor =
                new KubernetesStandaloneClusterDescriptor(conf, client)) {
            switch (mode) {
                case APPLICATION:
                    kubernetesClusterDescriptor.deployApplicationCluster(
                            getClusterSpecification(conf),
                            ApplicationConfiguration.fromConfiguration(conf));
                    break;
                case SESSION:
                    kubernetesClusterDescriptor.deploySessionCluster(getClusterSpecification(conf));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Unsupported running mode: %s", mode));
            }
        }
    }

    private ClusterSpecification getClusterSpecification(Configuration conf) {
        return new KubernetesClusterClientFactory().getClusterSpecification(conf);
    }

    private void deleteClusterInternal(ObjectMeta meta, boolean deleteHaConfigmaps) {
        final String clusterId = meta.getName();
        final String namespace = meta.getNamespace();

        LOG.info("Deleting Flink Standalone cluster TM resources");
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId))
                .cascading(true)
                .delete();

        LOG.info("Deleting Flink Standalone cluster JM resources");
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(StandaloneKubernetesUtils.getJobManagerDeploymentName(clusterId))
                .cascading(true)
                .delete();

        if (deleteHaConfigmaps) {
            // We need to wait for cluster shutdown otherwise HA configmaps might be recreated
            waitForClusterShutdown(
                    namespace,
                    clusterId,
                    configManager
                            .getOperatorConfiguration()
                            .getFlinkShutdownClusterTimeout()
                            .toSeconds());
            kubernetesClient
                    .configMaps()
                    .inNamespace(namespace)
                    .withLabels(
                            KubernetesUtils.getConfigMapLabels(
                                    clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                    .delete();
        }
    }

    @Override
    public boolean scale(ObjectMeta meta, JobSpec jobSpec, Configuration conf) {
        if (conf.get(JobManagerOptions.SCHEDULER_MODE) == null) {
            LOG.info("Reactive scaling is not enabled");
            return false;
        }

        var clusterId = meta.getName();
        var namespace = meta.getNamespace();
        var name = StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId);
        var deployment =
                kubernetesClient.apps().deployments().inNamespace(namespace).withName(name);

        if (deployment == null || deployment.get() == null) {
            LOG.warn("TM Deployment ({}) not found", name);
            return false;
        }

        var actualReplicas = deployment.get().getSpec().getReplicas();
        var desiredReplicas =
                conf.get(StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS);
        if (actualReplicas != desiredReplicas) {
            LOG.info(
                    "Scaling TM replicas: actual({}) -> desired({})",
                    actualReplicas,
                    desiredReplicas);
            deployment.scale(desiredReplicas);
        } else {
            LOG.info(
                    "Not scaling TM replicas: actual({}) == desired({})",
                    actualReplicas,
                    desiredReplicas);
        }
        return true;
    }
}
