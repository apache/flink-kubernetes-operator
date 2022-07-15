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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;

/**
 * Implementation of {@link FlinkService} submitting and interacting with Native Kubernetes Flink
 * clusters and jobs.
 */
public class NativeFlinkService extends AbstractFlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(NativeFlinkService.class);

    public NativeFlinkService(KubernetesClient kubernetesClient, FlinkConfigManager configManager) {
        super(kubernetesClient, configManager);
    }

    @Override
    public void submitApplicationCluster(
            JobSpec jobSpec, Configuration conf, boolean requireHaMetadata) throws Exception {
        LOG.info(
                "Deploying application cluster{}",
                requireHaMetadata ? " requiring last-state from HA metadata" : "");
        if (FlinkUtils.isKubernetesHAActivated(conf)) {
            final String clusterId = conf.get(KubernetesConfigOptions.CLUSTER_ID);
            final String namespace = conf.get(KubernetesConfigOptions.NAMESPACE);
            // Delete the job graph in the HA ConfigMaps so that the newly changed job config(e.g.
            // parallelism) could take effect
            FlinkUtils.deleteJobGraphInKubernetesHA(clusterId, namespace, kubernetesClient);
        }
        if (requireHaMetadata) {
            validateHaMetadataExists(conf);
        }
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ApplicationDeployer deployer =
                new ApplicationClusterDeployer(clusterClientServiceLoader);

        final ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(
                        jobSpec.getArgs() != null ? jobSpec.getArgs() : new String[0],
                        jobSpec.getEntryClass());

        deployer.run(conf, applicationConfiguration);
        LOG.info("Application cluster successfully deployed");
    }

    private void validateHaMetadataExists(Configuration conf) {
        if (!isHaMetadataAvailable(conf)) {
            throw new DeploymentFailedException(
                    "HA metadata not available to restore from last state. "
                            + "It is possible that the job has finished or terminally failed, or the configmaps have been deleted. "
                            + "Manual restore required.",
                    "RestoreFailed");
        }
    }

    @Override
    public void submitSessionCluster(Configuration conf) throws Exception {
        LOG.info("Deploying session cluster");
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ClusterClientFactory<String> kubernetesClusterClientFactory =
                clusterClientServiceLoader.getClusterClientFactory(conf);
        try (final ClusterDescriptor<String> kubernetesClusterDescriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(conf)) {
            kubernetesClusterDescriptor.deploySessionCluster(
                    kubernetesClusterClientFactory.getClusterSpecification(conf));
        }
        LOG.info("Session cluster successfully deployed");
    }

    @Override
    public void deleteClusterDeployment(
            ObjectMeta meta, FlinkDeploymentStatus status, boolean deleteHaData) {
        deleteCluster(
                status,
                meta,
                deleteHaData,
                configManager
                        .getOperatorConfiguration()
                        .getFlinkShutdownClusterTimeout()
                        .toSeconds());
    }

    @Override
    protected PodList getJmPodList(String namespace, String clusterId) {
        return kubernetesClient
                .pods()
                .inNamespace(namespace)
                .withLabels(KubernetesUtils.getJobManagerSelectors(clusterId))
                .list();
    }

    /**
     * Delete Flink kubernetes cluster by deleting the kubernetes resources directly. Optionally
     * allows deleting the native kubernetes HA resources as well.
     *
     * @param status Deployment status object
     * @param meta ObjectMeta of the deployment
     * @param deleteHaConfigmaps Flag to indicate whether k8s HA metadata should be removed as well
     * @param shutdownTimeout maximum time allowed for cluster shutdown
     */
    private void deleteCluster(
            FlinkDeploymentStatus status,
            ObjectMeta meta,
            boolean deleteHaConfigmaps,
            long shutdownTimeout) {

        String namespace = meta.getNamespace();
        String clusterId = meta.getName();

        LOG.info(
                "Deleting JobManager deployment {}.",
                deleteHaConfigmaps ? "and HA metadata" : "while preserving HA metadata");
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(KubernetesUtils.getDeploymentName(clusterId))
                .cascading(true)
                .delete();

        if (deleteHaConfigmaps) {
            // We need to wait for cluster shutdown otherwise HA configmaps might be recreated
            waitForClusterShutdown(namespace, clusterId, shutdownTimeout);
            kubernetesClient
                    .configMaps()
                    .inNamespace(namespace)
                    .withLabels(
                            KubernetesUtils.getConfigMapLabels(
                                    clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                    .delete();
        }
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        status.getJobStatus().setState(JobStatus.FINISHED.name());
    }
}
