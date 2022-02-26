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
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.ApplicationDeployer;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.exception.InvalidDeploymentException;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Service for submitting and interacting with Flink clusters and jobs. */
public class FlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkService.class);

    private final NamespacedKubernetesClient kubernetesClient;

    public FlinkService(NamespacedKubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    public void submitApplicationCluster(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        LOG.info("Deploying application cluster {}", deployment.getMetadata().getName());
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ApplicationDeployer deployer =
                new ApplicationClusterDeployer(clusterClientServiceLoader);

        JobSpec jobSpec = deployment.getSpec().getJob();
        final ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(
                        jobSpec.getArgs() != null ? jobSpec.getArgs() : new String[0],
                        jobSpec.getEntryClass());

        deployer.run(conf, applicationConfiguration);
        LOG.info("Application cluster {} deployed", deployment.getMetadata().getName());
    }

    public void submitSessionCluster(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        LOG.info("Deploying session cluster {}", deployment.getMetadata().getName());
        final ClusterClientServiceLoader clusterClientServiceLoader =
                new DefaultClusterClientServiceLoader();
        final ClusterClientFactory<String> kubernetesClusterClientFactory =
                clusterClientServiceLoader.getClusterClientFactory(conf);
        try (final ClusterDescriptor<String> kubernetesClusterDescriptor =
                kubernetesClusterClientFactory.createClusterDescriptor(conf)) {
            kubernetesClusterDescriptor.deploySessionCluster(
                    kubernetesClusterClientFactory.getClusterSpecification(conf));
        }
        LOG.info("Session cluster {} deployed", deployment.getMetadata().getName());
    }

    public boolean isJobManagerPortReady(Configuration config) {
        final URI uri;
        try (ClusterClient<String> clusterClient = getClusterClient(config)) {
            uri = URI.create(clusterClient.getWebInterfaceURL());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        SocketAddress socketAddress = new InetSocketAddress(uri.getHost(), uri.getPort());
        Socket socket = new Socket();
        try {
            socket.connect(socketAddress, 1000);
            socket.close();
            return true;
        } catch (SocketTimeoutException ste) {
        } catch (IOException e) {
        }
        return false;
    }

    public Collection<JobStatusMessage> listJobs(Configuration conf) throws Exception {
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            return clusterClient.listJobs().get(10, TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    protected ClusterClient<String> getClusterClient(Configuration config) throws Exception {
        final String clusterId = config.get(KubernetesConfigOptions.CLUSTER_ID);
        final String namespace = config.get(KubernetesConfigOptions.NAMESPACE);
        final int port = config.getInteger(RestOptions.PORT);
        final String host =
                config.getString(
                        RestOptions.ADDRESS, String.format("%s-rest.%s", clusterId, namespace));
        final String restServerAddress = String.format("http://%s:%s", host, port);
        LOG.debug("Creating RestClusterClient({})", restServerAddress);
        return new RestClusterClient<>(
                config, clusterId, (c, e) -> new StandaloneClientHAServices(restServerAddress));
    }

    public Optional<String> cancelJob(JobID jobID, UpgradeMode upgradeMode, Configuration conf)
            throws Exception {
        Optional<String> savepointOpt = Optional.empty();
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            switch (upgradeMode) {
                case STATELESS:
                    clusterClient.cancel(jobID).get(1, TimeUnit.MINUTES);
                    break;
                case SAVEPOINT:
                    String savepoint =
                            clusterClient
                                    .stopWithSavepoint(jobID, false, null)
                                    .get(1, TimeUnit.MINUTES);
                    savepointOpt = Optional.of(savepoint);
                    break;
                case LAST_STATE:
                    if (!HighAvailabilityMode.isHighAvailabilityModeActivated(conf)) {
                        throw new InvalidDeploymentException(
                                "Job could not be upgraded with last-state while HA disabled");
                    }
                    final String namespace = conf.getString(KubernetesConfigOptions.NAMESPACE);
                    final String clusterId = clusterClient.getClusterId();
                    FlinkUtils.deleteCluster(namespace, clusterId, kubernetesClient);
                    break;
                default:
                    throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
            }
        }
        waitForClusterShutdown(conf);
        return savepointOpt;
    }

    public void stopSessionCluster(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        FlinkUtils.deleteCluster(deployment, kubernetesClient);
        waitForClusterShutdown(conf);
    }

    /** We need this due to the buggy flink kube cluster client behaviour for now. */
    private void waitForClusterShutdown(Configuration conf) throws Exception {
        Fabric8FlinkKubeClient flinkKubeClient =
                new Fabric8FlinkKubeClient(
                        conf, kubernetesClient, Executors.newSingleThreadExecutor());
        for (int i = 0; i < 60; i++) {
            if (!flinkKubeClient
                    .getRestEndpoint(conf.get(KubernetesConfigOptions.CLUSTER_ID))
                    .isPresent()) {
                break;
            }
            LOG.info("Waiting for cluster shutdown... ({})", i);
            Thread.sleep(1000);
        }
    }
}
