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
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;

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
import java.util.concurrent.CompletableFuture;
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
                ExternalServiceDecorator.getNamespacedExternalServiceName(clusterId, namespace);
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
                    final String namespace = conf.getString(KubernetesConfigOptions.NAMESPACE);
                    final String clusterId = clusterClient.getClusterId();
                    FlinkUtils.deleteCluster(namespace, clusterId, kubernetesClient, false);
                    break;
                default:
                    throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
            }
        }
        FlinkUtils.waitForClusterShutdown(kubernetesClient, conf);
        return savepointOpt;
    }

    public void stopSessionCluster(
            FlinkDeployment deployment, Configuration conf, boolean deleteHaData) {
        FlinkUtils.deleteCluster(deployment, kubernetesClient, deleteHaData);
        FlinkUtils.waitForClusterShutdown(kubernetesClient, conf);
    }

    public void triggerSavepoint(FlinkDeployment deployment, Configuration conf) throws Exception {
        LOG.info("Triggering savepoint on " + deployment.getMetadata().getName());
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            SavepointTriggerHeaders savepointTriggerHeaders = SavepointTriggerHeaders.getInstance();
            SavepointTriggerMessageParameters savepointTriggerMessageParameters =
                    savepointTriggerHeaders.getUnresolvedMessageParameters();
            savepointTriggerMessageParameters.jobID.resolve(
                    JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId()));

            TriggerResponse response =
                    clusterClient
                            .sendRequest(
                                    savepointTriggerHeaders,
                                    savepointTriggerMessageParameters,
                                    new SavepointTriggerRequestBody(null, false))
                            .get();
            LOG.info("Savepoint triggered: " + response.getTriggerId().toHexString());

            org.apache.flink.kubernetes.operator.crd.status.SavepointInfo savepointInfo =
                    deployment.getStatus().getJobStatus().getSavepointInfo();

            savepointInfo.setTriggerId(response.getTriggerId().toHexString());
            savepointInfo.setTriggerTimestamp(System.currentTimeMillis());
        }
    }

    public SavepointFetchResult fetchSavepointInfo(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        LOG.info(
                "Fetching savepoint result with triggerId: "
                        + deployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId());
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            SavepointStatusHeaders savepointStatusHeaders = SavepointStatusHeaders.getInstance();
            SavepointStatusMessageParameters savepointStatusMessageParameters =
                    savepointStatusHeaders.getUnresolvedMessageParameters();
            savepointStatusMessageParameters.jobIdPathParameter.resolve(
                    JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId()));
            savepointStatusMessageParameters.triggerIdPathParameter.resolve(
                    TriggerId.fromHexString(
                            deployment
                                    .getStatus()
                                    .getJobStatus()
                                    .getSavepointInfo()
                                    .getTriggerId()));
            CompletableFuture<AsynchronousOperationResult<SavepointInfo>> response =
                    clusterClient.sendRequest(
                            savepointStatusHeaders,
                            savepointStatusMessageParameters,
                            EmptyRequestBody.getInstance());

            if (response.get() == null || response.get().resource() == null) {
                return SavepointFetchResult.notTriggered();
            }

            if (response.get().resource().getLocation() == null) {
                if (response.get().resource().getFailureCause() != null) {
                    LOG.error("Savepoint error", response.get().resource().getFailureCause());
                    return SavepointFetchResult.error(
                            response.get().resource().getFailureCause().getMessage());
                } else {
                    return SavepointFetchResult.pending();
                }
            }

            Savepoint savepoint =
                    new Savepoint(
                            System.currentTimeMillis(), response.get().resource().getLocation());
            LOG.info("Savepoint result: " + savepoint);
            return SavepointFetchResult.completed(savepoint);
        }
    }
}
