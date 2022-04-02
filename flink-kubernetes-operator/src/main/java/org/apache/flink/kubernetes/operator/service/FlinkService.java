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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.artifact.JarResolver;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarRunMessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.JarRunRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarRunResponseBody;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadResponseBody;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Service for submitting and interacting with Flink clusters and jobs. */
public class FlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkService.class);

    private final KubernetesClient kubernetesClient;
    private final FlinkOperatorConfiguration operatorConfiguration;
    private final JarResolver jarResolver;
    private final ExecutorService executorService;

    public FlinkService(
            KubernetesClient kubernetesClient, FlinkOperatorConfiguration operatorConfiguration) {
        this.kubernetesClient = kubernetesClient;
        this.operatorConfiguration = operatorConfiguration;
        this.jarResolver = new JarResolver();
        this.executorService =
                Executors.newFixedThreadPool(
                        4, new ExecutorThreadFactory("Flink-RestClusterClient-IO"));
    }

    public void submitApplicationCluster(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        if (FlinkUtils.isKubernetesHAActivated(conf)) {
            final String clusterId =
                    Preconditions.checkNotNull(conf.get(KubernetesConfigOptions.CLUSTER_ID));
            final String namespace =
                    Preconditions.checkNotNull(conf.get(KubernetesConfigOptions.NAMESPACE));
            // Delete the job graph in the HA ConfigMaps so that the newly changed job config(e.g.
            // parallelism) could take effect
            FlinkUtils.deleteJobGraphInKubernetesHA(clusterId, namespace, kubernetesClient);
        }
        LOG.info("Deploying application cluster");
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
        LOG.info("Application cluster successfully deployed");
    }

    public void submitSessionCluster(FlinkDeployment deployment, Configuration conf)
            throws Exception {
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

    public void submitJobToSessionCluster(FlinkSessionJob sessionJob, Configuration conf)
            throws Exception {
        var jarRunResponseBody = jarRun(sessionJob, jarUpload(sessionJob, conf), conf);
        String jobID = jarRunResponseBody.getJobId().toHexString();
        LOG.info("Submitted job: {} to session cluster.", jobID);
        sessionJob.getStatus().setJobStatus(JobStatus.builder().jobId(jobID).build());
    }

    private JarRunResponseBody jarRun(
            FlinkSessionJob sessionJob, JarUploadResponseBody response, Configuration conf) {
        String jarId =
                response.getFilename().substring(response.getFilename().lastIndexOf("/") + 1);
        // we generate jobID in advance to help deduplicate job submission.
        JobID jobID = new JobID();
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            JarRunHeaders headers = JarRunHeaders.getInstance();
            JarRunMessageParameters parameters = headers.getUnresolvedMessageParameters();
            parameters.jarIdPathParameter.resolve(jarId);
            JobSpec job = sessionJob.getSpec().getJob();
            JarRunRequestBody runRequestBody =
                    new JarRunRequestBody(
                            job.getEntryClass(),
                            null,
                            job.getArgs() == null ? null : Arrays.asList(job.getArgs()),
                            job.getParallelism() > 0 ? job.getParallelism() : null,
                            jobID,
                            null,
                            sessionJob.getSpec().getJob().getInitialSavepointPath());
            LOG.info("Submitting job: {} to session cluster.", jobID.toHexString());
            return clusterClient
                    .sendRequest(headers, parameters, runRequestBody)
                    .get(
                            operatorConfiguration.getFlinkClientTimeout().toSeconds(),
                            TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Failed to submit job to session cluster.", e);
            throw new FlinkRuntimeException(e);
        }
    }

    private JarUploadResponseBody jarUpload(FlinkSessionJob sessionJob, Configuration conf)
            throws Exception {
        Path path = jarResolver.resolve(sessionJob.getSpec().getJob().getJarURI());
        JarUploadHeaders headers = JarUploadHeaders.getInstance();
        String clusterId = sessionJob.getSpec().getClusterId();
        String namespace = sessionJob.getMetadata().getNamespace();
        int port = conf.getInteger(RestOptions.PORT);
        String host =
                ObjectUtils.firstNonNull(
                        operatorConfiguration.getFlinkServiceHostOverride(),
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                clusterId, namespace));

        try (RestClient restClient = new RestClient(conf, executorService)) {
            // TODO add method in flink#RestClusterClient to support upload jar.
            return restClient
                    .sendRequest(
                            host,
                            port,
                            headers,
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance(),
                            Collections.singletonList(
                                    new FileUpload(path, RestConstants.CONTENT_TYPE_JAR)))
                    .get(
                            operatorConfiguration.getFlinkClientTimeout().toSeconds(),
                            TimeUnit.SECONDS);
        }
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
            return clusterClient
                    .listJobs()
                    .get(
                            operatorConfiguration.getFlinkClientTimeout().getSeconds(),
                            TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    protected ClusterClient<String> getClusterClient(Configuration config) throws Exception {
        final String clusterId = config.get(KubernetesConfigOptions.CLUSTER_ID);
        final String namespace = config.get(KubernetesConfigOptions.NAMESPACE);
        final int port = config.getInteger(RestOptions.PORT);
        final String host =
                ObjectUtils.firstNonNull(
                        operatorConfiguration.getFlinkServiceHostOverride(),
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                clusterId, namespace));
        final String restServerAddress = String.format("http://%s:%s", host, port);
        LOG.debug("Creating RestClusterClient({})", restServerAddress);
        return new RestClusterClient<>(
                config, clusterId, (c, e) -> new StandaloneClientHAServices(restServerAddress));
    }

    public Optional<String> cancelJob(
            @Nullable JobID jobID, UpgradeMode upgradeMode, Configuration conf) throws Exception {
        Optional<String> savepointOpt = Optional.empty();
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            final String clusterId = clusterClient.getClusterId();
            switch (upgradeMode) {
                case STATELESS:
                    clusterClient
                            .cancel(jobID)
                            .get(
                                    operatorConfiguration.getCancelJobTimeout().toSeconds(),
                                    TimeUnit.SECONDS);
                    break;
                case SAVEPOINT:
                    final String savepointDirectory =
                            Preconditions.checkNotNull(
                                    conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
                    final long timeout =
                            conf.get(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT)
                                    .getSeconds();
                    try {
                        String savepoint =
                                clusterClient
                                        .stopWithSavepoint(jobID, false, savepointDirectory)
                                        .get(timeout, TimeUnit.SECONDS);
                        savepointOpt = Optional.of(savepoint);
                    } catch (TimeoutException exception) {
                        throw new FlinkException(
                                String.format(
                                        "Timed out stopping the job %s in Flink cluster %s with savepoint, "
                                                + "please configure a larger timeout via '%s'",
                                        jobID,
                                        clusterId,
                                        ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT.key()),
                                exception);
                    }
                    break;
                case LAST_STATE:
                    final String namespace = conf.getString(KubernetesConfigOptions.NAMESPACE);
                    FlinkUtils.deleteCluster(namespace, clusterId, kubernetesClient, false);
                    break;
                default:
                    throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
            }
        }
        FlinkUtils.waitForClusterShutdown(kubernetesClient, conf);
        return savepointOpt;
    }

    public void cancelSessionJob(JobID jobID, Configuration conf) throws Exception {
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            clusterClient
                    .cancel(jobID)
                    .get(operatorConfiguration.getCancelJobTimeout().toSeconds(), TimeUnit.SECONDS);
        }
    }

    public void stopSessionCluster(
            FlinkDeployment deployment, Configuration conf, boolean deleteHaData) {
        FlinkUtils.deleteCluster(deployment, kubernetesClient, deleteHaData);
        FlinkUtils.waitForClusterShutdown(kubernetesClient, conf);
    }

    public void triggerSavepoint(FlinkDeployment deployment, Configuration conf) throws Exception {
        LOG.info("Triggering new savepoint");
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            SavepointTriggerHeaders savepointTriggerHeaders = SavepointTriggerHeaders.getInstance();
            SavepointTriggerMessageParameters savepointTriggerMessageParameters =
                    savepointTriggerHeaders.getUnresolvedMessageParameters();
            savepointTriggerMessageParameters.jobID.resolve(
                    JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId()));

            final String savepointDirectory =
                    Preconditions.checkNotNull(conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
            final long timeout = operatorConfiguration.getFlinkClientTimeout().getSeconds();
            TriggerResponse response =
                    clusterClient
                            .sendRequest(
                                    savepointTriggerHeaders,
                                    savepointTriggerMessageParameters,
                                    new SavepointTriggerRequestBody(savepointDirectory, false))
                            .get(timeout, TimeUnit.SECONDS);
            LOG.info("Savepoint successfully triggered: " + response.getTriggerId().toHexString());

            org.apache.flink.kubernetes.operator.crd.status.SavepointInfo savepointInfo =
                    deployment.getStatus().getJobStatus().getSavepointInfo();
            savepointInfo.setTrigger(response.getTriggerId().toHexString());
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

    public PodList getJmPodList(FlinkDeployment deployment, Configuration conf) {
        final String namespace = conf.getString(KubernetesConfigOptions.NAMESPACE);
        final String clusterId;
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            clusterId = clusterClient.getClusterId();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return FlinkUtils.getJmPodList(kubernetesClient, namespace, clusterId);
    }
}
