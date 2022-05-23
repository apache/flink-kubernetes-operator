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
import org.apache.flink.api.common.JobStatus;
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
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.artifact.ArtifactManager;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
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
import org.apache.flink.runtime.state.memory.NonPersistentMetadataCheckpointStorageLocation;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteMessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarRunMessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.JarRunRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarRunResponseBody;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadResponseBody;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;

/** Service for submitting and interacting with Flink clusters and jobs. */
public class FlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkService.class);

    private final KubernetesClient kubernetesClient;
    private final ArtifactManager artifactManager;
    private final FlinkConfigManager configManager;
    private final ExecutorService executorService;

    public FlinkService(KubernetesClient kubernetesClient, FlinkConfigManager configManager) {
        this.kubernetesClient = kubernetesClient;
        this.artifactManager = new ArtifactManager(configManager);
        this.configManager = configManager;
        this.executorService =
                Executors.newFixedThreadPool(
                        4, new ExecutorThreadFactory("Flink-RestClusterClient-IO"));
    }

    public KubernetesClient getKubernetesClient() {
        return kubernetesClient;
    }

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

    public boolean isHaMetadataAvailable(Configuration conf) {
        return FlinkUtils.isHaMetadataAvailable(conf, kubernetesClient);
    }

    protected void validateHaMetadataExists(Configuration conf) {
        if (!isHaMetadataAvailable(conf)) {
            throw new DeploymentFailedException(
                    "HA metadata not available to restore from last state. "
                            + "It is possible that the job has finished or terminally failed, or the configmaps have been deleted. "
                            + "Manual restore required.",
                    "RestoreFailed");
        }
    }

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

    public JobID submitJobToSessionCluster(
            FlinkSessionJob sessionJob, Configuration conf, @Nullable String savepoint)
            throws Exception {
        var jarRunResponseBody = runJar(sessionJob, uploadJar(sessionJob, conf), conf, savepoint);
        var jobID = jarRunResponseBody.getJobId();
        LOG.info("Submitted job: {} to session cluster.", jobID);
        return jobID;
    }

    private JarRunResponseBody runJar(
            FlinkSessionJob sessionJob,
            JarUploadResponseBody response,
            Configuration conf,
            String savepoint) {
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
                            job.getAllowNonRestoredState(),
                            savepoint,
                            conf.get(FLINK_VERSION).isNewerVersionThan(FlinkVersion.v1_14)
                                    ? RestoreMode.DEFAULT
                                    : null);
            LOG.info("Submitting job: {} to session cluster.", jobID.toHexString());
            return clusterClient
                    .sendRequest(headers, parameters, runRequestBody)
                    .get(
                            configManager
                                    .getOperatorConfiguration()
                                    .getFlinkClientTimeout()
                                    .toSeconds(),
                            TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Failed to submit job to session cluster.", e);
            throw new FlinkRuntimeException(e);
        } finally {
            deleteJar(conf, jarId);
        }
    }

    private JarUploadResponseBody uploadJar(FlinkSessionJob sessionJob, Configuration conf)
            throws Exception {
        String targetDir = artifactManager.generateJarDir(sessionJob);
        File jarFile =
                artifactManager.fetch(sessionJob.getSpec().getJob().getJarURI(), conf, targetDir);
        Preconditions.checkArgument(
                jarFile.exists(),
                String.format("The jar file %s not exists", jarFile.getAbsolutePath()));
        JarUploadHeaders headers = JarUploadHeaders.getInstance();
        String clusterId = sessionJob.getSpec().getDeploymentName();
        String namespace = sessionJob.getMetadata().getNamespace();
        int port = conf.getInteger(RestOptions.PORT);
        String host =
                ObjectUtils.firstNonNull(
                        configManager.getOperatorConfiguration().getFlinkServiceHostOverride(),
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
                                    new FileUpload(
                                            jarFile.toPath(), RestConstants.CONTENT_TYPE_JAR)))
                    .get(
                            configManager
                                    .getOperatorConfiguration()
                                    .getFlinkClientTimeout()
                                    .toSeconds(),
                            TimeUnit.SECONDS);
        } finally {
            LOG.debug("Deleting the jar file {}", jarFile);
            FileUtils.deleteFileOrDirectory(jarFile);
        }
    }

    private void deleteJar(Configuration conf, String jarId) {
        LOG.debug("Deleting the jar: {}", jarId);
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            JarDeleteHeaders headers = JarDeleteHeaders.getInstance();
            JarDeleteMessageParameters parameters = headers.getUnresolvedMessageParameters();
            parameters.jarIdPathParameter.resolve(jarId);
            clusterClient
                    .sendRequest(headers, parameters, EmptyRequestBody.getInstance())
                    .get(
                            configManager
                                    .getOperatorConfiguration()
                                    .getFlinkClientTimeout()
                                    .toSeconds(),
                            TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Failed to delete the jar: {}.", jarId, e);
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
        } catch (IOException e) {
        }
        return false;
    }

    public Collection<JobStatusMessage> listJobs(Configuration conf) throws Exception {
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            return clusterClient
                    .listJobs()
                    .get(
                            configManager
                                    .getOperatorConfiguration()
                                    .getFlinkClientTimeout()
                                    .getSeconds(),
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
                        configManager.getOperatorConfiguration().getFlinkServiceHostOverride(),
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                clusterId, namespace));
        final String restServerAddress = String.format("http://%s:%s", host, port);
        LOG.debug("Creating RestClusterClient({})", restServerAddress);
        return new RestClusterClient<>(
                config, clusterId, (c, e) -> new StandaloneClientHAServices(restServerAddress));
    }

    public void cancelJob(FlinkDeployment deployment, UpgradeMode upgradeMode) throws Exception {
        var conf = configManager.getObserveConfig(deployment);
        var deploymentStatus = deployment.getStatus();
        var jobIdString = deploymentStatus.getJobStatus().getJobId();
        var jobId = jobIdString != null ? JobID.fromHexString(jobIdString) : null;

        Optional<String> savepointOpt = Optional.empty();
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            var clusterId = clusterClient.getClusterId();
            switch (upgradeMode) {
                case STATELESS:
                    if (ReconciliationUtils.isJobRunning(deployment.getStatus())) {
                        LOG.info("Job is running, cancelling job.");
                        try {
                            clusterClient
                                    .cancel(Preconditions.checkNotNull(jobId))
                                    .get(
                                            configManager
                                                    .getOperatorConfiguration()
                                                    .getFlinkCancelJobTimeout()
                                                    .toSeconds(),
                                            TimeUnit.SECONDS);
                            LOG.info("Job successfully cancelled.");
                        } catch (Exception e) {
                            LOG.error("Could not shut down cluster gracefully, deleting...", e);
                        }
                    }
                    deleteClusterDeployment(deployment.getMetadata(), deploymentStatus, true);
                    break;
                case SAVEPOINT:
                    final String savepointDirectory =
                            Preconditions.checkNotNull(
                                    conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
                    final long timeout =
                            conf.get(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT)
                                    .getSeconds();
                    try {
                        if (ReconciliationUtils.isJobRunning(deploymentStatus)) {
                            LOG.info("Suspending job with savepoint.");
                            String savepoint =
                                    clusterClient
                                            .stopWithSavepoint(
                                                    Preconditions.checkNotNull(jobId),
                                                    false,
                                                    savepointDirectory,
                                                    conf.get(FLINK_VERSION)
                                                                    .isNewerVersionThan(
                                                                            FlinkVersion.v1_14)
                                                            ? SavepointFormatType.DEFAULT
                                                            : null)
                                            .get(timeout, TimeUnit.SECONDS);
                            savepointOpt = Optional.of(savepoint);
                            LOG.info("Job successfully suspended with savepoint {}.", savepoint);
                        } else if (ReconciliationUtils.isJobInTerminalState(deploymentStatus)) {
                            LOG.info(
                                    "Job is already in terminal state skipping cancel-with-savepoint operation.");
                        } else {
                            throw new RuntimeException(
                                    "Unexpected non-terminal status: " + deploymentStatus);
                        }
                    } catch (TimeoutException exception) {
                        throw new FlinkException(
                                String.format(
                                        "Timed out stopping the job %s in Flink cluster %s with savepoint, "
                                                + "please configure a larger timeout via '%s'",
                                        jobId,
                                        clusterId,
                                        ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT.key()),
                                exception);
                    }
                    break;
                case LAST_STATE:
                    deleteClusterDeployment(deployment.getMetadata(), deploymentStatus, false);
                    break;
                default:
                    throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
            }
        }
        deploymentStatus.getJobStatus().setState(JobStatus.FINISHED.name());
        savepointOpt.ifPresent(
                location -> {
                    Savepoint sp = Savepoint.of(location);
                    deploymentStatus.getJobStatus().getSavepointInfo().setLastSavepoint(sp);
                    // this is required for Flink < 1.15
                    deploymentStatus.getJobStatus().getSavepointInfo().addSavepointToHistory(sp);
                });

        var shutdownDisabled =
                upgradeMode != UpgradeMode.LAST_STATE
                        && FlinkUtils.clusterShutdownDisabled(
                                ReconciliationUtils.getDeployedSpec(deployment));
        if (!shutdownDisabled) {
            waitForClusterShutdown(conf);
            deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        }
    }

    @VisibleForTesting
    protected void waitForClusterShutdown(Configuration conf) {
        FlinkUtils.waitForClusterShutdown(
                kubernetesClient,
                conf,
                configManager
                        .getOperatorConfiguration()
                        .getFlinkShutdownClusterTimeout()
                        .toSeconds());
    }

    public void deleteClusterDeployment(
            ObjectMeta meta, FlinkDeploymentStatus status, boolean deleteHaData) {
        FlinkUtils.deleteCluster(
                status,
                meta,
                kubernetesClient,
                deleteHaData,
                configManager
                        .getOperatorConfiguration()
                        .getFlinkShutdownClusterTimeout()
                        .toSeconds());
    }

    public Optional<String> cancelSessionJob(
            JobID jobID, UpgradeMode upgradeMode, Configuration conf) throws Exception {
        Optional<String> savepointOpt = Optional.empty();
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            final String clusterId = clusterClient.getClusterId();
            switch (upgradeMode) {
                case STATELESS:
                    LOG.info("Cancelling job.");
                    clusterClient
                            .cancel(jobID)
                            .get(
                                    configManager
                                            .getOperatorConfiguration()
                                            .getFlinkCancelJobTimeout()
                                            .toSeconds(),
                                    TimeUnit.SECONDS);
                    LOG.info("Job successfully cancelled.");
                    break;
                case SAVEPOINT:
                    LOG.info("Suspending job with savepoint.");
                    final String savepointDirectory =
                            Preconditions.checkNotNull(
                                    conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
                    final long timeout =
                            conf.get(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT)
                                    .getSeconds();
                    try {
                        String savepoint =
                                clusterClient
                                        .stopWithSavepoint(
                                                jobID,
                                                false,
                                                savepointDirectory,
                                                conf.get(FLINK_VERSION)
                                                                .isNewerVersionThan(
                                                                        FlinkVersion.v1_14)
                                                        ? SavepointFormatType.DEFAULT
                                                        : null)
                                        .get(timeout, TimeUnit.SECONDS);
                        savepointOpt = Optional.of(savepoint);
                        LOG.info("Job successfully suspended with savepoint {}.", savepoint);
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
                default:
                    throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
            }
        }
        return savepointOpt;
    }

    public void triggerSavepoint(
            String jobId,
            org.apache.flink.kubernetes.operator.crd.status.SavepointInfo savepointInfo,
            Configuration conf)
            throws Exception {
        LOG.info("Triggering new savepoint");
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            SavepointTriggerHeaders savepointTriggerHeaders = SavepointTriggerHeaders.getInstance();
            SavepointTriggerMessageParameters savepointTriggerMessageParameters =
                    savepointTriggerHeaders.getUnresolvedMessageParameters();
            savepointTriggerMessageParameters.jobID.resolve(JobID.fromHexString(jobId));

            final String savepointDirectory =
                    Preconditions.checkNotNull(conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
            final long timeout =
                    configManager.getOperatorConfiguration().getFlinkClientTimeout().getSeconds();
            TriggerResponse response =
                    clusterClient
                            .sendRequest(
                                    savepointTriggerHeaders,
                                    savepointTriggerMessageParameters,
                                    new SavepointTriggerRequestBody(
                                            savepointDirectory,
                                            false,
                                            conf.get(FLINK_VERSION)
                                                            .isNewerVersionThan(FlinkVersion.v1_14)
                                                    ? SavepointFormatType.DEFAULT
                                                    : null,
                                            null))
                            .get(timeout, TimeUnit.SECONDS);
            LOG.info("Savepoint successfully triggered: " + response.getTriggerId().toHexString());

            savepointInfo.setTrigger(response.getTriggerId().toHexString());
            savepointInfo.setTriggerTimestamp(System.currentTimeMillis());
        }
    }

    public Optional<Savepoint> getLastCheckpoint(JobID jobId, Configuration conf) throws Exception {
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {

            var headers = CustomCheckpointingStatisticsHeaders.getInstance();
            var params = headers.getUnresolvedMessageParameters();
            params.jobPathParameter.resolve(jobId);

            CompletableFuture<CheckpointHistoryWrapper> response =
                    clusterClient.sendRequest(headers, params, EmptyRequestBody.getInstance());

            var checkpoints =
                    response.get(
                            configManager
                                    .getOperatorConfiguration()
                                    .getFlinkClientTimeout()
                                    .getSeconds(),
                            TimeUnit.SECONDS);

            var latestCheckpointOpt = checkpoints.getLatestCheckpointPath();

            if (latestCheckpointOpt.isPresent()
                    && latestCheckpointOpt
                            .get()
                            .equals(
                                    NonPersistentMetadataCheckpointStorageLocation
                                            .EXTERNAL_POINTER)) {
                throw new DeploymentFailedException(
                        "Latest checkpoint not externally addressable, manual recovery required.",
                        "CheckpointNotFound");
            }
            return latestCheckpointOpt.map(Savepoint::of);
        }
    }

    public SavepointFetchResult fetchSavepointInfo(
            String triggerId, String jobId, Configuration conf) {
        LOG.info("Fetching savepoint result with triggerId: " + triggerId);
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            SavepointStatusHeaders savepointStatusHeaders = SavepointStatusHeaders.getInstance();
            SavepointStatusMessageParameters savepointStatusMessageParameters =
                    savepointStatusHeaders.getUnresolvedMessageParameters();
            savepointStatusMessageParameters.jobIdPathParameter.resolve(JobID.fromHexString(jobId));
            savepointStatusMessageParameters.triggerIdPathParameter.resolve(
                    TriggerId.fromHexString(triggerId));
            CompletableFuture<AsynchronousOperationResult<SavepointInfo>> response =
                    clusterClient.sendRequest(
                            savepointStatusHeaders,
                            savepointStatusMessageParameters,
                            EmptyRequestBody.getInstance());

            if (response.get() == null || response.get().resource() == null) {
                return SavepointFetchResult.pending();
            }

            if (response.get().resource().getLocation() == null) {
                if (response.get().resource().getFailureCause() != null) {
                    LOG.error(
                            "Failure occurred while fetching the savepoint result",
                            response.get().resource().getFailureCause());
                    return SavepointFetchResult.error(
                            response.get().resource().getFailureCause().toString());
                } else {
                    return SavepointFetchResult.pending();
                }
            }
            Savepoint savepoint =
                    new Savepoint(
                            System.currentTimeMillis(), response.get().resource().getLocation());
            LOG.info("Savepoint result: " + savepoint);
            return SavepointFetchResult.completed(savepoint);
        } catch (Exception e) {
            LOG.error("Exception while fetching the savepoint result", e);
            return SavepointFetchResult.error(e.getMessage());
        }
    }

    public void disposeSavepoint(String savepointPath, Configuration conf) throws Exception {
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            clusterClient.disposeSavepoint(savepointPath);
        }
    }

    public Map<String, String> getClusterInfo(Configuration conf) throws Exception {
        Map<String, String> runtimeVersion = new HashMap<>();

        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {

            CustomDashboardConfiguration dashboardConfiguration =
                    clusterClient
                            .sendRequest(
                                    CustomDashboardConfigurationHeaders.getInstance(),
                                    EmptyMessageParameters.getInstance(),
                                    EmptyRequestBody.getInstance())
                            .get(
                                    configManager
                                            .getOperatorConfiguration()
                                            .getFlinkClientTimeout()
                                            .toSeconds(),
                                    TimeUnit.SECONDS);

            runtimeVersion.put(
                    DashboardConfiguration.FIELD_NAME_FLINK_VERSION,
                    dashboardConfiguration.getFlinkVersion());
            runtimeVersion.put(
                    DashboardConfiguration.FIELD_NAME_FLINK_REVISION,
                    dashboardConfiguration.getFlinkRevision());
        }
        return runtimeVersion;
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
