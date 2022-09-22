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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.artifact.ArtifactManager;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalTriggerHeaders;
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
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;

/**
 * An abstract {@link FlinkService} containing some common implementations for the native and
 * standalone Flink Services.
 */
public abstract class AbstractFlinkService implements FlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkService.class);
    private static final String NOOP_JAR_FILENAME = "noop.jar";

    protected final KubernetesClient kubernetesClient;
    protected final FlinkConfigManager configManager;
    private final ExecutorService executorService;
    protected final ArtifactManager artifactManager;
    private final String noopJarPath;

    public AbstractFlinkService(
            KubernetesClient kubernetesClient, FlinkConfigManager configManager) {
        this.kubernetesClient = kubernetesClient;
        this.configManager = configManager;
        this.artifactManager = new ArtifactManager(configManager);
        this.executorService =
                Executors.newFixedThreadPool(
                        4, new ExecutorThreadFactory("Flink-RestClusterClient-IO"));
        this.noopJarPath = copyNoopJar();
    }

    protected abstract PodList getJmPodList(String namespace, String clusterId);

    protected abstract void deployApplicationCluster(JobSpec jobSpec, Configuration conf)
            throws Exception;

    @Override
    public KubernetesClient getKubernetesClient() {
        return kubernetesClient;
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
        deployApplicationCluster(jobSpec, conf);
    }

    @Override
    public boolean isHaMetadataAvailable(Configuration conf) {
        return FlinkUtils.isHaMetadataAvailable(conf, kubernetesClient);
    }

    @Override
    public JobID submitJobToSessionCluster(
            ObjectMeta meta,
            FlinkSessionJobSpec spec,
            Configuration conf,
            @Nullable String savepoint)
            throws Exception {
        // we generate jobID in advance to help deduplicate job submission.
        var jobID = FlinkUtils.generateSessionJobFixedJobID(meta);
        runJar(spec.getJob(), jobID, uploadJar(meta, spec, conf), conf, savepoint);
        LOG.info("Submitted job: {} to session cluster.", jobID);
        return jobID;
    }

    @Override
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
            return false;
        }
    }

    @Override
    public Collection<JobStatusMessage> listJobs(Configuration conf) throws Exception {
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            return clusterClient
                    .sendRequest(
                            JobsOverviewHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance())
                    .thenApply(AbstractFlinkService::toJobStatusMessage)
                    .get(
                            configManager
                                    .getOperatorConfiguration()
                                    .getFlinkClientTimeout()
                                    .toSeconds(),
                            TimeUnit.SECONDS);
        }
    }

    @Override
    public JobResult requestJobResult(Configuration conf, JobID jobID) throws Exception {
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            return clusterClient
                    .requestJobResult(jobID)
                    .get(
                            configManager
                                    .getOperatorConfiguration()
                                    .getFlinkClientTimeout()
                                    .getSeconds(),
                            TimeUnit.SECONDS);
        }
    }

    protected void cancelJob(
            FlinkDeployment deployment,
            UpgradeMode upgradeMode,
            Configuration conf,
            boolean deleteClusterAfterSavepoint)
            throws Exception {
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
                                                            ? conf.get(
                                                                    KubernetesOperatorConfigOptions
                                                                            .OPERATOR_SAVEPOINT_FORMAT_TYPE)
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
                    if (deleteClusterAfterSavepoint) {
                        deleteClusterDeployment(deployment.getMetadata(), deploymentStatus, true);
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
                    Savepoint sp = Savepoint.of(location, SavepointTriggerType.UPGRADE);
                    deploymentStatus.getJobStatus().getSavepointInfo().updateLastSavepoint(sp);
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

    @Override
    public void cancelSessionJob(
            FlinkSessionJob sessionJob, UpgradeMode upgradeMode, Configuration conf)
            throws Exception {

        var jobStatus = sessionJob.getStatus().getJobStatus();
        var jobIdString = jobStatus.getJobId();
        Preconditions.checkNotNull(jobIdString, "The job to be suspend should not be null");
        var jobId = JobID.fromHexString(jobIdString);
        Optional<String> savepointOpt = Optional.empty();
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            final String clusterId = clusterClient.getClusterId();
            switch (upgradeMode) {
                case STATELESS:
                    LOG.info("Cancelling job.");
                    clusterClient
                            .cancel(jobId)
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
                                                jobId,
                                                false,
                                                savepointDirectory,
                                                conf.get(FLINK_VERSION)
                                                                .isNewerVersionThan(
                                                                        FlinkVersion.v1_14)
                                                        ? conf.get(
                                                                KubernetesOperatorConfigOptions
                                                                        .OPERATOR_SAVEPOINT_FORMAT_TYPE)
                                                        : null)
                                        .get(timeout, TimeUnit.SECONDS);
                        savepointOpt = Optional.of(savepoint);
                        LOG.info("Job successfully suspended with savepoint {}.", savepoint);
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
                default:
                    throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
            }
        }
        jobStatus.setState(JobStatus.FINISHED.name());
        savepointOpt.ifPresent(
                location -> {
                    Savepoint sp = Savepoint.of(location, SavepointTriggerType.UPGRADE);
                    jobStatus.getSavepointInfo().updateLastSavepoint(sp);
                });
    }

    @Override
    public void triggerSavepoint(
            String jobId,
            SavepointTriggerType triggerType,
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
                                                    ? conf.get(
                                                            KubernetesOperatorConfigOptions
                                                                    .OPERATOR_SAVEPOINT_FORMAT_TYPE)
                                                    : null,
                                            null))
                            .get(timeout, TimeUnit.SECONDS);
            LOG.info("Savepoint successfully triggered: " + response.getTriggerId().toHexString());

            savepointInfo.setTrigger(response.getTriggerId().toHexString(), triggerType);
        }
    }

    @Override
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
            return latestCheckpointOpt.map(
                    pointer -> Savepoint.of(pointer, SavepointTriggerType.UNKNOWN));
        }
    }

    @Override
    public void disposeSavepoint(String savepointPath, Configuration conf) throws Exception {
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            clusterClient
                    .sendRequest(
                            SavepointDisposalTriggerHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(),
                            new SavepointDisposalRequest(savepointPath))
                    .get(
                            configManager
                                    .getOperatorConfiguration()
                                    .getFlinkClientTimeout()
                                    .getSeconds(),
                            TimeUnit.SECONDS);
        }
    }

    @Override
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
            String location = response.get().resource().getLocation();
            LOG.info("Savepoint result: {}", location);
            return SavepointFetchResult.completed(location);
        } catch (Exception e) {
            LOG.error("Exception while fetching the savepoint result", e);
            return SavepointFetchResult.error(e.getMessage());
        }
    }

    @Override
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

    @Override
    public PodList getJmPodList(FlinkDeployment deployment, Configuration conf) {
        final String namespace = conf.getString(KubernetesConfigOptions.NAMESPACE);
        final String clusterId;
        try (ClusterClient<String> clusterClient = getClusterClient(conf)) {
            clusterId = clusterClient.getClusterId();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return getJmPodList(namespace, clusterId);
    }

    @Override
    public void waitForClusterShutdown(Configuration conf) {
        waitForClusterShutdown(
                conf.getString(KubernetesConfigOptions.NAMESPACE),
                conf.getString(KubernetesConfigOptions.CLUSTER_ID),
                configManager
                        .getOperatorConfiguration()
                        .getFlinkShutdownClusterTimeout()
                        .toSeconds());
    }

    @VisibleForTesting
    protected ClusterClient<String> getClusterClient(Configuration conf) throws Exception {
        final String clusterId = conf.get(KubernetesConfigOptions.CLUSTER_ID);
        final String namespace = conf.get(KubernetesConfigOptions.NAMESPACE);
        final int port = conf.getInteger(RestOptions.PORT);
        final String host =
                ObjectUtils.firstNonNull(
                        configManager.getOperatorConfiguration().getFlinkServiceHostOverride(),
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                clusterId, namespace));
        final String restServerAddress = String.format("http://%s:%s", host, port);
        LOG.debug("Creating RestClusterClient({})", restServerAddress);
        return new RestClusterClient<>(
                conf, clusterId, (c, e) -> new StandaloneClientHAServices(restServerAddress));
    }

    private JarRunResponseBody runJar(
            JobSpec job,
            JobID jobID,
            JarUploadResponseBody response,
            Configuration conf,
            String savepoint) {
        String jarId =
                response.getFilename().substring(response.getFilename().lastIndexOf("/") + 1);
        try (RestClusterClient<String> clusterClient =
                (RestClusterClient<String>) getClusterClient(conf)) {
            JarRunHeaders headers = JarRunHeaders.getInstance();
            JarRunMessageParameters parameters = headers.getUnresolvedMessageParameters();
            parameters.jarIdPathParameter.resolve(jarId);
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

    private JarUploadResponseBody uploadJar(
            ObjectMeta objectMeta, FlinkSessionJobSpec spec, Configuration conf) throws Exception {
        String targetDir = artifactManager.generateJarDir(objectMeta, spec);
        File jarFile = artifactManager.fetch(findJarURI(spec.getJob()), conf, targetDir);
        Preconditions.checkArgument(
                jarFile.exists(),
                String.format("The jar file %s not exists", jarFile.getAbsolutePath()));
        JarUploadHeaders headers = JarUploadHeaders.getInstance();
        String clusterId = spec.getDeploymentName();
        String namespace = objectMeta.getNamespace();
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

    private String findJarURI(JobSpec jobSpec) {
        if (jobSpec.getJarURI() != null) {
            return jobSpec.getJarURI();
        } else {
            return noopJarPath;
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

    /** Wait until the FLink cluster has completely shut down. */
    @VisibleForTesting
    void waitForClusterShutdown(String namespace, String clusterId, long shutdownTimeout) {
        boolean jobManagerRunning = true;
        boolean serviceRunning = true;

        for (int i = 0; i < shutdownTimeout; i++) {
            if (jobManagerRunning) {
                PodList jmPodList = getJmPodList(namespace, clusterId);

                if (jmPodList == null || jmPodList.getItems().isEmpty()) {
                    jobManagerRunning = false;
                }
            }

            if (serviceRunning) {
                Service service =
                        kubernetesClient
                                .services()
                                .inNamespace(namespace)
                                .withName(
                                        ExternalServiceDecorator.getExternalServiceName(clusterId))
                                .fromServer()
                                .get();
                if (service == null) {
                    serviceRunning = false;
                }
            }

            if (!jobManagerRunning && !serviceRunning) {
                break;
            }
            // log a message waiting to shutdown Flink cluster every 5 seconds.
            if ((i + 1) % 5 == 0) {
                LOG.info("Waiting for cluster shutdown... ({}s)", i + 1);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("Cluster shutdown completed.");
    }

    private static List<JobStatusMessage> toJobStatusMessage(
            MultipleJobsDetails multipleJobsDetails) {
        return multipleJobsDetails.getJobs().stream()
                .map(
                        details ->
                                new JobStatusMessage(
                                        details.getJobId(),
                                        details.getJobName(),
                                        getEffectiveStatus(details),
                                        details.getStartTime()))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    protected static JobStatus getEffectiveStatus(JobDetails details) {
        int numRunning = details.getTasksPerState()[ExecutionState.RUNNING.ordinal()];
        int numFinished = details.getTasksPerState()[ExecutionState.FINISHED.ordinal()];
        boolean allRunningOrFinished = details.getNumTasks() == (numRunning + numFinished);
        JobStatus effectiveStatus = details.getStatus();
        if (JobStatus.RUNNING.equals(effectiveStatus) && !allRunningOrFinished) {
            effectiveStatus = JobStatus.CREATED;
            LOG.debug("Adjusting job state from {} to {}", JobStatus.RUNNING, effectiveStatus);
        }
        return effectiveStatus;
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

    private String copyNoopJar() {
        try {
            InputStream noopJarSource =
                    AbstractFlinkService.class
                            .getClassLoader()
                            .getResourceAsStream(NOOP_JAR_FILENAME);

            String noopJarDestination =
                    Files.createTempDirectory("flink").toString() + "/" + NOOP_JAR_FILENAME;

            LOG.debug("Copying noop jar to {}", noopJarDestination);
            org.apache.commons.io.FileUtils.copyToFile(noopJarSource, new File(noopJarDestination));

            return noopJarDestination;
        } catch (Exception e) {
            throw new RuntimeException("Failed to copy noop jar", e);
        }
    }
}
