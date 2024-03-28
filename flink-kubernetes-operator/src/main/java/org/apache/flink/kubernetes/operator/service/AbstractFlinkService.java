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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.autoscaler.utils.JobStatusUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CheckpointType;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.artifact.ArtifactManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.observer.CheckpointFetchResult;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.FileUpload;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.JobMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointStoppingException;
import org.apache.flink.runtime.state.memory.NonPersistentMetadataCheckpointStorageLocation;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteMessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarRunMessageParameters;
import org.apache.flink.runtime.webmonitor.handlers.JarRunRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadResponseBody;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.Waitable;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.K8S_OP_CONF_PREFIX;

/**
 * An abstract {@link FlinkService} containing some common implementations for the native and
 * standalone Flink Services.
 */
public abstract class AbstractFlinkService implements FlinkService {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkService.class);
    private static final String EMPTY_JAR_FILENAME = "empty.jar";
    public static final String FIELD_NAME_TOTAL_CPU = "total-cpu";
    public static final String FIELD_NAME_TOTAL_MEMORY = "total-memory";

    protected final KubernetesClient kubernetesClient;
    protected final ExecutorService executorService;
    protected final FlinkOperatorConfiguration operatorConfig;
    protected final ArtifactManager artifactManager;
    private static final String EMPTY_JAR = createEmptyJar();

    public AbstractFlinkService(
            KubernetesClient kubernetesClient,
            ArtifactManager artifactManager,
            ExecutorService executorService,
            FlinkOperatorConfiguration operatorConfig) {
        this.kubernetesClient = kubernetesClient;
        this.artifactManager = artifactManager;
        this.executorService = executorService;
        this.operatorConfig = operatorConfig;
    }

    protected abstract PodList getJmPodList(String namespace, String clusterId);

    protected abstract PodList getTmPodList(String namespace, String clusterId);

    protected abstract void deployApplicationCluster(JobSpec jobSpec, Configuration conf)
            throws Exception;

    protected abstract void deploySessionCluster(Configuration conf) throws Exception;

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

        // If Kubernetes or Zookeeper HA are activated, delete the job graph in HA storage so that
        // the newly changed job config (e.g. parallelism) could take effect
        if (FlinkUtils.isKubernetesHAActivated(conf)) {
            final String clusterId = conf.get(KubernetesConfigOptions.CLUSTER_ID);
            final String namespace = conf.get(KubernetesConfigOptions.NAMESPACE);
            FlinkUtils.deleteJobGraphInKubernetesHA(clusterId, namespace, kubernetesClient);
        } else if (FlinkUtils.isZookeeperHAActivated(conf)) {
            FlinkUtils.deleteJobGraphInZookeeperHA(conf);
        }

        if (requireHaMetadata) {
            validateHaMetadataExists(conf);
        }

        deployApplicationCluster(jobSpec, removeOperatorConfigs(conf));
    }

    @Override
    public void submitSessionCluster(Configuration conf) throws Exception {
        deploySessionCluster(conf);
    }

    @Override
    public boolean isHaMetadataAvailable(Configuration conf) {
        if (FlinkUtils.isKubernetesHAActivated(conf)) {
            return FlinkUtils.isKubernetesHaMetadataAvailable(conf, kubernetesClient);
        } else if (FlinkUtils.isZookeeperHAActivated(conf)) {
            return FlinkUtils.isZookeeperHaMetadataAvailable(conf);
        }

        return false;
    }

    @Override
    public JobID submitJobToSessionCluster(
            ObjectMeta meta,
            FlinkSessionJobSpec spec,
            JobID jobID,
            Configuration conf,
            @Nullable String savepoint)
            throws Exception {
        runJar(spec.getJob(), jobID, uploadJar(meta, spec, conf), conf, savepoint);
        LOG.info("Submitted job: {} to session cluster.", jobID);
        return jobID;
    }

    @Override
    public boolean isJobManagerPortReady(Configuration config) {
        SocketAddress socketAddress;
        try (var clusterClient = getClusterClient(config)) {
            socketAddress = getSocketAddress(clusterClient);

        } catch (Exception ex) {
            throw new FlinkRuntimeException(ex);
        }

        try (Socket socket = new Socket()) {
            socket.connect(socketAddress, 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    protected SocketAddress getSocketAddress(RestClusterClient<String> clusterClient)
            throws MalformedURLException {
        final URL url = new URL(clusterClient.getWebInterfaceURL());
        LOG.debug("JobManager webinterface url {}", clusterClient.getWebInterfaceURL());
        return new InetSocketAddress(url.getHost(), url.getPort());
    }

    @Override
    public Collection<JobStatusMessage> listJobs(Configuration conf) throws Exception {
        try (var clusterClient = getClusterClient(conf)) {
            return clusterClient
                    .sendRequest(
                            JobsOverviewHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance())
                    .thenApply(JobStatusUtils::toJobStatusMessage)
                    .get(operatorConfig.getFlinkClientTimeout().toSeconds(), TimeUnit.SECONDS);
        }
    }

    @Override
    public JobResult requestJobResult(Configuration conf, JobID jobID) throws Exception {
        try (var clusterClient = getClusterClient(conf)) {
            return clusterClient
                    .requestJobResult(jobID)
                    .get(operatorConfig.getFlinkClientTimeout().getSeconds(), TimeUnit.SECONDS);
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
        var savepointFormatType =
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);
        try (var clusterClient = getClusterClient(conf)) {
            var clusterId = clusterClient.getClusterId();
            switch (upgradeMode) {
                case STATELESS:
                    if (ReconciliationUtils.isJobRunning(deployment.getStatus())) {
                        LOG.info("Job is running, cancelling job.");
                        try {
                            clusterClient
                                    .cancel(Preconditions.checkNotNull(jobId))
                                    .get(
                                            operatorConfig.getFlinkCancelJobTimeout().toSeconds(),
                                            TimeUnit.SECONDS);
                            LOG.info("Job successfully cancelled.");
                        } catch (Exception e) {
                            LOG.error("Could not shut down cluster gracefully, deleting...", e);
                        }
                    }
                    deleteClusterDeployment(deployment.getMetadata(), deploymentStatus, conf, true);
                    break;
                case SAVEPOINT:
                    final String savepointDirectory =
                            Preconditions.checkNotNull(
                                    conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
                    final long timeout =
                            conf.get(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT)
                                    .getSeconds();
                    if (ReconciliationUtils.isJobRunning(deploymentStatus)) {
                        try {
                            LOG.info("Suspending job with savepoint.");
                            String savepoint =
                                    clusterClient
                                            .stopWithSavepoint(
                                                    Preconditions.checkNotNull(jobId),
                                                    conf.getBoolean(
                                                            KubernetesOperatorConfigOptions
                                                                    .DRAIN_ON_SAVEPOINT_DELETION),
                                                    savepointDirectory,
                                                    savepointFormatType)
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
                                            ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT
                                                    .key()),
                                    exception);
                        } catch (Exception e) {
                            var stopWithSavepointException =
                                    ExceptionUtils.findThrowableSerializedAware(
                                            e,
                                            StopWithSavepointStoppingException.class,
                                            getClass().getClassLoader());
                            if (stopWithSavepointException.isPresent()) {
                                // Handle edge case where the savepoint completes but the job fails
                                // right afterward.
                                savepointOpt =
                                        Optional.of(
                                                stopWithSavepointException
                                                        .get()
                                                        .getSavepointPath());
                            } else {
                                // Rethrow if savepoint was not completed successfully.
                                throw e;
                            }
                        }
                    } else if (ReconciliationUtils.isJobInTerminalState(deploymentStatus)) {
                        LOG.info(
                                "Job is already in terminal state skipping cancel-with-savepoint operation.");
                    } else {
                        throw new RuntimeException(
                                "Unexpected non-terminal status: " + deploymentStatus);
                    }
                    if (deleteClusterAfterSavepoint) {
                        LOG.info("Cleaning up deployment after stop-with-savepoint");

                        deleteClusterDeployment(
                                deployment.getMetadata(), deploymentStatus, conf, true);
                    }
                    break;
                case LAST_STATE:
                    deleteClusterDeployment(
                            deployment.getMetadata(), deploymentStatus, conf, false);
                    break;
                default:
                    throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
            }
        }
        deploymentStatus.getJobStatus().setState(JobStatus.FINISHED.name());
        savepointOpt.ifPresent(
                location -> {
                    Savepoint sp =
                            Savepoint.of(
                                    location,
                                    SnapshotTriggerType.UPGRADE,
                                    SavepointFormatType.valueOf(savepointFormatType.name()));
                    deploymentStatus.getJobStatus().getSavepointInfo().updateLastSavepoint(sp);
                });

        // Unless we leave the jm around after savepoint, we should wait until it has finished
        // shutting down
        if (deleteClusterAfterSavepoint || upgradeMode != UpgradeMode.SAVEPOINT) {
            deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        }
    }

    @Override
    public void cancelSessionJob(
            FlinkSessionJob sessionJob, UpgradeMode upgradeMode, Configuration conf)
            throws Exception {

        var sessionJobStatus = sessionJob.getStatus();
        var jobStatus = sessionJobStatus.getJobStatus();
        var jobIdString = jobStatus.getJobId();
        Preconditions.checkNotNull(jobIdString, "The job to be suspend should not be null");
        var jobId = JobID.fromHexString(jobIdString);
        Optional<String> savepointOpt = Optional.empty();

        LOG.debug("Current job state: {}", jobStatus.getState());

        if (!ReconciliationUtils.isJobInTerminalState(sessionJobStatus)) {
            LOG.debug("Job is not in terminal state, cancelling it");

            try (var clusterClient = getClusterClient(conf)) {
                final String clusterId = clusterClient.getClusterId();
                switch (upgradeMode) {
                    case STATELESS:
                        LOG.info("Cancelling job.");
                        clusterClient
                                .cancel(jobId)
                                .get(
                                        operatorConfig.getFlinkCancelJobTimeout().toSeconds(),
                                        TimeUnit.SECONDS);
                        LOG.info("Job successfully cancelled.");
                        break;
                    case SAVEPOINT:
                        if (ReconciliationUtils.isJobRunning(sessionJobStatus)) {
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
                                                        conf.getBoolean(
                                                                KubernetesOperatorConfigOptions
                                                                        .DRAIN_ON_SAVEPOINT_DELETION),
                                                        savepointDirectory,
                                                        conf.get(
                                                                KubernetesOperatorConfigOptions
                                                                        .OPERATOR_SAVEPOINT_FORMAT_TYPE))
                                                .get(timeout, TimeUnit.SECONDS);
                                savepointOpt = Optional.of(savepoint);
                                LOG.info(
                                        "Job successfully suspended with savepoint {}.", savepoint);
                            } catch (TimeoutException exception) {
                                throw new FlinkException(
                                        String.format(
                                                "Timed out stopping the job %s in Flink cluster %s with savepoint, "
                                                        + "please configure a larger timeout via '%s'",
                                                jobId,
                                                clusterId,
                                                ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT
                                                        .key()),
                                        exception);
                            }
                        } else {
                            throw new RuntimeException(
                                    "Unexpected non-terminal status: " + jobStatus.getState());
                        }
                        break;
                    case LAST_STATE:
                    default:
                        throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
                }
            }
        } else {
            LOG.debug("Job is in terminal state, skipping cancel");
        }

        jobStatus.setState(JobStatus.FINISHED.name());
        savepointOpt.ifPresent(
                location -> {
                    Savepoint sp = Savepoint.of(location, SnapshotTriggerType.UPGRADE);
                    jobStatus.getSavepointInfo().updateLastSavepoint(sp);
                });
    }

    @Override
    public void triggerSavepoint(
            String jobId,
            SnapshotTriggerType triggerType,
            org.apache.flink.kubernetes.operator.api.status.SavepointInfo savepointInfo,
            Configuration conf)
            throws Exception {
        LOG.info("Triggering new savepoint");
        try (var clusterClient = getClusterClient(conf)) {
            var savepointTriggerHeaders = SavepointTriggerHeaders.getInstance();
            var savepointTriggerMessageParameters =
                    savepointTriggerHeaders.getUnresolvedMessageParameters();
            savepointTriggerMessageParameters.jobID.resolve(JobID.fromHexString(jobId));

            var savepointDirectory =
                    Preconditions.checkNotNull(conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
            var timeout = operatorConfig.getFlinkClientTimeout().getSeconds();

            var savepointFormatType =
                    conf.get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);

            var response =
                    clusterClient
                            .sendRequest(
                                    savepointTriggerHeaders,
                                    savepointTriggerMessageParameters,
                                    new SavepointTriggerRequestBody(
                                            savepointDirectory, false, savepointFormatType, null))
                            .get(timeout, TimeUnit.SECONDS);
            LOG.info("Savepoint successfully triggered: " + response.getTriggerId().toHexString());

            savepointInfo.setTrigger(
                    response.getTriggerId().toHexString(),
                    triggerType,
                    SavepointFormatType.valueOf(savepointFormatType.name()));
        }
    }

    @Override
    public void triggerCheckpoint(
            String jobId,
            SnapshotTriggerType triggerType,
            org.apache.flink.kubernetes.operator.api.status.CheckpointInfo checkpointInfo,
            Configuration conf)
            throws Exception {
        LOG.info("Triggering new checkpoint");
        try (var clusterClient = getClusterClient(conf)) {
            var checkpointTriggerHeaders = CheckpointTriggerHeaders.getInstance();
            var checkpointTriggerMessageParameters =
                    checkpointTriggerHeaders.getUnresolvedMessageParameters();
            checkpointTriggerMessageParameters.jobID.resolve(JobID.fromHexString(jobId));

            var timeout = operatorConfig.getFlinkClientTimeout().getSeconds();

            var checkpointFormatType = org.apache.flink.core.execution.CheckpointType.FULL;

            var response =
                    clusterClient
                            .sendRequest(
                                    checkpointTriggerHeaders,
                                    checkpointTriggerMessageParameters,
                                    new CheckpointTriggerRequestBody(checkpointFormatType, null))
                            .get(timeout, TimeUnit.SECONDS);
            LOG.info("Checkpoint successfully triggered: " + response.getTriggerId().toHexString());

            checkpointInfo.setTrigger(
                    response.getTriggerId().toHexString(),
                    triggerType,
                    CheckpointType.valueOf(checkpointFormatType.name()));
        }
    }

    @Override
    public Optional<Savepoint> getLastCheckpoint(JobID jobId, Configuration conf) throws Exception {
        var latestCheckpointOpt = getCheckpointInfo(jobId, conf).f0;

        if (latestCheckpointOpt.isPresent()
                && latestCheckpointOpt
                        .get()
                        .getExternalPointer()
                        .equals(NonPersistentMetadataCheckpointStorageLocation.EXTERNAL_POINTER)) {
            throw new RecoveryFailureException(
                    "Latest checkpoint not externally addressable, manual recovery required.",
                    "CheckpointNotFound");
        }
        return latestCheckpointOpt.map(
                pointer ->
                        Savepoint.of(
                                pointer.getExternalPointer(),
                                pointer.getTimestamp(),
                                SnapshotTriggerType.UNKNOWN));
    }

    @Override
    public Tuple2<
                    Optional<CheckpointHistoryWrapper.CompletedCheckpointInfo>,
                    Optional<CheckpointHistoryWrapper.PendingCheckpointInfo>>
            getCheckpointInfo(JobID jobId, Configuration conf) throws Exception {
        try (var clusterClient = getClusterClient(conf)) {

            var headers = CustomCheckpointingStatisticsHeaders.getInstance();
            var params = headers.getUnresolvedMessageParameters();
            params.jobPathParameter.resolve(jobId);

            CompletableFuture<CheckpointHistoryWrapper> response =
                    clusterClient.sendRequest(headers, params, EmptyRequestBody.getInstance());

            var checkpoints =
                    response.get(
                            operatorConfig.getFlinkClientTimeout().getSeconds(), TimeUnit.SECONDS);

            return Tuple2.of(
                    checkpoints.getLatestCompletedCheckpoint(),
                    checkpoints.getInProgressCheckpoint());
        }
    }

    @Override
    public void disposeSavepoint(String savepointPath, Configuration conf) throws Exception {
        try (var clusterClient = getClusterClient(conf)) {
            clusterClient
                    .sendRequest(
                            SavepointDisposalTriggerHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(),
                            new SavepointDisposalRequest(savepointPath))
                    .get(operatorConfig.getFlinkClientTimeout().getSeconds(), TimeUnit.SECONDS);
        }
    }

    @Override
    public SavepointFetchResult fetchSavepointInfo(
            String triggerId, String jobId, Configuration conf) {
        LOG.info("Fetching savepoint result with triggerId: " + triggerId);
        try (var clusterClient = getClusterClient(conf)) {
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
    public CheckpointFetchResult fetchCheckpointInfo(
            String triggerId, String jobId, Configuration conf) {
        LOG.info("Fetching checkpoint result with triggerId: " + triggerId);
        try (RestClusterClient<String> clusterClient = getClusterClient(conf)) {
            CheckpointStatusHeaders checkpointStatusHeaders = CheckpointStatusHeaders.getInstance();
            CheckpointStatusMessageParameters checkpointStatusMessageParameters =
                    checkpointStatusHeaders.getUnresolvedMessageParameters();
            checkpointStatusMessageParameters.jobIdPathParameter.resolve(
                    JobID.fromHexString(jobId));
            checkpointStatusMessageParameters.triggerIdPathParameter.resolve(
                    TriggerId.fromHexString(triggerId));
            CompletableFuture<AsynchronousOperationResult<CheckpointInfo>> response =
                    clusterClient.sendRequest(
                            checkpointStatusHeaders,
                            checkpointStatusMessageParameters,
                            EmptyRequestBody.getInstance());

            if (response.get() == null || response.get().resource() == null) {
                return CheckpointFetchResult.pending();
            }

            if (response.get().resource().getFailureCause() != null) {
                LOG.error(
                        "Failure occurred while fetching the checkpoint result",
                        response.get().resource().getFailureCause());
                return CheckpointFetchResult.error(
                        response.get().resource().getFailureCause().toString());
            }

            QueueStatus.Id operationStatus = response.get().queueStatus().getId();
            switch (operationStatus) {
                case IN_PROGRESS:
                    return CheckpointFetchResult.pending();
                case COMPLETED:
                    LOG.info(
                            "Checkpoint {} triggered by the operator for job {} completed:",
                            triggerId,
                            jobId);
                    return CheckpointFetchResult.completed();
                default:
                    throw new IllegalStateException(
                            String.format(
                                    "Checkpoint %s for job %s is reported to be in an unknown status: %s",
                                    triggerId, jobId, operationStatus.name()));
            }
        } catch (Exception e) {
            LOG.error("Exception while fetching the checkpoint result", e);
            return CheckpointFetchResult.error(e.getMessage());
        }
    }

    @Override
    public Map<String, String> getClusterInfo(Configuration conf) throws Exception {
        Map<String, String> clusterInfo = new HashMap<>();

        try (var clusterClient = getClusterClient(conf)) {

            CustomDashboardConfiguration dashboardConfiguration =
                    clusterClient
                            .sendRequest(
                                    CustomDashboardConfigurationHeaders.getInstance(),
                                    EmptyMessageParameters.getInstance(),
                                    EmptyRequestBody.getInstance())
                            .get(
                                    operatorConfig.getFlinkClientTimeout().toSeconds(),
                                    TimeUnit.SECONDS);

            clusterInfo.put(
                    DashboardConfiguration.FIELD_NAME_FLINK_VERSION,
                    dashboardConfiguration.getFlinkVersion());
            clusterInfo.put(
                    DashboardConfiguration.FIELD_NAME_FLINK_REVISION,
                    dashboardConfiguration.getFlinkRevision());
        }

        var taskManagerReplicas = getTaskManagersInfo(conf).getTaskManagerInfos().size();
        clusterInfo.put(
                FIELD_NAME_TOTAL_CPU,
                String.valueOf(FlinkUtils.calculateClusterCpuUsage(conf, taskManagerReplicas)));
        clusterInfo.put(
                FIELD_NAME_TOTAL_MEMORY,
                String.valueOf(FlinkUtils.calculateClusterMemoryUsage(conf, taskManagerReplicas)));

        return clusterInfo;
    }

    @Override
    public PodList getJmPodList(FlinkDeployment deployment, Configuration conf) {
        final String namespace = conf.getString(KubernetesConfigOptions.NAMESPACE);
        final String clusterId = conf.getString(KubernetesConfigOptions.CLUSTER_ID);
        return getJmPodList(namespace, clusterId);
    }

    @Override
    public RestClusterClient<String> getClusterClient(Configuration conf) throws Exception {
        final String clusterId = conf.get(KubernetesConfigOptions.CLUSTER_ID);
        final String namespace = conf.get(KubernetesConfigOptions.NAMESPACE);
        final int port = conf.getInteger(RestOptions.PORT);
        Configuration operatorRestConf = conf;
        if (SecurityOptions.isRestSSLEnabled(conf)) {
            operatorRestConf = getOperatorRestConfig(conf);
        }
        final String host =
                ObjectUtils.firstNonNull(
                        operatorConfig.getFlinkServiceHostOverride(),
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                clusterId, namespace));
        final String restServerAddress = String.format("http://%s:%s", host, port);
        LOG.debug("Creating RestClusterClient({})", restServerAddress);
        return new RestClusterClient<>(
                operatorRestConf,
                clusterId,
                (c, e) -> new StandaloneClientHAServices(restServerAddress));
    }

    @VisibleForTesting
    protected void runJar(
            JobSpec job,
            JobID jobID,
            JarUploadResponseBody response,
            Configuration conf,
            String savepoint) {
        String jarId =
                response.getFilename().substring(response.getFilename().lastIndexOf("/") + 1);
        try (var clusterClient = getClusterClient(conf)) {
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
                            RestoreMode.DEFAULT,
                            conf.get(FLINK_VERSION).isEqualOrNewer(FlinkVersion.v1_17)
                                    ? conf.toMap()
                                    : null);
            LOG.info("Submitting job: {} to session cluster.", jobID);
            clusterClient
                    .sendRequest(headers, parameters, runRequestBody)
                    .get(operatorConfig.getFlinkClientTimeout().toSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Failed to submit job to session cluster.", e);
            throw new FlinkRuntimeException(e);
        } finally {
            deleteJar(conf, jarId);
        }
    }

    @VisibleForTesting
    protected JarUploadResponseBody uploadJar(
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
                        operatorConfig.getFlinkServiceHostOverride(),
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                clusterId, namespace));
        try (var restClient = getRestClient(conf)) {
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
                    .get(operatorConfig.getFlinkClientTimeout().toSeconds(), TimeUnit.SECONDS);
        } finally {
            LOG.debug("Deleting the jar file {}", jarFile);
            FileUtils.deleteFileOrDirectory(jarFile);
        }
    }

    @VisibleForTesting
    protected RestClient getRestClient(Configuration conf) throws Exception {
        Configuration operatorRestConf = conf;
        if (SecurityOptions.isRestSSLEnabled(conf)) {
            operatorRestConf = getOperatorRestConfig(operatorRestConf);
        }
        return new RestClient(operatorRestConf, executorService);
    }

    private String findJarURI(JobSpec jobSpec) {
        if (jobSpec.getJarURI() != null) {
            return jobSpec.getJarURI();
        } else {
            return EMPTY_JAR;
        }
    }

    private void deleteJar(Configuration conf, String jarId) {
        LOG.debug("Deleting the jar: {}", jarId);
        try (var clusterClient = getClusterClient(conf)) {
            JarDeleteHeaders headers = JarDeleteHeaders.getInstance();
            JarDeleteMessageParameters parameters = headers.getUnresolvedMessageParameters();
            parameters.jarIdPathParameter.resolve(jarId);
            clusterClient
                    .sendRequest(headers, parameters, EmptyRequestBody.getInstance())
                    .get(operatorConfig.getFlinkClientTimeout().toSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Failed to delete the jar: {}.", jarId, e);
        }
    }

    /** Wait until Deployment is removed, return remaining timeout. */
    @VisibleForTesting
    protected Duration deleteDeploymentBlocking(
            String name,
            Resource<Deployment> deployment,
            DeletionPropagation propagation,
            Duration timeout) {
        return deleteBlocking(
                String.format("Deleting %s Deployment", name),
                () -> {
                    deployment.withPropagationPolicy(propagation).delete();
                    return deployment;
                },
                timeout);
    }

    @VisibleForTesting
    protected static Configuration removeOperatorConfigs(Configuration config) {
        Configuration newConfig = new Configuration();
        config.toMap()
                .forEach(
                        (k, v) -> {
                            if (!k.startsWith(K8S_OP_CONF_PREFIX)) {
                                newConfig.setString(k, v);
                            }
                        });

        return newConfig;
    }

    private void validateHaMetadataExists(Configuration conf) {
        if (!isHaMetadataAvailable(conf)) {
            throw new RecoveryFailureException(
                    "HA metadata not available to restore from last state. "
                            + "It is possible that the job has finished or terminally failed, or the configmaps have been deleted. ",
                    "RestoreFailed");
        }
    }

    private static String createEmptyJar() {
        try {
            String emptyJarPath =
                    Files.createTempDirectory("flink").toString() + "/" + EMPTY_JAR_FILENAME;

            LOG.debug("Creating empty jar to {}", emptyJarPath);
            JarOutputStream target =
                    new JarOutputStream(new FileOutputStream(emptyJarPath), new Manifest());
            target.close();

            return emptyJarPath;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create empty jar", e);
        }
    }

    public Map<String, String> getMetrics(
            Configuration conf, String jobId, List<String> metricNames) throws Exception {
        try (var clusterClient = getClusterClient(conf)) {
            var jobMetricsMessageParameters =
                    JobMetricsHeaders.getInstance().getUnresolvedMessageParameters();
            jobMetricsMessageParameters.jobPathParameter.resolve(JobID.fromHexString(jobId));
            jobMetricsMessageParameters.metricsFilterParameter.resolve(metricNames);
            var responseBody =
                    clusterClient
                            .sendRequest(
                                    JobMetricsHeaders.getInstance(),
                                    jobMetricsMessageParameters,
                                    EmptyRequestBody.getInstance())
                            .get(
                                    operatorConfig.getFlinkClientTimeout().toSeconds(),
                                    TimeUnit.SECONDS);
            return responseBody.getMetrics().stream()
                    .map(metric -> Tuple2.of(metric.getId(), metric.getValue()))
                    .collect(Collectors.toMap((t) -> t.f0, (t) -> t.f1));
        }
    }

    private TaskManagersInfo getTaskManagersInfo(Configuration conf) throws Exception {
        try (var clusterClient = getClusterClient(conf)) {
            return clusterClient
                    .sendRequest(
                            TaskManagersHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance())
                    .get(operatorConfig.getFlinkClientTimeout().toSeconds(), TimeUnit.SECONDS);
        }
    }

    @Override
    public final void deleteClusterDeployment(
            ObjectMeta meta,
            FlinkDeploymentStatus status,
            Configuration conf,
            boolean deleteHaData) {

        var namespace = meta.getNamespace();
        var clusterId = meta.getName();

        var deletionPropagation = operatorConfig.getDeletionPropagation();
        LOG.info("Deleting cluster with {} propagation", deletionPropagation);
        deleteClusterInternal(namespace, clusterId, conf, deletionPropagation);
        if (deleteHaData) {
            deleteHAData(namespace, clusterId, conf);
        } else {
            LOG.info("Keeping HA metadata for last-state restore");
        }
        updateStatusAfterClusterDeletion(status);
    }

    /**
     * Delete Flink kubernetes cluster by deleting the kubernetes resources directly.
     *
     * @param namespace Namespace
     * @param clusterId ClusterId
     * @param conf Configuration of the Flink application
     * @param deletionPropagation Resource deletion propagation policy
     */
    protected abstract void deleteClusterInternal(
            String namespace,
            String clusterId,
            Configuration conf,
            DeletionPropagation deletionPropagation);

    protected void deleteHAData(String namespace, String clusterId, Configuration conf) {
        if (FlinkUtils.isKubernetesHAActivated(conf)) {
            LOG.info("Deleting Kubernetes HA metadata");
            FlinkUtils.deleteKubernetesHAMetadata(clusterId, namespace, kubernetesClient);
        } else if (FlinkUtils.isZookeeperHAActivated(conf)) {
            LOG.info("Deleting Zookeeper HA metadata");
            FlinkUtils.deleteZookeeperHAMetadata(conf);
        }
    }

    protected void updateStatusAfterClusterDeletion(FlinkDeploymentStatus status) {
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        var currentJobState = status.getJobStatus().getState();
        if (currentJobState == null
                || !JobStatus.valueOf(currentJobState).isGloballyTerminalState()) {
            status.getJobStatus().setState(JobStatus.FINISHED.name());
        }
    }

    private Configuration getOperatorRestConfig(Configuration origConfig) throws IOException {
        Configuration conf = new Configuration(origConfig);
        EnvUtils.get(EnvUtils.ENV_OPERATOR_TRUSTSTORE_PATH)
                .ifPresent(
                        path -> {
                            if (Files.notExists(Paths.get(path))) {
                                return;
                            }
                            conf.set(
                                    SecurityOptions.SSL_REST_TRUSTSTORE,
                                    EnvUtils.getRequired(EnvUtils.ENV_OPERATOR_TRUSTSTORE_PATH));
                            conf.set(
                                    SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD,
                                    EnvUtils.getRequired(EnvUtils.ENV_OPERATOR_KEYSTORE_PASSWORD));
                            if (SecurityOptions.isRestSSLAuthenticationEnabled(conf)
                                    && EnvUtils.get(EnvUtils.ENV_OPERATOR_KEYSTORE_PATH)
                                            .isPresent()) {
                                conf.set(
                                        SecurityOptions.SSL_REST_KEYSTORE,
                                        EnvUtils.getRequired(EnvUtils.ENV_OPERATOR_KEYSTORE_PATH));
                                conf.set(
                                        SecurityOptions.SSL_REST_KEYSTORE_PASSWORD,
                                        EnvUtils.getRequired(
                                                EnvUtils.ENV_OPERATOR_KEYSTORE_PASSWORD));
                                conf.set(
                                        SecurityOptions.SSL_REST_KEY_PASSWORD,
                                        EnvUtils.getRequired(
                                                EnvUtils.ENV_OPERATOR_KEYSTORE_PASSWORD));
                            } else {
                                conf.removeConfig(SecurityOptions.SSL_REST_KEYSTORE);
                                conf.removeConfig(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD);
                            }
                            conf.removeConfig(SecurityOptions.SSL_TRUSTSTORE);
                            conf.removeConfig(SecurityOptions.SSL_TRUSTSTORE_PASSWORD);
                            conf.removeConfig(SecurityOptions.SSL_KEYSTORE);
                            conf.removeConfig(SecurityOptions.SSL_KEYSTORE_PASSWORD);
                        });
        return conf;
    }

    /**
     * Generic blocking delete operation implementation for triggering and waiting for removal of
     * the selected resources. By returning the remaining timeout we allow chaining multiple delete
     * operations under a single timeout setting easily.
     *
     * @param operation Name of the operation for logging
     * @param delete Call that should trigger the async deletion and return the resource to be
     *     watched
     * @param timeout Timeout for the operation
     * @return Remaining timeout after deletion.
     */
    @SneakyThrows
    protected static Duration deleteBlocking(
            String operation, Callable<Waitable> delete, Duration timeout) {
        LOG.info("{} with {} seconds timeout...", operation, timeout.toSeconds());
        long start = System.currentTimeMillis();

        Waitable deleted = null;
        try {
            deleted = delete.call();
        } catch (KubernetesClientException kce) {
            // During the deletion we need to throw other types of errors
            if (kce.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                throw kce;
            }
        }

        if (deleted != null) {
            try {
                deleted.waitUntilCondition(
                        Objects::isNull, timeout.toMillis(), TimeUnit.MILLISECONDS);
                LOG.info("Completed {}", operation);
            } catch (KubernetesClientException kce) {
                // We completely ignore not found errors and simply log others
                if (kce.getCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                    LOG.warn("Error while " + operation, kce);
                }
            }
        }

        long elapsedMillis = System.currentTimeMillis() - start;
        return Duration.ofMillis(Math.max(0, timeout.toMillis() - elapsedMillis));
    }
}
