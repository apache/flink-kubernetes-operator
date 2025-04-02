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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.UpgradeFailureException;
import org.apache.flink.kubernetes.operator.observer.CheckpointFetchResult;
import org.apache.flink.kubernetes.operator.observer.CheckpointStatsResult;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.kubernetes.operator.service.CheckpointHistoryWrapper;
import org.apache.flink.kubernetes.operator.service.SuspendMode;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Flink service mock for tests. */
public class TestingFlinkService extends AbstractFlinkService {

    public static final Map<String, String> CLUSTER_INFO =
            Map.of(
                    DashboardConfiguration.FIELD_NAME_FLINK_VERSION,
                    "15.0.0",
                    DashboardConfiguration.FIELD_NAME_FLINK_REVISION,
                    "1234567 @ 1970-01-01T00:00:00+00:00");
    public static final String SNAPSHOT_ERROR_MESSAGE = "Failed";

    private int savepointCounter = 0;
    private int savepointTriggerCounter = 0;

    private int checkpointCounter = 0;
    private int checkpointTriggerCounter = 0;

    private final List<Tuple3<String, JobStatusMessage, Configuration>> jobs = new ArrayList<>();
    private final Map<JobID, String> jobErrors = new HashMap<>();
    @Getter private final Set<String> sessions = new HashSet<>();
    @Setter private boolean isFlinkJobNotFound = false;
    @Setter private boolean isFlinkJobTerminatedWithoutCancellation = false;
    @Setter private boolean isPortReady = true;
    @Setter private boolean haDataAvailable = true;
    @Setter private boolean checkpointAvailable = true;
    @Setter private boolean jobManagerReady = true;
    @Setter private boolean deployFailure = false;
    @Setter private Exception makeItFailWith;
    @Setter private boolean triggerSavepointFailure = false;
    @Setter private boolean disposeSavepointFailure = false;
    @Setter private Runnable sessionJobSubmittedCallback;
    @Setter private PodList podList = new PodList();
    @Setter private Consumer<Configuration> listJobConsumer = conf -> {};
    @Getter private final List<String> disposedSavepoints = new ArrayList<>();
    @Getter private final Map<String, Boolean> savepointTriggers = new HashMap<>();
    @Getter private final Map<String, Boolean> checkpointTriggers = new HashMap<>();
    private final Map<Long, String> checkpointStats = new HashMap<>();
    @Setter private boolean throwCheckpointingDisabledError = false;
    @Setter private Throwable jobFailedErr;

    @Getter private int desiredReplicas = 0;
    @Getter private int cancelJobCallCount = 0;

    @Getter private Configuration submittedConf;

    @Setter
    private Tuple2<
                    Optional<CheckpointHistoryWrapper.CompletedCheckpointInfo>,
                    Optional<CheckpointHistoryWrapper.PendingCheckpointInfo>>
            checkpointInfo;

    private Map<String, String> metricsValues = new HashMap<>();

    @Setter
    private Collection<AggregatedMetric> aggregatedMetricsResponse = Collections.emptyList();

    public TestingFlinkService() {
        this(null);
    }

    public TestingFlinkService(KubernetesClient kubernetesClient) {
        super(
                kubernetesClient,
                null,
                Executors.newDirectExecutorService(),
                FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
    }

    public <T extends HasMetadata> Context<T> getContext() {
        return new TestUtils.TestingContext<>() {

            @Override
            public Optional<T> getSecondaryResource(Class aClass, String s) {
                if (jobs.isEmpty() && sessions.isEmpty()) {
                    return Optional.empty();
                }
                return (Optional<T>) Optional.of(TestUtils.createDeployment(jobManagerReady));
            }

            @Override
            public KubernetesClient getClient() {
                return kubernetesClient;
            }
        };
    }

    public void clear() {
        jobs.clear();
        sessions.clear();
        savepointTriggerCounter = 0;
        savepointCounter = 0;
        checkpointTriggerCounter = 0;
        checkpointCounter = 0;
    }

    public void clearJobsInTerminalState() {
        jobs.removeIf(job -> job.f1.getJobState().isTerminalState());
    }

    @Override
    public void submitApplicationCluster(
            JobSpec jobSpec, Configuration conf, boolean requireHaMetadata) throws Exception {

        if (requireHaMetadata) {
            validateHaMetadataExists(conf);
        }
        deployApplicationCluster(jobSpec, removeOperatorConfigs(conf));
        submittedConf = conf.clone();
    }

    protected void deployApplicationCluster(JobSpec jobSpec, Configuration conf) throws Exception {
        if (makeItFailWith != null) {
            throw makeItFailWith;
        }
        if (deployFailure) {
            throw new Exception("Deployment failure");
        }
        if (!jobs.isEmpty()) {
            throw new Exception("Cannot submit 2 application clusters at the same time");
        }
        JobID jobID = new JobID();
        if (conf.contains(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)) {
            jobID = JobID.fromHexString(conf.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));
        }
        JobStatusMessage jobStatusMessage =
                new JobStatusMessage(
                        jobID,
                        conf.getString(KubernetesConfigOptions.CLUSTER_ID),
                        JobStatus.RUNNING,
                        System.currentTimeMillis());

        jobs.add(
                Tuple3.of(conf.get(SavepointConfigOptions.SAVEPOINT_PATH), jobStatusMessage, conf));
    }

    protected void validateHaMetadataExists(Configuration conf) {
        if (!isHaMetadataAvailable(conf)) {
            throw new UpgradeFailureException(
                    "HA metadata not available to restore from last state. "
                            + "It is possible that the job has finished or terminally failed, or the configmaps have been deleted. "
                            + "Manual restore required.",
                    "RestoreFailed");
        }
    }

    @Override
    public boolean atLeastOneCheckpoint(Configuration conf) {
        return isHaMetadataAvailable(conf) && checkpointAvailable;
    }

    @Override
    public boolean isHaMetadataAvailable(Configuration conf) {
        return HighAvailabilityMode.isHighAvailabilityModeActivated(conf) && haDataAvailable;
    }

    @Override
    public void deploySessionCluster(Configuration conf) throws Exception {
        if (deployFailure) {
            throw new Exception("Deployment failure");
        }
        sessions.add(conf.get(KubernetesConfigOptions.CLUSTER_ID));
    }

    @Override
    public JobID submitJobToSessionCluster(
            ObjectMeta meta,
            FlinkSessionJobSpec spec,
            JobID jobID,
            Configuration conf,
            @Nullable String savepoint)
            throws Exception {

        if (makeItFailWith != null) {
            throw makeItFailWith;
        }

        if (deployFailure) {
            throw new Exception("Deployment failure");
        }
        JobStatusMessage jobStatusMessage =
                new JobStatusMessage(
                        jobID,
                        conf.getString(KubernetesConfigOptions.CLUSTER_ID),
                        JobStatus.RUNNING,
                        System.currentTimeMillis());

        jobs.add(Tuple3.of(savepoint, jobStatusMessage, conf));
        if (sessionJobSubmittedCallback != null) {
            sessionJobSubmittedCallback.run();
        }
        return jobID;
    }

    @Override
    public Optional<JobStatusMessage> getJobStatus(Configuration conf, JobID jobID)
            throws Exception {
        if (!isPortReady) {
            throw new TimeoutException("JM port is unavailable");
        }

        if (jobFailedErr != null) {
            return Optional.of(new JobStatusMessage(jobID, "n", JobStatus.FAILED, 0));
        }

        return super.getJobStatus(conf, jobID);
    }

    @Override
    public JobResult requestJobResult(Configuration conf, JobID jobID) throws Exception {
        if (jobFailedErr != null) {
            return new JobResult.Builder()
                    .jobId(jobID)
                    .serializedThrowable(new SerializedThrowable(jobFailedErr))
                    .netRuntime(1)
                    .accumulatorResults(new HashMap<>())
                    .applicationStatus(ApplicationStatus.FAILED)
                    .build();
        }

        return super.requestJobResult(conf, jobID);
    }

    public List<Tuple3<String, JobStatusMessage, Configuration>> listJobs() {
        return jobs;
    }

    public long getRunningCount() {
        return jobs.stream().filter(t -> !t.f1.getJobState().isTerminalState()).count();
    }

    public void triggerSavepointLegacy(
            String jobId,
            SnapshotTriggerType triggerType,
            AbstractFlinkResource<?, ?> flinkResource,
            Configuration conf)
            throws Exception {
        var savepointFormatType =
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);
        var triggerId = triggerSavepoint(jobId, savepointFormatType, null, conf);

        flinkResource
                .getStatus()
                .getJobStatus()
                .getSavepointInfo()
                .setTrigger(
                        triggerId,
                        triggerType,
                        SavepointFormatType.valueOf(savepointFormatType.name()));
        savepointTriggers.put(triggerId, false);
    }

    @Override
    public String triggerSavepoint(
            String jobId,
            org.apache.flink.core.execution.SavepointFormatType savepointFormatType,
            String savepointDirectory,
            Configuration conf)
            throws Exception {
        if (triggerSavepointFailure) {
            throw new Exception(SNAPSHOT_ERROR_MESSAGE);
        }
        var triggerId = "savepoint_trigger_" + savepointTriggerCounter++;
        savepointTriggers.put(triggerId, false);
        return triggerId;
    }

    @Override
    public String triggerCheckpoint(
            String jobId,
            org.apache.flink.core.execution.CheckpointType checkpointType,
            Configuration conf) {
        var triggerId = "checkpoint_trigger_" + checkpointTriggerCounter++;
        checkpointTriggers.put(triggerId, false);

        return triggerId;
    }

    @Override
    public SavepointFetchResult fetchSavepointInfo(
            String triggerId, String jobId, Configuration conf) {

        if (savepointTriggers.containsKey(triggerId)) {
            if (savepointTriggers.get(triggerId)) {
                return SavepointFetchResult.completed("savepoint_" + savepointCounter++);
            }
            savepointTriggers.put(triggerId, true);
            return SavepointFetchResult.pending();
        }

        return SavepointFetchResult.error(SNAPSHOT_ERROR_MESSAGE);
    }

    @Override
    public CheckpointFetchResult fetchCheckpointInfo(
            String triggerId, String jobId, Configuration conf) {

        if (checkpointTriggers.containsKey(triggerId)) {
            if (checkpointTriggers.get(triggerId)) {
                // Mark completed checkpoint
                checkpointInfo =
                        Tuple2.of(
                                Optional.of(
                                        new CheckpointHistoryWrapper.CompletedCheckpointInfo(
                                                checkpointCounter,
                                                "ck_" + checkpointCounter,
                                                System.currentTimeMillis())),
                                Optional.empty());

                checkpointCounter++;
                checkpointStats.put(
                        (long) checkpointCounter,
                        String.format("checkpoint_%d", (long) checkpointCounter));
                return CheckpointFetchResult.completed((long) checkpointCounter);
            }
            checkpointTriggers.put(triggerId, true);
            return CheckpointFetchResult.pending();
        }

        return CheckpointFetchResult.error(SNAPSHOT_ERROR_MESSAGE);
    }

    @Override
    public CheckpointStatsResult fetchCheckpointStats(
            String jobId, Long checkpointId, Configuration conf) {
        if (checkpointStats.containsKey(checkpointId)) {
            return CheckpointStatsResult.completed(checkpointStats.get(checkpointId));
        }
        return CheckpointStatsResult.pending();
    }

    @Override
    public RestClusterClient<String> getClusterClient(Configuration config) throws Exception {

        TestingClusterClient<String> clusterClient = new TestingClusterClient<>(config);
        FlinkVersion flinkVersion = config.get(FlinkConfigBuilder.FLINK_VERSION);
        clusterClient.setListJobsFunction(
                () -> {
                    listJobConsumer.accept(config);
                    if (jobs.isEmpty()
                            && !sessions.isEmpty()
                            && config.get(DeploymentOptions.TARGET)
                                    .equals(KubernetesDeploymentTarget.APPLICATION.getName())) {
                        throw new RuntimeException("Trying to list a job without submitting it");
                    }
                    return CompletableFuture.completedFuture(
                            jobs.stream().map(t -> t.f1).collect(Collectors.toList()));
                });

        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceEventTime, savepointDir) -> {
                    try {
                        return CompletableFuture.completedFuture(cancelJob(jobID, true));
                    } catch (Exception e) {
                        return CompletableFuture.failedFuture(e);
                    }
                });

        clusterClient.setCancelFunction(
                jobID -> {
                    try {
                        cancelJob(jobID, false);
                    } catch (Exception e) {
                        return CompletableFuture.failedFuture(e);
                    }
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        clusterClient.setRequestResultFunction(
                jobID -> {
                    var builder = new JobResult.Builder().jobId(jobID).netRuntime(1);
                    if (jobErrors.containsKey(jobID)) {
                        builder.serializedThrowable(
                                new SerializedThrowable(
                                        new RuntimeException(jobErrors.get(jobID))));
                    }
                    return CompletableFuture.completedFuture(builder.build());
                });
        clusterClient.setRequestProcessor(
                (messageHeaders, messageParameters, requestBody) -> {
                    if (messageHeaders instanceof JobsOverviewHeaders) {
                        return CompletableFuture.completedFuture(getMultipleJobsDetails());
                    } else if (messageHeaders instanceof AggregatedSubtaskMetricsHeaders) {
                        return CompletableFuture.completedFuture(getSubtaskMetrics());
                    }
                    return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
                });
        return clusterClient;
    }

    private MultipleJobsDetails getMultipleJobsDetails() {
        return new MultipleJobsDetails(
                jobs.stream()
                        .map(tuple -> tuple.f1)
                        .map(TestingFlinkService::toJobDetails)
                        .collect(Collectors.toList()));
    }

    private AggregatedMetricsResponseBody getSubtaskMetrics() {
        return new AggregatedMetricsResponseBody(aggregatedMetricsResponse);
    }

    private static JobDetails toJobDetails(JobStatusMessage jobStatus) {
        return new JobDetails(
                jobStatus.getJobId(),
                jobStatus.getJobName(),
                jobStatus.getStartTime(),
                -1,
                System.currentTimeMillis() - jobStatus.getStartTime(),
                jobStatus.getJobState(),
                System.currentTimeMillis(),
                new int[ExecutionState.values().length],
                0);
    }

    @Override
    public CancelResult cancelJob(
            FlinkDeployment deployment, SuspendMode upgradeMode, Configuration configuration)
            throws Exception {
        return cancelJob(deployment, upgradeMode, configuration, false);
    }

    @VisibleForTesting
    public String cancelJob(JobID jobID, boolean savepoint) throws Exception {
        cancelJobCallCount++;

        if (!isPortReady) {
            throw new TimeoutException("JM port is unavailable");
        }

        if (isFlinkJobNotFound) {
            // Throw different exceptions randomly, see
            // https://github.com/apache/flink-kubernetes-operator/pull/818
            if (new Random().nextBoolean()) {
                throw new RestClientException(
                        "Job could not be found.",
                        new FlinkJobNotFoundException(jobID),
                        HttpResponseStatus.NOT_FOUND);
            } else {
                throw new FlinkJobNotFoundException(jobID);
            }
        }

        var jobOpt = jobs.stream().filter(js -> js.f1.getJobId().equals(jobID)).findAny();

        if (isFlinkJobTerminatedWithoutCancellation) {
            JobStatusMessage oldStatus = jobOpt.get().f1;
            jobOpt.get().f1 =
                    new JobStatusMessage(
                            oldStatus.getJobId(),
                            oldStatus.getJobName(),
                            JobStatus.FAILED,
                            oldStatus.getStartTime());
            throw new FlinkJobTerminatedWithoutCancellationException(jobID, JobStatus.FAILED);
        }

        if (jobOpt.isEmpty()) {
            throw new Exception("Job not found");
        }

        var sp = savepoint ? "savepoint_" + savepointCounter++ : null;

        JobStatusMessage oldStatus = jobOpt.get().f1;
        jobOpt.get().f1 =
                new JobStatusMessage(
                        oldStatus.getJobId(),
                        oldStatus.getJobName(),
                        savepoint ? JobStatus.FINISHED : JobStatus.CANCELED,
                        oldStatus.getStartTime());
        jobOpt.get().f0 = sp;

        return sp;
    }

    @Override
    protected void deleteClusterInternal(
            String namespace,
            String clusterId,
            Configuration conf,
            DeletionPropagation deletionPropagation) {
        jobs.clear();
        sessions.remove(clusterId);
    }

    @Override
    protected Duration deleteDeploymentBlocking(
            String name,
            Resource<Deployment> deployment,
            DeletionPropagation propagation,
            Duration timeout) {
        return timeout;
    }

    @Override
    public void disposeSavepoint(String savepointPath, Configuration conf) throws Exception {
        if (disposeSavepointFailure) {
            throw new Exception(SNAPSHOT_ERROR_MESSAGE);
        }
        disposedSavepoints.add(savepointPath);
    }

    @Override
    public Optional<Savepoint> getLastCheckpoint(JobID jobId, Configuration conf) {
        jobs.stream()
                .filter(js -> js.f1.getJobId().equals(jobId))
                .findAny()
                .ifPresent(
                        t -> {
                            // TODO: check this... for example getting the SP/CP
                            //   in RUNNING state should be valid
                            // if (!t.f1.getJobState().isGloballyTerminalState()) {
                            //      throw new RuntimeException(
                            //          "Checkpoint should not be
                            //          queried if job is not in terminal state");
                            // }
                        });

        return super.getLastCheckpoint(jobId, conf);
    }

    @Override
    public Tuple2<
                    Optional<CheckpointHistoryWrapper.CompletedCheckpointInfo>,
                    Optional<CheckpointHistoryWrapper.PendingCheckpointInfo>>
            getCheckpointInfo(JobID jobId, Configuration conf) throws Exception {
        if (throwCheckpointingDisabledError) {
            throw new ExecutionException(
                    new RestClientException(
                            "Checkpointing has not been enabled", HttpResponseStatus.BAD_REQUEST));
        }

        if (checkpointInfo != null) {
            return checkpointInfo;
        }

        var jobOpt = jobs.stream().filter(js -> js.f1.getJobId().equals(jobId)).findAny();

        if (jobOpt.isEmpty()) {
            throw new Exception("Job not found");
        }

        var t = jobOpt.get();

        if (t.f0 != null) {
            return Tuple2.of(
                    Optional.of(
                            new CheckpointHistoryWrapper.CompletedCheckpointInfo(
                                    0L, t.f0, System.currentTimeMillis())),
                    Optional.empty());
        } else {
            return Tuple2.of(Optional.empty(), Optional.empty());
        }
    }

    @Override
    public boolean isJobManagerPortReady(Configuration config) {
        return isPortReady;
    }

    @Override
    public PodList getJmPodList(FlinkDeployment deployment, Configuration conf) {
        return podList;
    }

    @Override
    protected PodList getJmPodList(String namespace, String clusterId) {
        return podList;
    }

    public void markApplicationJobFailedWithError(JobID jobID, String error) throws Exception {
        var job = jobs.stream().filter(tuple -> tuple.f1.getJobId().equals(jobID)).findFirst();
        if (job.isEmpty()) {
            throw new Exception("The target job missed");
        }
        var oldStatus = job.get().f1;
        job.get().f1 =
                new JobStatusMessage(
                        oldStatus.getJobId(),
                        oldStatus.getJobName(),
                        JobStatus.FAILED,
                        oldStatus.getStartTime());
        jobErrors.put(jobID, error);
    }

    @Override
    public Map<String, String> getClusterInfo(Configuration conf, String jobId)
            throws TimeoutException {
        if (!isPortReady) {
            throw new TimeoutException("JM port is unavailable");
        }
        return CLUSTER_INFO;
    }

    @Override
    public boolean scale(FlinkResourceContext<?> ctx, Configuration deployConfig) {
        boolean standalone = ctx.getDeploymentMode() == KubernetesDeploymentMode.STANDALONE;
        boolean session = ctx.getResource().getSpec().getJob() == null;
        boolean reactive =
                ctx.getObserveConfig().get(JobManagerOptions.SCHEDULER_MODE)
                        == SchedulerExecutionMode.REACTIVE;

        if (standalone && (session || reactive)) {
            desiredReplicas =
                    ctx.getDeployConfig(ctx.getResource().getSpec())
                            .get(
                                    StandaloneKubernetesConfigOptionsInternal
                                            .KUBERNETES_TASKMANAGER_REPLICAS);
            return true;
        }

        return false;
    }

    public void setMetricValue(String name, String value) {
        metricsValues.put(name, value);
    }

    @Override
    public Map<String, String> getMetrics(
            Configuration conf, String jobId, List<String> metricNames) {
        return metricsValues;
    }
}
