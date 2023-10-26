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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CheckpointInfo;
import org.apache.flink.kubernetes.operator.api.status.CheckpointType;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.observer.CheckpointFetchResult;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.kubernetes.operator.service.CheckpointHistoryWrapper;
import org.apache.flink.kubernetes.operator.service.NativeFlinkServiceTest;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
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
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
    @Setter private boolean jobManagerReady = true;
    @Setter private boolean deployFailure = false;
    @Setter private Runnable sessionJobSubmittedCallback;
    @Setter private PodList podList = new PodList();
    @Setter private Consumer<Configuration> listJobConsumer = conf -> {};
    private final List<String> disposedSavepoints = new ArrayList<>();
    private final Map<String, Boolean> savepointTriggers = new HashMap<>();
    private final Map<String, Boolean> checkpointTriggers = new HashMap<>();

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

    @Setter private boolean scalingCompleted;

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
            throw new RecoveryFailureException(
                    "HA metadata not available to restore from last state. "
                            + "It is possible that the job has finished or terminally failed, or the configmaps have been deleted. "
                            + "Manual restore required.",
                    "RestoreFailed");
        }
    }

    @Override
    public boolean isHaMetadataAvailable(Configuration conf) {
        return HighAvailabilityMode.isHighAvailabilityModeActivated(conf) && haDataAvailable;
    }

    @Override
    public void submitSessionCluster(Configuration conf) throws Exception {
        if (deployFailure) {
            throw new Exception("Deployment failure");
        }
        sessions.add(conf.get(KubernetesConfigOptions.CLUSTER_ID));
    }

    @Override
    public JobID submitJobToSessionCluster(
            ObjectMeta meta,
            FlinkSessionJobSpec spec,
            Configuration conf,
            @Nullable String savepoint)
            throws Exception {

        if (deployFailure) {
            throw new Exception("Deployment failure");
        }
        JobID jobID = FlinkUtils.generateSessionJobFixedJobID(meta);
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
    public Collection<JobStatusMessage> listJobs(Configuration conf) throws Exception {
        if (!isPortReady) {
            throw new TimeoutException("JM port is unavailable");
        }
        return super.listJobs(conf);
    }

    public List<Tuple3<String, JobStatusMessage, Configuration>> listJobs() {
        return jobs;
    }

    public long getRunningCount() {
        return jobs.stream().filter(t -> !t.f1.getJobState().isTerminalState()).count();
    }

    @Override
    public void triggerSavepoint(
            String jobId,
            SnapshotTriggerType triggerType,
            SavepointInfo savepointInfo,
            Configuration conf) {
        var triggerId = "savepoint_trigger_" + savepointTriggerCounter++;

        var savepointFormatType =
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);
        savepointInfo.setTrigger(
                triggerId, triggerType, SavepointFormatType.valueOf(savepointFormatType.name()));
        savepointTriggers.put(triggerId, false);
    }

    @Override
    public void triggerCheckpoint(
            String jobId,
            SnapshotTriggerType triggerType,
            CheckpointInfo checkpointInfo,
            Configuration conf) {
        var triggerId = "checkpoint_trigger_" + checkpointTriggerCounter++;

        var checkpointType = conf.get(KubernetesOperatorConfigOptions.OPERATOR_CHECKPOINT_TYPE);
        checkpointInfo.setTrigger(
                triggerId, triggerType, CheckpointType.valueOf(checkpointType.name()));
        checkpointTriggers.put(triggerId, false);
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

        return SavepointFetchResult.error("Failed");
    }

    @Override
    public CheckpointFetchResult fetchCheckpointInfo(
            String triggerId, String jobId, Configuration conf) {

        if (checkpointTriggers.containsKey(triggerId)) {
            if (checkpointTriggers.get(triggerId)) {
                checkpointCounter++;
                return CheckpointFetchResult.completed();
            }
            checkpointTriggers.put(triggerId, true);
            return CheckpointFetchResult.pending();
        }

        return CheckpointFetchResult.error("Failed");
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
                        return CompletableFuture.completedFuture(
                                cancelJob(flinkVersion, jobID, true));
                    } catch (Exception e) {
                        return CompletableFuture.failedFuture(e);
                    }
                });

        clusterClient.setCancelFunction(
                jobID -> {
                    try {
                        cancelJob(flinkVersion, jobID, false);
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
    public void cancelJob(
            FlinkDeployment deployment, UpgradeMode upgradeMode, Configuration configuration)
            throws Exception {
        cancelJob(deployment, upgradeMode, configuration, false);
    }

    private String cancelJob(FlinkVersion flinkVersion, JobID jobID, boolean savepoint)
            throws Exception {
        cancelJobCallCount++;

        if (!isPortReady) {
            throw new TimeoutException("JM port is unavailable");
        }

        if (isFlinkJobNotFound) {
            throw new FlinkJobNotFoundException(jobID);
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
                        JobStatus.FINISHED,
                        oldStatus.getStartTime());
        jobOpt.get().f0 = sp;

        return sp;
    }

    @Override
    protected void deleteClusterInternal(
            ObjectMeta meta,
            Configuration conf,
            boolean deleteHaMeta,
            DeletionPropagation deletionPropagation) {
        jobs.clear();
        sessions.remove(meta.getName());
    }

    @Override
    public void waitForClusterShutdown(Configuration conf) {}

    @Override
    public void disposeSavepoint(String savepointPath, Configuration conf) {
        disposedSavepoints.add(savepointPath);
    }

    public List<String> getDisposedSavepoints() {
        return disposedSavepoints;
    }

    @Override
    public Optional<Savepoint> getLastCheckpoint(JobID jobId, Configuration conf) throws Exception {
        jobs.stream()
                .filter(js -> js.f1.getJobId().equals(jobId))
                .findAny()
                .ifPresent(
                        t -> {
                            if (!t.f1.getJobState().isGloballyTerminalState()) {
                                throw new RuntimeException(
                                        "Checkpoint should not be queried if job is not in terminal state");
                            }
                        });

        return super.getLastCheckpoint(jobId, conf);
    }

    @Override
    public Tuple2<
                    Optional<CheckpointHistoryWrapper.CompletedCheckpointInfo>,
                    Optional<CheckpointHistoryWrapper.PendingCheckpointInfo>>
            getCheckpointInfo(JobID jobId, Configuration conf) throws Exception {

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

    @Override
    protected PodList getTmPodList(String namespace, String clusterId) {
        return new PodList();
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
    public Map<String, String> getClusterInfo(Configuration conf) {
        return CLUSTER_INFO;
    }

    @Override
    public ScalingResult scale(FlinkResourceContext<?> ctx, Configuration deployConfig) {
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
            return ScalingResult.SCALING_TRIGGERED;
        }

        return ScalingResult.CANNOT_SCALE;
    }

    @Override
    public boolean scalingCompleted(FlinkResourceContext<?> resourceContext) {
        return scalingCompleted;
    }

    public void setMetricValue(String name, String value) {
        metricsValues.put(name, value);
    }

    @Override
    public Map<String, String> getMetrics(
            Configuration conf, String jobId, List<String> metricNames) {
        return metricsValues;
    }

    @Override
    public JobDetailsInfo getJobDetailsInfo(JobID jobID, Configuration conf) {
        return NativeFlinkServiceTest.createJobDetailsFor(List.of());
    }
}
