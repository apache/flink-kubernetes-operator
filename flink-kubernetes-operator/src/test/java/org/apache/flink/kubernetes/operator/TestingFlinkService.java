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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.util.SerializedThrowable;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedDependentResourceContext;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Flink service mock for tests. */
public class TestingFlinkService extends FlinkService {

    public static final Map<String, String> CLUSTER_INFO =
            Map.of(
                    DashboardConfiguration.FIELD_NAME_FLINK_VERSION,
                    "15.0.0",
                    DashboardConfiguration.FIELD_NAME_FLINK_REVISION,
                    "1234567 @ 1970-01-01T00:00:00+00:00");

    private int savepointCounter = 0;
    private int triggerCounter = 0;

    private final List<Tuple2<String, JobStatusMessage>> jobs = new ArrayList<>();
    private final Map<JobID, String> jobErrors = new HashMap<>();
    private final Set<String> sessions = new HashSet<>();
    private boolean isPortReady = true;
    private boolean haDataAvailable = true;
    private boolean deployFailure = false;
    private Runnable sessionJobSubmittedCallback;
    private PodList podList = new PodList();
    private Consumer<Configuration> listJobConsumer = conf -> {};
    private List<String> disposedSavepoints = new ArrayList<>();
    private Map<String, Boolean> savepointTriggers = new HashMap<>();

    public TestingFlinkService() {
        super(null, new FlinkConfigManager(new Configuration()));
    }

    public TestingFlinkService(KubernetesClient kubernetesClient) {
        super(kubernetesClient, new FlinkConfigManager(new Configuration()));
    }

    public Context getContext() {
        return new Context() {
            @Override
            public Optional<RetryInfo> getRetryInfo() {
                return Optional.empty();
            }

            @Override
            public Optional getSecondaryResource(Class aClass, String s) {
                if (jobs.isEmpty() && sessions.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(TestUtils.createDeployment(true));
            }

            @Override
            public Set getSecondaryResources(Class expectedType) {
                return null;
            }

            @Override
            public ControllerConfiguration getControllerConfiguration() {
                return null;
            }

            @Override
            public ManagedDependentResourceContext managedDependentResourceContext() {
                return null;
            }
        };
    }

    public void clear() {
        jobs.clear();
        sessions.clear();
    }

    @Override
    public void submitApplicationCluster(
            JobSpec jobSpec, Configuration conf, boolean requireHaMetadata) throws Exception {

        if (requireHaMetadata) {
            validateHaMetadataExists(conf);
        }
        if (deployFailure) {
            throw new Exception("Deployment failure");
        }
        if (!jobs.isEmpty()) {
            throw new Exception("Cannot submit 2 application clusters at the same time");
        }
        JobID jobID = new JobID();
        JobStatusMessage jobStatusMessage =
                new JobStatusMessage(
                        jobID,
                        conf.getString(KubernetesConfigOptions.CLUSTER_ID),
                        JobStatus.RUNNING,
                        System.currentTimeMillis());

        jobs.add(Tuple2.of(conf.get(SavepointConfigOptions.SAVEPOINT_PATH), jobStatusMessage));
    }

    @Override
    public boolean isHaMetadataAvailable(Configuration conf) {
        return FlinkUtils.isKubernetesHAActivated(conf) && haDataAvailable;
    }

    public void setHaDataAvailable(boolean haDataAvailable) {
        this.haDataAvailable = haDataAvailable;
    }

    public void setDeployFailure(boolean deployFailure) {
        this.deployFailure = deployFailure;
    }

    public void setSessionJobSubmittedCallback(Runnable sessionJobSubmittedCallback) {
        this.sessionJobSubmittedCallback = sessionJobSubmittedCallback;
    }

    @Override
    public void submitSessionCluster(Configuration conf) {
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

        jobs.add(Tuple2.of(savepoint, jobStatusMessage));
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

    @Override
    public JobDetailsInfo getJobDetailsInfo(JobID jobID, Configuration conf) throws Exception {
        if (!isPortReady) {
            throw new TimeoutException("JM port is unavailable");
        }
        return super.getJobDetailsInfo(jobID, conf);
    }

    public void setListJobConsumer(Consumer<Configuration> listJobConsumer) {
        this.listJobConsumer = listJobConsumer;
    }

    public List<Tuple2<String, JobStatusMessage>> listJobs() {
        return jobs;
    }

    public long getRunningCount() {
        return jobs.stream().filter(t -> !t.f1.getJobState().isTerminalState()).count();
    }

    @Override
    public void triggerSavepoint(
            String jobId,
            SavepointTriggerType triggerType,
            SavepointInfo savepointInfo,
            Configuration conf) {
        var triggerId = "trigger_" + triggerCounter++;
        savepointInfo.setTrigger(triggerId, triggerType);
        savepointTriggers.put(triggerId, false);
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
    protected ClusterClient<String> getClusterClient(Configuration config) throws Exception {
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
                    } else if (messageHeaders instanceof JobDetailsHeaders) {
                        JobID jobID =
                                ((JobMessageParameters) messageParameters)
                                        .jobPathParameter.getValue();

                        Optional<Tuple2<String, JobStatusMessage>> jobOpt =
                                jobs.stream()
                                        .filter(js -> js.f1.getJobId().equals(jobID))
                                        .findAny();

                        if (!jobOpt.isPresent()) {
                            return CompletableFuture.failedFuture(new Exception("Job not found"));
                        }

                        final Random random = new Random();
                        final int numJobVertexDetailsInfos = 4;
                        final String jsonPlan = "{\"id\":\"1234\"}";

                        final Map<JobStatus, Long> timestamps =
                                new HashMap<>(JobStatus.values().length);
                        final Collection<JobDetailsInfo.JobVertexDetailsInfo> jobVertexInfos =
                                new ArrayList<>(numJobVertexDetailsInfos);
                        final Map<ExecutionState, Integer> jobVerticesPerState =
                                new HashMap<>(ExecutionState.values().length);

                        for (JobStatus jobStatus : JobStatus.values()) {
                            timestamps.put(jobStatus, random.nextLong());
                        }

                        for (int i = 0; i < numJobVertexDetailsInfos; i++) {
                            jobVertexInfos.add(createJobVertexDetailsInfo(random));
                        }

                        for (ExecutionState executionState : ExecutionState.values()) {
                            jobVerticesPerState.put(executionState, random.nextInt());
                        }

                        JobDetailsInfo jobDetailsInfo =
                                new JobDetailsInfo(
                                        jobOpt.get().f1.getJobId(),
                                        jobOpt.get().f1.getJobName(),
                                        true,
                                        jobOpt.get().f1.getJobState(),
                                        jobOpt.get().f1.getStartTime(),
                                        -1L,
                                        1L,
                                        8888L,
                                        1984L,
                                        timestamps,
                                        jobVertexInfos,
                                        jobVerticesPerState,
                                        jsonPlan);

                        return CompletableFuture.completedFuture(jobDetailsInfo);
                    }
                    return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
                });
        return clusterClient;
    }

    private JobDetailsInfo.JobVertexDetailsInfo createJobVertexDetailsInfo(Random random) {
        final Map<ExecutionState, Integer> tasksPerState =
                new HashMap<>(ExecutionState.values().length);
        final IOMetricsInfo jobVertexMetrics =
                new IOMetricsInfo(
                        random.nextLong(),
                        random.nextBoolean(),
                        random.nextLong(),
                        random.nextBoolean(),
                        random.nextLong(),
                        random.nextBoolean(),
                        random.nextLong(),
                        random.nextBoolean());

        for (ExecutionState executionState : ExecutionState.values()) {
            tasksPerState.put(executionState, random.nextInt());
        }

        int parallelism = 1 + (random.nextInt() / 3);
        return new JobDetailsInfo.JobVertexDetailsInfo(
                new JobVertexID(),
                "jobVertex" + random.nextLong(),
                2 * parallelism,
                parallelism,
                ExecutionState.values()[random.nextInt(ExecutionState.values().length)],
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                tasksPerState,
                jobVertexMetrics);
    }

    private MultipleJobsDetails getMultipleJobsDetails() {
        return new MultipleJobsDetails(
                jobs.stream()
                        .map(tuple -> tuple.f1)
                        .map(TestingFlinkService::toJobDetails)
                        .collect(Collectors.toList()));
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

    private String cancelJob(FlinkVersion flinkVersion, JobID jobID, boolean savepoint)
            throws Exception {
        Optional<Tuple2<String, JobStatusMessage>> jobOpt =
                jobs.stream().filter(js -> js.f1.getJobId().equals(jobID)).findAny();

        if (!jobOpt.isPresent()) {
            throw new Exception("Job not found");
        }

        var sp = savepoint ? "savepoint_" + savepointCounter++ : null;

        if (flinkVersion.isNewerVersionThan(FlinkVersion.v1_14)) {
            JobStatusMessage oldStatus = jobOpt.get().f1;
            jobOpt.get().f1 =
                    new JobStatusMessage(
                            oldStatus.getJobId(),
                            oldStatus.getJobName(),
                            JobStatus.FINISHED,
                            oldStatus.getStartTime());
            jobOpt.get().f0 = sp;
        } else {
            jobs.removeIf(js -> js.f1.getJobId().equals(jobID));
        }

        return sp;
    }

    @Override
    public void deleteClusterDeployment(
            ObjectMeta meta, FlinkDeploymentStatus status, boolean deleteHaMeta) {
        jobs.clear();
        sessions.remove(meta.getName());
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        status.getJobStatus().setState(JobStatus.FINISHED.name());
    }

    @Override
    protected void waitForClusterShutdown(Configuration conf) {}

    @Override
    public void disposeSavepoint(String savepointPath, Configuration conf) {
        disposedSavepoints.add(savepointPath);
    }

    public List<String> getDisposedSavepoints() {
        return disposedSavepoints;
    }

    @Override
    public Optional<Savepoint> getLastCheckpoint(JobID jobId, Configuration conf) throws Exception {
        Optional<Tuple2<String, JobStatusMessage>> jobOpt =
                jobs.stream().filter(js -> js.f1.getJobId().equals(jobId)).findAny();

        if (!jobOpt.isPresent()) {
            throw new Exception("Job not found");
        }

        Tuple2<String, JobStatusMessage> t = jobOpt.get();
        if (!t.f1.getJobState().isGloballyTerminalState()) {
            throw new Exception("Checkpoint should not be queried if job is not in terminal state");
        }

        if (t.f0 != null) {
            return Optional.of(Savepoint.of(t.f0, SavepointTriggerType.UNKNOWN));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean isJobManagerPortReady(Configuration config) {
        return isPortReady;
    }

    public void setPortReady(boolean isPortReady) {
        this.isPortReady = isPortReady;
    }

    @Override
    public PodList getJmPodList(FlinkDeployment deployment, Configuration conf) {
        return podList;
    }

    public void setJmPodList(PodList podList) {
        this.podList = podList;
    }

    public void markApplicationJobFailedWithError(JobID jobID, String error) throws Exception {
        var job = jobs.stream().filter(tuple -> tuple.f1.getJobId().equals(jobID)).findFirst();
        if (!job.isPresent()) {
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

    /** The information collector of a submitted job. */
    public static class SubmittedJobInfo {
        public final String savepointPath;
        public final JobStatusMessage jobStatusMessage;
        public final Configuration effectiveConfig;

        public SubmittedJobInfo(
                String savepointPath,
                JobStatusMessage jobStatusMessage,
                Configuration effectiveConfig) {
            this.savepointPath = savepointPath;
            this.jobStatusMessage = jobStatusMessage;
            this.effectiveConfig = effectiveConfig;
        }
    }

    @Override
    public Map<String, String> getClusterInfo(Configuration conf) throws Exception {
        return CLUSTER_INFO;
    }
}
