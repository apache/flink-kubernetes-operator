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

package org.apache.flink.kubernetes.operator.service.minicluster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.spec.FlinkMiniClusterSpec;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.observer.CheckpointFetchResult;
import org.apache.flink.kubernetes.operator.observer.CheckpointStatsResult;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.service.CheckpointHistoryWrapper;
import org.apache.flink.kubernetes.operator.service.ClusterInfo;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.Config;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.ConfigBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link MiniClusterFlinkService} for tests.
 *
 * <p>The Kubernetes resources of the MiniCluster are created and deleted for real, against the
 * Kubernetes client it is given. The job, and everything else that would reach the REST API of the
 * MiniCluster, is simulated by a {@link TestingFlinkService}: deploying the MiniCluster submits the
 * job to it, and deleting the MiniCluster removes the job from it.
 */
public class TestingMiniClusterFlinkService extends MiniClusterFlinkService {

    @Getter private final TestingFlinkService delegate;

    @Setter private PodList podList = new PodList();

    /** Thrown when the job status is requested, to simulate the REST API being unreachable. */
    @Setter @Nullable private Exception jobStatusException;

    @Getter private int deployCount = 0;

    @Getter private int deleteCount = 0;

    public TestingMiniClusterFlinkService(KubernetesClient kubernetesClient) {
        super(
                kubernetesClient,
                null,
                Executors.newDirectExecutorService(),
                FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
        this.delegate = new TestingFlinkService(kubernetesClient);
    }

    /**
     * Creates a Flink Kubernetes client that talks to the same server as the given client.
     *
     * @param kubernetesClient Kubernetes client of the operator, usually of a mock server
     * @param conf Configuration naming the namespace of the client
     * @return a Flink Kubernetes client for the server
     */
    public static FlinkKubeClient createFlinkKubeClient(
            KubernetesClient kubernetesClient, Configuration conf) {
        return new Fabric8FlinkKubeClient(
                conf,
                createShadedKubernetesClient(kubernetesClient, conf),
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor());
    }

    /**
     * Creates a client of Flink's shaded Kubernetes model that talks to the same server as the
     * given client.
     *
     * @param kubernetesClient Kubernetes client of the operator, usually of a mock server
     * @param conf Configuration naming the namespace of the client
     * @return a shaded Kubernetes client for the server
     */
    public static NamespacedKubernetesClient createShadedKubernetesClient(
            KubernetesClient kubernetesClient, Configuration conf) {
        var config =
                new ConfigBuilder(Config.empty())
                        .withMasterUrl(kubernetesClient.getConfiguration().getMasterUrl())
                        .withTrustCerts(true)
                        .withHttp2Disable(true)
                        .withNamespace(conf.get(KubernetesConfigOptions.NAMESPACE))
                        .build();
        return new KubernetesClientBuilder()
                .withConfig(config)
                .build()
                .adapt(NamespacedKubernetesClient.class);
    }

    @Override
    protected FlinkKubeClient createFlinkKubeClient(Configuration conf) {
        return createFlinkKubeClient(kubernetesClient, conf);
    }

    public <T extends HasMetadata> Context<T> getContext() {
        return delegate.getContext();
    }

    @Override
    public void submitMiniCluster(
            FlinkMiniCluster miniCluster,
            FlinkMiniClusterSpec spec,
            Configuration conf,
            boolean requireHaMetadata)
            throws Exception {
        super.submitMiniCluster(miniCluster, spec, conf, requireHaMetadata);
        delegate.submitApplicationCluster(spec.getJob(), conf, false);
        deployCount++;
    }

    /** The configuration the MiniCluster was last deployed with. */
    public Configuration getSubmittedConf() {
        return delegate.getSubmittedConf();
    }

    @Override
    protected void deleteClusterInternal(
            String namespace,
            String clusterId,
            Configuration conf,
            DeletionPropagation deletionPropagation) {
        super.deleteClusterInternal(namespace, clusterId, conf, deletionPropagation);
        delegate.listJobs().clear();
        deleteCount++;
    }

    @Override
    protected Duration deleteDeploymentBlocking(
            String name,
            Resource<Deployment> deployment,
            DeletionPropagation propagation,
            Duration timeout) {
        deployment.withPropagationPolicy(propagation).delete();
        return timeout;
    }

    @Override
    protected PodList getJmPodList(String namespace, String clusterId) {
        return podList;
    }

    @Override
    public boolean isHaMetadataAvailable(Configuration conf) {
        return delegate.isHaMetadataAvailable(conf);
    }

    @Override
    public boolean atLeastOneCheckpoint(Configuration conf) {
        return delegate.atLeastOneCheckpoint(conf);
    }

    @Override
    public boolean isJobManagerPortReady(Configuration config) {
        return delegate.isJobManagerPortReady(config);
    }

    @Override
    public Optional<JobStatusMessage> getJobStatus(Configuration conf, JobID jobId)
            throws Exception {
        if (jobStatusException != null) {
            throw jobStatusException;
        }
        return delegate.getJobStatus(conf, jobId);
    }

    @Override
    public JobExceptionsInfoWithHistory getJobExceptions(
            AbstractFlinkResource resource, JobID jobId, Configuration observeConfig) {
        return delegate.getJobExceptions(resource, jobId, observeConfig);
    }

    @Override
    public JobResult requestJobResult(Configuration conf, JobID jobID) throws Exception {
        return delegate.requestJobResult(conf, jobID);
    }

    @Override
    public String triggerSavepoint(
            String jobId,
            SavepointFormatType savepointFormatType,
            String savepointDirectory,
            Configuration conf)
            throws Exception {
        return delegate.triggerSavepoint(jobId, savepointFormatType, savepointDirectory, conf);
    }

    @Override
    public String triggerCheckpoint(String jobId, CheckpointType checkpointType, Configuration conf)
            throws Exception {
        return delegate.triggerCheckpoint(jobId, checkpointType, conf);
    }

    @Override
    public SavepointFetchResult fetchSavepointInfo(
            String triggerId, String jobId, Configuration conf) {
        return delegate.fetchSavepointInfo(triggerId, jobId, conf);
    }

    @Override
    public CheckpointFetchResult fetchCheckpointInfo(
            String triggerId, String jobId, Configuration conf) {
        return delegate.fetchCheckpointInfo(triggerId, jobId, conf);
    }

    @Override
    public CheckpointStatsResult fetchCheckpointStats(
            String jobId, Long checkpointId, Configuration conf) {
        return delegate.fetchCheckpointStats(jobId, checkpointId, conf);
    }

    @Override
    public Optional<Savepoint> getLastCheckpoint(JobID jobId, Configuration conf) {
        return delegate.getLastCheckpoint(jobId, conf);
    }

    @Override
    public Tuple2<
                    Optional<CheckpointHistoryWrapper.CompletedCheckpointInfo>,
                    Optional<CheckpointHistoryWrapper.PendingCheckpointInfo>>
            getCheckpointInfo(JobID jobId, Configuration conf) throws Exception {
        return delegate.getCheckpointInfo(jobId, conf);
    }

    @Override
    public void disposeSavepoint(String savepointPath, Configuration conf) throws Exception {
        delegate.disposeSavepoint(savepointPath, conf);
    }

    @Override
    public ClusterInfo getClusterInfo(Configuration conf, @Nullable String jobId) throws Exception {
        return delegate.getClusterInfo(conf, jobId);
    }

    @Override
    public Map<String, String> getMetrics(
            Configuration conf, String jobId, List<String> metricNames) throws Exception {
        return delegate.getMetrics(conf, jobId, metricNames);
    }

    @Override
    public RestClusterClient<String> getClusterClient(Configuration conf) throws Exception {
        return delegate.getClusterClient(conf);
    }
}
