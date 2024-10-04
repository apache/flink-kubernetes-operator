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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.observer.CheckpointFetchResult;
import org.apache.flink.kubernetes.operator.observer.CheckpointStatsResult;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmaster.JobResult;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Service for submitting and interacting with Flink clusters and jobs. */
public interface FlinkService {

    KubernetesClient getKubernetesClient();

    void submitApplicationCluster(JobSpec jobSpec, Configuration conf, boolean requireHaMetadata)
            throws Exception;

    boolean isHaMetadataAvailable(Configuration conf);

    boolean atLeastOneCheckpoint(Configuration conf);

    void submitSessionCluster(Configuration conf) throws Exception;

    JobID submitJobToSessionCluster(
            ObjectMeta meta,
            FlinkSessionJobSpec spec,
            JobID jobID,
            Configuration conf,
            @Nullable String savepoint)
            throws Exception;

    boolean isJobManagerPortReady(Configuration config);

    Optional<JobStatusMessage> getJobStatus(Configuration conf, JobID jobId) throws Exception;

    JobResult requestJobResult(Configuration conf, JobID jobID) throws Exception;

    CancelResult cancelJob(FlinkDeployment deployment, SuspendMode suspendMode, Configuration conf)
            throws Exception;

    void deleteClusterDeployment(
            ObjectMeta meta,
            FlinkDeploymentStatus status,
            Configuration conf,
            boolean deleteHaData);

    CancelResult cancelSessionJob(
            FlinkSessionJob sessionJob, SuspendMode suspendMode, Configuration conf)
            throws Exception;

    String triggerSavepoint(
            String jobId,
            org.apache.flink.core.execution.SavepointFormatType savepointFormatType,
            String savepointDirectory,
            Configuration conf)
            throws Exception;

    String triggerCheckpoint(
            String jobId,
            org.apache.flink.core.execution.CheckpointType checkpointType,
            Configuration conf)
            throws Exception;

    Optional<Savepoint> getLastCheckpoint(JobID jobId, Configuration conf);

    SavepointFetchResult fetchSavepointInfo(String triggerId, String jobId, Configuration conf);

    CheckpointFetchResult fetchCheckpointInfo(String triggerId, String jobId, Configuration conf);

    CheckpointStatsResult fetchCheckpointStats(String jobId, Long checkpointId, Configuration conf);

    Tuple2<
                    Optional<CheckpointHistoryWrapper.CompletedCheckpointInfo>,
                    Optional<CheckpointHistoryWrapper.PendingCheckpointInfo>>
            getCheckpointInfo(JobID jobId, Configuration conf) throws Exception;

    void disposeSavepoint(String savepointPath, Configuration conf) throws Exception;

    Map<String, String> getClusterInfo(Configuration conf) throws Exception;

    PodList getJmPodList(FlinkDeployment deployment, Configuration conf);

    boolean scale(FlinkResourceContext<?> resourceContext, Configuration deployConfig)
            throws Exception;

    Map<String, String> getMetrics(Configuration conf, String jobId, List<String> metricNames)
            throws Exception;

    RestClusterClient<String> getClusterClient(Configuration conf) throws Exception;

    /** Result of a cancel operation. */
    @AllArgsConstructor
    class CancelResult {
        @Getter boolean pending;
        String savepointPath;

        public static CancelResult completed(String path) {
            return new CancelResult(false, path);
        }

        public static CancelResult pending() {
            return new CancelResult(true, null);
        }

        public Optional<String> getSavepointPath() {
            return Optional.ofNullable(savepointPath);
        }
    }
}
