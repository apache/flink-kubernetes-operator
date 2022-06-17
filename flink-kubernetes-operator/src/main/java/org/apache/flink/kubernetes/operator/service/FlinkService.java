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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmaster.JobResult;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/** Service for submitting and interacting with Flink clusters and jobs. */
public interface FlinkService {

    KubernetesClient getKubernetesClient();

    void submitApplicationCluster(JobSpec jobSpec, Configuration conf, boolean requireHaMetadata)
            throws Exception;

    boolean isHaMetadataAvailable(Configuration conf);

    void submitSessionCluster(Configuration conf) throws Exception;

    JobID submitJobToSessionCluster(
            ObjectMeta meta,
            FlinkSessionJobSpec spec,
            Configuration conf,
            @Nullable String savepoint)
            throws Exception;

    boolean isJobManagerPortReady(Configuration config);

    Collection<JobStatusMessage> listJobs(Configuration conf) throws Exception;

    JobResult requestJobResult(Configuration conf, JobID jobID) throws Exception;

    void cancelJob(FlinkDeployment deployment, UpgradeMode upgradeMode, Configuration conf)
            throws Exception;

    void deleteClusterDeployment(
            ObjectMeta meta, FlinkDeploymentStatus status, boolean deleteHaData);

    void cancelSessionJob(FlinkSessionJob sessionJob, UpgradeMode upgradeMode, Configuration conf)
            throws Exception;

    void triggerSavepoint(
            String jobId,
            SavepointTriggerType triggerType,
            org.apache.flink.kubernetes.operator.crd.status.SavepointInfo savepointInfo,
            Configuration conf)
            throws Exception;

    Optional<Savepoint> getLastCheckpoint(JobID jobId, Configuration conf) throws Exception;

    SavepointFetchResult fetchSavepointInfo(String triggerId, String jobId, Configuration conf);

    void disposeSavepoint(String savepointPath, Configuration conf) throws Exception;

    Map<String, String> getClusterInfo(Configuration conf) throws Exception;

    PodList getJmPodList(FlinkDeployment deployment, Configuration conf);

    void waitForClusterShutdown(Configuration conf);
}
