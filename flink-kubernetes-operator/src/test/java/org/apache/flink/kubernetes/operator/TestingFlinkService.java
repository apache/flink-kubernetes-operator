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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.api.model.PodList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Flink service mock for tests. */
public class TestingFlinkService extends FlinkService {

    private int savepointCounter = 0;
    private int triggerCounter = 0;

    private List<Tuple2<String, JobStatusMessage>> jobs = new ArrayList<>();
    private Set<String> sessions = new HashSet<>();
    private boolean isPortReady = true;
    private PodList podList = new PodList();
    private Consumer<Configuration> listJobConsumer = conf -> {};

    public TestingFlinkService() {
        super(null, null);
    }

    public void clear() {
        jobs.clear();
        sessions.clear();
    }

    @Override
    public void submitApplicationCluster(FlinkDeployment deployment, Configuration conf) {
        JobID jobID = new JobID();
        JobStatusMessage jobStatusMessage =
                new JobStatusMessage(
                        jobID,
                        deployment.getMetadata().getName(),
                        JobStatus.RUNNING,
                        System.currentTimeMillis());

        jobs.add(Tuple2.of(conf.get(SavepointConfigOptions.SAVEPOINT_PATH), jobStatusMessage));
    }

    @Override
    public void submitSessionCluster(FlinkDeployment deployment, Configuration conf) {
        sessions.add(deployment.getMetadata().getName());
    }

    @Override
    public void submitJobToSessionCluster(FlinkSessionJob sessionJob, Configuration conf) {
        JobID jobID = new JobID();
        JobStatusMessage jobStatusMessage =
                new JobStatusMessage(
                        jobID,
                        sessionJob.getMetadata().getName(),
                        JobStatus.RUNNING,
                        System.currentTimeMillis());
        jobs.add(Tuple2.of(conf.get(SavepointConfigOptions.SAVEPOINT_PATH), jobStatusMessage));
    }

    @Override
    public List<JobStatusMessage> listJobs(Configuration conf) throws Exception {
        listJobConsumer.accept(conf);
        if (jobs.isEmpty() && !sessions.isEmpty()) {
            throw new Exception("Trying to list a job without submitting it");
        }
        if (!isPortReady) {
            throw new TimeoutException("JM port is unavailable");
        }
        return jobs.stream().map(t -> t.f1).collect(Collectors.toList());
    }

    public void setListJobConsumer(Consumer<Configuration> listJobConsumer) {
        this.listJobConsumer = listJobConsumer;
    }

    public List<Tuple2<String, JobStatusMessage>> listJobs() {
        return new ArrayList<>(jobs);
    }

    @Override
    public Optional<String> cancelJob(JobID jobID, UpgradeMode upgradeMode, Configuration conf)
            throws Exception {

        if (upgradeMode == UpgradeMode.LAST_STATE) {
            jobs.clear();
            return Optional.empty();
        }

        if (!jobs.removeIf(js -> js.f1.getJobId().equals(jobID))) {
            throw new Exception("Job not found");
        }

        if (upgradeMode != UpgradeMode.STATELESS) {
            return Optional.of("savepoint_" + savepointCounter++);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void cancelSessionJob(JobID jobID, Configuration conf) throws Exception {
        if (!jobs.removeIf(js -> js.f1.getJobId().equals(jobID))) {
            throw new Exception("Job not found");
        }
    }

    @Override
    public void stopSessionCluster(
            FlinkDeployment deployment, Configuration conf, boolean deleteHa) {
        sessions.remove(deployment.getMetadata().getName());
    }

    @Override
    public void triggerSavepoint(FlinkDeployment deployment, Configuration conf) throws Exception {
        SavepointInfo savepointInfo = deployment.getStatus().getJobStatus().getSavepointInfo();
        savepointInfo.setTrigger("trigger_" + triggerCounter++);
    }

    @Override
    public SavepointFetchResult fetchSavepointInfo(FlinkDeployment deployment, Configuration conf)
            throws Exception {
        return SavepointFetchResult.completed(Savepoint.of("savepoint_" + savepointCounter++));
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
}
