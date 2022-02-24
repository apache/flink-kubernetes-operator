/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.function.TriFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Testing ClusterClient used implementation. */
public class TestingClusterClient<T> implements ClusterClient<T> {

    private Function<JobID, CompletableFuture<Acknowledge>> cancelFunction =
            ignore -> CompletableFuture.completedFuture(Acknowledge.get());
    private TriFunction<JobID, Boolean, String, CompletableFuture<String>>
            stopWithSavepointFunction =
                    (ignore1, ignore2, savepointPath) ->
                            CompletableFuture.completedFuture(savepointPath);

    private final T clusterId;

    public TestingClusterClient(T clusterId) {
        this.clusterId = clusterId;
    }

    public void setCancelFunction(Function<JobID, CompletableFuture<Acknowledge>> cancelFunction) {
        this.cancelFunction = cancelFunction;
    }

    public void setStopWithSavepointFunction(
            TriFunction<JobID, Boolean, String, CompletableFuture<String>>
                    stopWithSavepointFunction) {
        this.stopWithSavepointFunction = stopWithSavepointFunction;
    }

    @Override
    public T getClusterId() {
        return clusterId;
    }

    @Override
    public Configuration getFlinkConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutDownCluster() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWebInterfaceURL() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<JobID> submitJob(@Nonnull JobGraph jobGraph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<JobResult> requestJobResult(@Nonnull JobID jobId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Map<String, Object>> getAccumulators(JobID jobID, ClassLoader loader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(JobID jobId) {
        return cancelFunction.apply(jobId);
    }

    @Override
    public CompletableFuture<String> cancelWithSavepoint(
            JobID jobId, @Nullable String savepointDirectory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            JobID jobId, boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
        return stopWithSavepointFunction.apply(jobId, advanceToEndOfEventTime, savepointDirectory);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            JobID jobId, @Nullable String savepointDirectory) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendCoordinationRequest(
            JobID jobId, OperatorID operatorId, CoordinationRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
}
