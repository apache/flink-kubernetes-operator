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
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.function.TriFunction;

import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/** Testing ClusterClient implementation. */
public class TestingClusterClient<T> extends RestClusterClient<T> {

    @Setter
    private Function<JobID, CompletableFuture<Acknowledge>> cancelFunction =
            ignore -> CompletableFuture.completedFuture(Acknowledge.get());

    @Setter
    private TriFunction<JobID, Boolean, String, CompletableFuture<String>>
            stopWithSavepointFunction =
                    (ignore1, ignore2, savepointPath) ->
                            CompletableFuture.completedFuture(savepointPath);

    @Setter
    private TriFunction<JobID, SavepointFormatType, String, CompletableFuture<String>>
            stopWithSavepointFormat;

    @Setter
    private TriFunction<
                    MessageHeaders<?, ?, ?>,
                    MessageParameters,
                    RequestBody,
                    CompletableFuture<ResponseBody>>
            requestProcessor =
                    (ignore1, ignore2, ignore) ->
                            CompletableFuture.completedFuture(EmptyResponseBody.getInstance());

    @Setter
    private Supplier<CompletableFuture<Collection<JobStatusMessage>>> listJobsFunction =
            () -> {
                throw new UnsupportedOperationException();
            };

    @Setter
    private Function<JobID, CompletableFuture<JobResult>> requestResultFunction =
            jobID ->
                    CompletableFuture.completedFuture(
                            new JobResult.Builder().jobId(jobID).netRuntime(1).build());

    @Getter private final T clusterId;

    public TestingClusterClient(Configuration configuration, T clusterId) throws Exception {
        super(configuration, clusterId, (c, e) -> new StandaloneClientHAServices("localhost"));
        this.clusterId = clusterId;
    }

    public TestingClusterClient(Configuration configuration) throws Exception {
        this(configuration, (T) configuration.get(KubernetesConfigOptions.CLUSTER_ID));
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
        return listJobsFunction.get();
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
        return requestResultFunction.apply(jobId);
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
            JobID jobId, @Nullable String savepointDirectory, SavepointFormatType formatType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            JobID jobId,
            boolean advanceToEndOfTime,
            @Nullable String savepointDirectory,
            SavepointFormatType formatType) {
        return stopWithSavepointFormat == null
                ? stopWithSavepointFunction.apply(jobId, advanceToEndOfTime, savepointDirectory)
                : stopWithSavepointFormat.apply(jobId, formatType, savepointDirectory);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            JobID jobId, @Nullable String savepointDirectory, SavepointFormatType formatType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendCoordinationRequest(
            JobID jobId, OperatorID operatorId, CoordinationRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

    @Override
    public <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters, R request) {
        return (CompletableFuture<P>)
                requestProcessor.apply(messageHeaders, messageParameters, request);
    }
}
