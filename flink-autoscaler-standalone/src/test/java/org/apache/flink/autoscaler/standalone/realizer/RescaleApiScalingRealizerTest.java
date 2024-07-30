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

package org.apache.flink.autoscaler.standalone.realizer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsHeaders;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.autoscaler.standalone.realizer.RescaleApiScalingRealizer.SCALING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for {@link RescaleApiScalingRealizer}. */
class RescaleApiScalingRealizerTest {

    /**
     * Test whether scalingRealizer behaves as expected when the resource is changed or isn't
     * changed.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUpdateResourceRequirements(boolean resourceIsChanged) throws Exception {
        var jobID = new JobID();
        var jobVertex1 = new JobVertexID().toHexString();
        var jobVertex2 = new JobVertexID().toHexString();
        var currentResourceRequirements = Map.of(jobVertex1, "5", jobVertex2, "10");

        var newResourceRequirements = currentResourceRequirements;
        if (resourceIsChanged) {
            newResourceRequirements = Map.of(jobVertex1, "7", jobVertex2, "12");
        }

        var closeFuture = new CompletableFuture<Void>();
        var updatedRequirements = new CompletableFuture<Optional<JobResourceRequirements>>();

        var jobContext =
                createJobAutoScalerContext(
                        jobID,
                        getRestClusterClient(
                                jobID,
                                createResourceRequirements(currentResourceRequirements),
                                updatedRequirements,
                                closeFuture));
        jobContext
                .getConfiguration()
                .set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        RescaleApiScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer =
                new RescaleApiScalingRealizer<>(eventCollector);

        assertThat(updatedRequirements).isNotDone();
        assertThat(closeFuture).isNotDone();
        scalingRealizer.realizeParallelismOverrides(jobContext, newResourceRequirements);

        // The ResourceRequirements should be updated when the newResourceRequirements is changed.
        if (resourceIsChanged) {
            assertThat(updatedRequirements)
                    .isCompletedWithValue(
                            Optional.of(createResourceRequirements(newResourceRequirements)));
            assertThat(eventCollector.events).hasSize(1);
            var event = eventCollector.events.peek();
            assertThat(event.getContext()).isEqualTo(jobContext);
            assertThat(event.getReason()).isEqualTo(SCALING);
        } else {
            assertThat(updatedRequirements).isNotDone();
            assertThat(eventCollector.events).isEmpty();
        }
        assertThat(closeFuture).isDone();
    }

    @Test
    void testDisableAdaptiveScheduler() throws Exception {
        var jobID = new JobID();
        var jobVertex1 = new JobVertexID().toHexString();
        var jobVertex2 = new JobVertexID().toHexString();
        var resourceRequirements = Map.of(jobVertex1, "5", jobVertex2, "10");

        var jobContext =
                createJobAutoScalerContext(
                        jobID,
                        () ->
                                fail(
                                        "The rest client shouldn't be created if the adaptive scheduler is disable."));

        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        RescaleApiScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer =
                new RescaleApiScalingRealizer<>(eventCollector);

        scalingRealizer.realizeParallelismOverrides(jobContext, resourceRequirements);
        assertThat(eventCollector.events).isEmpty();
    }

    @Test
    void testJobNotRunning() throws Exception {
        var jobID = new JobID();
        var jobVertex1 = new JobVertexID().toHexString();
        var jobVertex2 = new JobVertexID().toHexString();
        var resourceRequirements = Map.of(jobVertex1, "5", jobVertex2, "10");

        var conf = new Configuration();
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        var jobContext =
                new JobAutoScalerContext<>(
                        jobID,
                        jobID,
                        JobStatus.CANCELLING,
                        conf,
                        null,
                        () ->
                                fail(
                                        "The rest client shouldn't be created if the job isn't running."));

        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        RescaleApiScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer =
                new RescaleApiScalingRealizer<>(eventCollector);

        scalingRealizer.realizeParallelismOverrides(jobContext, resourceRequirements);
        assertThat(eventCollector.events).isEmpty();
    }

    private static JobAutoScalerContext<JobID> createJobAutoScalerContext(
            JobID jobID,
            SupplierWithException<RestClusterClient<String>, Exception> restClientSupplier) {
        return new JobAutoScalerContext<>(
                jobID, jobID, JobStatus.RUNNING, new Configuration(), null, restClientSupplier);
    }

    private JobResourceRequirements createResourceRequirements(
            Map<String, String> parallelismOverrides) {
        Map<JobVertexID, JobVertexResourceRequirements> vertexResources = new HashMap<>();
        parallelismOverrides.forEach(
                (key, value) ->
                        vertexResources.put(
                                JobVertexID.fromHexString(key),
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(
                                                1, Integer.parseInt(value)))));
        return new JobResourceRequirements(vertexResources);
    }

    private static SupplierWithException<RestClusterClient<String>, Exception> getRestClusterClient(
            JobID expectedJobID,
            JobResourceRequirements currentRequirements,
            CompletableFuture<Optional<JobResourceRequirements>> updatedRequirements,
            CompletableFuture<Void> closeFuture) {

        return () ->
                new RestClusterClient<>(
                        new Configuration(),
                        "test-cluster",
                        (c, e) -> new StandaloneClientHAServices("localhost")) {

                    @Override
                    public <
                                    M extends MessageHeaders<R, P, U>,
                                    U extends MessageParameters,
                                    R extends RequestBody,
                                    P extends ResponseBody>
                            CompletableFuture<P> sendRequest(M h, U p, R r) {
                        if (h instanceof JobResourceRequirementsHeaders) {
                            if (expectedJobID.equals(
                                    ((JobMessageParameters) p).jobPathParameter.getValue())) {
                                return (CompletableFuture<P>)
                                        CompletableFuture.completedFuture(
                                                new JobResourceRequirementsBody(
                                                        currentRequirements));
                            }
                        } else if (r instanceof JobResourceRequirementsBody) {
                            if (expectedJobID.equals(
                                    ((JobMessageParameters) p).jobPathParameter.getValue())) {
                                updatedRequirements.complete(
                                        ((JobResourceRequirementsBody) r)
                                                .asJobResourceRequirements());
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                        fail("Unknown request");
                        return null;
                    }

                    @Override
                    public void close() {
                        super.close();
                        closeFuture.complete(null);
                    }
                };
    }
}
