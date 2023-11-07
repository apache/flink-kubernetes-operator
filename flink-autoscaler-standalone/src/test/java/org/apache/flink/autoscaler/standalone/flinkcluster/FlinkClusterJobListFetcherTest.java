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

package org.apache.flink.autoscaler.standalone.flinkcluster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.rest.messages.ConfigurationInfo;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobManagerJobConfigurationHeaders;
import org.apache.flink.types.Either;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for {@link FlinkClusterJobListFetcher}. */
class FlinkClusterJobListFetcherTest {

    /** Test whether the job list and confs are expected. */
    @Test
    void testFetchJobListAndConfigurationInfo() throws Exception {
        var job1 =
                new JobStatusMessage(
                        new JobID(), "", JobStatus.RUNNING, Instant.now().toEpochMilli());
        var job2 =
                new JobStatusMessage(
                        new JobID(), "", JobStatus.CANCELLING, Instant.now().toEpochMilli());

        Configuration expectedConf1 = new Configuration();
        expectedConf1.setString("option_key1", "option_value1");

        Configuration expectedConf2 = new Configuration();
        expectedConf2.setString("option_key2", "option_value2");
        expectedConf2.setString("option_key3", "option_value3");

        var jobs = Map.of(job1.getJobId(), job1, job2.getJobId(), job2);
        var configurations = Map.of(job1.getJobId(), expectedConf1, job2.getJobId(), expectedConf2);
        var closeCounter = new AtomicLong();
        FlinkClusterJobListFetcher jobListFetcher =
                new FlinkClusterJobListFetcher(
                        getRestClusterClient(
                                Either.Left(List.of(job1, job2)),
                                Either.Left(
                                        Map.of(
                                                job1.getJobId(),
                                                ConfigurationInfo.from(expectedConf1),
                                                job2.getJobId(),
                                                ConfigurationInfo.from(expectedConf2))),
                                closeCounter),
                        Duration.ofSeconds(10));

        // Fetch multiple times and check whether the results are as expected each time
        for (int i = 1; i <= 3; i++) {
            var fetchedJobList = jobListFetcher.fetch();
            // Check whether rest client is closed.
            assertThat(closeCounter).hasValue(i);

            assertThat(fetchedJobList).hasSize(2);
            for (var jobContext : fetchedJobList) {
                JobStatusMessage expectedJobStatusMessage = jobs.get(jobContext.getJobID());
                Configuration expectedConf = configurations.get(jobContext.getJobID());
                assertThat(expectedJobStatusMessage).isNotNull();
                assertThat(jobContext.getJobStatus())
                        .isEqualTo(expectedJobStatusMessage.getJobState());
                assertThat(jobContext.getConfiguration()).isNotNull().isEqualTo(expectedConf);
            }
        }
    }

    /**
     * Test whether the exception is expected after rest client fetches job list throws exception,
     * and restClient can be closed normally.
     */
    @Test
    void testFetchJobListException() {
        var expectedException = new RuntimeException("Expected exception.");
        var closeCounter = new AtomicLong();

        FlinkClusterJobListFetcher jobListFetcher =
                new FlinkClusterJobListFetcher(
                        getRestClusterClient(
                                Either.Right(expectedException),
                                Either.Left(Map.of()),
                                closeCounter),
                        Duration.ofSeconds(10));
        assertThatThrownBy(jobListFetcher::fetch).getCause().isEqualTo(expectedException);
        assertThat(closeCounter).hasValue(1);
    }

    /**
     * Test whether the exception is expected after rest client fetches conf throws exception, and
     * restClient can be closed normally.
     */
    @Test
    void testFetchConfigurationException() {
        var job1 =
                new JobStatusMessage(
                        new JobID(), "", JobStatus.RUNNING, Instant.now().toEpochMilli());
        var expectedException = new RuntimeException("Expected exception.");
        var closeCounter = new AtomicLong();

        FlinkClusterJobListFetcher jobListFetcher =
                new FlinkClusterJobListFetcher(
                        getRestClusterClient(
                                Either.Left(List.of(job1)),
                                Either.Right(expectedException),
                                closeCounter),
                        Duration.ofSeconds(10));

        assertThatThrownBy(jobListFetcher::fetch).getRootCause().isEqualTo(expectedException);
        assertThat(closeCounter).hasValue(1);
    }

    /**
     * Test whether the exception is expected after rest client fetches job list timeout, and
     * restClient can be closed normally.
     */
    @Test
    void testFetchJobListTimeout() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        FlinkClusterJobListFetcher jobListFetcher =
                new FlinkClusterJobListFetcher(
                        getTimeoutableRestClusterClient(null, null, closeFuture),
                        Duration.ofSeconds(2));

        assertThat(closeFuture).isNotDone();
        assertThatThrownBy(jobListFetcher::fetch).isInstanceOf(TimeoutException.class);
        assertThat(closeFuture).isDone();
    }

    /**
     * Test whether the exception is expected after rest client fetches conf timeout, and restClient
     * can be closed normally.
     */
    @Test
    void testFetchConfigurationTimeout() {
        var job1 =
                new JobStatusMessage(
                        new JobID(), "", JobStatus.RUNNING, Instant.now().toEpochMilli());
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        FlinkClusterJobListFetcher jobListFetcher =
                new FlinkClusterJobListFetcher(
                        getTimeoutableRestClusterClient(List.of(job1), null, closeFuture),
                        Duration.ofSeconds(2));

        assertThat(closeFuture).isNotDone();
        assertThatThrownBy(jobListFetcher::fetch)
                .getRootCause()
                .isInstanceOf(TimeoutException.class);
        assertThat(closeFuture).isDone();
    }

    /**
     * @param jobListOrException When listJobs is called, return jobList if Either is left, return
     *     failedFuture if Either is right.
     * @param configurationsOrException When fetch job conf, return configuration if Either is left,
     *     return failedFuture if Either is right.
     * @param closeCounter Increment the count each time the {@link RestClusterClient#close} is
     *     called
     */
    private static FunctionWithException<Configuration, RestClusterClient<String>, Exception>
            getRestClusterClient(
                    Either<Collection<JobStatusMessage>, Throwable> jobListOrException,
                    Either<Map<JobID, ConfigurationInfo>, Throwable> configurationsOrException,
                    AtomicLong closeCounter) {
        return conf ->
                new RestClusterClient<>(
                        conf,
                        "test-cluster",
                        (c, e) -> new StandaloneClientHAServices("localhost")) {

                    @Override
                    public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
                        if (jobListOrException.isLeft()) {
                            return CompletableFuture.completedFuture(jobListOrException.left());
                        }
                        return CompletableFuture.failedFuture(jobListOrException.right());
                    }

                    @Override
                    public <
                                    M extends MessageHeaders<R, P, U>,
                                    U extends MessageParameters,
                                    R extends RequestBody,
                                    P extends ResponseBody>
                            CompletableFuture<P> sendRequest(M h, U p, R r) {
                        if (h instanceof JobManagerJobConfigurationHeaders) {
                            if (configurationsOrException.isRight()) {
                                return CompletableFuture.failedFuture(
                                        configurationsOrException.right());
                            }
                            var jobID = ((JobMessageParameters) p).jobPathParameter.getValue();
                            return (CompletableFuture<P>)
                                    CompletableFuture.completedFuture(
                                            configurationsOrException.left().get(jobID));
                        }
                        fail("Unknown request");
                        return null;
                    }

                    @Override
                    public void close() {
                        super.close();
                        closeCounter.incrementAndGet();
                    }
                };
    }

    /**
     * @param jobList When listJobs is called, return jobList if it's not null, don't complete
     *     future if it's null.
     * @param configuration When fetch job conf, return configuration if it's not null, don't
     *     complete future if it's null.
     * @param closeFuture Complete this closeFuture when {@link RestClusterClient#close} is called.
     */
    private static FunctionWithException<Configuration, RestClusterClient<String>, Exception>
            getTimeoutableRestClusterClient(
                    @Nullable Collection<JobStatusMessage> jobList,
                    @Nullable ConfigurationInfo configuration,
                    CompletableFuture<Void> closeFuture) {
        return conf ->
                new RestClusterClient<>(
                        conf,
                        "test-cluster",
                        (c, e) -> new StandaloneClientHAServices("localhost")) {

                    @Override
                    public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
                        if (jobList == null) {
                            return new CompletableFuture<>();
                        }
                        return CompletableFuture.completedFuture(jobList);
                    }

                    @Override
                    public <
                                    M extends MessageHeaders<R, P, U>,
                                    U extends MessageParameters,
                                    R extends RequestBody,
                                    P extends ResponseBody>
                            CompletableFuture<P> sendRequest(M h, U p, R r) {
                        if (h instanceof JobManagerJobConfigurationHeaders) {
                            if (configuration == null) {
                                return new CompletableFuture<>();
                            }
                            return (CompletableFuture<P>)
                                    CompletableFuture.completedFuture(configuration);
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
