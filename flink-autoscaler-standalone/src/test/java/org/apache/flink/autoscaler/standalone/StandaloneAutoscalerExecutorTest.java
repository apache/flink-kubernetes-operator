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

package org.apache.flink.autoscaler.standalone;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.CONTROL_LOOP_INTERVAL;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.CONTROL_LOOP_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for {@link StandaloneAutoscalerExecutor}. */
class StandaloneAutoscalerExecutorTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testScaling(boolean throwExceptionWhileScale) throws Exception {
        var jobContext1 = createJobAutoScalerContext();
        var jobContext2 = createJobAutoScalerContext();
        var jobList = List.of(jobContext1, jobContext2);
        var exceptionKeys =
                throwExceptionWhileScale
                        ? Set.of(jobContext1.getJobKey(), jobContext2.getJobKey())
                        : Set.of();

        var actualScaleContexts =
                Collections.synchronizedList(new ArrayList<JobAutoScalerContext<JobID>>());

        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        final var conf = new Configuration();
        conf.set(CONTROL_LOOP_PARALLELISM, 1);
        var countDownLatch = new CountDownLatch(jobList.size());

        final var jobAutoScaler =
                new JobAutoScaler<JobID, JobAutoScalerContext<JobID>>() {
                    @Override
                    public void scale(JobAutoScalerContext<JobID> context) {
                        actualScaleContexts.add(context);
                        if (exceptionKeys.contains(context.getJobKey())) {
                            throw new RuntimeException("Excepted exception.");
                        }
                    }

                    @Override
                    public void cleanup(JobID jobKey) {
                        fail("Should be called.");
                    }
                };

        try (var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        conf, baseConf -> jobList, eventCollector, jobAutoScaler) {
                    @Override
                    protected void scalingSingleJob(JobAutoScalerContext<JobID> jobContext) {
                        super.scalingSingleJob(jobContext);
                        countDownLatch.countDown();
                    }
                }) {

            autoscalerExecutor.scaling();
            // Wait for all scalings to go finished.
            countDownLatch.await();

            assertThat(actualScaleContexts).isEqualTo(jobList);
            assertThat(eventCollector.events)
                    .hasSameSizeAs(exceptionKeys)
                    .allMatch(
                            event ->
                                    event.getReason()
                                            .equals(StandaloneAutoscalerExecutor.AUTOSCALER_ERROR));
        }
    }

    @Test
    void testFetchException() {
        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        try (var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        new Configuration(),
                        baseConf -> {
                            throw new RuntimeException("Excepted exception.");
                        },
                        eventCollector,
                        new JobAutoScaler<>() {
                            @Override
                            public void scale(JobAutoScalerContext<JobID> context) {
                                fail("Should be called.");
                            }

                            @Override
                            public void cleanup(JobID jobID) {
                                fail("Should be called.");
                            }
                        })) {

            // scaling shouldn't throw exception even if fetch fails
            assertDoesNotThrow(autoscalerExecutor::scaling);
        }
    }

    @Test
    void testScalingParallelism() {
        var parallelism = 10;

        var jobList = new ArrayList<JobAutoScalerContext<JobID>>();
        for (int i = 0; i < parallelism; i++) {
            jobList.add(createJobAutoScalerContext());
        }

        final var countDownLatch = new CountDownLatch(parallelism);
        final Configuration conf = new Configuration();
        conf.set(CONTROL_LOOP_PARALLELISM, parallelism);

        try (var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        conf,
                        baseConf -> jobList,
                        new TestingEventCollector<>(),
                        new JobAutoScaler<>() {
                            @Override
                            public void scale(JobAutoScalerContext<JobID> context)
                                    throws Exception {
                                countDownLatch.countDown();
                                // The await can be done when all jobs are scaling together.
                                countDownLatch.await();
                            }

                            @Override
                            public void cleanup(JobID jobID) {
                                fail("Should be called.");
                            }
                        })) {
            autoscalerExecutor.scaling();
        }
    }

    /** Test the rest of jobs aren't affected when scaling of one job is very slow. */
    @Test
    void testOneJobScalingSlow() throws Exception {
        var parallelism = 10;

        final var jobContextWithIndex = new HashMap<JobAutoScalerContext<JobID>, Integer>();
        final var jobContextWithScalingCounter =
                new HashMap<JobAutoScalerContext<JobID>, AtomicLong>();
        for (int i = 0; i < parallelism; i++) {
            final JobAutoScalerContext<JobID> jobAutoScalerContext = createJobAutoScalerContext();
            jobContextWithIndex.put(jobAutoScalerContext, i);
            jobContextWithScalingCounter.put(jobAutoScalerContext, new AtomicLong(0));
        }

        // Quick jobs will count down after scaling expectedScalingCount times, and the slow job
        // will wait for these quick jobs.
        // Slow job completes slowJobFuture when all quick jobs are done.
        final var expectedScalingCount = 20;
        final var countDownLatch = new CountDownLatch(parallelism - 1);
        final var slowJobFuture = new CompletableFuture<Void>();

        final Configuration conf = new Configuration();
        conf.set(CONTROL_LOOP_PARALLELISM, 2);
        conf.set(CONTROL_LOOP_INTERVAL, Duration.ofMillis(100));

        try (var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        conf,
                        baseConf -> jobContextWithIndex.keySet(),
                        new TestingEventCollector<>(),
                        new JobAutoScaler<>() {
                            @Override
                            public void scale(JobAutoScalerContext<JobID> context)
                                    throws Exception {
                                final int index = jobContextWithIndex.get(context);
                                final long scalingCounter =
                                        jobContextWithScalingCounter.get(context).incrementAndGet();
                                if (index == 0) {
                                    // index 0 is slot context
                                    countDownLatch.await();
                                    // The scaling count of each quick job reaches
                                    // expectedScalingCount.
                                    slowJobFuture.complete(null);
                                } else if (scalingCounter == expectedScalingCount) {
                                    countDownLatch.countDown();
                                }
                            }

                            @Override
                            public void cleanup(JobID jobID) {
                                fail("Should be called.");
                            }
                        })) {
            autoscalerExecutor.start();
            slowJobFuture.get();
        }
    }

    private static JobAutoScalerContext<JobID> createJobAutoScalerContext() {
        var jobID = new JobID();
        return new JobAutoScalerContext<>(
                jobID, jobID, JobStatus.RUNNING, new Configuration(), null, null);
    }
}
