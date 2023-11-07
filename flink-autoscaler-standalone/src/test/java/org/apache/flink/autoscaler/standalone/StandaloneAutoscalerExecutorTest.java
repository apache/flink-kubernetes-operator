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
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for {@link StandaloneAutoscalerExecutor}. */
class StandaloneAutoscalerExecutorTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testScaling(boolean throwExceptionWhileScale) {
        JobAutoScalerContext<JobID> jobContext1 = createJobAutoScalerContext();
        JobAutoScalerContext<JobID> jobContext2 = createJobAutoScalerContext();
        var jobList = List.of(jobContext1, jobContext2);
        Set<JobID> exceptionKeys =
                throwExceptionWhileScale
                        ? Set.of(jobContext1.getJobKey(), jobContext2.getJobKey())
                        : Set.of();

        var actualScaleContexts = new ArrayList<JobAutoScalerContext<JobID>>();

        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        Duration.ofSeconds(2),
                        () -> jobList,
                        eventCollector,
                        createJobAutoScaler(actualScaleContexts, exceptionKeys));

        autoscalerExecutor.scaling();
        assertThat(actualScaleContexts).isEqualTo(jobList);
        assertThat(eventCollector.events)
                .hasSameSizeAs(exceptionKeys)
                .allMatch(
                        event ->
                                event.getReason()
                                        .equals(StandaloneAutoscalerExecutor.AUTOSCALER_ERROR));
    }

    @Test
    void testFetchException() {
        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();
        var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        Duration.ofSeconds(2),
                        () -> {
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
                        });

        // scaling shouldn't throw exception even if fetch fails
        assertDoesNotThrow(autoscalerExecutor::scaling);
    }

    private static JobAutoScalerContext<JobID> createJobAutoScalerContext() {
        var jobID = new JobID();
        return new JobAutoScalerContext<>(
                jobID, jobID, JobStatus.RUNNING, new Configuration(), null, null);
    }

    private static JobAutoScaler<JobID, JobAutoScalerContext<JobID>> createJobAutoScaler(
            List<JobAutoScalerContext<JobID>> actualScaleContexts, Set<JobID> exceptionKeys) {
        return new JobAutoScaler<>() {
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
    }
}
