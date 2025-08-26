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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class StandaloneAutoscalerValidatorTest {
    private List<JobAutoScalerContext<JobID>> jobList;
    private TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector;
    private ConcurrentHashMap<JobID, Integer> scaleCounter;
    private Configuration correctConfiguration;
    private Configuration invalidConfiguration;

    @BeforeEach
    void setUp() {
        jobList = new ArrayList<>();
        eventCollector = new TestingEventCollector<>();
        scaleCounter = new ConcurrentHashMap<>();

        correctConfiguration = new Configuration();
        correctConfiguration.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);

        invalidConfiguration = new Configuration();
        invalidConfiguration.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        invalidConfiguration.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, -1.0);
    }

    @Test
    void testAutoScalerWithInvalidConfig() throws Exception {
        JobAutoScalerContext<JobID> validJob = createJobAutoScalerContext(correctConfiguration);
        JobAutoScalerContext<JobID> invalidJob = createJobAutoScalerContext(invalidConfiguration);

        jobList.add(validJob);
        jobList.add(invalidJob);

        final var jobAutoScaler =
                new JobAutoScaler<JobID, JobAutoScalerContext<JobID>>() {
                    @Override
                    public void scale(JobAutoScalerContext<JobID> context) {
                        scaleCounter.merge(context.getJobKey(), 1, Integer::sum);
                    }

                    @Override
                    public void cleanup(JobAutoScalerContext<JobID> context) {
                        // No cleanup required for the test
                    }
                };

        try (var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        new Configuration(), baseConf -> jobList, eventCollector, jobAutoScaler)) {

            List<CompletableFuture<Void>> scaledFutures = autoscalerExecutor.scaling();

            // Verification triggers two scaling tasks
            assertThat(scaledFutures).hasSize(2);

            // Only legally configured tasks are scaled
            assertThat(scaleCounter).hasSize(1).containsKey(validJob.getJobKey());

            // Verification Event Collector captures an event
            assertThat(eventCollector.events).hasSize(1);
            assertThat(eventCollector.events)
                    .allMatch(event -> event.getContext().equals(invalidJob));
        }
    }

    private JobAutoScalerContext<JobID> createJobAutoScalerContext(Configuration configuration) {
        JobID jobID = new JobID();
        return new JobAutoScalerContext<>(
                jobID, jobID, JobStatus.RUNNING, configuration, null, null);
    }
}
