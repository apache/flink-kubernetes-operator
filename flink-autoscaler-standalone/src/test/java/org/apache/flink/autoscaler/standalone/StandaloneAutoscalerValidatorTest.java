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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class StandaloneAutoscalerValidatorTest {
    @Test
    public void testAutoScalerWithInvalidConfig() {
        var jobList = new ArrayList<JobAutoScalerContext<JobID>>();
        var eventCollector = new TestingEventCollector<JobID, JobAutoScalerContext<JobID>>();

        Configuration correctConfiguration = new Configuration();
        correctConfiguration.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        Configuration invalidConfiguration = new Configuration();
        invalidConfiguration.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        invalidConfiguration.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, -1.);

        var correctConfigurationJob = createJobAutoScalerContextWithConf(correctConfiguration);
        var illegalConfigurationJob = createJobAutoScalerContextWithConf(invalidConfiguration);
        var scaleCounter = new ConcurrentHashMap<JobID, Integer>();

        try (var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        new Configuration(),
                        baseConf -> jobList,
                        eventCollector,
                        new JobAutoScaler<>() {
                            @Override
                            public void scale(JobAutoScalerContext<JobID> context) {
                                scaleCounter.put(
                                        context.getJobKey(),
                                        scaleCounter.getOrDefault(context.getJobKey(), 0) + 1);
                            }

                            @Override
                            public void cleanup(JobAutoScalerContext<JobID> context) {
                                // do nothing
                            }
                        })) {
            jobList.add(correctConfigurationJob);
            jobList.add(illegalConfigurationJob);
            List<CompletableFuture<Void>> scaledFutures = autoscalerExecutor.scaling();

            assertThat(scaledFutures).hasSize(2);
            assertThat(scaleCounter).containsOnlyKeys(correctConfigurationJob.getJobKey());

            assertThat(eventCollector.events).size().isEqualTo(1);
            assertThat(eventCollector.events)
                    .allMatch(event -> event.getContext().equals(illegalConfigurationJob));
        }
    }

    private static JobAutoScalerContext<JobID> createJobAutoScalerContextWithConf(
            Configuration configuration) {
        var jobID = new JobID();
        return new JobAutoScalerContext<>(
                jobID, jobID, JobStatus.RUNNING, configuration, null, null);
    }
}
