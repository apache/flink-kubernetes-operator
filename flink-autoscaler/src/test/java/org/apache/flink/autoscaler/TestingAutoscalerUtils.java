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

package org.apache.flink.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.util.function.SupplierWithException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** The utils for test autoscaler. */
public class TestingAutoscalerUtils {

    public static JobAutoScalerContext<JobID> createDefaultJobAutoScalerContext() {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup metricGroup = new GenericMetricGroup(registry, null, "test");
        final JobID jobID = new JobID();
        return new JobAutoScalerContext<JobID>(
                jobID,
                jobID,
                JobStatus.RUNNING,
                new Configuration(),
                metricGroup,
                getRestClusterClientSupplier());
    }

    public static JobAutoScalerContext<JobID> createResourceAwareContext() {
        return createResourceAwareContext(100., "30 gb");
    }

    public static JobAutoScalerContext<JobID> createResourceAwareContext(
            Double cpu, String memSize) {
        JobID jobId = JobID.generate();
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;
        GenericMetricGroup metricGroup = new GenericMetricGroup(registry, null, "test");
        return new JobAutoScalerContext<>(
                jobId,
                jobId,
                JobStatus.RUNNING,
                new Configuration(),
                metricGroup,
                TestingAutoscalerUtils.getRestClusterClientSupplier()) {
            @Override
            public Optional<Double> getTaskManagerCpu() {
                return Optional.ofNullable(cpu);
            }

            @Override
            public Optional<MemorySize> getTaskManagerMemory() {
                return Optional.ofNullable(memSize).map(MemorySize::parse);
            }
        };
    }

    public static SupplierWithException<RestClusterClient<String>, Exception>
            getRestClusterClientSupplier() {
        return () ->
                new RestClusterClient<>(
                        new Configuration(),
                        "test-cluster",
                        (c, e) -> new StandaloneClientHAServices("localhost")) {

                    @Override
                    public CompletableFuture<JobDetailsInfo> getJobDetails(JobID jobId) {
                        CompletableFuture<JobDetailsInfo> result = new CompletableFuture<>();
                        result.complete(
                                new JobDetailsInfo(
                                        jobId,
                                        "",
                                        false,
                                        JobStatus.RUNNING,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        Map.of(),
                                        List.of(),
                                        Map.of(),
                                        new JobPlanInfo.RawJson("")));
                        return result;
                    }
                };
    }
}
