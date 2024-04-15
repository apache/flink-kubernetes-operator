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
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.standalone.JobListFetcher;
import org.apache.flink.autoscaler.utils.JobStatusUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.job.JobManagerJobConfigurationHeaders;
import org.apache.flink.util.function.FunctionWithException;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Fetch JobAutoScalerContext based on flink cluster. */
public class FlinkClusterJobListFetcher
        implements JobListFetcher<JobID, JobAutoScalerContext<JobID>> {

    private final FunctionWithException<Configuration, RestClusterClient<String>, Exception>
            restClientGetter;
    private final Duration restClientTimeout;

    public FlinkClusterJobListFetcher(
            FunctionWithException<Configuration, RestClusterClient<String>, Exception>
                    restClientGetter,
            Duration restClientTimeout) {
        this.restClientGetter = restClientGetter;
        this.restClientTimeout = restClientTimeout;
    }

    @Override
    public Collection<JobAutoScalerContext<JobID>> fetch(Configuration baseConf) throws Exception {
        try (var restClusterClient = restClientGetter.apply(new Configuration())) {
            return restClusterClient
                    .sendRequest(
                            JobsOverviewHeaders.getInstance(),
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance())
                    .thenApply(JobStatusUtils::toJobStatusMessage)
                    .get(restClientTimeout.toSeconds(), TimeUnit.SECONDS)
                    .stream()
                    .map(
                            jobStatusMessage -> {
                                try {
                                    return generateJobContext(
                                            baseConf, restClusterClient, jobStatusMessage);
                                } catch (Throwable e) {
                                    throw new RuntimeException(
                                            "generateJobContext throw exception", e);
                                }
                            })
                    .collect(Collectors.toList());
        }
    }

    private JobAutoScalerContext<JobID> generateJobContext(
            Configuration baseConf,
            RestClusterClient<String> restClusterClient,
            JobStatusMessage jobStatusMessage)
            throws Exception {
        var jobId = jobStatusMessage.getJobId();
        var conf = getConfiguration(baseConf, restClusterClient, jobId);

        return new JobAutoScalerContext<>(
                jobId,
                jobId,
                jobStatusMessage.getJobState(),
                conf,
                new UnregisteredMetricsGroup(),
                () -> restClientGetter.apply(conf));
    }

    private Configuration getConfiguration(
            Configuration baseConf, RestClusterClient<String> restClusterClient, JobID jobId)
            throws Exception {
        var jobParameters = new JobMessageParameters();
        jobParameters.jobPathParameter.resolve(jobId);

        var configurationInfo =
                restClusterClient
                        .sendRequest(
                                JobManagerJobConfigurationHeaders.getInstance(),
                                jobParameters,
                                EmptyRequestBody.getInstance())
                        .get(restClientTimeout.toSeconds(), TimeUnit.SECONDS);

        var conf = new Configuration(baseConf);
        configurationInfo.forEach(entry -> conf.setString(entry.getKey(), entry.getValue()));
        return conf;
    }
}
