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

package org.apache.flink.autoscaler.standalone.yarn;

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.standalone.JobListFetcher;
import org.apache.flink.autoscaler.utils.JobStatusUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.job.JobManagerJobConfigurationHeaders;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.FETCHER_FLINK_CLUSTER_HOST;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.FETCHER_FLINK_CLUSTER_PORT;

/** Fetch JobAutoScalerContext based on Flink on YARN cluster. */
public class YarnJobListFetcher implements JobListFetcher<JobID, JobAutoScalerContext<JobID>> {

    private final FunctionWithException<Configuration, RestClusterClient<String>, Exception>
            restClientGetter;
    private final Duration restClientTimeout;

    public YarnJobListFetcher(
            FunctionWithException<Configuration, RestClusterClient<String>, Exception>
                    restClientGetter,
            Duration restClientTimeout) {
        this.restClientGetter = restClientGetter;
        this.restClientTimeout = restClientTimeout;
    }

    @Override
    public Collection<JobAutoScalerContext<JobID>> fetch(Configuration baseConf) throws Exception {

        List<JobAutoScalerContext<JobID>> discovered = tryFetchFromFirstRunningYarnApp(baseConf);
        if (!discovered.isEmpty()) {
            return discovered;
        }

        // use supplied client factory (may point to direct JM or a reverse proxy)
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

    private List<JobAutoScalerContext<JobID>> tryFetchFromFirstRunningYarnApp(
            Configuration baseConf) {
        List<JobAutoScalerContext<JobID>> contexts = new ArrayList<>();
        YarnClient yarnClient = null;
        try {
            yarnClient = YarnClient.createYarnClient();
            org.apache.hadoop.conf.Configuration yarnConf =
                    new org.apache.hadoop.conf.Configuration();
            yarnClient.init(yarnConf);
            yarnClient.start();

            Set<String> appTypes = new HashSet<>();
            appTypes.add("Apache Flink");
            List<ApplicationReport> apps = yarnClient.getApplications(appTypes);

            String rmBase =
                    String.format(
                            "http://%s:%s",
                            baseConf.get(FETCHER_FLINK_CLUSTER_HOST),
                            baseConf.get(FETCHER_FLINK_CLUSTER_PORT));

            for (ApplicationReport app : apps) {
                if (app.getYarnApplicationState() != YarnApplicationState.RUNNING) {
                    continue;
                }
                String appId = app.getApplicationId().toString();
                String proxyBase = rmBase + "/proxy/" + appId;

                try (var client =
                        new RestClusterClient<>(
                                new Configuration(),
                                "clusterId",
                                (c, e) -> new StandaloneClientHAServices(proxyBase))) {
                    var fetched =
                            client
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
                                                    return generateJobContextForEndpoint(
                                                            baseConf, proxyBase, jobStatusMessage);
                                                } catch (Throwable e) {
                                                    throw new RuntimeException(
                                                            "generateJobContext throw exception",
                                                            e);
                                                }
                                            })
                                    .collect(Collectors.toList());
                    contexts.addAll(fetched);
                }
                break;
            }
        } catch (Throwable ignore) {
            // Ignore
        } finally {
            if (yarnClient != null) {
                try {
                    yarnClient.stop();
                } catch (Throwable ignored) {
                }
            }
        }
        return contexts;
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

    private JobAutoScalerContext<JobID> generateJobContextForEndpoint(
            Configuration baseConf, String endpointBase, JobStatusMessage jobStatusMessage)
            throws Exception {
        var jobId = jobStatusMessage.getJobId();
        try (var client =
                new RestClusterClient<>(
                        new Configuration(),
                        "clusterId",
                        (c, e) -> new StandaloneClientHAServices(endpointBase))) {
            var conf = getConfiguration(baseConf, client, jobId);
            return new JobAutoScalerContext<>(
                    jobId,
                    jobId,
                    jobStatusMessage.getJobState(),
                    conf,
                    new UnregisteredMetricsGroup(),
                    () ->
                            new RestClusterClient<>(
                                    conf,
                                    "clusterId",
                                    (c, e) -> new StandaloneClientHAServices(endpointBase)));
        }
    }
}
