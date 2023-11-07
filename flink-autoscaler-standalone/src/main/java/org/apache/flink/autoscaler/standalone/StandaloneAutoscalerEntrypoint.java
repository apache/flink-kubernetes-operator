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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.JobAutoScalerImpl;
import org.apache.flink.autoscaler.RestApiMetricsCollector;
import org.apache.flink.autoscaler.ScalingExecutor;
import org.apache.flink.autoscaler.ScalingMetricEvaluator;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.event.LoggingEventHandler;
import org.apache.flink.autoscaler.standalone.flinkcluster.FlinkClusterJobListFetcher;
import org.apache.flink.autoscaler.standalone.realizer.RescaleApiScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.util.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.FLINK_CLIENT_TIMEOUT;

/** The entrypoint of the standalone autoscaler. */
@Experimental
public class StandaloneAutoscalerEntrypoint {

    private static final Logger LOG = LoggerFactory.getLogger(StandaloneAutoscalerEntrypoint.class);

    public static final String SCALING_INTERVAL = "scalingInterval";
    private static final Duration DEFAULT_SCALING_INTERVAL = Duration.ofSeconds(10);

    // This timeout option is used before the job config is got, such as: listJobs, get
    // Configuration, etc.
    public static final String REST_CLIENT_TIMEOUT = "restClientTimeout";

    /** Arguments related to {@link FlinkClusterJobListFetcher}. */
    public static final String FLINK_CLUSTER_HOST = "flinkClusterHost";

    private static final String DEFAULT_FLINK_CLUSTER_HOST = "localhost";

    public static final String FLINK_CLUSTER_PORT = "flinkClusterPort";
    private static final int DEFAULT_FLINK_CLUSTER_PORT = 8081;

    public static <KEY, Context extends JobAutoScalerContext<KEY>> void main(String[] args) {
        var parameters = ParameterTool.fromArgs(args);
        LOG.info("The standalone autoscaler is started, parameters: {}", parameters.toMap());

        var scalingInterval = DEFAULT_SCALING_INTERVAL;
        if (parameters.get(SCALING_INTERVAL) != null) {
            scalingInterval = TimeUtils.parseDuration(parameters.get(SCALING_INTERVAL));
        }

        var restClientTimeout = FLINK_CLIENT_TIMEOUT.defaultValue();
        if (parameters.get(REST_CLIENT_TIMEOUT) != null) {
            restClientTimeout = TimeUtils.parseDuration(parameters.get(REST_CLIENT_TIMEOUT));
        }

        // Initialize JobListFetcher and JobAutoScaler.
        var eventHandler = new LoggingEventHandler<KEY, Context>();
        JobListFetcher<KEY, Context> jobListFetcher =
                createJobListFetcher(parameters, restClientTimeout);
        var autoScaler = createJobAutoscaler(eventHandler);

        var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(
                        scalingInterval, jobListFetcher, eventHandler, autoScaler);
        autoscalerExecutor.start();
    }

    private static <KEY, Context extends JobAutoScalerContext<KEY>>
            JobListFetcher<KEY, Context> createJobListFetcher(
                    ParameterTool parameters, Duration restClientTimeout) {
        var host = parameters.get(FLINK_CLUSTER_HOST, DEFAULT_FLINK_CLUSTER_HOST);
        var port = parameters.getInt(FLINK_CLUSTER_PORT, DEFAULT_FLINK_CLUSTER_PORT);
        var restServerAddress = String.format("http://%s:%s", host, port);

        return (JobListFetcher<KEY, Context>)
                new FlinkClusterJobListFetcher(
                        conf ->
                                new RestClusterClient<>(
                                        conf,
                                        "clusterId",
                                        (c, e) ->
                                                new StandaloneClientHAServices(restServerAddress)),
                        restClientTimeout);
    }

    private static <KEY, Context extends JobAutoScalerContext<KEY>>
            JobAutoScaler<KEY, Context> createJobAutoscaler(
                    AutoScalerEventHandler<KEY, Context> eventHandler) {
        AutoScalerStateStore<KEY, Context> stateStore = new InMemoryAutoScalerStateStore<>();
        return new JobAutoScalerImpl<>(
                new RestApiMetricsCollector<>(),
                new ScalingMetricEvaluator(),
                new ScalingExecutor<>(eventHandler, stateStore),
                eventHandler,
                new RescaleApiScalingRealizer<>(eventHandler),
                stateStore);
    }
}
