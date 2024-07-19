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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.JobAutoScalerImpl;
import org.apache.flink.autoscaler.RestApiMetricsCollector;
import org.apache.flink.autoscaler.ScalingExecutor;
import org.apache.flink.autoscaler.ScalingMetricEvaluator;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.standalone.flinkcluster.FlinkClusterJobListFetcher;
import org.apache.flink.autoscaler.standalone.realizer.RescaleApiScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.FLINK_CLIENT_TIMEOUT;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.FETCHER_FLINK_CLUSTER_HOST;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.FETCHER_FLINK_CLUSTER_PORT;

/** The entrypoint of the standalone autoscaler. */
@Experimental
public class StandaloneAutoscalerEntrypoint {

    private static final Logger LOG = LoggerFactory.getLogger(StandaloneAutoscalerEntrypoint.class);

    public static <KEY, Context extends JobAutoScalerContext<KEY>> void main(String[] args)
            throws Exception {
        var conf = loadConfiguration(args);
        LOG.info("The standalone autoscaler is started, configuration: {}", conf);

        // Initialize JobListFetcher and JobAutoScaler.
        JobListFetcher<KEY, Context> jobListFetcher = createJobListFetcher(conf);

        AutoScalerStateStore<KEY, Context> stateStore = AutoscalerStateStoreFactory.create(conf);
        AutoScalerEventHandler<KEY, Context> eventHandler =
                AutoscalerEventHandlerFactory.create(conf);

        var autoScaler = createJobAutoscaler(eventHandler, stateStore);

        var autoscalerExecutor =
                new StandaloneAutoscalerExecutor<>(conf, jobListFetcher, eventHandler, autoScaler);
        autoscalerExecutor.start();
    }

    private static <KEY, Context extends JobAutoScalerContext<KEY>>
            JobListFetcher<KEY, Context> createJobListFetcher(Configuration conf) {
        var host = conf.get(FETCHER_FLINK_CLUSTER_HOST);
        var port = conf.get(FETCHER_FLINK_CLUSTER_PORT);
        var restServerAddress = String.format("http://%s:%s", host, port);

        return (JobListFetcher<KEY, Context>)
                new FlinkClusterJobListFetcher(
                        configuration ->
                                new RestClusterClient<>(
                                        configuration,
                                        "clusterId",
                                        (c, e) ->
                                                new StandaloneClientHAServices(restServerAddress)),
                        conf.get(FLINK_CLIENT_TIMEOUT));
    }

    private static <KEY, Context extends JobAutoScalerContext<KEY>>
            JobAutoScaler<KEY, Context> createJobAutoscaler(
                    AutoScalerEventHandler<KEY, Context> eventHandler,
                    AutoScalerStateStore<KEY, Context> stateStore) {
        return new JobAutoScalerImpl<>(
                new RestApiMetricsCollector<>(),
                new ScalingMetricEvaluator(),
                new ScalingExecutor<>(eventHandler, stateStore),
                eventHandler,
                new RescaleApiScalingRealizer<>(eventHandler),
                stateStore);
    }

    @VisibleForTesting
    static Configuration loadConfiguration(String[] args) {
        return GlobalConfiguration.loadConfiguration(
                ParameterTool.fromArgs(args).getConfiguration());
    }
}
