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

package org.apache.flink.autoscaler.standalone.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Config options related to the autoscaler standalone module. */
public class AutoscalerStandaloneOptions {

    private static final String AUTOSCALER_STANDALONE_CONF_PREFIX = "autoscaler.standalone.";

    private static ConfigOptions.OptionBuilder autoscalerStandaloneConfig(String key) {
        return ConfigOptions.key(AUTOSCALER_STANDALONE_CONF_PREFIX + key);
    }

    public static final ConfigOption<Duration> CONTROL_LOOP_INTERVAL =
            autoscalerStandaloneConfig("control-loop.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDeprecatedKeys("scalingInterval")
                    .withDescription("The interval of autoscaler standalone control loop.");

    public static final ConfigOption<Integer> CONTROL_LOOP_PARALLELISM =
            autoscalerStandaloneConfig("control-loop.parallelism")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The parallelism of autoscaler standalone control loop.");

    public static final ConfigOption<String> FETCHER_FLINK_CLUSTER_HOST =
            autoscalerStandaloneConfig("fetcher.flink-cluster.host")
                    .stringType()
                    .defaultValue("localhost")
                    .withDeprecatedKeys("flinkClusterHost")
                    .withDescription(
                            "The host name of flink cluster when the flink-cluster fetcher is used.");

    public static final ConfigOption<Integer> FETCHER_FLINK_CLUSTER_PORT =
            autoscalerStandaloneConfig("fetcher.flink-cluster.port")
                    .intType()
                    .defaultValue(8081)
                    .withDeprecatedKeys("flinkClusterPort")
                    .withDescription(
                            "The port of flink cluster when the flink-cluster fetcher is used.");
}
