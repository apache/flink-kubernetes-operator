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

import org.apache.flink.autoscaler.standalone.AutoscalerEventHandlerFactory.EventHandlerType;
import org.apache.flink.autoscaler.standalone.AutoscalerStateStoreFactory.StateStoreType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.code;

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

    public static final ConfigOption<StateStoreType> STATE_STORE_TYPE =
            autoscalerStandaloneConfig("state-store.type")
                    .enumType(StateStoreType.class)
                    .defaultValue(StateStoreType.MEMORY)
                    .withDescription("The autoscaler state store type.");

    public static final ConfigOption<EventHandlerType> EVENT_HANDLER_TYPE =
            autoscalerStandaloneConfig("event-handler.type")
                    .enumType(EventHandlerType.class)
                    .defaultValue(EventHandlerType.LOGGING)
                    .withDescription("The autoscaler event handler type.");

    public static final ConfigOption<String> JDBC_URL =
            autoscalerStandaloneConfig("jdbc.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The jdbc url when %s or %s has been set to %s, such as: %s.",
                                            code(STATE_STORE_TYPE.key()),
                                            code(EVENT_HANDLER_TYPE.key()),
                                            code("JDBC"),
                                            code("jdbc:mysql://localhost:3306/flink_autoscaler"))
                                    .linebreak()
                                    .text(
                                            "This option is required when using JDBC state store or JDBC event handler.")
                                    .build());

    public static final ConfigOption<String> JDBC_USERNAME =
            autoscalerStandaloneConfig("jdbc.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The jdbc username when %s or %s has been set to %s.",
                                            code(STATE_STORE_TYPE.key()),
                                            code(EVENT_HANDLER_TYPE.key()),
                                            code("JDBC"))
                                    .build());

    public static final ConfigOption<String> JDBC_PASSWORD_ENV_VARIABLE =
            autoscalerStandaloneConfig("jdbc.password-env-variable")
                    .stringType()
                    .defaultValue("JDBC_PWD")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The environment variable name of jdbc password when %s or %s has been set to %s. "
                                                    + "In general, the environment variable name doesn't need to be changed. Users need to "
                                                    + "export the password using this environment variable.",
                                            code(STATE_STORE_TYPE.key()),
                                            code(EVENT_HANDLER_TYPE.key()),
                                            code("JDBC"))
                                    .build());

    public static final ConfigOption<Duration> JDBC_EVENT_HANDLER_TTL =
            autoscalerStandaloneConfig("jdbc.event-handler.ttl")
                    .durationType()
                    .defaultValue(Duration.ofDays(90))
                    .withDescription(
                            "The time to live based on create time for the JDBC event handler records. "
                                    + "When the config is set as '0', the ttl strategy for the records would be disabled.");
}
