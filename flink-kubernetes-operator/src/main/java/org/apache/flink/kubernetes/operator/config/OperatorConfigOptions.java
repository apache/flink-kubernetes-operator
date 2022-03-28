/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import io.javaoperatorsdk.operator.api.config.ConfigurationService;

import java.time.Duration;

/** This class holds configuration constants used by flink operator. */
public class OperatorConfigOptions {

    public static final ConfigOption<Duration> OPERATOR_RECONCILER_RESCHEDULE_INTERVAL =
            ConfigOptions.key("operator.reconciler.reschedule.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            "The interval for the controller to reschedule the reconcile process");

    public static final ConfigOption<Duration> OPERATOR_OBSERVER_REST_READY_DELAY =
            ConfigOptions.key("operator.observer.rest-ready.delay")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "Final delay before deployment is marked ready after port becomes accessible.");

    public static final ConfigOption<Integer> OPERATOR_RECONCILER_MAX_PARALLELISM =
            ConfigOptions.key("operator.reconciler.max.parallelism")
                    .intType()
                    .defaultValue(ConfigurationService.DEFAULT_RECONCILIATION_THREADS_NUMBER)
                    .withDescription(
                            "The maximum number of threads running the reconciliation loop. Use -1 for infinite.");

    public static final ConfigOption<Duration> OPERATOR_OBSERVER_PROGRESS_CHECK_INTERVAL =
            ConfigOptions.key("operator.observer.progress-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The interval for observing status for in-progress operations such as deployment and savepoints.");

    public static final ConfigOption<Duration> OPERATOR_OBSERVER_SAVEPOINT_TRIGGER_GRACE_PERIOD =
            ConfigOptions.key("operator.observer.savepoint.trigger.grace-period")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The interval before a savepoint trigger attempt is marked as unsuccessful");

    public static final ConfigOption<Duration> OPERATOR_OBSERVER_FLINK_CLIENT_TIMEOUT =
            ConfigOptions.key("operator.observer.flink.client.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The timeout for the observer to wait the flink rest client to return.");
}
