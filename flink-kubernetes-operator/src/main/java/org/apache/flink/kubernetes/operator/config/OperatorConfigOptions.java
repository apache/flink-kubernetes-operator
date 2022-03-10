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

/** This class holds configuration constants used by flink operator. */
public class OperatorConfigOptions {

    public static final ConfigOption<Integer> OPERATOR_RECONCILER_RESCHEDULE_INTERVAL_IN_SEC =
            ConfigOptions.key("operator.reconciler.reschedule.interval.sec")
                    .intType()
                    .defaultValue(60)
                    .withDescription(
                            "The interval in second for the controller to reschedule the reconcile process");

    public static final ConfigOption<Integer> OPERATOR_OBSERVER_REST_READY_DELAY_IN_SEC =
            ConfigOptions.key("operator.observer.rest-ready.delay.sec")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Final delay before deployment is marked ready after port becomes accessible.");

    public static final ConfigOption<Integer> OPERATOR_OBSERVER_PROGRESS_CHECK_INTERVAL_IN_SEC =
            ConfigOptions.key("operator.observer.progress-check.interval.sec")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The interval for observing status for in-progress operations such as deployment and savepoints.");

    public static final ConfigOption<Integer>
            OPERATOR_OBSERVER_SAVEPOINT_TRIGGER_GRACE_PERIOD_IN_SEC =
                    ConfigOptions.key("operator.observer.savepoint.trigger.grace-period.sec")
                            .intType()
                            .defaultValue(10)
                            .withDescription(
                                    "The interval in seconds before a savepoint trigger attempt is marked as unsuccessful");
}
