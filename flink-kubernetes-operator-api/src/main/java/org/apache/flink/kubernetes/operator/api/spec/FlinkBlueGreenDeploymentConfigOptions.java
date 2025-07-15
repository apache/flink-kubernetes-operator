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

package org.apache.flink.kubernetes.operator.api.spec;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Configuration options to be used by the Flink Blue/Green Deployments. */
public class FlinkBlueGreenDeploymentConfigOptions {

    public static final String K8S_OP_CONF_PREFIX = "kubernetes.operator.";

    public static final String BLUE_GREEN_CONF_PREFIX = K8S_OP_CONF_PREFIX + "bluegreen.";

    public static final int MIN_ABORT_GRACE_PERIOD_MS = 120000; // 2 mins

    public static ConfigOptions.OptionBuilder operatorConfig(String key) {
        return ConfigOptions.key(BLUE_GREEN_CONF_PREFIX + key);
    }

    public static final ConfigOption<Duration> ABORT_GRACE_PERIOD =
            operatorConfig("abort.grace-period")
                    .durationType()
                    .defaultValue(Duration.ofMillis(MIN_ABORT_GRACE_PERIOD_MS))
                    .withDescription(
                            "The max time to wait in milliseconds for a deployment to become ready before aborting it. Cannot be smaller than 2 minutes.");

    public static final ConfigOption<Duration> RECONCILIATION_RESCHEDULING_INTERVAL =
            operatorConfig("reconciliation.reschedule-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(15000)) // 15 seconds
                    .withDescription(
                            "Configurable delay in milliseconds to use when the operator reschedules a reconciliation.");

    public static final ConfigOption<Duration> DEPLOYMENT_DELETION_DELAY =
            operatorConfig("deployment-deletion.delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "Configurable delay in milliseconds before deleting a deployment after being marked done.");
}
