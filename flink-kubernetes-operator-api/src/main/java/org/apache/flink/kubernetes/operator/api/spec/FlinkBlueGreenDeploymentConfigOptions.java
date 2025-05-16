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

/** Configuration options to be used by the Flink Blue/Green Deployments. */
public class FlinkBlueGreenDeploymentConfigOptions {

    public static final String BLUE_GREEN_CONF_PREFIX = "bluegreen.";

    public static final int MIN_ABORT_GRACE_PERIOD_MS = 120000; // 2 mins

    public static ConfigOptions.OptionBuilder operatorConfig(String key) {
        return ConfigOptions.key(BLUE_GREEN_CONF_PREFIX + key);
    }

    public static final ConfigOption<Integer> ABORT_GRACE_PERIOD_MS =
            operatorConfig("abortGracePeriodMs")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The max time to wait for a deployment to become ready before aborting it, in milliseconds. Cannot be smaller than 2 minutes.");

    public static final ConfigOption<Integer> RECONCILIATION_RESCHEDULING_INTERVAL_MS =
            operatorConfig("reconciliationReschedulingIntervalMs")
                    .intType()
                    .defaultValue(15000) // 15 seconds
                    .withDescription(
                            "Configurable delay in milliseconds to use when the operator reschedules a reconciliation.");

    public static final ConfigOption<Integer> DEPLOYMENT_DELETION_DELAY_MS =
            operatorConfig("deploymentDeletionDelayMs")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Configurable delay before deleting a deployment after being marked done.");
}
