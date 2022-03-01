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

import org.apache.flink.configuration.Configuration;

import lombok.Value;

/** Configuration class for operator. */
@Value
public class FlinkOperatorConfiguration {

    int reconcileIntervalInSec;

    int portCheckIntervalInSec;

    int savepointTriggerGracePeriodInSec;

    public static FlinkOperatorConfiguration fromConfiguration(Configuration operatorConfig) {
        int reconcileIntervalInSec =
                operatorConfig.getInteger(
                        OperatorConfigOptions.OPERATOR_RECONCILER_RESCHEDULE_INTERVAL_IN_SEC);
        int portCheckIntervalInSec =
                operatorConfig.getInteger(
                        OperatorConfigOptions.OPERATOR_OBSERVER_PORT_CHECK_INTERVAL_IN_SEC);

        int savepointTriggerGracePeriodInSec =
                operatorConfig.getInteger(
                        OperatorConfigOptions
                                .OPERATOR_OBSERVER_SAVEPOINT_TRIGGER_GRACE_PERIOD_IN_SEC);

        return new FlinkOperatorConfiguration(
                reconcileIntervalInSec, portCheckIntervalInSec, savepointTriggerGracePeriodInSec);
    }
}
