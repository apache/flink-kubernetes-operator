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

package org.apache.flink.autoscaler.jdbc.state;

import org.apache.flink.util.StringUtils;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The state type. */
public enum StateType {
    SCALING_HISTORY("scalingHistory"),
    SCALING_TRACKING("scalingTracking"),
    COLLECTED_METRICS("collectedMetrics"),
    PARALLELISM_OVERRIDES("parallelismOverrides"),
    CONFIG_OVERRIDES("configOverrides"),
    DELAYED_SCALE_DOWN("delayedScaleDown");

    /**
     * The identifier of each state type, it will be used to store. Please ensure the identifier is
     * different for each state type.
     */
    private final String identifier;

    StateType(String identifier) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(identifier) && identifier.length() < 30,
                "Ensure the identifier length is less than 30 to reduce the storage cost.");
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    public static StateType createFromIdentifier(String identifier) {
        for (StateType stateType : StateType.values()) {
            if (stateType.getIdentifier().equals(identifier)) {
                return stateType;
            }
        }

        throw new IllegalArgumentException(
                String.format("Unknown StateType identifier : %s.", identifier));
    }
}
