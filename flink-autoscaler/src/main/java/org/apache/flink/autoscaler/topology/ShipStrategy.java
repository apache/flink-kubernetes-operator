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

package org.apache.flink.autoscaler.topology;

import javax.annotation.Nonnull;

/** The ship strategy between 2 JobVertices. */
public enum ShipStrategy {
    HASH,

    REBALANCE,

    RESCALE,

    FORWARD,

    CUSTOM,

    BROADCAST,

    GLOBAL,

    SHUFFLE,

    UNKNOWN;

    /**
     * Generates a ShipStrategy from a string, or returns {@link #UNKNOWN} if the value cannot match
     * any ShipStrategy.
     */
    @Nonnull
    public static ShipStrategy of(String value) {
        for (ShipStrategy shipStrategy : ShipStrategy.values()) {
            if (shipStrategy.toString().equalsIgnoreCase(value)) {
                return shipStrategy;
            }
        }
        return UNKNOWN;
    }
}
