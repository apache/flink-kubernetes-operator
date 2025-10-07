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

package org.apache.flink.kubernetes.operator.health;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Clock;

/** Represents information about job health. */
@Experimental
@Data
@AllArgsConstructor
public class ClusterHealthInfo {
    /** Millisecond timestamp of the last observed health information. */
    private long timeStamp;

    /** Number of restarts. */
    private int numRestarts;

    /** Millisecond timestamp lastly evaluated the number of restarts. */
    private long numRestartsEvaluationTimeStamp;

    /** Number of successfully completed checkpoints. */
    private int numCompletedCheckpoints;

    /** Millisecond timestamp lastly increased the number of completed checkpoints. */
    private long numCompletedCheckpointsIncreasedTimeStamp;

    /** Calculated field whether the cluster is healthy or not. */
    private ClusterHealthResult healthResult;

    public ClusterHealthInfo() {
        this(Clock.systemDefaultZone());
    }

    public ClusterHealthInfo(Clock clock) {
        timeStamp = clock.millis();
        healthResult = ClusterHealthResult.healthy();
    }

    public static boolean isValid(ClusterHealthInfo clusterHealthInfo) {
        return clusterHealthInfo.timeStamp != 0;
    }

    public static String serialize(ClusterHealthInfo clusterHealthInfo) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(clusterHealthInfo);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    "Could not serialize ClusterHealthInfo, this indicates a bug...", e);
        }
    }

    public static ClusterHealthInfo deserialize(String data) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(data, ClusterHealthInfo.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    "Could not deserialize ClusterHealthInfo, this indicates a bug...", e);
        }
    }
}
