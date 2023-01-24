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

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.ZoneId;

import static java.time.Instant.ofEpochSecond;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterHealthInfoTest {

    @Test
    public void isValidShouldReturnFalseWhenTimestampIsZero() {
        var clock = Clock.fixed(ofEpochSecond(0), ZoneId.systemDefault());
        assertFalse(ClusterHealthInfo.isValid(new ClusterHealthInfo(clock)));
    }

    @Test
    public void isValidShouldReturnTrueWhenTimestampIsNonzero() {
        var clock = Clock.fixed(ofEpochSecond(1), ZoneId.systemDefault());
        assertTrue(ClusterHealthInfo.isValid(new ClusterHealthInfo(clock)));
    }

    @Test
    public void deserializeWithOldVersionShouldDeserializeCorrectly() {
        var clusterHealthInfoJson = "{\"timeStamp\":1,\"numRestarts\":2,\"healthy\":true}";
        var clusterHealthInfoFromJson = ClusterHealthInfo.deserialize(clusterHealthInfoJson);
        assertEquals(1, clusterHealthInfoFromJson.getTimeStamp());
        assertEquals(2, clusterHealthInfoFromJson.getNumRestarts());
        assertTrue(clusterHealthInfoFromJson.isHealthy());
    }

    @Test
    public void serializationRoundTrip() {
        var clock = Clock.fixed(ofEpochSecond(1), ZoneId.systemDefault());
        var clusterHealthInfo = new ClusterHealthInfo(clock);
        clusterHealthInfo.setNumRestarts(2);
        clusterHealthInfo.setNumRestartsEvaluationTimeStamp(3);
        clusterHealthInfo.setNumCompletedCheckpoints(4);
        clusterHealthInfo.setNumCompletedCheckpointsIncreasedTimeStamp(5);
        clusterHealthInfo.setHealthy(false);
        var clusterHealthInfoJson = ClusterHealthInfo.serialize(clusterHealthInfo);

        var clusterHealthInfoFromJson = ClusterHealthInfo.deserialize(clusterHealthInfoJson);
        assertEquals(clusterHealthInfo, clusterHealthInfoFromJson);
    }
}
