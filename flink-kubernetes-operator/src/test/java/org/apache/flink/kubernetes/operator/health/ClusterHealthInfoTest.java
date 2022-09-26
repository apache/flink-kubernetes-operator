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
        assertFalse(ClusterHealthInfo.isValid(ClusterHealthInfo.of(clock, 0)));
    }

    @Test
    public void isValidShouldReturnTrueWhenTimestampIsNonzero() {
        var clock = Clock.fixed(ofEpochSecond(1), ZoneId.systemDefault());
        assertTrue(ClusterHealthInfo.isValid(ClusterHealthInfo.of(clock, 0)));
    }

    @Test
    public void serializationRoundTrip() {
        var clock = Clock.fixed(ofEpochSecond(123), ZoneId.systemDefault());
        var clusterHealthInfo = ClusterHealthInfo.of(clock, 456);
        var clusterHealthInfoJson = ClusterHealthInfo.serialize(clusterHealthInfo);

        var clusterHealthInfoFromJson = ClusterHealthInfo.deserialize(clusterHealthInfoJson);
        assertEquals(clusterHealthInfo, clusterHealthInfoFromJson);
    }
}
