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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlinkVersionTest {

    @Test
    void isEqualOrNewer() {
        assertFalse(FlinkVersion.v1_16.isEqualOrNewer(FlinkVersion.v1_17));
        assertTrue(FlinkVersion.v1_17.isEqualOrNewer(FlinkVersion.v1_17));
        assertTrue(FlinkVersion.v1_18.isEqualOrNewer(FlinkVersion.v1_17));
    }

    @Test
    void current() {
        assertEquals(FlinkVersion.v1_20, FlinkVersion.current());
    }

    @Test
    void isSupported() {
        assertTrue(FlinkVersion.isSupported(FlinkVersion.v1_20));
    }

    @Test
    void fromMajorMinor() {
        assertEquals(FlinkVersion.fromMajorMinor(1, 17), FlinkVersion.v1_17);
        assertEquals(FlinkVersion.fromMajorMinor(1, 18), FlinkVersion.v1_18);
        assertEquals(FlinkVersion.fromMajorMinor(1, 19), FlinkVersion.v1_19);
        assertEquals(FlinkVersion.fromMajorMinor(1, 20), FlinkVersion.v1_20);
        assertThrows(IllegalArgumentException.class, () -> FlinkVersion.fromMajorMinor(0, 1));
    }
}
