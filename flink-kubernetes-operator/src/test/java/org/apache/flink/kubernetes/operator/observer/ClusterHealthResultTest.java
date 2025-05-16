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

package org.apache.flink.kubernetes.operator.observer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterHealthResultTest {

    @Test
    void error() {
        ClusterHealthResult clusterHealthResult = ClusterHealthResult.error("test-error");

        assertFalse(clusterHealthResult.isHealthy());
        assertEquals("test-error", clusterHealthResult.getError());
    }

    @Test
    void healthy() {
        ClusterHealthResult clusterHealthResult = ClusterHealthResult.healthy();

        assertTrue(clusterHealthResult.isHealthy());
        assertNull(clusterHealthResult.getError());
    }

    @Test
    void join() {
        ClusterHealthResult clusterHealthResult =
                ClusterHealthResult.healthy()
                        .join(ClusterHealthResult.error("test-error-1"))
                        .join(ClusterHealthResult.error("test-error-2"));

        assertFalse(clusterHealthResult.isHealthy());
        assertEquals("test-error-1|test-error-2", clusterHealthResult.getError());
    }
}
