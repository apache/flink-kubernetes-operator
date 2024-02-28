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

package runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo}. */
public class IOMetricsInfoTest {

    @Test
    void testNullableAggregatedMetrics() {
        var info1 = new IOMetricsInfo(0, false, 0, false, 0, false, 0, false, 1L, 2L, 3.4);
        assertEquals(1, info1.getAccumulatedBackpressured());
        assertEquals(2, info1.getAccumulatedIdle());
        assertEquals(3.4, info1.getAccumulatedBusy());
        var info2 = new IOMetricsInfo(0, false, 0, false, 0, false, 0, false, null, null, null);
        assertEquals(0, info2.getAccumulatedBackpressured());
        assertEquals(0, info2.getAccumulatedIdle());
        assertEquals(Double.NaN, info2.getAccumulatedBusy());
    }
}
