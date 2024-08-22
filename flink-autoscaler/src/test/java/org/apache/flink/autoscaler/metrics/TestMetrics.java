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

package org.apache.flink.autoscaler.metrics;

import org.apache.flink.autoscaler.topology.IOMetrics;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/** Class to capture collected test metrics. */
@Data
@Builder
public class TestMetrics {
    private long numRecordsIn;
    private long numRecordsOut;
    private long maxBusyTimePerSec;
    private long avgBackpressureTimePerSec;
    private long accumulatedBusyTime;

    private Long pendingRecords;
    private Double numRecordsInPerSec;

    public Map<FlinkMetric, AggregatedMetric> toFlinkMetrics() {
        var fm = new HashMap<FlinkMetric, AggregatedMetric>();
        if (pendingRecords != null) {
            fm.put(FlinkMetric.PENDING_RECORDS, sum(pendingRecords));
        }
        if (numRecordsInPerSec != null) {
            fm.put(FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC, sum(numRecordsInPerSec));
        }
        fm.put(FlinkMetric.BUSY_TIME_PER_SEC, max(maxBusyTimePerSec));
        fm.put(FlinkMetric.BACKPRESSURE_TIME_PER_SEC, avg(avgBackpressureTimePerSec));
        fm.put(FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT, sum(numRecordsIn));
        return fm;
    }

    public IOMetrics toIoMetrics() {
        return new IOMetrics(numRecordsIn, numRecordsOut, accumulatedBusyTime);
    }

    private AggregatedMetric sum(double d) {
        return new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, d, Double.NaN);
    }

    private AggregatedMetric max(double d) {
        return new AggregatedMetric("", Double.NaN, d, Double.NaN, Double.NaN, Double.NaN);
    }

    private AggregatedMetric avg(double d) {
        return new AggregatedMetric("", Double.NaN, Double.NaN, d, Double.NaN, Double.NaN);
    }
}
