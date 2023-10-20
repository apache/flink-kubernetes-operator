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

package org.apache.flink.autoscaler.event;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;

import static org.apache.flink.autoscaler.metrics.ScalingMetric.EXPECTED_PROCESSING_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TARGET_DATA_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;

/**
 * Handler for autoscaler events.
 *
 * @param <KEY> The job key.
 * @param <Context> Instance of JobAutoScalerContext.
 */
@Experimental
public interface AutoScalerEventHandler<KEY, Context extends JobAutoScalerContext<KEY>> {
    String SCALING_SUMMARY_ENTRY =
            " Vertex ID %s | Parallelism %d -> %d | Processing capacity %.2f -> %.2f | Target data rate %.2f";
    String SCALING_SUMMARY_HEADER_SCALING_DISABLED = "Recommended parallelism change:";
    String SCALING_SUMMARY_HEADER_SCALING_ENABLED = "Scaling vertices:";
    String SCALING_REPORT_REASON = "ScalingReport";
    String SCALING_REPORT_KEY = "ScalingExecutor";

    /**
     * Handle the event.
     *
     * @param interval Define the interval to suppress duplicate events. No dedupe if null.
     */
    void handleEvent(
            Context context,
            Type type,
            String reason,
            String message,
            @Nullable String messageKey,
            @Nullable Duration interval);

    /**
     * Handle scaling reports.
     *
     * @param interval Define the interval to suppress duplicate events.
     * @param scaled Whether AutoScaler actually scaled the Flink job or just generate advice for
     *     scaling.
     */
    default void handleScalingEvent(
            Context context,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            boolean scaled,
            Duration interval) {
        // Provide default implementation without proper deduplication
        var scalingReport = scalingReport(scalingSummaries, scaled);
        handleEvent(
                context,
                Type.Normal,
                SCALING_REPORT_REASON,
                scalingReport,
                SCALING_REPORT_KEY,
                interval);
    }

    static String scalingReport(
            Map<JobVertexID, ScalingSummary> scalingSummaries, boolean scalingEnabled) {
        StringBuilder sb =
                new StringBuilder(
                        scalingEnabled
                                ? SCALING_SUMMARY_HEADER_SCALING_ENABLED
                                : SCALING_SUMMARY_HEADER_SCALING_DISABLED);
        scalingSummaries.forEach(
                (v, s) ->
                        sb.append(
                                String.format(
                                        SCALING_SUMMARY_ENTRY,
                                        v,
                                        s.getCurrentParallelism(),
                                        s.getNewParallelism(),
                                        s.getMetrics().get(TRUE_PROCESSING_RATE).getAverage(),
                                        s.getMetrics().get(EXPECTED_PROCESSING_RATE).getCurrent(),
                                        s.getMetrics().get(TARGET_DATA_RATE).getAverage())));
        return sb.toString();
    }

    /** The type of the events. */
    enum Type {
        Normal,
        Warning
    }
}
