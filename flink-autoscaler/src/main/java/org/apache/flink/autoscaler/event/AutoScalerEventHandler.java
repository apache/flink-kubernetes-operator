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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

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
public interface AutoScalerEventHandler<KEY, Context extends JobAutoScalerContext<KEY>>
        extends Closeable {
    String SCALING_SUMMARY_ENTRY =
            "{ Vertex ID %s | Parallelism %d -> %d | Processing capacity %.2f -> %.2f | Target data rate %.2f}";
    String SCALING_EXECUTION_DISABLED_REASON = "%s:%s, recommended parallelism change:";
    String SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED =
            "Scaling execution disabled by config ";
    String SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED =
            "Scaling execution enabled, begin scaling vertices:";
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
     * Handle exception, and the exception event is warning type and don't deduplicate by default.
     */
    default void handleException(Context context, String reason, Throwable e) {
        var message = e.getMessage();
        if (message == null) {
            message = StringUtils.abbreviate(ExceptionUtils.getStackTrace(e), 2048);
        }
        handleEvent(context, Type.Warning, reason, message, null, null);
    }

    /**
     * Handle scaling reports.
     *
     * @param interval Define the interval to suppress duplicate events.
     * @param message Message describe the event.
     */
    default void handleScalingEvent(
            Context context,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            String message,
            Duration interval) {
        // Provide default implementation without proper deduplication
        var scalingReport = scalingReport(scalingSummaries, message);
        handleEvent(
                context,
                Type.Normal,
                SCALING_REPORT_REASON,
                scalingReport,
                SCALING_REPORT_KEY,
                interval);
    }

    /** Close the related resource. */
    default void close() {}

    static String scalingReport(Map<JobVertexID, ScalingSummary> scalingSummaries, String message) {
        StringBuilder sb = new StringBuilder(message);
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

    static String getParallelismHashCode(Map<JobVertexID, ScalingSummary> scalingSummaryHashMap) {
        return Integer.toString(
                scalingSummaryHashMap.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                e -> e.getKey().toString(),
                                                e ->
                                                        String.format(
                                                                "Parallelism %d -> %d",
                                                                e.getValue()
                                                                        .getCurrentParallelism(),
                                                                e.getValue().getNewParallelism())))
                                .hashCode()
                        & 0x7FFFFFFF);
    }

    /** The type of the events. */
    enum Type {
        Normal,
        Warning
    }
}
