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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.javaoperatorsdk.operator.processing.event.ResourceID;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/** An event handler which posts events to the Kubernetes events API. */
public class KubernetesAutoScalerEventHandler
        implements AutoScalerEventHandler<ResourceID, KubernetesJobAutoScalerContext> {

    public static final String PARALLELISM_MAP_KEY = "parallelismMap";
    private final EventRecorder eventRecorder;

    public KubernetesAutoScalerEventHandler(EventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
    }

    @Override
    public void handleEvent(
            KubernetesJobAutoScalerContext context,
            Type type,
            String reason,
            String message,
            @Nullable String messageKey,
            @Nullable Duration interval) {
        eventRecorder.triggerEventWithInterval(
                context.getResource(),
                EventRecorder.Type.valueOf(type.name()),
                reason,
                message,
                EventRecorder.Component.Operator,
                messageKey,
                context.getKubernetesClient(),
                interval);
    }

    @Override
    public void handleScalingEvent(
            KubernetesJobAutoScalerContext context,
            Map<JobVertexID, ScalingSummary> scalingSummaries,
            String message,
            Duration interval) {
        if (message.contains(SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED)) {
            AutoScalerEventHandler.super.handleScalingEvent(
                    context, scalingSummaries, message, null);
        } else {
            var scalingReport = AutoScalerEventHandler.scalingReport(scalingSummaries, message);
            var labels =
                    Map.of(
                            PARALLELISM_MAP_KEY,
                            AutoScalerEventHandler.getParallelismHashCode(scalingSummaries));

            @Nullable
            Predicate<Map<String, String>> dedupePredicate =
                    new Predicate<Map<String, String>>() {
                        @Override
                        public boolean test(Map<String, String> stringStringMap) {
                            return stringStringMap != null
                                    && Objects.equals(
                                            stringStringMap.get(PARALLELISM_MAP_KEY),
                                            AutoScalerEventHandler.getParallelismHashCode(
                                                    scalingSummaries));
                        }
                    };

            eventRecorder.triggerEventWithLabels(
                    context.getResource(),
                    EventRecorder.Type.Normal,
                    AutoScalerEventHandler.SCALING_REPORT_REASON,
                    scalingReport,
                    EventRecorder.Component.Operator,
                    AutoScalerEventHandler.SCALING_REPORT_KEY,
                    context.getKubernetesClient(),
                    interval,
                    dedupePredicate,
                    labels);
        }
    }

    @Override
    public void close() throws Exception {}
}
