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
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.state.ConfigMapStore;
import org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Map;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.EXCLUDED_PERIODS;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_EXECUTION_DISABLED_REASON;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_ENTRY;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
import static org.apache.flink.kubernetes.operator.autoscaler.KubernetesAutoScalerEventHandler.PARALLELISM_MAP_KEY;
import static org.apache.flink.kubernetes.operator.autoscaler.TestingKubernetesAutoscalerUtils.createContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link KubernetesAutoScalerStateStore}. */
@EnableKubernetesMockClient(crud = true)
public class KubernetesAutoScalerEventHandlerTest {

    private KubernetesClient kubernetesClient;

    private KubernetesAutoScalerEventHandler eventHandler;

    private KubernetesJobAutoScalerContext ctx;

    ConfigMapStore configMapStore;

    KubernetesAutoScalerStateStore stateStore;

    private FlinkResourceEventCollector flinkResourceEventCollector;

    @BeforeEach
    void setup() {
        flinkResourceEventCollector = new FlinkResourceEventCollector();
        var eventRecorder =
                new EventRecorder(
                        flinkResourceEventCollector, new FlinkStateSnapshotEventCollector());
        ctx = createContext("cr1", kubernetesClient);
        eventHandler = new KubernetesAutoScalerEventHandler(eventRecorder);
        stateStore = new KubernetesAutoScalerStateStore(configMapStore);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "0", "1800"})
    void testHandEventsWithNoMessageKey(String intervalString) {
        testHandEvents(intervalString, null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "0", "1800"})
    void testHandEventsWithMessageKey(String intervalString) {
        testHandEvents(intervalString, "key");
    }

    private void testHandEvents(String intervalString, String messageKey) {
        Duration interval =
                intervalString.isBlank() ? null : Duration.ofSeconds(Long.valueOf(intervalString));
        var jobVertexID = new JobVertexID();

        eventHandler.handleEvent(
                ctx,
                AutoScalerEventHandler.Type.Normal,
                EventRecorder.Reason.IneffectiveScaling.name(),
                "message",
                messageKey,
                interval);
        var event = flinkResourceEventCollector.events.poll();
        assertEquals(EventRecorder.Reason.IneffectiveScaling.name(), event.getReason());
        assertEquals(1, event.getCount());

        // Resend
        eventHandler.handleEvent(
                ctx,
                AutoScalerEventHandler.Type.Normal,
                EventRecorder.Reason.IneffectiveScaling.name(),
                "message",
                messageKey,
                interval);
        if (interval != null && interval.toMillis() > 0) {
            assertEquals(0, flinkResourceEventCollector.events.size());
        } else {
            assertEquals(1, flinkResourceEventCollector.events.size());
            event = flinkResourceEventCollector.events.poll();
            assertEquals("message", event.getMessage());
            assertEquals(2, event.getCount());
        }

        // Message changed
        eventHandler.handleEvent(
                ctx,
                AutoScalerEventHandler.Type.Normal,
                EventRecorder.Reason.IneffectiveScaling.name(),
                "message1",
                messageKey,
                interval);
        if (messageKey != null && interval != null && interval.toMillis() > 0) {
            assertEquals(0, flinkResourceEventCollector.events.size());
        } else {
            assertEquals(1, flinkResourceEventCollector.events.size());
            event = flinkResourceEventCollector.events.poll();
            assertEquals("message1", event.getMessage());
            assertEquals(messageKey == null ? 1 : 3, event.getCount());
        }

        // Message key changed
        eventHandler.handleEvent(
                ctx,
                AutoScalerEventHandler.Type.Normal,
                EventRecorder.Reason.IneffectiveScaling.name(),
                "message1",
                "newKey",
                interval);
        assertEquals(1, flinkResourceEventCollector.events.size());
        event = flinkResourceEventCollector.events.poll();
        assertEquals("message1", event.getMessage());
        assertEquals(1, event.getCount());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHandleScalingEventsWith0Interval(boolean scalingEnabled) {
        testHandleScalingEvents(scalingEnabled, Duration.ofSeconds(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHandleScalingEventsWithInterval(boolean scalingEnabled) {
        testHandleScalingEvents(scalingEnabled, Duration.ofSeconds(1800));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHandleScalingEventsWithNullInterval(boolean scalingEnabled) {
        testHandleScalingEvents(scalingEnabled, null);
    }

    private void testHandleScalingEvents(boolean scaled, Duration interval) {
        var jobVertexID = JobVertexID.fromHexString("1b51e99e55e89e404d9a0443fd98d9e2");

        var evaluatedScalingMetric = new EvaluatedScalingMetric();
        evaluatedScalingMetric.setAverage(1);
        evaluatedScalingMetric.setCurrent(2);
        Map<JobVertexID, ScalingSummary> scalingSummaries1 =
                Map.of(
                        jobVertexID,
                        new ScalingSummary(
                                1,
                                2,
                                Map.of(
                                        ScalingMetric.TRUE_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.EXPECTED_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.TARGET_DATA_RATE,
                                        evaluatedScalingMetric)));

        String message =
                scaled
                        ? SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED
                        : SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED
                                + String.format(
                                        SCALING_EXECUTION_DISABLED_REASON,
                                        SCALING_ENABLED.key(),
                                        false);

        eventHandler.handleScalingEvent(ctx, scalingSummaries1, message, interval);
        var event = flinkResourceEventCollector.events.poll();
        assertTrue(
                event.getMessage()
                        .contains(
                                String.format(
                                        SCALING_SUMMARY_ENTRY,
                                        jobVertexID,
                                        1,
                                        2,
                                        1.00,
                                        2.00,
                                        1.00)));

        assertEquals(EventRecorder.Reason.ScalingReport.name(), event.getReason());
        assertEquals(
                scaled ? null : "1286380436",
                event.getMetadata().getLabels().get(PARALLELISM_MAP_KEY));
        assertEquals(1, event.getCount());

        // Parallelism map doesn't change.
        eventHandler.handleScalingEvent(ctx, scalingSummaries1, message, interval);
        Event newEvent;
        if (interval != null && interval.toMillis() > 0 && !scaled) {
            assertEquals(0, flinkResourceEventCollector.events.size());
        } else {
            assertEquals(1, flinkResourceEventCollector.events.size());
            newEvent = flinkResourceEventCollector.events.poll();
            assertEquals(event.getMetadata().getUid(), newEvent.getMetadata().getUid());
            assertEquals(2, newEvent.getCount());
        }

        // Parallelism map changes. New recommendation
        Map<JobVertexID, ScalingSummary> scalingSummaries2 =
                Map.of(
                        jobVertexID,
                        new ScalingSummary(
                                1,
                                3,
                                Map.of(
                                        ScalingMetric.TRUE_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.EXPECTED_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.TARGET_DATA_RATE,
                                        evaluatedScalingMetric)));
        eventHandler.handleScalingEvent(ctx, scalingSummaries2, message, interval);

        assertEquals(1, flinkResourceEventCollector.events.size());

        newEvent = flinkResourceEventCollector.events.poll();
        assertEquals(event.getMetadata().getUid(), newEvent.getMetadata().getUid());
        assertEquals(
                interval != null && interval.toMillis() > 0 && !scaled ? 2 : 3,
                newEvent.getCount());

        // Parallelism map doesn't change but metrics changed.
        evaluatedScalingMetric.setCurrent(3);
        Map<JobVertexID, ScalingSummary> scalingSummaries3 =
                Map.of(
                        jobVertexID,
                        new ScalingSummary(
                                1,
                                3,
                                Map.of(
                                        ScalingMetric.TRUE_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.EXPECTED_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.TARGET_DATA_RATE,
                                        evaluatedScalingMetric)));
        eventHandler.handleScalingEvent(ctx, scalingSummaries2, message, interval);

        if (interval != null && interval.toMillis() > 0 && !scaled) {
            assertEquals(0, flinkResourceEventCollector.events.size());
        } else {
            assertEquals(1, flinkResourceEventCollector.events.size());
            newEvent = flinkResourceEventCollector.events.poll();
            assertEquals(4, newEvent.getCount());
        }
    }

    @Test
    public void testSwitchingScalingEnabled() {
        var jobVertexID = JobVertexID.fromHexString("1b51e99e55e89e404d9a0443fd98d9e2");
        var evaluatedScalingMetric = new EvaluatedScalingMetric();
        var interval = Duration.ofSeconds(1800);
        evaluatedScalingMetric.setAverage(1);
        evaluatedScalingMetric.setCurrent(2);
        Map<JobVertexID, ScalingSummary> scalingSummaries1 =
                Map.of(
                        jobVertexID,
                        new ScalingSummary(
                                1,
                                2,
                                Map.of(
                                        ScalingMetric.TRUE_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.EXPECTED_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.TARGET_DATA_RATE,
                                        evaluatedScalingMetric)));

        var enabledMessage = SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
        var disabledMessage =
                SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED
                        + String.format(
                                SCALING_EXECUTION_DISABLED_REASON, SCALING_ENABLED.key(), false);
        eventHandler.handleScalingEvent(ctx, scalingSummaries1, enabledMessage, interval);
        var event = flinkResourceEventCollector.events.poll();
        assertEquals(null, event.getMetadata().getLabels().get(PARALLELISM_MAP_KEY));
        assertEquals(1, event.getCount());

        // Get recommendation event even parallelism map doesn't change and within supression
        // interval
        eventHandler.handleScalingEvent(ctx, scalingSummaries1, disabledMessage, interval);
        assertEquals(1, flinkResourceEventCollector.events.size());
        event = flinkResourceEventCollector.events.poll();
        assertTrue(
                event.getMessage()
                        .contains(
                                String.format(
                                        SCALING_SUMMARY_ENTRY,
                                        jobVertexID,
                                        1,
                                        2,
                                        1.00,
                                        2.00,
                                        1.00)));

        assertEquals("1286380436", event.getMetadata().getLabels().get(PARALLELISM_MAP_KEY));
        assertEquals(2, event.getCount());

        // Get recommendation event even parallelism map doesn't change and within supression
        // interval
        eventHandler.handleScalingEvent(ctx, scalingSummaries1, enabledMessage, interval);
        assertEquals(1, flinkResourceEventCollector.events.size());
        event = flinkResourceEventCollector.events.poll();
        assertTrue(
                event.getMessage()
                        .contains(
                                String.format(
                                        SCALING_SUMMARY_ENTRY,
                                        jobVertexID,
                                        1,
                                        2,
                                        1.00,
                                        2.00,
                                        1.00)));

        assertEquals(null, event.getMetadata().getLabels().get(PARALLELISM_MAP_KEY));
        assertEquals(3, event.getCount());
    }

    @Test
    public void testSwitchingExcludedPeriods() {
        var jobVertexID = JobVertexID.fromHexString("1b51e99e55e89e404d9a0443fd98d9e2");
        var evaluatedScalingMetric = new EvaluatedScalingMetric();
        var interval = Duration.ofSeconds(1800);
        evaluatedScalingMetric.setAverage(1);
        evaluatedScalingMetric.setCurrent(2);
        Map<JobVertexID, ScalingSummary> scalingSummaries1 =
                Map.of(
                        jobVertexID,
                        new ScalingSummary(
                                1,
                                2,
                                Map.of(
                                        ScalingMetric.TRUE_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.EXPECTED_PROCESSING_RATE,
                                        evaluatedScalingMetric,
                                        ScalingMetric.TARGET_DATA_RATE,
                                        evaluatedScalingMetric)));

        var enabledMessage = SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
        var disabledMessage =
                SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED
                        + String.format(
                                SCALING_EXECUTION_DISABLED_REASON,
                                EXCLUDED_PERIODS.key(),
                                "10:00-11:00");
        eventHandler.handleScalingEvent(ctx, scalingSummaries1, enabledMessage, interval);
        var event = flinkResourceEventCollector.events.poll();
        assertNull(event.getMetadata().getLabels().get(PARALLELISM_MAP_KEY));
        assertEquals(1, event.getCount());

        // Get recommendation event even parallelism map doesn't change and within supression
        // interval
        eventHandler.handleScalingEvent(ctx, scalingSummaries1, disabledMessage, interval);
        assertEquals(1, flinkResourceEventCollector.events.size());
        event = flinkResourceEventCollector.events.poll();
        assertTrue(
                event.getMessage()
                        .contains(
                                String.format(
                                        SCALING_SUMMARY_ENTRY,
                                        jobVertexID,
                                        1,
                                        2,
                                        1.00,
                                        2.00,
                                        1.00)));

        assertEquals("1286380436", event.getMetadata().getLabels().get(PARALLELISM_MAP_KEY));
        assertEquals(2, event.getCount());

        // Get recommendation event even parallelism map doesn't change and within supression
        // interval
        eventHandler.handleScalingEvent(ctx, scalingSummaries1, enabledMessage, interval);
        assertEquals(1, flinkResourceEventCollector.events.size());
        event = flinkResourceEventCollector.events.poll();
        assertTrue(
                event.getMessage()
                        .contains(
                                String.format(
                                        SCALING_SUMMARY_ENTRY,
                                        jobVertexID,
                                        1,
                                        2,
                                        1.00,
                                        2.00,
                                        1.00)));

        assertNull(event.getMetadata().getLabels().get(PARALLELISM_MAP_KEY));
        assertEquals(3, event.getCount());
    }
}
