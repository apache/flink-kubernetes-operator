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

package org.apache.flink.kubernetes.operator.bluegreen.client;

import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateContext;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateKubernetesService;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.TRANSITION_STAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link WatermarkGateProcessFunction}. */
public class WatermarkGateProcessFunctionTest {

    private static final String TEST_NAMESPACE = "test-namespace";
    private static final String TEST_CONFIGMAP_NAME = "test-configmap";
    private static final long TEST_WATERMARK_VALUE = 1000L;
    private static final long TEST_TEARDOWN_DELAY = 5000L;

    private TestWatermarkGateProcessFunction watermarkGateFunction;
    private Function<TestMessage, Long> watermarkExtractor;
    private OneInputStreamOperatorTestHarness<TestMessage, TestMessage> testHarness;

    @BeforeEach
    void setUp() throws Exception {
        watermarkExtractor = TestMessage::getTimestamp;
        watermarkGateFunction =
                new TestWatermarkGateProcessFunction(
                        BlueGreenDeploymentType.BLUE,
                        TEST_NAMESPACE,
                        TEST_CONFIGMAP_NAME,
                        watermarkExtractor);

        testHarness = ProcessFunctionTestHarnesses.forProcessFunction(watermarkGateFunction);
        testHarness.open();
    }

    // ==================== Context Update Tests ====================

    @Test
    void testContextInitialization() throws Exception {
        // isFirstDeployment=false so the watermark value is read from data, not forced to 0
        GateContext baseContext = createBaseContext(TransitionStage.RUNNING, false);
        Map<String, String> data =
                createWatermarkData(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);

        watermarkGateFunction.onContextUpdate(baseContext, data);

        assertNotNull(getWatermarkContext());
        assertEquals(TEST_WATERMARK_VALUE, getWatermarkContext().getWatermarkToggleValue());
        assertEquals(
                WatermarkGateStage.WATERMARK_SET, getWatermarkContext().getWatermarkGateStage());
    }

    @Test
    void testContextUpdate() throws Exception {
        // isFirstDeployment=false so the watermark value is read from data, not forced to 0
        GateContext baseContext = createBaseContext(TransitionStage.RUNNING, false);
        Map<String, String> initialData =
                createWatermarkData(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        watermarkGateFunction.onContextUpdate(baseContext, initialData);

        Map<String, String> updatedData =
                createWatermarkData(2000L, WatermarkGateStage.WAITING_FOR_WATERMARK);
        watermarkGateFunction.onContextUpdate(baseContext, updatedData);

        assertEquals(2000L, getWatermarkContext().getWatermarkToggleValue());
        assertEquals(
                WatermarkGateStage.WAITING_FOR_WATERMARK,
                getWatermarkContext().getWatermarkGateStage());
    }

    @Test
    void testConfigMapInformerCallback() throws Exception {
        // Simulates a ConfigMap update arriving via the Kubernetes informer
        Map<String, String> configMapUpdate =
                createWatermarkData(500L, WatermarkGateStage.WATERMARK_SET);

        watermarkGateFunction.simulateConfigMapUpdate(configMapUpdate);

        assertNotNull(getWatermarkContext());
        assertEquals(500L, getWatermarkContext().getWatermarkToggleValue());
        assertEquals(
                WatermarkGateStage.WATERMARK_SET, getWatermarkContext().getWatermarkGateStage());

        // Message ts=600 >= toggle=500 → passes the gate
        TestMessage message = new TestMessage("test", 600L);
        testHarness.processElement(message, 550L);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
    }

    // ==================== Active Processing Tests ====================

    @Test
    void testProcessElementActiveWithWatermarkToggleValueNormal() throws Exception {
        setupActiveContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        // message ts == toggle value → passes (wmToggle <= extractedWatermark)
        TestMessage message = new TestMessage("test", TEST_WATERMARK_VALUE);

        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
    }

    @Test
    void testProcessElementActiveWaitingForWatermark() throws Exception {
        setupActiveContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        // message ts < toggle value → gated, waits for watermark to advance
        TestMessage message = new TestMessage("test", TEST_WATERMARK_VALUE - 100);

        testHarness.processElement(message, TEST_WATERMARK_VALUE - 200);

        assertEquals(0, testHarness.extractOutputValues().size());
        assertTrue(
                watermarkGateFunction.getLogMessages().stream()
                        .anyMatch(
                                msg ->
                                        msg.startsWith(
                                                "Passing the records from the watermark toggle 1000 on")));
    }

    @Test
    void testProcessElementActiveWithNullWatermarkToggleTransitioning() throws Exception {
        // During TRANSITIONING the watermark toggle is not yet set (null); the active deployment
        // should signal that it is waiting for the toggle value to be written to the ConfigMap.
        setupActiveContext(
                null, WatermarkGateStage.WATERMARK_NOT_SET, TransitionStage.TRANSITIONING);
        TestMessage message = new TestMessage("test", TEST_WATERMARK_VALUE);

        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(0, testHarness.extractOutputValues().size());
        assertTrue(watermarkGateFunction.notifyWaitingForWatermarkCalled);
    }

    @Test
    void testProcessElementActiveOpensOnceItsWatermarkPassesToggle() throws Exception {
        setupActiveContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        TestMessage late = new TestMessage("late", TEST_WATERMARK_VALUE - 500);

        // A watermark at the toggle is not past it: the standby may still emit this record
        testHarness.processWatermark(TEST_WATERMARK_VALUE);
        testHarness.processElement(late, late.getTimestamp());
        assertEquals(0, testHarness.extractOutputValues().size());

        // Past the toggle the standby has stopped, so every record is the active's to emit
        testHarness.processWatermark(TEST_WATERMARK_VALUE + 1);
        testHarness.processElement(late, late.getTimestamp());
        assertEquals(List.of(late), testHarness.extractOutputValues());
    }

    @Test
    void testProcessElementActiveIsNotOpenedByProcessingTime() throws Exception {
        setupActiveContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        // No watermark yet and the wall clock is far past the toggle: only record time counts
        testHarness.setProcessingTime(TEST_WATERMARK_VALUE * 10);
        TestMessage late = new TestMessage("late", TEST_WATERMARK_VALUE - 500);

        testHarness.processElement(late, late.getTimestamp());

        assertEquals(0, testHarness.extractOutputValues().size());
    }

    @Test
    void testProcessElementActivePassesEveryRecordWithoutStandby() throws Exception {
        // First deployment, or the deployment kept after an abort: nothing to share the stream with
        watermarkGateFunction.onContextUpdate(
                createBaseContext(TransitionStage.RUNNING, true), new HashMap<>());
        TestMessage noTimestamp = new TestMessage("no-timestamp", Long.MIN_VALUE);

        testHarness.processElement(noTimestamp, 0L);

        assertEquals(List.of(noTimestamp), testHarness.extractOutputValues());
    }

    // ==================== Standby Processing Tests ====================

    @Test
    void testProcessElementStandbyWithinWatermarkBoundary() throws Exception {
        // Standby deployment: message ts < toggle value and no watermark past the toggle yet
        // → still within valid range, element passes through
        setupStandbyContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        TestMessage message = new TestMessage("test", TEST_WATERMARK_VALUE - 100);

        testHarness.processElement(message, TEST_WATERMARK_VALUE - 200);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
    }

    @Test
    void testProcessElementStandbyPastWatermarkToggle() throws Exception {
        // Standby deployment: message ts > toggle value → past the cutoff, element is blocked
        setupStandbyContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        TestMessage message = new TestMessage("test", TEST_WATERMARK_VALUE + 100);

        testHarness.processElement(message, TEST_WATERMARK_VALUE + 50);

        assertEquals(0, testHarness.extractOutputValues().size());
        assertTrue(
                watermarkGateFunction.getLogMessages().stream()
                        .anyMatch(
                                msg ->
                                        msg.startsWith(
                                                "Passing the records older than the watermark toggle 1000")));
    }

    @Test
    void testProcessElementStandbyNullWatermarkToggle() throws Exception {
        // Standby deployment: no toggle value yet → old active job is transitioning to standby,
        // elements pass through and the watermark value is written to the ConfigMap.
        setupStandbyContext(null, WatermarkGateStage.WATERMARK_NOT_SET);
        TestMessage message = new TestMessage("test", TEST_WATERMARK_VALUE);

        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
        assertTrue(watermarkGateFunction.updateWatermarkInConfigMapCalled);
    }

    @Test
    void testProcessElementStandbyIsNotStoppedByProcessingTime() throws Exception {
        setupStandbyContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        // No watermark yet and the wall clock is far past the toggle: only record time stops it
        testHarness.setProcessingTime(TEST_WATERMARK_VALUE * 10);
        TestMessage old = new TestMessage("old", TEST_WATERMARK_VALUE - 100);

        testHarness.processElement(old, old.getTimestamp());

        assertEquals(List.of(old), testHarness.extractOutputValues());
    }

    @Test
    void testStandbyProposesToggleOnlyFromAWatermark() {
        assertNull(
                WatermarkGateProcessFunction.nextWatermarkToggleValue(
                        Long.MIN_VALUE, TEST_TEARDOWN_DELAY));
        assertEquals(
                TEST_WATERMARK_VALUE + TEST_TEARDOWN_DELAY,
                WatermarkGateProcessFunction.nextWatermarkToggleValue(
                        TEST_WATERMARK_VALUE, TEST_TEARDOWN_DELAY));
    }

    @Test
    void testStandbyUsesTheStoredToggleNotItsOwnProposal() throws Exception {
        setupStandbyContext(null, WatermarkGateStage.WAITING_FOR_WATERMARK);
        // Another standby subtask already stored its toggle
        var configMap = new RecordingGateKubernetesService("1500");
        watermarkGateFunction.writeTo(configMap);

        testHarness.processWatermark(900L);
        testHarness.processElement(new TestMessage("a", 800L), 800L); // schedules the write
        testHarness.setProcessingTime(1_000L); // past the write delay
        testHarness.processElement(new TestMessage("b", 850L), 850L); // performs it

        // It proposed its own watermark plus the delay, only if no toggle was stored yet
        assertEquals(
                List.of(
                        Arrays.asList(
                                BlueGreenDeploymentType.BLUE,
                                WatermarkGateContext.WATERMARK_TOGGLE_VALUE,
                                null,
                                Map.of(
                                        WatermarkGateContext.WATERMARK_TOGGLE_VALUE,
                                        Long.toString(900L + TEST_TEARDOWN_DELAY),
                                        WatermarkGateContext.WATERMARK_STAGE,
                                        WatermarkGateStage.WATERMARK_SET.toString()))),
                configMap.calls);
        assertEquals(1500L, getWatermarkContext().getWatermarkToggleValue());
    }

    @Test
    void testStandbySignalsTeardownOnlyFromTransitioning() throws Exception {
        setupStandbyContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        var configMap = new RecordingGateKubernetesService(null);
        watermarkGateFunction.writeTo(configMap);

        testHarness.processWatermark(TEST_WATERMARK_VALUE + 1);
        testHarness.processElement(new TestMessage("a", 0L), 0L); // past the toggle: schedules
        testHarness.setProcessingTime(1_000L);
        testHarness.processElement(new TestMessage("b", 0L), 0L); // performs the write

        assertEquals(
                List.of(
                        Arrays.asList(
                                BlueGreenDeploymentType.BLUE,
                                TRANSITION_STAGE.getLabel(),
                                TransitionStage.TRANSITIONING.toString(),
                                Map.of(
                                        TRANSITION_STAGE.getLabel(),
                                        TransitionStage.CLEAR_TO_TEARDOWN.toString()))),
                configMap.calls);
    }

    @Test
    void testStandbyIsDoneOnceItsWatermarkPassesTheToggle() throws Exception {
        setupStandbyContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        long now = System.currentTimeMillis();
        testHarness.processElement(new TestMessage("old", TEST_WATERMARK_VALUE - 100), 0L);
        assertFalse(watermarkGateFunction.isHandOverDone(now));

        testHarness.processWatermark(TEST_WATERMARK_VALUE + 1);
        testHarness.processElement(new TestMessage("new", TEST_WATERMARK_VALUE + 100), 0L);
        assertTrue(watermarkGateFunction.isHandOverDone(now));
    }

    @Test
    void testStandbyWithoutRecordsIsDoneAfterTheIdleTime() throws Exception {
        setupStandbyContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        long now = System.currentTimeMillis();

        // It never sees its watermark pass the toggle, but it has nothing left to emit either
        assertFalse(watermarkGateFunction.isHandOverDone(now));
        assertTrue(
                watermarkGateFunction.isHandOverDone(
                        now + WatermarkGateProcessFunction.IDLE_DONE_MS + 1_000));
    }

    @Test
    void testANewTransitionResetsTheHandOverProgress() throws Exception {
        setupStandbyContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        testHarness.processWatermark(TEST_WATERMARK_VALUE + 1);
        testHarness.processElement(new TestMessage("new", TEST_WATERMARK_VALUE + 100), 0L);
        assertTrue(watermarkGateFunction.isHandOverDone(System.currentTimeMillis()));

        // The operator rewrites the ConfigMap without a toggle when the next transition starts
        watermarkGateFunction.onContextUpdate(
                createBaseContext(TransitionStage.INITIALIZING, false), new HashMap<>());
        assertFalse(
                watermarkGateFunction.isHandOverDone(
                        System.currentTimeMillis() + WatermarkGateProcessFunction.IDLE_DONE_MS));
    }

    // ==================== Watermark Control Tests ====================

    @Test
    void testWatermarkProgression() throws Exception {
        setupActiveContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        TestMessage earlyMessage = new TestMessage("early", TEST_WATERMARK_VALUE - 200);
        TestMessage lateMessage = new TestMessage("late", TEST_WATERMARK_VALUE + 100);

        // Early message ts < toggle value → gated
        testHarness.processElement(earlyMessage, TEST_WATERMARK_VALUE - 300);
        assertEquals(0, testHarness.extractOutputValues().size());

        // Advance the watermark to just below the toggle value: the gate stays closed
        testHarness.processWatermark(TEST_WATERMARK_VALUE - 50);

        // Late message ts > toggle value → passes
        testHarness.processElement(lateMessage, TEST_WATERMARK_VALUE + 50);
        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(lateMessage, testHarness.extractOutputValues().get(0));
    }

    // ==================== Hand-over Tests ====================

    @Test
    void testHandOverEmitsEveryRecordExactlyOnce() throws Exception {
        // Both deployments read the same records, so each record reaches both gates. In this
        // test's ConfigMap BLUE is the active (incoming) deployment and GREEN the standby.
        OneInputStreamOperatorTestHarness<TestMessage, TestMessage> outgoing =
                handOverGate(BlueGreenDeploymentType.GREEN);
        OneInputStreamOperatorTestHarness<TestMessage, TestMessage> incoming =
                handOverGate(BlueGreenDeploymentType.BLUE);
        TestMessage old = new TestMessage("old", TEST_WATERMARK_VALUE - 50);
        TestMessage fresh = new TestMessage("new", TEST_WATERMARK_VALUE + 50);
        TestMessage late = new TestMessage("late", TEST_WATERMARK_VALUE - 500);
        TestMessage newer = new TestMessage("newer", TEST_WATERMARK_VALUE + 100);

        // Before the watermarks pass the toggle, the record timestamp decides
        feed(TEST_WATERMARK_VALUE - 100, List.of(old, fresh), outgoing, incoming);
        // After they pass it, the standby stops and the active emits everything, late or not
        feed(TEST_WATERMARK_VALUE + 1, List.of(late, newer), outgoing, incoming);

        assertEquals(List.of(old), outgoing.extractOutputValues());
        assertEquals(List.of(fresh, late, newer), incoming.extractOutputValues());
        outgoing.close();
        incoming.close();
    }

    @Test
    void testHandOverJudgesRecordsByTheExtractorOnly() throws Exception {
        // Each record's Flink timestamp falls on the other side of the toggle than the timestamp
        // the extractor returns. The extractor is the record's time, so both gates follow it.
        OneInputStreamOperatorTestHarness<TestMessage, TestMessage> outgoing =
                handOverGate(BlueGreenDeploymentType.GREEN);
        OneInputStreamOperatorTestHarness<TestMessage, TestMessage> incoming =
                handOverGate(BlueGreenDeploymentType.BLUE);
        TestMessage old = new TestMessage("old", TEST_WATERMARK_VALUE - 50);
        TestMessage fresh = new TestMessage("new", TEST_WATERMARK_VALUE + 50);

        for (OneInputStreamOperatorTestHarness<TestMessage, TestMessage> gate :
                List.of(outgoing, incoming)) {
            gate.processWatermark(TEST_WATERMARK_VALUE - 100);
            gate.processElement(old, TEST_WATERMARK_VALUE + 50);
            gate.processElement(fresh, TEST_WATERMARK_VALUE - 50);
        }

        assertEquals(List.of(old), outgoing.extractOutputValues());
        assertEquals(List.of(fresh), incoming.extractOutputValues());
        outgoing.close();
        incoming.close();
    }

    @Test
    void testHandOverEmitsRecordsWithoutTimestampOnce() throws Exception {
        // The extractor finds no timestamp on these records: they count as older than the toggle
        Function<TestMessage, Long> noTimestamp = message -> null;
        OneInputStreamOperatorTestHarness<TestMessage, TestMessage> outgoing =
                handOverGate(BlueGreenDeploymentType.GREEN, noTimestamp);
        OneInputStreamOperatorTestHarness<TestMessage, TestMessage> incoming =
                handOverGate(BlueGreenDeploymentType.BLUE, noTimestamp);
        TestMessage before = new TestMessage("before", 0L);
        TestMessage after = new TestMessage("after", 0L);

        feed(TEST_WATERMARK_VALUE - 100, List.of(before), outgoing, incoming);
        feed(TEST_WATERMARK_VALUE + 1, List.of(after), outgoing, incoming);

        assertEquals(List.of(before), outgoing.extractOutputValues());
        assertEquals(List.of(after), incoming.extractOutputValues());
        outgoing.close();
        incoming.close();
    }

    // ==================== Factory Method Tests ====================

    @Test
    void testCreateFromFlinkConfig() {
        Map<String, String> flinkConfig = new HashMap<>();
        flinkConfig.put("bluegreen.active-deployment-type", "BLUE");
        flinkConfig.put("kubernetes.namespace", "test-ns");
        flinkConfig.put("bluegreen.configmap.name", "test-cm");

        WatermarkGateProcessFunction<String> function =
                WatermarkGateProcessFunction.create(flinkConfig, s -> (long) s.length());

        assertNotNull(function);
    }

    @Test
    void testActiveLogsWhatItDoesOncePerChange() throws Exception {
        setupActiveContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        watermarkGateFunction.getLogMessages().clear();

        // Records on both sides of the toggle, which this subtask passes or holds back
        testHarness.processWatermark(TEST_WATERMARK_VALUE - 500);
        for (int i = 0; i < 100; i++) {
            long timestamp = TEST_WATERMARK_VALUE + (i % 2 == 0 ? -50 : 50);
            testHarness.processElement(new TestMessage("record", timestamp), timestamp);
        }
        testHarness.processWatermark(TEST_WATERMARK_VALUE + 1);
        for (int i = 0; i < 100; i++) {
            testHarness.processElement(new TestMessage("record", TEST_WATERMARK_VALUE - 50), 0);
        }

        assertEquals(150, testHarness.extractOutputValues().size());
        assertEquals(
                List.of(
                        "Passing the records from the watermark toggle 1000 on, until this"
                                + " subtask's watermark passes it",
                        "Passing every record, this subtask's watermark passed the watermark"
                                + " toggle 1000"),
                watermarkGateFunction.getLogMessages());
    }

    @Test
    void testStandbyLogsWhatItDoesOncePerChange() throws Exception {
        setupStandbyContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        watermarkGateFunction.getLogMessages().clear();

        testHarness.processWatermark(TEST_WATERMARK_VALUE - 500);
        for (int i = 0; i < 100; i++) {
            long timestamp = TEST_WATERMARK_VALUE + (i % 2 == 0 ? -50 : 50);
            testHarness.processElement(new TestMessage("record", timestamp), timestamp);
        }
        testHarness.processWatermark(TEST_WATERMARK_VALUE + 1);
        for (int i = 0; i < 100; i++) {
            testHarness.processElement(new TestMessage("record", TEST_WATERMARK_VALUE - 50), 0);
        }

        assertEquals(50, testHarness.extractOutputValues().size());
        assertEquals(
                List.of(
                        "Passing the records older than the watermark toggle 1000, until this"
                                + " subtask's watermark passes it",
                        "Holding back every record, this subtask's watermark passed the"
                                + " watermark toggle 1000"),
                watermarkGateFunction.getLogMessages());
    }

    // ==================== Helper Methods ====================

    private void setupActiveContext(Long watermarkToggleValue, WatermarkGateStage stage)
            throws Exception {
        setupActiveContext(watermarkToggleValue, stage, TransitionStage.RUNNING);
    }

    private void setupActiveContext(
            Long watermarkToggleValue, WatermarkGateStage stage, TransitionStage gateStage)
            throws Exception {
        GateContext baseContext = createBaseContext(gateStage, false);
        Map<String, String> data = createWatermarkData(watermarkToggleValue, stage);
        watermarkGateFunction.onContextUpdate(baseContext, data);
    }

    private void setupStandbyContext(Long watermarkToggleValue, WatermarkGateStage stage)
            throws Exception {
        // Recreate function as GREEN (standby) deployment
        watermarkGateFunction =
                new TestWatermarkGateProcessFunction(
                        BlueGreenDeploymentType.GREEN,
                        TEST_NAMESPACE,
                        TEST_CONFIGMAP_NAME,
                        watermarkExtractor);

        testHarness.close();
        testHarness = ProcessFunctionTestHarnesses.forProcessFunction(watermarkGateFunction);
        testHarness.open();

        GateContext baseContext = createBaseContext(TransitionStage.RUNNING, false);
        Map<String, String> data = createWatermarkData(watermarkToggleValue, stage);
        watermarkGateFunction.onContextUpdate(baseContext, data);
    }

    private OneInputStreamOperatorTestHarness<TestMessage, TestMessage> handOverGate(
            BlueGreenDeploymentType deploymentType) throws Exception {
        return handOverGate(deploymentType, watermarkExtractor);
    }

    private OneInputStreamOperatorTestHarness<TestMessage, TestMessage> handOverGate(
            BlueGreenDeploymentType deploymentType, Function<TestMessage, Long> extractor)
            throws Exception {
        TestWatermarkGateProcessFunction function =
                new TestWatermarkGateProcessFunction(
                        deploymentType, TEST_NAMESPACE, TEST_CONFIGMAP_NAME, extractor);
        OneInputStreamOperatorTestHarness<TestMessage, TestMessage> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(function);
        harness.open();
        function.onContextUpdate(
                createBaseContext(TransitionStage.TRANSITIONING, false),
                createWatermarkData(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET));
        return harness;
    }

    @SafeVarargs
    private static void feed(
            long watermark,
            List<TestMessage> records,
            OneInputStreamOperatorTestHarness<TestMessage, TestMessage>... gates)
            throws Exception {
        for (OneInputStreamOperatorTestHarness<TestMessage, TestMessage> gate : gates) {
            gate.processWatermark(watermark);
            for (TestMessage record : records) {
                gate.processElement(record, record.getTimestamp());
            }
        }
    }

    private GateContext createBaseContext(TransitionStage gateStage, boolean isFirstDeployment) {
        Map<String, String> data = new HashMap<>();
        data.put(
                GateContextOptions.ACTIVE_DEPLOYMENT_TYPE.getLabel(),
                BlueGreenDeploymentType.BLUE.toString());
        data.put(GateContextOptions.TRANSITION_STAGE.getLabel(), gateStage.toString());
        data.put(
                GateContextOptions.DEPLOYMENT_DELETION_DELAY.getLabel(),
                String.valueOf(TEST_TEARDOWN_DELAY));
        data.put(
                GateContextOptions.IS_FIRST_DEPLOYMENT.getLabel(),
                String.valueOf(isFirstDeployment));

        return GateContext.create(data, BlueGreenDeploymentType.BLUE);
    }

    private Map<String, String> createWatermarkData(
            Long watermarkToggleValue, WatermarkGateStage stage) {
        Map<String, String> data = new HashMap<>();
        if (watermarkToggleValue != null) {
            data.put(WatermarkGateContext.WATERMARK_TOGGLE_VALUE, watermarkToggleValue.toString());
        }
        data.put(WatermarkGateContext.WATERMARK_STAGE, stage.toString());
        return data;
    }

    private WatermarkGateContext getWatermarkContext() {
        return watermarkGateFunction.getCurrentWatermarkGateContext();
    }

    // ==================== Test Helper Classes ====================

    private static class TestMessage {
        private final String content;
        private final long timestamp;

        public TestMessage(String content, long timestamp) {
            this.content = content;
            this.timestamp = timestamp;
        }

        public String getContent() {
            return content;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestMessage that = (TestMessage) o;
            return timestamp == that.timestamp && content.equals(that.content);
        }

        @Override
        public int hashCode() {
            return content.hashCode() + (int) timestamp;
        }

        @Override
        public String toString() {
            return "TestMessage{content='" + content + "', timestamp=" + timestamp + '}';
        }
    }

    /** Records each compare-and-set and answers with the value already stored, if any. */
    private static class RecordingGateKubernetesService extends GateKubernetesService {
        private final List<List<Object>> calls = new ArrayList<>();
        private final String stored;

        RecordingGateKubernetesService(String stored) {
            super(null, TEST_NAMESPACE, TEST_CONFIGMAP_NAME);
            this.stored = stored;
        }

        @Override
        public Optional<String> compareAndSet(
                BlueGreenDeploymentType activeDeploymentType,
                String key,
                String expected,
                Map<String, String> entries) {
            calls.add(Arrays.asList(activeDeploymentType, key, expected, entries));
            return Optional.of(stored != null ? stored : entries.get(key));
        }
    }

    /** Test implementation of WatermarkGateProcessFunction that captures method calls and state. */
    private static class TestWatermarkGateProcessFunction
            extends WatermarkGateProcessFunction<TestMessage> {
        private final List<String> logMessages = new ArrayList<>();
        private GateContext mockBaseContext;

        public boolean notifyWaitingForWatermarkCalled = false;
        public boolean updateWatermarkInConfigMapCalled = false;

        TestWatermarkGateProcessFunction(
                BlueGreenDeploymentType blueGreenDeploymentType,
                String namespace,
                String configMapName,
                Function<TestMessage, Long> watermarkExtractor) {
            super(blueGreenDeploymentType, namespace, configMapName, watermarkExtractor);
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext openContext)
                throws Exception {
            // Build a mock ConfigMap context representing a non-first deployment in RUNNING state.
            // isFirstDeployment=false ensures watermark values are read from data rather than
            // defaulted to 0.
            Map<String, String> mockConfigMapData = new HashMap<>();
            mockConfigMapData.put(
                    GateContextOptions.ACTIVE_DEPLOYMENT_TYPE.getLabel(),
                    BlueGreenDeploymentType.BLUE.toString());
            mockConfigMapData.put(
                    GateContextOptions.TRANSITION_STAGE.getLabel(),
                    TransitionStage.RUNNING.toString());
            mockConfigMapData.put(GateContextOptions.DEPLOYMENT_DELETION_DELAY.getLabel(), "5000");
            mockConfigMapData.put(GateContextOptions.IS_FIRST_DEPLOYMENT.getLabel(), "false");

            // Use the actual deployment type so GREEN functions get STANDBY output mode
            mockBaseContext = GateContext.create(mockConfigMapData, blueGreenDeploymentType);

            // Set baseContext so processElement() can dispatch to active/standby
            baseContext = mockBaseContext;

            // Simulate the parent's initialization by calling onContextUpdate
            onContextUpdate(mockBaseContext, new HashMap<>());

            // Skip the actual Kubernetes service initialization but keep the important logic
            logInfo(
                    "Mock initialization completed - skipping Kubernetes service setup for testing");
        }

        @Override
        protected void logInfo(String message) {
            logMessages.add(message);
        }

        /** Override to capture the call without invoking Kubernetes. */
        @Override
        protected void notifyWaitingForWatermark(Context ctx) {
            notifyWaitingForWatermarkCalled = true;
            if (writesTo != null) {
                super.notifyWaitingForWatermark(ctx);
            }
        }

        /** Override to capture the call without invoking Kubernetes. */
        @Override
        protected void updateWatermarkInConfigMap(Context ctx) {
            updateWatermarkInConfigMapCalled = true;
            if (writesTo != null) {
                super.updateWatermarkInConfigMap(ctx);
            }
        }

        private RecordingGateKubernetesService writesTo;

        /** Performs the scheduled ConfigMap writes, against a recording service. */
        void writeTo(RecordingGateKubernetesService service) throws Exception {
            writesTo = service;
            java.lang.reflect.Field field =
                    GateProcessFunction.class.getDeclaredField("gateKubernetesService");
            field.setAccessible(true);
            field.set(this, service);
        }

        // Helper to access the private currentWatermarkGateContext field for assertions
        public WatermarkGateContext getCurrentWatermarkGateContext() {
            try {
                java.lang.reflect.Field field =
                        WatermarkGateProcessFunction.class.getDeclaredField(
                                "currentWatermarkGateContext");
                field.setAccessible(true);
                return (WatermarkGateContext) field.get(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Method to simulate ConfigMap updates from tests (mirrors the informer callback path)
        public void simulateConfigMapUpdate(Map<String, String> newData) {
            if (mockBaseContext != null) {
                onContextUpdate(mockBaseContext, newData);
            }
        }

        public List<String> getLogMessages() {
            return logMessages;
        }
    }
}
