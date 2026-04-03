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
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

        // Processing time=0, watermark boundary=0 <= toggle=500; message ts=600 > 500
        // → message passes the gate
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
                        .anyMatch(msg -> msg.contains("Waiting to Reach WM")));
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

    // ==================== Standby Processing Tests ====================

    @Test
    void testProcessElementStandbyWithinWatermarkBoundary() throws Exception {
        // Standby deployment: message ts < toggle value and processing time <= toggle value
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
                        .anyMatch(msg -> msg.contains("Past WM")));
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

    // ==================== Watermark Control Tests ====================

    @Test
    void testWatermarkProgression() throws Exception {
        setupActiveContext(TEST_WATERMARK_VALUE, WatermarkGateStage.WATERMARK_SET);
        TestMessage earlyMessage = new TestMessage("early", TEST_WATERMARK_VALUE - 200);
        TestMessage lateMessage = new TestMessage("late", TEST_WATERMARK_VALUE + 100);

        // Early message ts < toggle value → gated
        testHarness.processElement(earlyMessage, TEST_WATERMARK_VALUE - 300);
        assertEquals(0, testHarness.extractOutputValues().size());

        // Advance watermark; does not affect the toggle-value gate directly, but verifies
        // the harness progresses without errors
        testHarness.processWatermark(TEST_WATERMARK_VALUE - 50);

        // Late message ts > toggle value → passes
        testHarness.processElement(lateMessage, TEST_WATERMARK_VALUE + 50);
        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(lateMessage, testHarness.extractOutputValues().get(0));
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
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
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
        }

        /** Override to capture the call without invoking Kubernetes. */
        @Override
        protected void updateWatermarkInConfigMap(Context ctx) {
            updateWatermarkInConfigMapCalled = true;
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
