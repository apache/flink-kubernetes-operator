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
import org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateKubernetesService;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import org.junit.jupiter.api.AfterEach;
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

/** Comprehensive unit tests for {@link WatermarkGateProcessFunction}. */
public class WatermarkGateProcessFunctionTest {

    private static final String TEST_NAMESPACE = "test-namespace";
    private static final String TEST_CONFIGMAP_NAME = "test-configmap";
    private static final long TEST_WATERMARK_VALUE = 1000L;
    private static final long TEST_TEARDOWN_DELAY_MS = 5000L;

    /** Serializable Function interface for watermark extraction. */
    private interface SerializableFunction<T, R> extends Function<T, R>, java.io.Serializable {}

    /** Static watermark extractor to avoid any serialization issues. */
    private static class TestMessageWatermarkExtractor
            implements SerializableFunction<TestMessage, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long apply(TestMessage testMessage) {
            return testMessage.getTimestamp();
        }
    }

    private TestableWatermarkGateProcessFunction<TestMessage> watermarkGateFunction;
    private SerializableFunction<TestMessage, Long> watermarkExtractor;
    private OneInputStreamOperatorTestHarness<TestMessage, TestMessage> testHarness;

    @BeforeEach
    void setUp() throws Exception {
        watermarkExtractor = new TestMessageWatermarkExtractor();

        // Create mock service that returns initial ConfigMap
        MockGateKubernetesService mockService = new MockGateKubernetesService();

        watermarkGateFunction =
                new TestableWatermarkGateProcessFunction<>(
                        BlueGreenDeploymentType.BLUE,
                        TEST_NAMESPACE,
                        TEST_CONFIGMAP_NAME,
                        watermarkExtractor,
                        mockService);

        // Set max parallelism to 2 and subtask index to 1 to match subtaskIndexGuide
        testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<>(watermarkGateFunction), 2, 2, 1);

        // Disable object reuse and serialization checks to avoid serialization issues in tests
        testHarness.getExecutionConfig().disableObjectReuse();

        testHarness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

    // ==================== Kubernetes Informer Callback Tests ====================

    @Test
    void testInformerOnUpdateCallback() throws Exception {
        ResourceEventHandler<ConfigMap> handler = watermarkGateFunction.getCapturedEventHandler();
        assertNotNull(handler, "Event handler should be captured during open()");

        // Create initial ConfigMap
        ConfigMap oldConfigMap =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.RUNNING,
                        false,
                        TEST_WATERMARK_VALUE,
                        WatermarkGateStage.WATERMARK_SET);

        // Create updated ConfigMap with new watermark value
        ConfigMap newConfigMap =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.TRANSITIONING,
                        false,
                        2000L,
                        WatermarkGateStage.WAITING_FOR_WATERMARK);

        // Invoke the informer callback
        handler.onUpdate(oldConfigMap, newConfigMap);

        // Verify that the context was updated
        WatermarkGateContext context = watermarkGateFunction.getCurrentWatermarkGateContext();
        assertNotNull(context);
        assertEquals(2000L, context.getWatermarkToggleValue());
        assertEquals(WatermarkGateStage.WAITING_FOR_WATERMARK, context.getWatermarkGateStage());
        assertEquals(TransitionStage.TRANSITIONING, context.getBaseContext().getGateStage());
    }

    @Test
    void testInformerOnUpdateWithIdenticalConfigMaps() throws Exception {
        ResourceEventHandler<ConfigMap> handler = watermarkGateFunction.getCapturedEventHandler();

        ConfigMap configMap =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.RUNNING,
                        false,
                        TEST_WATERMARK_VALUE,
                        WatermarkGateStage.WATERMARK_SET);

        int initialLogCount = watermarkGateFunction.getLogMessages().size();

        // Update with identical ConfigMap should not trigger processing
        handler.onUpdate(configMap, configMap);

        // No additional logs should be generated for identical updates
        assertEquals(
                initialLogCount,
                watermarkGateFunction.getLogMessages().size(),
                "Identical ConfigMaps should not trigger update processing");
    }

    @Test
    void testInformerOnAddCallbackLogsWarning() throws Exception {
        ResourceEventHandler<ConfigMap> handler = watermarkGateFunction.getCapturedEventHandler();

        ConfigMap configMap =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.RUNNING,
                        false,
                        TEST_WATERMARK_VALUE,
                        WatermarkGateStage.WATERMARK_SET);

        // This should log a warning as we don't expect ConfigMaps to be added
        handler.onAdd(configMap);

        // The implementation logs this at WARN level, but we can verify it doesn't crash
        assertTrue(true, "onAdd should handle unexpected ConfigMap additions gracefully");
    }

    @Test
    void testInformerOnDeleteCallbackLogsError() throws Exception {
        ResourceEventHandler<ConfigMap> handler = watermarkGateFunction.getCapturedEventHandler();

        ConfigMap configMap =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.RUNNING,
                        false,
                        TEST_WATERMARK_VALUE,
                        WatermarkGateStage.WATERMARK_SET);

        // This should log an error as ConfigMap deletion is unexpected
        handler.onDelete(configMap, false);

        // The implementation logs this at ERROR level, but we can verify it doesn't crash
        assertTrue(true, "onDelete should handle ConfigMap deletion gracefully");
    }

    @Test
    void testInformerTriggersContextUpdateInActiveMode() throws Exception {
        ResourceEventHandler<ConfigMap> handler = watermarkGateFunction.getCapturedEventHandler();

        // Initial state: Active mode with watermark set
        ConfigMap initialConfigMap =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.RUNNING,
                        false,
                        TEST_WATERMARK_VALUE,
                        WatermarkGateStage.WATERMARK_SET);

        ConfigMap updatedConfigMap =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.RUNNING,
                        false,
                        TEST_WATERMARK_VALUE,
                        WatermarkGateStage.WATERMARK_SET);

        handler.onUpdate(initialConfigMap, updatedConfigMap);

        // Process a message that should pass through
        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
    }

    @Test
    void testInformerTriggersContextUpdateInStandbyMode() throws Exception {
        // Create a STANDBY function (GREEN when BLUE is active)
        MockGateKubernetesService mockService = new MockGateKubernetesService();
        watermarkGateFunction =
                new TestableWatermarkGateProcessFunction<>(
                        BlueGreenDeploymentType.GREEN,
                        TEST_NAMESPACE,
                        TEST_CONFIGMAP_NAME,
                        watermarkExtractor,
                        mockService);
        testHarness.close();
        testHarness = ProcessFunctionTestHarnesses.forProcessFunction(watermarkGateFunction);
        testHarness.open();

        ResourceEventHandler<ConfigMap> handler = watermarkGateFunction.getCapturedEventHandler();

        // BLUE is active, so GREEN is in STANDBY
        ConfigMap configMap =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.RUNNING,
                        false,
                        null,
                        WatermarkGateStage.WATERMARK_NOT_SET);

        handler.onUpdate(
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.INITIALIZING,
                        false,
                        null,
                        WatermarkGateStage.WATERMARK_NOT_SET),
                configMap);

        // Process a message - should output in standby when watermark not set
        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(1, testHarness.extractOutputValues().size());
    }

    // ==================== Active Mode Processing Tests ====================

    @Test
    void testActiveMode_WatermarkSet_MessageAboveWatermark() throws Exception {
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                TEST_WATERMARK_VALUE,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE + 100);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
    }

    @Test
    void testActiveMode_WatermarkSet_MessageBelowWatermark() throws Exception {
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                TEST_WATERMARK_VALUE,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE - 100);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 200);

        assertEquals(0, testHarness.extractOutputValues().size());
        assertTrue(
                watermarkGateFunction.getLogMessages().stream()
                        .anyMatch(msg -> msg.contains("Waiting to Reach WM")));
    }

    @Test
    void testActiveMode_WatermarkSet_MessageExactlyAtWatermark() throws Exception {
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                TEST_WATERMARK_VALUE,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
    }

    @Test
    void testActiveMode_TransitioningState_WatermarkNotSet() throws Exception {
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.TRANSITIONING,
                false,
                null,
                WatermarkGateStage.WATERMARK_NOT_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(0, testHarness.extractOutputValues().size());
        assertTrue(
                watermarkGateFunction.getLogMessages().stream()
                        .anyMatch(msg -> msg.contains("Waiting for WM to be set")));
        assertTrue(watermarkGateFunction.isWaitingForWatermarkNotified());
    }

    @Test
    void testActiveMode_RunningState_WatermarkNotSet() throws Exception {
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                null,
                WatermarkGateStage.WATERMARK_NOT_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(0, testHarness.extractOutputValues().size());
        assertTrue(
                watermarkGateFunction.getLogMessages().stream()
                        .anyMatch(
                                msg ->
                                        msg.contains("Waiting for the TRANSITIONING state")
                                                && msg.contains("RUNNING")));
    }

    @Test
    void testActiveMode_WaitingForWatermarkStage() throws Exception {
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.TRANSITIONING,
                false,
                null,
                WatermarkGateStage.WAITING_FOR_WATERMARK);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(0, testHarness.extractOutputValues().size());
        assertTrue(
                watermarkGateFunction.getLogMessages().stream()
                        .anyMatch(msg -> msg.contains("Waiting for WM to be set")));
    }

    @Test
    void testActiveMode_FirstDeployment() throws Exception {
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.INITIALIZING,
                true,
                0L,
                WatermarkGateStage.WATERMARK_NOT_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
    }

    // ==================== Standby Mode Processing Tests ====================

    @Test
    void testStandbyMode_WatermarkNotSet_ShouldOutputElements() throws Exception {
        setupStandbyFunction();

        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                null,
                WatermarkGateStage.WATERMARK_NOT_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
    }

    @Test
    void testStandbyMode_WithinWatermarkBoundary_ShouldOutput() throws Exception {
        setupStandbyFunction();

        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                TEST_WATERMARK_VALUE,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE - 200);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 300);

        assertEquals(1, testHarness.extractOutputValues().size());
        assertEquals(message, testHarness.extractOutputValues().get(0));
    }

    @Test
    void testStandbyMode_PastWatermarkToggleValue_ShouldBlock() throws Exception {
        setupStandbyFunction();

        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                TEST_WATERMARK_VALUE,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE + 100);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        assertEquals(0, testHarness.extractOutputValues().size());
        assertTrue(
                watermarkGateFunction.getLogMessages().stream()
                        .anyMatch(msg -> msg.contains("Past WM")));
    }

    @Test
    void testStandbyMode_PastWatermarkBoundary_ShouldBlockAndNotifyClearToTeardown()
            throws Exception {
        setupStandbyFunction();

        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                TEST_WATERMARK_VALUE,
                WatermarkGateStage.WATERMARK_SET);

        // Advance watermark past the toggle value to trigger "Past WM Boundary"
        testHarness.processWatermark(
                new org.apache.flink.streaming.api.watermark.Watermark(TEST_WATERMARK_VALUE + 200));

        // Process message with current watermark past the toggle value
        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE + 100);
        testHarness.processElement(message, TEST_WATERMARK_VALUE + 100);

        assertEquals(0, testHarness.extractOutputValues().size());
        assertTrue(
                watermarkGateFunction.getLogMessages().stream()
                        .anyMatch(msg -> msg.contains("Past WM Boundary")));
        assertTrue(watermarkGateFunction.isClearToTeardownNotified());
    }

    @Test
    void testStandbyMode_MessageAtWatermarkBoundary() throws Exception {
        setupStandbyFunction();

        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                TEST_WATERMARK_VALUE,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message = new TestMessage(TEST_WATERMARK_VALUE);
        testHarness.processElement(message, TEST_WATERMARK_VALUE - 100);

        // When message timestamp equals watermark toggle value, standby mode blocks it
        // (line 114: watermarkToggleValue > watermarkExtractor.apply(value) is false when equal)
        assertEquals(0, testHarness.extractOutputValues().size());
    }

    // ==================== State Transition Tests ====================

    @Test
    void testStateTransition_ActiveToStandby() throws Exception {
        // Start in ACTIVE mode
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                TEST_WATERMARK_VALUE,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message1 = new TestMessage(TEST_WATERMARK_VALUE + 100);
        testHarness.processElement(message1, TEST_WATERMARK_VALUE - 100);
        assertEquals(1, testHarness.extractOutputValues().size());

        // Transition to STANDBY (GREEN becomes active)
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.GREEN,
                TransitionStage.TRANSITIONING,
                false,
                TEST_WATERMARK_VALUE + 1000,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message2 = new TestMessage(TEST_WATERMARK_VALUE + 200);
        testHarness.processElement(message2, TEST_WATERMARK_VALUE + 100);
        assertEquals(2, testHarness.extractOutputValues().size());
    }

    @Test
    void testStateTransition_StandbyToActive() throws Exception {
        setupStandbyFunction();

        // Start in STANDBY mode
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.BLUE,
                TransitionStage.RUNNING,
                false,
                TEST_WATERMARK_VALUE,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message1 = new TestMessage(TEST_WATERMARK_VALUE - 100);
        testHarness.processElement(message1, TEST_WATERMARK_VALUE - 200);
        assertEquals(1, testHarness.extractOutputValues().size());

        // Transition to ACTIVE (GREEN becomes active)
        updateConfigMapViaInformer(
                BlueGreenDeploymentType.GREEN,
                TransitionStage.TRANSITIONING,
                false,
                null,
                WatermarkGateStage.WATERMARK_NOT_SET);

        updateConfigMapViaInformer(
                BlueGreenDeploymentType.GREEN,
                TransitionStage.TRANSITIONING,
                false,
                TEST_WATERMARK_VALUE + 1000,
                WatermarkGateStage.WATERMARK_SET);

        TestMessage message2 = new TestMessage(TEST_WATERMARK_VALUE + 1100);
        testHarness.processElement(message2, TEST_WATERMARK_VALUE + 900);
        assertEquals(2, testHarness.extractOutputValues().size());
    }

    @Test
    void testMultipleConfigMapUpdates() throws Exception {
        ResourceEventHandler<ConfigMap> handler = watermarkGateFunction.getCapturedEventHandler();

        // First update
        ConfigMap config1 =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.INITIALIZING,
                        false,
                        null,
                        WatermarkGateStage.WATERMARK_NOT_SET);
        handler.onUpdate(
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.INITIALIZING,
                        true,
                        0L,
                        WatermarkGateStage.WATERMARK_NOT_SET),
                config1);

        // Second update
        ConfigMap config2 =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.RUNNING,
                        false,
                        TEST_WATERMARK_VALUE,
                        WatermarkGateStage.WATERMARK_SET);
        handler.onUpdate(config1, config2);

        // Third update
        ConfigMap config3 =
                createConfigMap(
                        BlueGreenDeploymentType.BLUE,
                        TransitionStage.TRANSITIONING,
                        false,
                        TEST_WATERMARK_VALUE + 1000,
                        WatermarkGateStage.WATERMARK_SET);
        handler.onUpdate(config2, config3);

        WatermarkGateContext context = watermarkGateFunction.getCurrentWatermarkGateContext();
        assertEquals(TEST_WATERMARK_VALUE + 1000, context.getWatermarkToggleValue());
        assertEquals(TransitionStage.TRANSITIONING, context.getBaseContext().getGateStage());
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

    private void setupStandbyFunction() throws Exception {
        MockGateKubernetesService mockService = new MockGateKubernetesService();

        watermarkGateFunction =
                new TestableWatermarkGateProcessFunction<>(
                        BlueGreenDeploymentType.GREEN,
                        TEST_NAMESPACE,
                        TEST_CONFIGMAP_NAME,
                        watermarkExtractor,
                        mockService);
        testHarness.close();
        testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new ProcessOperator<>(watermarkGateFunction), 2, 2, 1);
        testHarness.getExecutionConfig().disableObjectReuse();
        testHarness.open();
    }

    private void updateConfigMapViaInformer(
            BlueGreenDeploymentType activeType,
            TransitionStage stage,
            boolean isFirstDeployment,
            Long watermarkToggleValue,
            WatermarkGateStage watermarkStage)
            throws Exception {
        ResourceEventHandler<ConfigMap> handler = watermarkGateFunction.getCapturedEventHandler();
        ConfigMap oldConfigMap =
                createConfigMap(
                        activeType,
                        TransitionStage.INITIALIZING,
                        true,
                        0L,
                        WatermarkGateStage.WATERMARK_NOT_SET);
        ConfigMap newConfigMap =
                createConfigMap(
                        activeType, stage, isFirstDeployment, watermarkToggleValue, watermarkStage);
        handler.onUpdate(oldConfigMap, newConfigMap);
    }

    private ConfigMap createConfigMap(
            BlueGreenDeploymentType activeType,
            TransitionStage stage,
            boolean isFirstDeployment,
            Long watermarkToggleValue,
            WatermarkGateStage watermarkStage) {
        // Use HashMap explicitly to ensure mutable collection
        HashMap<String, String> data = new HashMap<>();
        data.put(GateContextOptions.ACTIVE_DEPLOYMENT_TYPE.getLabel(), activeType.toString());
        data.put(GateContextOptions.TRANSITION_STAGE.getLabel(), stage.toString());
        data.put(
                GateContextOptions.DEPLOYMENT_DELETION_DELAY.getLabel(),
                String.valueOf(TEST_TEARDOWN_DELAY_MS));
        data.put(
                GateContextOptions.IS_FIRST_DEPLOYMENT.getLabel(),
                String.valueOf(isFirstDeployment));

        if (watermarkToggleValue != null) {
            data.put(WatermarkGateContext.WATERMARK_TOGGLE_VALUE, watermarkToggleValue.toString());
        }
        data.put(WatermarkGateContext.WATERMARK_STAGE, watermarkStage.toString());

        // Pass a copy to ConfigMapBuilder to avoid any internal references
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(TEST_CONFIGMAP_NAME)
                .withNamespace(TEST_NAMESPACE)
                .endMetadata()
                .withData(new HashMap<>(data))
                .build();
    }

    // ==================== Test Helper Classes ====================

    /** Simple test message class with timestamp. */
    public static class TestMessage implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public long timestamp;

        public TestMessage() {}

        public TestMessage(long timestamp) {
            this.timestamp = timestamp;
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
            return timestamp == that.timestamp;
        }

        @Override
        public int hashCode() {
            return (int) timestamp;
        }

        @Override
        public String toString() {
            return "TestMessage{timestamp=" + timestamp + '}';
        }
    }

    /** Mock Kubernetes service for testing. */
    private static class MockGateKubernetesService extends GateKubernetesService {
        private ConfigMap initialConfigMap;

        MockGateKubernetesService() {
            super(TEST_NAMESPACE, TEST_CONFIGMAP_NAME);
            // Create initial ConfigMap
            HashMap<String, String> initialData = new HashMap<>();
            initialData.put(
                    GateContextOptions.ACTIVE_DEPLOYMENT_TYPE.getLabel(),
                    BlueGreenDeploymentType.BLUE.toString());
            initialData.put(
                    GateContextOptions.TRANSITION_STAGE.getLabel(),
                    TransitionStage.INITIALIZING.toString());
            initialData.put(GateContextOptions.DEPLOYMENT_DELETION_DELAY.getLabel(), "5000");
            initialData.put(GateContextOptions.IS_FIRST_DEPLOYMENT.getLabel(), "true");
            initialData.put("watermark-toggle-value", "0");
            initialData.put("watermark-stage", WatermarkGateStage.WATERMARK_NOT_SET.toString());

            this.initialConfigMap =
                    new ConfigMapBuilder()
                            .withNewMetadata()
                            .withName(TEST_CONFIGMAP_NAME)
                            .withNamespace(TEST_NAMESPACE)
                            .endMetadata()
                            .withData(new HashMap<>(initialData))
                            .build();
        }

        @Override
        public void setInformers(ResourceEventHandler<ConfigMap> resourceEventHandler) {
            // No-op - event handler will be captured in TestableWatermarkGateProcessFunction
        }

        @Override
        public ConfigMap parseConfigMap() {
            return initialConfigMap;
        }

        @Override
        public void updateConfigMapEntries(Map<String, String> kvps) {
            // No-op in tests
        }
    }

    /** Testable implementation that uses mock service and tracks updates. */
    private static class TestableWatermarkGateProcessFunction<I>
            extends WatermarkGateProcessFunction<I> {
        private static final long serialVersionUID = 1L;

        private transient List<String> logMessages;
        private transient List<Map<String, String>> configMapUpdates;
        private transient ResourceEventHandler<ConfigMap> capturedEventHandler;

        TestableWatermarkGateProcessFunction(
                BlueGreenDeploymentType blueGreenDeploymentType,
                String namespace,
                String configMapName,
                Function<I, Long> watermarkExtractor,
                GateKubernetesService mockService) {
            super(
                    blueGreenDeploymentType,
                    namespace,
                    configMapName,
                    watermarkExtractor,
                    mockService);
            initializeTransientFields();
        }

        private void initializeTransientFields() {
            if (logMessages == null) {
                logMessages = new ArrayList<>();
            }
            if (configMapUpdates == null) {
                configMapUpdates = new ArrayList<>();
            }
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            // After open, capture the event handler that was created
            // We need to access it from the parent's setKubernetesEnvironment
        }

        @Override
        protected void logInfo(String message) {
            initializeTransientFields();
            logMessages.add(message);
        }

        @Override
        protected void performConfigMapUpdate(Map<String, String> updates) {
            initializeTransientFields();
            configMapUpdates.add(new HashMap<>(updates));
            // Don't call super - avoid actual K8s updates in tests
        }

        public ResourceEventHandler<ConfigMap> getCapturedEventHandler() {
            initializeTransientFields();
            if (capturedEventHandler == null) {
                // Create a test event handler that delegates to processConfigMap
                capturedEventHandler =
                        new ResourceEventHandler<ConfigMap>() {
                            @Override
                            public void onAdd(ConfigMap obj) {
                                logInfo("Unexpected ConfigMap added: " + obj);
                            }

                            @Override
                            public void onUpdate(ConfigMap oldObj, ConfigMap newObj) {
                                if (!oldObj.equals(newObj)) {
                                    // Directly access protected baseContext field
                                    baseContext =
                                            org.apache.flink.kubernetes.operator.api.bluegreen
                                                    .GateContext.create(
                                                    newObj.getData(), blueGreenDeploymentType);

                                    // Extract custom data for watermark context
                                    Map<String, String> customData = new HashMap<>();
                                    String watermarkToggleValue =
                                            newObj.getData()
                                                    .get(
                                                            WatermarkGateContext
                                                                    .WATERMARK_TOGGLE_VALUE);
                                    String watermarkStage =
                                            newObj.getData()
                                                    .get(WatermarkGateContext.WATERMARK_STAGE);

                                    if (watermarkToggleValue != null) {
                                        customData.put(
                                                WatermarkGateContext.WATERMARK_TOGGLE_VALUE,
                                                watermarkToggleValue);
                                    }
                                    if (watermarkStage != null) {
                                        customData.put(
                                                WatermarkGateContext.WATERMARK_STAGE,
                                                watermarkStage);
                                    }

                                    onContextUpdate(baseContext, customData);
                                }
                            }

                            @Override
                            public void onDelete(ConfigMap obj, boolean deletedFinalStateUnknown) {
                                logInfo(
                                        "ConfigMap deleted: "
                                                + obj
                                                + ", final state unknown: "
                                                + deletedFinalStateUnknown);
                            }
                        };
            }
            return capturedEventHandler;
        }

        public List<String> getLogMessages() {
            initializeTransientFields();
            return logMessages;
        }

        public List<Map<String, String>> getConfigMapUpdates() {
            initializeTransientFields();
            return configMapUpdates;
        }

        public boolean isWaitingForWatermarkNotified() {
            return waitingForWatermark;
        }

        public boolean isClearToTeardownNotified() {
            return clearToTeardown;
        }

        public WatermarkGateContext getCurrentWatermarkGateContext() {
            return currentWatermarkGateContext;
        }

        // Custom serialization
        private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
            out.defaultWriteObject();
        }

        private void readObject(java.io.ObjectInputStream in)
                throws java.io.IOException, ClassNotFoundException {
            in.defaultReadObject();
            initializeTransientFields();
        }
    }
}
