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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.metrics.Histogram;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.GroupVersionKind;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/** {@link OperatorJosdkMetrics} tests. */
public class OperatorJosdkMetricsTest {

    private static final ResourceID resourceId = new ResourceID("testname", "testns");
    private static final HasMetadata resource = testResource(resourceId);

    private static HasMetadata testResource(ResourceID resourceId) {
        var flinkDeployment = new FlinkDeployment();
        flinkDeployment.getMetadata().setName(resourceId.getName());
        flinkDeployment.getMetadata().setNamespace(resourceId.getNamespace().orElseThrow());
        return flinkDeployment;
    }

    private static final String controllerName = FlinkDeploymentController.class.getSimpleName();
    private static final Map<String, Object> metadata =
            Map.of(Constants.RESOURCE_GVK_KEY, GroupVersionKind.gvkFor(FlinkDeployment.class));

    private OperatorJosdkMetrics operatorMetrics;
    private TestingMetricListener listener;

    @BeforeEach
    public void setup() {
        listener = new TestingMetricListener(new Configuration());
        operatorMetrics =
                new OperatorJosdkMetrics(
                        listener.getMetricGroup(), new FlinkConfigManager(new Configuration()));
    }

    @Test
    public void testTimeControllerExecution() throws Exception {
        var successExecution = new TestingExecutionBase<>();
        operatorMetrics.timeControllerExecution(successExecution);

        assertEquals(1, listener.size());
        assertEquals(1, getHistogram("reconcile", "resource").getCount());
        assertEquals(1, getHistogram("reconcile", "resource").getStatistics().getMin());
        operatorMetrics.timeControllerExecution(successExecution);
        operatorMetrics.timeControllerExecution(successExecution);
        assertEquals(1, listener.size());
        assertEquals(3, getHistogram("reconcile", "resource").getCount());
        assertEquals(1, getHistogram("reconcile", "resource").getStatistics().getMin());

        var failureExecution =
                new TestingExecutionBase<>() {
                    @Override
                    public String name() {
                        return "cleanup";
                    }

                    @Override
                    public Object execute() throws Exception {
                        Thread.sleep(1000);
                        throw new ReconciliationException(new RuntimeException());
                    }
                };
        try {
            operatorMetrics.timeControllerExecution(failureExecution);
            fail();
        } catch (Exception e) {
            assertEquals(2, listener.size());
            assertEquals(1, getHistogram("cleanup", "failed").getCount());
            assertEquals(1, getHistogram("cleanup", "failed").getStatistics().getMin());
        }
        try {
            operatorMetrics.timeControllerExecution(failureExecution);
            fail();
        } catch (Exception ignored) {
        }
        try {
            operatorMetrics.timeControllerExecution(failureExecution);
            fail();
        } catch (Exception e) {
            assertEquals(2, listener.size());
            assertEquals(3, getHistogram("cleanup", "failed").getCount());
            assertEquals(1, getHistogram("cleanup", "failed").getStatistics().getMin());
        }
    }

    @Test
    public void testMetrics() {
        operatorMetrics.reconciliationSubmitted(resource, null, metadata);
        assertEquals(1, listener.size());
        assertEquals(1, getCount("Reconciliation"));

        operatorMetrics.reconciliationFailed(resource, null, null, metadata);
        operatorMetrics.reconciliationFailed(resource, null, null, metadata);
        operatorMetrics.reconciliationFailed(resource, null, null, metadata);
        assertEquals(2, listener.size());
        assertEquals(3, getCount("Reconciliation.failed"));

        operatorMetrics.reconciliationSubmitted(
                resource,
                new RetryInfo() {
                    @Override
                    public int getAttemptCount() {
                        return 0;
                    }

                    @Override
                    public boolean isLastAttempt() {
                        return false;
                    }
                },
                metadata);
        assertEquals(3, listener.size());
        assertEquals(2, getCount("Reconciliation"));
        assertEquals(1, getCount("Reconciliation.retries"));

        operatorMetrics.eventReceived(
                new ResourceEvent(ResourceAction.ADDED, resourceId, null), metadata);
        assertEquals(5, listener.size());
        assertEquals(1, getCount("Resource.Event"));
        assertEquals(1, getCount("Resource.Event.ADDED"));

        operatorMetrics.reconciliationFinished(resource, null, metadata);
        assertEquals(6, listener.size());
        assertEquals(1, getCount("Reconciliation.finished"));

        operatorMetrics.reconciliationSubmitted(
                testResource(new ResourceID("other", "otherns")), null, metadata);
        assertEquals(7, listener.size());
        assertEquals(
                1,
                listener.getCounter(
                                listener.getResourceMetricId(
                                        FlinkDeployment.class,
                                        "otherns",
                                        "other",
                                        "JOSDK",
                                        "Reconciliation",
                                        "Count"))
                        .get()
                        .getCount());
    }

    @Test
    public void testCleanupMetrics() {
        operatorMetrics.reconciliationSubmitted(resource, null, metadata);
        operatorMetrics.reconciliationFailed(resource, null, null, metadata);
        assertEquals(2, listener.size());
        assertEquals(1, operatorMetrics.getResourceMetricsCacheSize());

        operatorMetrics.cleanupDone(resourceId, metadata);
        assertEquals(0, listener.size());
        assertEquals(0, operatorMetrics.getResourceMetricsCacheSize());

        operatorMetrics.reconciliationFinished(resource, null, metadata);
        operatorMetrics.reconciliationFailed(resource, null, null, metadata);
        assertEquals(0, listener.size());
        assertEquals(0, operatorMetrics.getResourceMetricsCacheSize());

        // cleanupDone is idempotent.
        operatorMetrics.cleanupDone(resourceId, metadata);
        assertEquals(0, listener.size());
        assertEquals(0, operatorMetrics.getResourceMetricsCacheSize());

        operatorMetrics.reconciliationSubmitted(resource, null, metadata);
        assertEquals(1, listener.size());
        assertEquals(1, getCount("Reconciliation"));
        assertEquals(1, operatorMetrics.getResourceMetricsCacheSize());
    }

    @Test
    public void testDifferentKindsWithSameResourceIdAreIndependent() {
        Map<String, Object> snapshotMetadata =
                Map.of(
                        Constants.RESOURCE_GVK_KEY,
                        GroupVersionKind.gvkFor(FlinkStateSnapshot.class));
        var snapshot = new FlinkStateSnapshot();
        snapshot.getMetadata().setName(resourceId.getName());
        snapshot.getMetadata().setNamespace(resourceId.getNamespace().orElseThrow());

        operatorMetrics.reconciliationSubmitted(resource, null, metadata);
        operatorMetrics.reconciliationSubmitted(snapshot, null, snapshotMetadata);
        assertEquals(2, listener.size());
        assertEquals(2, operatorMetrics.getResourceMetricsCacheSize());

        // Cleaning up the deployment must not touch the snapshot with the same name/namespace.
        operatorMetrics.cleanupDone(resourceId, metadata);
        assertEquals(1, listener.size());
        assertEquals(1, operatorMetrics.getResourceMetricsCacheSize());

        operatorMetrics.cleanupDone(resourceId, snapshotMetadata);
        assertEquals(0, listener.size());
        assertEquals(0, operatorMetrics.getResourceMetricsCacheSize());
    }

    @Test
    public void testCleanupWithDynamicScopeFormat() {
        var shortScopeConfig = new Configuration();
        shortScopeConfig.set(
                KubernetesOperatorMetricOptions.SCOPE_NAMING_KUBERNETES_OPERATOR_RESOURCE,
                "<resourcename>");
        var customListener = new TestingMetricListener(shortScopeConfig);
        var configManager = new FlinkConfigManager(shortScopeConfig);
        var customMetrics =
                new OperatorJosdkMetrics(customListener.getMetricGroup(), configManager);
        var otherResourceId = new ResourceID("other", "testns");
        var otherResource = testResource(otherResourceId);

        customMetrics.reconciliationSubmitted(resource, null, metadata);
        configManager.updateDefaultConfig(new Configuration());
        customMetrics.reconciliationSubmitted(otherResource, null, metadata);
        assertEquals(2, customListener.size());
        assertEquals(2, customMetrics.getResourceMetricsCacheSize());

        customMetrics.cleanupDone(otherResourceId, metadata);
        assertEquals(1, customListener.size());
        assertEquals(1, customMetrics.getResourceMetricsCacheSize());
        assertEquals(
                1,
                customListener
                        .getCounter(
                                customListener.getResourceMetricId(
                                        FlinkDeployment.class,
                                        "testns",
                                        "testname",
                                        "JOSDK",
                                        "Reconciliation",
                                        "Count"))
                        .orElseThrow()
                        .getCount());

        customMetrics.cleanupDone(resourceId, metadata);
        assertEquals(0, customListener.size());
        assertEquals(0, customMetrics.getResourceMetricsCacheSize());
    }

    @Test
    public void testConcurrentCleanupAndFinished() throws Exception {
        runConcurrentCleanupRace(
                currentResource ->
                        operatorMetrics.reconciliationFinished(currentResource, null, metadata));
    }

    @Test
    public void testConcurrentCleanupAndFailed() throws Exception {
        runConcurrentCleanupRace(
                currentResource ->
                        operatorMetrics.reconciliationFailed(
                                currentResource, null, null, metadata));
    }

    private void runConcurrentCleanupRace(Consumer<HasMetadata> completion) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 500; i++) {
                var currentResourceId = new ResourceID("testname-" + i, "testns");
                var currentResource = testResource(currentResourceId);
                operatorMetrics.reconciliationSubmitted(currentResource, null, metadata);

                var start = new CountDownLatch(1);
                var cleanup =
                        executor.submit(
                                () -> {
                                    awaitQuietly(start);
                                    operatorMetrics.cleanupDone(currentResourceId, metadata);
                                });
                var completionTask =
                        executor.submit(
                                () -> {
                                    awaitQuietly(start);
                                    completion.accept(currentResource);
                                });
                start.countDown();
                cleanup.get();
                completionTask.get();

                assertEquals(0, listener.size());
                assertEquals(0, operatorMetrics.getResourceMetricsCacheSize());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private Histogram getHistogram(String... names) {
        return listener.getHistogram(
                        listener.getNamespaceMetricId(
                                FlinkDeployment.class,
                                "testns",
                                "JOSDK",
                                String.join(".", names),
                                "TimeSeconds"))
                .get();
    }

    private long getCount(String name) {
        return listener.getCounter(
                        listener.getResourceMetricId(
                                FlinkDeployment.class,
                                "testns",
                                "testname",
                                "JOSDK",
                                name,
                                "Count"))
                .get()
                .getCount();
    }

    private static class TestingExecutionBase<T> implements Metrics.ControllerExecution<T> {
        @Override
        public String controllerName() {
            return controllerName;
        }

        @Override
        public String successTypeName(Object o) {
            return "resource";
        }

        @Override
        public ResourceID resourceID() {
            return resourceId;
        }

        @Override
        public Map<String, Object> metadata() {
            return metadata;
        }

        @Override
        public String name() {
            return "reconcile";
        }

        @Override
        public T execute() throws Exception {
            Thread.sleep(1000);
            return null;
        }
    }
}
