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

package org.apache.flink.kubernetes.operator.hooks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.hooks.flink.FlinkCluster;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnableKubernetesMockClient(crud = true)
class FlinkResourceHooksManagerTest {

    private KubernetesClient kubernetesClient;
    private EventCollector eventCollector;
    private EventRecorder eventRecorder;
    private TestHookContext context;

    @BeforeEach
    void setUp() {
        eventCollector = new EventCollector();
        eventRecorder = new EventRecorder(eventCollector);
        FlinkSessionJob flinkSessionJob = new FlinkSessionJob();
        flinkSessionJob.getMetadata().setNamespace("default");
        context = new TestHookContext(flinkSessionJob, kubernetesClient);
    }

    @Test
    void testExecuteAllHooks_NoHooks() {
        FlinkResourceHooksManager manager =
                new FlinkResourceHooksManager(Collections.emptyList(), eventRecorder);
        FlinkResourceHookStatus status =
                manager.executeAllHooks(FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP, context);

        assertEquals(FlinkResourceHookStatus.Status.NOT_APPLICABLE, status.getStatus());
        assertTrue(eventCollector.events.isEmpty());
    }

    @Test
    void testExecuteAllHooks_AllCompleted() {
        FlinkResourceHooksManager manager =
                new FlinkResourceHooksManager(
                        List.of(
                                new TestingFlinkResourceHook(
                                        "HookA", FlinkResourceHookStatus.Status.COMPLETED)),
                        eventRecorder);

        FlinkResourceHookStatus status =
                manager.executeAllHooks(FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP, context);

        assertEquals(FlinkResourceHookStatus.Status.COMPLETED, status.getStatus());
        assertEquals(1, eventCollector.events.size());

        List<Event> events =
                eventCollector.events.stream()
                        .sorted(Comparator.comparing(Event::getType))
                        .collect(Collectors.toList());
        assertEquals("Normal", events.get(0).getType());
        assertTrue(events.get(0).getReason().contains("FlinkResourceHookFinished"));
        assertTrue(events.get(0).getMessage().contains("HookA"));
    }

    @Test
    void testExecuteAllHooks_AllNotApplicable() {
        FlinkResourceHooksManager manager =
                new FlinkResourceHooksManager(
                        Arrays.asList(
                                new TestingFlinkResourceHook(
                                        "HookA", FlinkResourceHookStatus.Status.NOT_APPLICABLE),
                                new TestingFlinkResourceHook(
                                        "HookB", FlinkResourceHookStatus.Status.NOT_APPLICABLE)),
                        eventRecorder);

        FlinkResourceHookStatus status =
                manager.executeAllHooks(FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP, context);

        assertEquals(FlinkResourceHookStatus.Status.NOT_APPLICABLE, status.getStatus());
        assertTrue(eventCollector.events.isEmpty());
    }

    @Test
    void testExecuteAllHooks_OneFailed() {
        FlinkResourceHooksManager manager =
                new FlinkResourceHooksManager(
                        Arrays.asList(
                                new TestingFlinkResourceHook(
                                        "HookA", FlinkResourceHookStatus.Status.COMPLETED),
                                new TestingFlinkResourceHook(
                                        "HookB", FlinkResourceHookStatus.Status.FAILED)),
                        eventRecorder);

        FlinkResourceHookStatus status =
                manager.executeAllHooks(FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP, context);

        assertEquals(FlinkResourceHookStatus.Status.FAILED, status.getStatus());
        assertEquals(2, eventCollector.events.size());

        List<Event> events =
                eventCollector.events.stream()
                        .sorted(Comparator.comparing(Event::getType))
                        .collect(Collectors.toList());

        assertEquals("Normal", events.get(0).getType());
        assertTrue(events.get(0).getReason().contains("FlinkResourceHookFinished"));

        assertEquals("Warning", events.get(1).getType());
        assertTrue(events.get(1).getReason().contains("FlinkResourceHookFailed"));
    }

    @Test
    void testExecuteAllHooks_OnePending() {
        Duration reconcileInterval = Duration.ofSeconds(30);
        FlinkResourceHooksManager manager =
                new FlinkResourceHooksManager(
                        Arrays.asList(
                                new TestingFlinkResourceHook(
                                        "HookA", FlinkResourceHookStatus.Status.COMPLETED),
                                new TestingFlinkResourceHook(
                                        "HookB",
                                        FlinkResourceHookStatus.Status.PENDING,
                                        reconcileInterval)),
                        eventRecorder);

        FlinkResourceHookStatus status =
                manager.executeAllHooks(FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP, context);

        assertEquals(FlinkResourceHookStatus.Status.PENDING, status.getStatus());
        assertEquals(reconcileInterval, status.getReconcileInterval());
        assertEquals(2, eventCollector.events.size());

        List<Event> events =
                eventCollector.events.stream()
                        .sorted(Comparator.comparing(Event::getReason))
                        .collect(Collectors.toList());
        assertEquals("Normal", events.get(0).getType());
        assertTrue(events.get(0).getReason().contains("FlinkResourceHookFinished"));

        assertEquals("Normal", events.get(1).getType());
        assertTrue(events.get(1).getReason().contains("FlinkResourceHookPending"));
    }

    @Test
    void testExecuteAllHooks_MultiplePending_MaxReconcileInterval() {
        Duration shortInterval = Duration.ofSeconds(10);
        Duration longInterval = Duration.ofSeconds(60);

        FlinkResourceHooksManager manager =
                new FlinkResourceHooksManager(
                        Arrays.asList(
                                new TestingFlinkResourceHook(
                                        "HookA",
                                        FlinkResourceHookStatus.Status.PENDING,
                                        shortInterval),
                                new TestingFlinkResourceHook(
                                        "HookB",
                                        FlinkResourceHookStatus.Status.PENDING,
                                        longInterval)),
                        eventRecorder);

        FlinkResourceHookStatus status =
                manager.executeAllHooks(FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP, context);

        assertEquals(FlinkResourceHookStatus.Status.PENDING, status.getStatus());
        assertEquals(longInterval, status.getReconcileInterval());
        assertEquals(2, eventCollector.events.size());
    }

    @Test
    void testExecuteAllHooks_PendingAndFailed_PendingTakesPrecedence() {
        Duration reconcileInterval = Duration.ofSeconds(30);
        FlinkResourceHooksManager manager =
                new FlinkResourceHooksManager(
                        Arrays.asList(
                                new TestingFlinkResourceHook(
                                        "HookA", FlinkResourceHookStatus.Status.FAILED),
                                new TestingFlinkResourceHook(
                                        "HookB",
                                        FlinkResourceHookStatus.Status.PENDING,
                                        reconcileInterval)),
                        eventRecorder);

        FlinkResourceHookStatus status =
                manager.executeAllHooks(FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP, context);

        assertEquals(FlinkResourceHookStatus.Status.PENDING, status.getStatus());
        assertEquals(reconcileInterval, status.getReconcileInterval());
    }

    @Test
    void testGetHooks() {
        var hookA = new TestingFlinkResourceHook("HookA", FlinkResourceHookStatus.Status.COMPLETED);
        var hookB = new TestingFlinkResourceHook("HookB", FlinkResourceHookStatus.Status.COMPLETED);

        FlinkResourceHooksManager manager =
                new FlinkResourceHooksManager(Arrays.asList(hookA, hookB), eventRecorder);

        var sessionJobHooks = manager.getHooks(FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP);
        assertEquals(2, sessionJobHooks.size());

        // Test for a hook type with no hooks registered
        var emptyHooks = manager.getHooks(null);
        assertEquals(Set.of(), emptyHooks);
    }

    @Test
    void testExecuteAllHooks_MixedApplicability() {
        FlinkResourceHooksManager manager =
                new FlinkResourceHooksManager(
                        Arrays.asList(
                                new TestingFlinkResourceHook(
                                        "HookA", FlinkResourceHookStatus.Status.NOT_APPLICABLE),
                                new TestingFlinkResourceHook(
                                        "HookB", FlinkResourceHookStatus.Status.COMPLETED)),
                        eventRecorder);

        FlinkResourceHookStatus status =
                manager.executeAllHooks(FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP, context);

        assertEquals(FlinkResourceHookStatus.Status.COMPLETED, status.getStatus());
        assertEquals(1, eventCollector.events.size());

        Event event = eventCollector.events.getFirst();
        assertEquals("Normal", event.getType());
        assertTrue(event.getReason().contains("FlinkResourceHookFinished"));
    }

    // Custom test hook that returns a predefined status
    private static class TestingFlinkResourceHook implements FlinkResourceHook {
        private final String name;
        private final FlinkResourceHookStatus.Status status;
        private final Duration reconcileInterval;

        public TestingFlinkResourceHook(String name, FlinkResourceHookStatus.Status status) {
            this(name, status, null);
        }

        public TestingFlinkResourceHook(
                String name, FlinkResourceHookStatus.Status status, Duration reconcileInterval) {
            this.name = name;
            this.status = status;
            this.reconcileInterval = reconcileInterval;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public FlinkResourceHookStatus execute(FlinkResourceHookContext context) {
            return new FlinkResourceHookStatus(reconcileInterval, status);
        }

        @Override
        public FlinkResourceHookType getHookType() {
            return FlinkResourceHookType.SESSION_JOB_PRE_SCALE_UP;
        }
    }

    // Custom test hook context
    private static class TestHookContext implements FlinkResourceHook.FlinkResourceHookContext {
        private final FlinkSessionJob flinkSessionJob;
        private final KubernetesClient kubernetesClient;

        public TestHookContext(FlinkSessionJob flinkSessionJob, KubernetesClient kubernetesClient) {
            this.flinkSessionJob = flinkSessionJob;
            this.kubernetesClient = kubernetesClient;
        }

        @Override
        public FlinkSessionJob getFlinkSessionJob() {
            return flinkSessionJob;
        }

        @Override
        public FlinkCluster getFlinkSessionCluster() {
            return null;
        }

        @Override
        public Configuration getDeployConfig() {
            return new Configuration();
        }

        @Override
        public Configuration getCurrentDeployedConfig() {
            return new Configuration();
        }

        @Override
        public KubernetesClient getKubernetesClient() {
            return kubernetesClient;
        }
    }
}
