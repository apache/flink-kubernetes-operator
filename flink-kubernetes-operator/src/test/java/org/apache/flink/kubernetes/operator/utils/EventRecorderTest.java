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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for the configurable event labels of {@link EventRecorder}. */
@EnableKubernetesMockClient(crud = true)
public class EventRecorderTest {

    private static final Map<String, String> CONFIGURED_LABELS =
            Map.of("env", "prod", "owner", "flink");

    private KubernetesClient kubernetesClient;

    @Test
    public void testNoLabelsByDefault() {
        var recorder = EventRecorder.create(kubernetesClient, List.of(), new Configuration());
        var flinkApp = TestUtils.buildApplicationCluster();

        recorder.triggerEvent(
                flinkApp,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.Cleanup,
                EventRecorder.Component.Operator,
                "message",
                kubernetesClient);

        var labels = getEvent(flinkApp, "message").getMetadata().getLabels();
        assertTrue(labels == null || labels.isEmpty(), "Unexpected labels: " + labels);
    }

    @Test
    public void testConfiguredLabelsAddedToEvents() {
        var recorder = EventRecorder.create(kubernetesClient, List.of(), configWithLabels());
        var flinkApp = TestUtils.buildApplicationCluster();

        // Event created via createOrUpdateEventWithInterval
        recorder.triggerEvent(
                flinkApp,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.Cleanup,
                EventRecorder.Component.Operator,
                "message",
                kubernetesClient);
        assertEquals(CONFIGURED_LABELS, getEvent(flinkApp, "message").getMetadata().getLabels());

        // The labels are kept when the same event is updated
        recorder.triggerEvent(
                flinkApp,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.Cleanup,
                EventRecorder.Component.Operator,
                "message",
                kubernetesClient);
        var updated = getEvent(flinkApp, "message");
        assertEquals(2, updated.getCount());
        assertEquals(CONFIGURED_LABELS, updated.getMetadata().getLabels());

        // Event created via createIfNotExists
        recorder.triggerEventOnce(
                flinkApp,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.Cleanup,
                "message",
                EventRecorder.Component.Operator,
                "once",
                kubernetesClient);
        assertEquals(CONFIGURED_LABELS, getEvent(flinkApp, "once").getMetadata().getLabels());

        // Event created via createWithAnnotations
        recorder.triggerEventWithAnnotations(
                flinkApp,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.JobException,
                "message",
                EventRecorder.Component.Job,
                "annotated",
                kubernetesClient,
                Map.of("a", "b"));
        var annotated =
                getEvent(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.JobException.name(),
                        "annotated",
                        EventRecorder.Component.Job);
        assertEquals(CONFIGURED_LABELS, annotated.getMetadata().getLabels());
        assertEquals(Map.of("a", "b"), annotated.getMetadata().getAnnotations());
    }

    @Test
    public void testConfiguredLabelsMergedWithCallerLabels() {
        var recorder = EventRecorder.create(kubernetesClient, List.of(), configWithLabels());
        var flinkApp = TestUtils.buildApplicationCluster();

        // Caller labels (used for dedupe) are merged with the configured ones, caller wins on
        // conflicting keys
        recorder.triggerEventWithLabels(
                flinkApp,
                EventRecorder.Type.Normal,
                EventRecorder.Reason.ScalingReport.name(),
                "message",
                EventRecorder.Component.Operator,
                "scaling",
                kubernetesClient,
                Duration.ofMinutes(30),
                labels -> false,
                Map.of("parallelismMap", "hash", "owner", "autoscaler"));

        assertEquals(
                Map.of("env", "prod", "owner", "autoscaler", "parallelismMap", "hash"),
                getEvent(
                                flinkApp,
                                EventRecorder.Type.Normal,
                                EventRecorder.Reason.ScalingReport.name(),
                                "scaling",
                                EventRecorder.Component.Operator)
                        .getMetadata()
                        .getLabels());
    }

    @Test
    public void testLabelsNotSetWithoutConfigOnLegacyPaths() {
        var recorder = EventRecorder.create(kubernetesClient, List.of(), new Configuration());
        var flinkApp = TestUtils.buildApplicationCluster();

        recorder.triggerEventOnce(
                flinkApp,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.Cleanup,
                "message",
                EventRecorder.Component.Operator,
                "once",
                kubernetesClient);

        var labels = getEvent(flinkApp, "once").getMetadata().getLabels();
        assertTrue(labels == null || labels.isEmpty(), "Unexpected labels: " + labels);
    }

    private static Configuration configWithLabels() {
        var conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.OPERATOR_EVENT_LABELS, CONFIGURED_LABELS);
        return conf;
    }

    private Event getEvent(FlinkDeployment flinkApp, String messageKey) {
        return getEvent(
                flinkApp,
                EventRecorder.Type.Warning,
                EventRecorder.Reason.Cleanup.name(),
                messageKey,
                EventRecorder.Component.Operator);
    }

    private Event getEvent(
            FlinkDeployment flinkApp,
            EventRecorder.Type type,
            String reason,
            String messageKey,
            EventRecorder.Component component) {
        var eventName = EventUtils.generateEventName(flinkApp, type, reason, messageKey, component);
        return kubernetesClient
                .v1()
                .events()
                .inNamespace(flinkApp.getMetadata().getNamespace())
                .withName(eventName)
                .get();
    }
}
