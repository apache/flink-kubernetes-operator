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

import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodConditionBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.net.HttpURLConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for {@link EventUtils}. */
@EnableKubernetesMockClient(crud = true)
public class EventUtilsTest {

    private KubernetesMockServer mockServer;
    private KubernetesClient kubernetesClient;
    private Event eventConsumed = null;

    @Test
    public void testCreateOrReplaceEvent() {
        var consumer =
                new Consumer<Event>() {
                    @Override
                    public void accept(Event event) {
                        eventConsumed = event;
                    }
                };
        var flinkApp = TestUtils.buildApplicationCluster();
        var reason = "Cleanup";
        var message = "message";
        var eventName =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator);
        Assertions.assertTrue(
                EventUtils.createOrUpdateEventWithInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        consumer,
                        null,
                        null));
        var event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertEquals(eventConsumed, event);
        Assertions.assertEquals(1, event.getCount());
        Assertions.assertEquals(reason, event.getReason());

        eventConsumed = null;
        Assertions.assertFalse(
                EventUtils.createOrUpdateEventWithInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        consumer,
                        null,
                        null));

        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertEquals(eventConsumed, event);
        Assertions.assertEquals(2, event.getCount());

        Assertions.assertTrue(
                EventUtils.createOrUpdateEventWithInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        null,
                        EventRecorder.Component.Operator,
                        consumer,
                        null,
                        null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "0", "1800"})
    public void testCreateWithInterval(String intervalString) {
        Duration interval =
                intervalString.isBlank() ? null : Duration.ofSeconds(Long.valueOf(intervalString));
        var consumer =
                new Consumer<Event>() {
                    @Override
                    public void accept(Event event) {
                        eventConsumed = event;
                    }
                };
        var flinkApp = TestUtils.buildApplicationCluster();
        var reason = "Cleanup";
        var eventName =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "mk",
                        EventRecorder.Component.Operator);

        Assertions.assertTrue(
                EventUtils.createOrUpdateEventWithInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message1",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk",
                        interval));
        var event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertEquals("message1", event.getMessage());
        Assertions.assertEquals(1, event.getCount());

        Assertions.assertFalse(
                EventUtils.createOrUpdateEventWithInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message2",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk",
                        null));

        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertEquals("message2", event.getMessage());
        Assertions.assertEquals(2, event.getCount());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "0", "1800"})
    public void testCreateWithLabelsAndAllTruePredicate(String intervalString) {
        @Nullable
        Predicate<Map<String, String>> dedupePredicate =
                new Predicate<Map<String, String>>() {
                    @Override
                    public boolean test(Map<String, String> stringStringMap) {
                        return true;
                    }
                };
        testCreateWithIntervalLabelsAndPredicate(intervalString, dedupePredicate);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "0", "1800"})
    public void testCreateWithLabelsAndAllFalsePredicate(String intervalString) {
        @Nullable
        Predicate<Map<String, String>> dedupePredicate =
                new Predicate<Map<String, String>>() {
                    @Override
                    public boolean test(Map<String, String> stringStringMap) {
                        return false;
                    }
                };
        testCreateWithIntervalLabelsAndPredicate(intervalString, dedupePredicate);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "0", "1800"})
    public void testCreateWithLabelsAndNullPredicate(String intervalString) {
        testCreateWithIntervalLabelsAndPredicate(intervalString, null);
    }

    private void testCreateWithIntervalLabelsAndPredicate(
            String intervalString, @Nullable Predicate<Map<String, String>> dedupePredicate) {
        Duration interval =
                intervalString.isBlank() ? null : Duration.ofSeconds(Long.valueOf(intervalString));
        var consumer =
                new Consumer<Event>() {
                    @Override
                    public void accept(Event event) {
                        eventConsumed = event;
                    }
                };
        var flinkApp = TestUtils.buildApplicationCluster();
        var reason = "Cleanup";
        var eventName =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "mk",
                        EventRecorder.Component.Operator);

        // Set up an event with empty labels
        Assertions.assertTrue(
                EventUtils.createIfNotExists(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk"));

        // Update the event with label.
        var labels = Map.of("a", "b");
        Assertions.assertFalse(
                EventUtils.createOrUpdateEventWithLabels(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message1",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk",
                        interval,
                        dedupePredicate,
                        labels));
        var event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        if ((dedupePredicate == null || (dedupePredicate.test(labels)))
                && interval != null
                && interval.toMillis() > 0) {
            Assertions.assertEquals("message", event.getMessage());
            Assertions.assertEquals(1, event.getCount());
            Assertions.assertEquals(null, event.getMetadata().getLabels().get("a"));
        } else {
            Assertions.assertEquals("message1", event.getMessage());
            Assertions.assertEquals(2, event.getCount());
            Assertions.assertEquals("b", event.getMetadata().getLabels().get("a"));
        }

        // Update with duplicate labels.
        Assertions.assertFalse(
                EventUtils.createOrUpdateEventWithLabels(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message2",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk",
                        interval,
                        dedupePredicate,
                        labels));

        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        if ((dedupePredicate == null || (dedupePredicate.test(labels)))
                && interval != null
                && interval.toMillis() > 0) {
            Assertions.assertEquals("message", event.getMessage());
            Assertions.assertEquals(1, event.getCount());
            Assertions.assertEquals(null, event.getMetadata().getLabels().get("a"));
        } else {
            Assertions.assertEquals("message2", event.getMessage());
            Assertions.assertEquals(3, event.getCount());
            Assertions.assertEquals("b", event.getMetadata().getLabels().get("a"));
        }

        // Update with empty label.
        labels = Map.of();
        Assertions.assertFalse(
                EventUtils.createOrUpdateEventWithLabels(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message3",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk",
                        interval,
                        dedupePredicate,
                        labels));
        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        if ((dedupePredicate == null || (dedupePredicate.test(labels)))
                && interval != null
                && interval.toMillis() > 0) {
            Assertions.assertEquals("message", event.getMessage());
            Assertions.assertEquals(1, event.getCount());
            Assertions.assertEquals(null, event.getMetadata().getLabels().get("a"));
        } else {
            Assertions.assertEquals("message3", event.getMessage());
            Assertions.assertEquals(4, event.getCount());
            Assertions.assertEquals(null, event.getMetadata().getLabels().get("a"));
        }

        // Update the event with null label
        Assertions.assertFalse(
                EventUtils.createOrUpdateEventWithLabels(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message4",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk",
                        interval,
                        dedupePredicate,
                        null));

        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        if ((dedupePredicate == null || (dedupePredicate.test(labels)))
                && interval != null
                && interval.toMillis() > 0) {
            Assertions.assertEquals("message", event.getMessage());
            Assertions.assertEquals(1, event.getCount());
            Assertions.assertEquals(null, event.getMetadata().getLabels().get("a"));
        } else {
            Assertions.assertEquals("message4", event.getMessage());
            Assertions.assertEquals(
                    dedupePredicate != null
                                    && dedupePredicate.test(labels)
                                    && interval != null
                                    && interval.toMillis() > 0
                            ? 4
                            : 5,
                    event.getCount());
            Assertions.assertEquals(null, event.getMetadata().getLabels().get("a"));
        }
        // Create a new event
        Assertions.assertTrue(
                EventUtils.createOrUpdateEventWithLabels(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message1",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk2",
                        interval,
                        dedupePredicate,
                        Map.of("a", "b")));
        eventName =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "mk2",
                        EventRecorder.Component.Operator);
        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertEquals("message1", event.getMessage());
        Assertions.assertEquals(1, event.getCount());
        Assertions.assertEquals(event.getMetadata().getLabels().get("a"), "b");
    }

    @Test
    public void testSameResourceNameWithDifferentUidNotShareEvents() {
        var flinkApp = TestUtils.buildApplicationCluster();
        flinkApp.getMetadata().setUid("uid1");
        var reason = "Cleanup";
        var message = "message";
        var name1 =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator);
        flinkApp.getMetadata().setUid("uid2");
        var name2 =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator);
        Assertions.assertNotEquals(name1, name2);
    }

    @Test
    public void testCreateIfNotExists() {
        var consumer =
                new Consumer<Event>() {
                    @Override
                    public void accept(Event event) {
                        eventConsumed = event;
                    }
                };
        var flinkApp = TestUtils.buildApplicationCluster();
        var reason = "test";
        var message = "mk";
        var eventName =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "mk",
                        EventRecorder.Component.Operator);
        Assertions.assertTrue(
                EventUtils.createIfNotExists(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk"));
        var event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertEquals(eventConsumed, event);
        Assertions.assertEquals(1, event.getCount());
        Assertions.assertEquals(reason, event.getReason());

        // Make sure we didn't bump the count
        eventConsumed = null;
        Assertions.assertFalse(
                EventUtils.createIfNotExists(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk"));

        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertEquals(1, event.getCount());
        Assertions.assertNull(eventConsumed);
    }

    @Test
    public void testCreateOrReplaceEventOnDeletedNamespace() {
        var consumer =
                new Consumer<Event>() {
                    @Override
                    public void accept(Event event) {
                        eventConsumed = event;
                    }
                };
        var flinkApp = TestUtils.buildApplicationCluster();
        var reason = "Cleanup";
        var message = "message";
        var eventName =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator);

        var namespaceName = flinkApp.getMetadata().getNamespace();

        String eventCreatePath = String.format("/api/v1/namespaces/%s/events", namespaceName);

        mockServer
                .expect()
                .post()
                .withPath(eventCreatePath)
                .andReturn(HttpURLConnection.HTTP_FORBIDDEN, new EventBuilder().build())
                .once();

        Assertions.assertTrue(
                EventUtils.createOrUpdateEventWithInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        consumer,
                        null,
                        null));
        Assertions.assertNull(eventConsumed);
    }

    @Test
    public void testVolumeMountErrors() {
        var pod =
                new PodBuilder()
                        .withNewMetadata()
                        .withName("test")
                        .withNamespace("default")
                        .endMetadata()
                        .withNewStatus()
                        .endStatus()
                        .build();

        var events =
                List.of(
                        createPodEvent("e1", "reason1", "msg1", pod),
                        createPodEvent("e2", "FailedMount", "mountErr", pod));

        // No conditions, no error expected
        EventUtils.checkForVolumeMountErrors(pod, () -> events);

        var conditions = new ArrayList<PodCondition>();
        pod.getStatus().setConditions(conditions);

        // No conditions, no error expected
        EventUtils.checkForVolumeMountErrors(pod, () -> events);

        var conditionMap = new HashMap<String, String>();

        // Pod initialized completely, shouldn't check events
        conditionMap.put("Initialized", "True");
        conditionMap.put("Ready", "False");

        conditions.clear();
        conditionMap.forEach(
                (t, s) ->
                        conditions.add(
                                new PodConditionBuilder().withType(t).withStatus(s).build()));
        EventUtils.checkForVolumeMountErrors(pod, () -> events);

        // Pod initialized completely, shouldn't check events
        conditionMap.put("PodReadyToStartContainers", "True");
        conditionMap.put("Initialized", "False");

        conditions.clear();
        conditionMap.forEach(
                (t, s) ->
                        conditions.add(
                                new PodConditionBuilder().withType(t).withStatus(s).build()));
        EventUtils.checkForVolumeMountErrors(pod, () -> events);

        // Check event only when not ready to start
        conditionMap.put("PodReadyToStartContainers", "False");
        conditions.clear();
        conditionMap.forEach(
                (t, s) ->
                        conditions.add(
                                new PodConditionBuilder().withType(t).withStatus(s).build()));

        try {
            EventUtils.checkForVolumeMountErrors(pod, () -> events);
            fail("Exception not thrown");
        } catch (DeploymentFailedException dfe) {
            assertEquals("FailedMount", dfe.getReason());
            assertEquals("mountErr", dfe.getMessage());
        }

        // Old kubernetes without PodReadyToStartContainers
        conditionMap.remove("PodReadyToStartContainers");
        conditionMap.put("Initialized", "False");
        conditions.clear();
        conditionMap.forEach(
                (t, s) ->
                        conditions.add(
                                new PodConditionBuilder().withType(t).withStatus(s).build()));

        try {
            EventUtils.checkForVolumeMountErrors(pod, () -> events);
            fail("Exception not thrown");
        } catch (DeploymentFailedException dfe) {
            assertEquals("FailedMount", dfe.getReason());
            assertEquals("mountErr", dfe.getMessage());
        }
    }

    private Event createPodEvent(String name, String reason, String msg, Pod pod) {
        return new EventBuilder()
                .withApiVersion("v1")
                .withInvolvedObject(EventUtils.getObjectReference(pod))
                .withType("type")
                .withReason(reason)
                .withFirstTimestamp(Instant.now().toString())
                .withLastTimestamp(Instant.now().toString())
                .withNewSource()
                .withComponent("pod")
                .endSource()
                .withCount(1)
                .withMessage(msg)
                .withNewMetadata()
                .withName(name)
                .withNamespace(pod.getMetadata().getNamespace())
                .endMetadata()
                .build();
    }
}
