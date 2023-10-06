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

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.Consumer;

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
                EventUtils.createOrUpdateEvent(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        consumer,
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
                EventUtils.createOrUpdateEvent(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        consumer,
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
                EventUtils.createOrUpdateEvent(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        null,
                        EventRecorder.Component.Operator,
                        consumer,
                        null));
    }

    @Test
    public void testCreateUpdatedEvent() {
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
                EventUtils.createByInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        consumer,
                        null,
                        Duration.ofSeconds(1800)));
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
                EventUtils.createByInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        consumer,
                        null,
                        Duration.ofSeconds(1800)));
        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertNull(eventConsumed);
        Assertions.assertEquals(1, event.getCount());

        Assertions.assertTrue(
                EventUtils.createByInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        null,
                        EventRecorder.Component.Operator,
                        consumer,
                        null,
                        Duration.ofSeconds(1800)));
    }

    @Test
    public void testCreateWithMessageKey() {
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
                EventUtils.createOrUpdateEvent(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message1",
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
        Assertions.assertEquals("message1", event.getMessage());
        Assertions.assertEquals(1, event.getCount());

        Assertions.assertFalse(
                EventUtils.createOrUpdateEvent(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message2",
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
        Assertions.assertNotNull(event);
        Assertions.assertEquals("message2", event.getMessage());
        Assertions.assertEquals(2, event.getCount());
    }

    @Test
    public void testCreateByIntervalWithMessageKey() {
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

        eventConsumed = null;
        Assertions.assertTrue(
                EventUtils.createByInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message1",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk",
                        Duration.ofSeconds(1800)));
        var event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertEquals(eventConsumed, event);
        Assertions.assertEquals("message1", event.getMessage());
        Assertions.assertEquals(1, event.getCount());

        eventConsumed = null;
        Assertions.assertFalse(
                EventUtils.createByInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message2",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk",
                        Duration.ofSeconds(1800)));

        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertEquals(eventConsumed, event);
        Assertions.assertEquals("message2", event.getMessage());
        Assertions.assertEquals(2, event.getCount());

        eventConsumed = null;
        Assertions.assertFalse(
                EventUtils.createByInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message2",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk",
                        Duration.ofSeconds(1800)));

        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertNull(eventConsumed);

        eventConsumed = null;
        Assertions.assertTrue(
                EventUtils.createByInterval(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        "message2",
                        EventRecorder.Component.Operator,
                        consumer,
                        "mk2",
                        Duration.ofSeconds(1800)));
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
        Assertions.assertEquals(eventConsumed, event);
        Assertions.assertEquals("message2", event.getMessage());
        Assertions.assertEquals(1, event.getCount());
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
}
