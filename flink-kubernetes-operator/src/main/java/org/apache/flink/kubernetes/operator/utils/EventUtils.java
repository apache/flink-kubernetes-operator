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

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * The util to generate an event for the target resource. It is copied from
 * https://github.com/EnMasseProject/enmasse/blob/master/k8s-api/src/main/java/io/enmasse/k8s/api/KubeEventLogger.java
 */
public class EventUtils {

    public static String generateEventName(
            HasMetadata target,
            EventRecorder.Type type,
            String reason,
            String message,
            EventRecorder.Component component) {
        return component
                + "."
                + ((reason
                                        + message
                                        + type
                                        + target.getKind()
                                        + target.getMetadata().getName()
                                        + target.getMetadata().getUid())
                                .hashCode()
                        & 0x7FFFFFFF);
    }

    public static boolean createOrUpdateEvent(
            KubernetesClient client,
            HasMetadata target,
            EventRecorder.Type type,
            String reason,
            String message,
            EventRecorder.Component component,
            Consumer<Event> eventListener,
            @Nullable String messageKey) {
        return createByInterval(
                client,
                target,
                type,
                reason,
                message,
                component,
                eventListener,
                messageKey,
                Duration.ofSeconds(0));
    }

    private static Event findExistingEvent(
            KubernetesClient client, HasMetadata target, String eventName) {
        return client.v1()
                .events()
                .inNamespace(target.getMetadata().getNamespace())
                .withName(eventName)
                .get();
    }

    public static boolean createIfNotExists(
            KubernetesClient client,
            HasMetadata target,
            EventRecorder.Type type,
            String reason,
            String message,
            EventRecorder.Component component,
            Consumer<Event> eventListener,
            @Nullable String messageKey) {

        String eventName =
                generateEventName(
                        target, type, reason, messageKey != null ? messageKey : message, component);
        Event existing = findExistingEvent(client, target, eventName);

        if (existing != null) {
            return false;
        } else {
            createNewEvent(
                    client, target, type, reason, message, component, eventListener, eventName);
            return true;
        }
    }

    public static boolean createByInterval(
            KubernetesClient client,
            HasMetadata target,
            EventRecorder.Type type,
            String reason,
            String message,
            EventRecorder.Component component,
            Consumer<Event> eventListener,
            @Nullable String messageKey,
            Duration interval) {

        String eventName =
                generateEventName(
                        target, type, reason, messageKey != null ? messageKey : message, component);
        Event existing = findExistingEvent(client, target, eventName);

        if (existing != null) {
            if (Objects.equals(existing.getMessage(), message)
                    && Instant.now()
                            .isBefore(
                                    Instant.parse(existing.getLastTimestamp())
                                            .plusMillis(interval.toMillis()))) {
                return false;
            } else {
                createUpdatedEvent(existing, client, message, eventListener);
                return false;
            }
        } else {
            createNewEvent(
                    client, target, type, reason, message, component, eventListener, eventName);
            return true;
        }
    }

    private static void createUpdatedEvent(
            Event existing,
            KubernetesClient client,
            String message,
            Consumer<Event> eventListener) {
        existing.setLastTimestamp(Instant.now().toString());
        existing.setCount(existing.getCount() + 1);
        existing.setMessage(message);
        eventListener.accept(client.resource(existing).createOrReplace());
    }

    private static void createNewEvent(
            KubernetesClient client,
            HasMetadata target,
            EventRecorder.Type type,
            String reason,
            String message,
            EventRecorder.Component component,
            Consumer<Event> eventListener,
            String eventName) {
        Event event = buildEvent(target, type, reason, message, component, eventName);
        eventListener.accept(client.resource(event).createOrReplace());
    }

    private static Event buildEvent(
            HasMetadata target,
            EventRecorder.Type type,
            String reason,
            String message,
            EventRecorder.Component component,
            String eventName) {
        return new EventBuilder()
                .withApiVersion("v1")
                .withInvolvedObject(
                        new ObjectReferenceBuilder()
                                .withKind(target.getKind())
                                .withUid(target.getMetadata().getUid())
                                .withName(target.getMetadata().getName())
                                .withNamespace(target.getMetadata().getNamespace())
                                .build())
                .withType(type.name())
                .withReason(reason)
                .withFirstTimestamp(Instant.now().toString())
                .withLastTimestamp(Instant.now().toString())
                .withNewSource()
                .withComponent(component.name())
                .endSource()
                .withCount(1)
                .withMessage(message)
                .withNewMetadata()
                .withName(eventName)
                .withNamespace(target.getMetadata().getNamespace())
                .endMetadata()
                .build();
    }
}
