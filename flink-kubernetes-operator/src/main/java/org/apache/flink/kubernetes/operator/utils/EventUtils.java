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

import java.time.Instant;
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
            Consumer<Event> eventListener) {
        var eventName = generateEventName(target, type, reason, message, component);

        var existing =
                client.v1()
                        .events()
                        .inNamespace(target.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();

        if (existing != null
                && existing.getType().equals(type.name())
                && existing.getReason().equals(reason)
                && existing.getInvolvedObject().getName().equals(target.getMetadata().getName())
                && existing.getInvolvedObject().getKind().equals(target.getKind())) {
            // update
            existing.setLastTimestamp(Instant.now().toString());
            existing.setCount(existing.getCount() + 1);
            existing.setMessage(message);
            client.v1()
                    .events()
                    .inNamespace(target.getMetadata().getNamespace())
                    .createOrReplace(existing);
            eventListener.accept(existing);
            return false;
        } else {
            var event =
                    new EventBuilder()
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
            client.v1().events().inNamespace(target.getMetadata().getNamespace()).create(event);
            eventListener.accept(event);
            return true;
        }
    }
}
