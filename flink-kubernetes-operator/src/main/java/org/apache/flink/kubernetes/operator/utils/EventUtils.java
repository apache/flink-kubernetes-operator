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

import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.time.Instant;

/** The util to generate an event for the target resource. */
public class EventUtils {

    /** The type of the events. */
    public enum Type {
        Normal,
        Warning
    }

    /** The component of events. */
    public enum Component {
        Operator
    }

    public static String generateEventName(
            HasMetadata target, String type, String reason, String message, String componentName) {
        return componentName
                + "."
                + ((reason + message + type + target.getKind() + target.getMetadata().getName())
                                .hashCode()
                        & 0x7FFFFFFF);
    }

    public static boolean createOrUpdateEvent(
            KubernetesClient client,
            HasMetadata target,
            String type,
            String reason,
            String message,
            String componentName) {
        var eventName = generateEventName(target, type, reason, message, componentName);

        var existing =
                client.v1()
                        .events()
                        .inNamespace(target.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();

        if (existing != null
                && existing.getType().equals(type)
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
                            .withType(type)
                            .withReason(reason)
                            .withFirstTimestamp(Instant.now().toString())
                            .withLastTimestamp(Instant.now().toString())
                            .withNewSource()
                            .withComponent(componentName)
                            .endSource()
                            .withCount(1)
                            .withMessage(message)
                            .withNewMetadata()
                            .withName(eventName)
                            .withNamespace(target.getMetadata().getNamespace())
                            .endMetadata()
                            .build();
            client.v1().events().inNamespace(target.getMetadata().getNamespace()).create(event);
            return true;
        }
    }
}
