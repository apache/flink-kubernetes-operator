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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.HttpURLConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The util to generate an event for the target resource. It is copied from
 * https://github.com/EnMasseProject/enmasse/blob/master/k8s-api/src/main/java/io/enmasse/k8s/api/KubeEventLogger.java
 */
public class EventUtils {
    private static final Logger LOG = LoggerFactory.getLogger(EventUtils.class);

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

    public static boolean createOrUpdateEventWithInterval(
            KubernetesClient client,
            HasMetadata target,
            EventRecorder.Type type,
            String reason,
            String message,
            EventRecorder.Component component,
            Consumer<Event> eventListener,
            @Nullable String messageKey,
            @Nullable Duration interval) {
        return createOrUpdateEventWithLabels(
                client,
                target,
                type,
                reason,
                message,
                component,
                eventListener,
                messageKey,
                interval,
                null,
                Map.of());
    }

    public static Event findExistingEvent(
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
            Event event = buildEvent(target, type, reason, message, component, eventName);
            createOrReplaceEvent(client, event).ifPresent(eventListener);
            return true;
        }
    }

    public static boolean createOrUpdateEventWithLabels(
            KubernetesClient client,
            HasMetadata target,
            EventRecorder.Type type,
            String reason,
            String message,
            EventRecorder.Component component,
            Consumer<Event> eventListener,
            @Nullable String messageKey,
            @Nullable Duration interval,
            @Nullable Predicate<Map<String, String>> dedupePredicate,
            @Nullable Map<String, String> labels) {
        String eventName =
                generateEventName(
                        target, type, reason, messageKey != null ? messageKey : message, component);
        Event existing = findExistingEvent(client, target, eventName);

        if (existing != null) {
            if (labelCheck(existing, dedupePredicate) && intervalCheck(existing, interval)) {
                return false;
            }
            updatedEventWithLabels(existing, client, message, eventListener, labels);
            return false;
        } else {
            Event event = buildEvent(target, type, reason, message, component, eventName);
            setLabels(event, labels);
            createOrReplaceEvent(client, event).ifPresent(eventListener);
            return true;
        }
    }

    private static void updatedEventWithLabels(
            Event existing,
            KubernetesClient client,
            String message,
            Consumer<Event> eventListener,
            @Nullable Map<String, String> labels) {
        existing.setLastTimestamp(Instant.now().toString());
        existing.setCount(existing.getCount() + 1);
        existing.setMessage(message);
        setLabels(existing, labels);
        createOrReplaceEvent(client, existing).ifPresent(eventListener);
    }

    private static void setLabels(Event existing, @Nullable Map<String, String> labels) {
        if (labels == null) {
            return;
        }
        if (existing.getMetadata() == null) {
            existing.setMetadata(new ObjectMeta());
        }
        existing.getMetadata().setLabels(labels);
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
                .withInvolvedObject(getObjectReference(target))
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

    private static boolean intervalCheck(Event existing, @Nullable Duration interval) {
        return interval != null
                && Instant.now()
                        .isBefore(
                                Instant.parse(existing.getLastTimestamp())
                                        .plusMillis(interval.toMillis()));
    }

    private static boolean labelCheck(
            Event existing, Predicate<Map<String, String>> dedupePredicate) {
        return dedupePredicate == null
                || (existing.getMetadata() != null
                        && dedupePredicate.test(existing.getMetadata().getLabels()));
    }

    private static Optional<Event> createOrReplaceEvent(KubernetesClient client, Event event) {
        try {
            Event createdEvent = client.resource(event).createOrReplace();
            return Optional.of(createdEvent);
        } catch (KubernetesClientException e) {
            if (e.getCode() == HttpURLConnection.HTTP_FORBIDDEN) {
                // fail the reconcile when events cannot be delivered, unless FORBIDDEN
                // which can be a result of recoverable rbac issue, or namespace is terminating
                LOG.warn("Cannot create or update events, proceeding.", e);
            } else {
                throw e;
            }
        }
        return Optional.empty();
    }

    private static List<Event> getPodEvents(KubernetesClient client, Pod pod) {
        var ref = getObjectReference(pod);

        var eventList =
                client.v1()
                        .events()
                        .inNamespace(pod.getMetadata().getNamespace())
                        .withInvolvedObject(ref)
                        .list();

        if (eventList == null) {
            return new ArrayList<>();
        }

        var items = eventList.getItems();
        if (items == null) {
            return new ArrayList<>();
        }
        return items;
    }

    @VisibleForTesting
    protected static ObjectReference getObjectReference(HasMetadata resource) {
        var ref = new ObjectReference();
        ref.setApiVersion(resource.getApiVersion());
        ref.setKind(resource.getKind());
        ref.setName(resource.getMetadata().getName());
        ref.setNamespace(resource.getMetadata().getNamespace());
        ref.setUid(resource.getMetadata().getUid());
        return ref;
    }

    /**
     * Check that pod is stuck during volume mount stage and throw {@link DeploymentFailedException}
     * with the right reason message if that's the case.
     *
     * @param client Kubernetes client
     * @param pod Pod to be checked
     */
    public static void checkForVolumeMountErrors(KubernetesClient client, Pod pod) {
        var conditions = pod.getStatus().getConditions();
        if (conditions == null) {
            return;
        }
        var conditionMap =
                conditions.stream()
                        .collect(Collectors.toMap(PodCondition::getType, Function.identity()));

        // We use PodReadyToStartContainers if available otherwise use Initialized, but it's only
        // there k8s 1.29+
        boolean failedInitialization =
                checkStatusWasAlways(
                        pod,
                        conditionMap.getOrDefault(
                                "PodReadyToStartContainers", conditionMap.get("Initialized")),
                        "False");

        boolean notReady = checkStatusWasAlways(pod, conditionMap.get("Ready"), "False");

        if (notReady && failedInitialization) {
            getPodEvents(client, pod).stream()
                    .filter(e -> e.getReason().equals("FailedMount"))
                    .findAny()
                    .ifPresent(
                            e -> {
                                throw new DeploymentFailedException(e.getMessage(), e.getReason());
                            });
        }
    }

    private static boolean checkStatusWasAlways(Pod pod, PodCondition condition, String status) {
        return condition != null && condition.getStatus().equals(status);
    }
}
