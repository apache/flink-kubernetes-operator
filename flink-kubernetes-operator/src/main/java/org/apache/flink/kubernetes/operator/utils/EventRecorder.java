/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.listener.AuditUtils;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/** Helper class for creating Kubernetes events for Flink resources. */
public class EventRecorder {

    private final BiConsumer<AbstractFlinkResource<?, ?>, Event> eventListenerFlinkResource;
    private final BiConsumer<FlinkStateSnapshot, Event> eventListenerFlinkStateSnapshot;

    public EventRecorder(
            BiConsumer<AbstractFlinkResource<?, ?>, Event> eventListenerFlinkResource,
            BiConsumer<FlinkStateSnapshot, Event> eventListenerFlinkStateSnapshot) {
        this.eventListenerFlinkResource = eventListenerFlinkResource;
        this.eventListenerFlinkStateSnapshot = eventListenerFlinkStateSnapshot;
    }

    public boolean triggerSnapshotEvent(
            FlinkStateSnapshot resource,
            Type type,
            Reason reason,
            Component component,
            String message,
            KubernetesClient client) {
        return EventUtils.createOrUpdateEventWithInterval(
                client,
                resource,
                type,
                reason.toString(),
                message,
                component,
                e -> eventListenerFlinkStateSnapshot.accept(resource, e),
                null,
                null);
    }

    public boolean triggerEvent(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            Reason reason,
            Component component,
            String message,
            KubernetesClient client) {
        return triggerEvent(resource, type, reason, message, component, null, client);
    }

    public boolean triggerEventOnce(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            Reason reason,
            String message,
            Component component,
            String messageKey,
            KubernetesClient client) {
        return triggerEventOnce(
                resource, type, reason.toString(), message, component, messageKey, client);
    }

    public boolean triggerEvent(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            Reason reason,
            String message,
            Component component,
            @Nullable String messageKey,
            KubernetesClient client) {
        return triggerEvent(
                resource, type, reason.toString(), message, component, messageKey, client);
    }

    public boolean triggerEvent(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component,
            String messageKey,
            KubernetesClient client) {
        return EventUtils.createOrUpdateEventWithInterval(
                client,
                resource,
                type,
                reason,
                message,
                component,
                e -> eventListenerFlinkResource.accept(resource, e),
                messageKey,
                null);
    }

    /**
     * @param interval Interval for dedupe. Null mean no dedupe.
     * @return
     */
    public boolean triggerEventWithInterval(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component,
            String messageKey,
            KubernetesClient client,
            @Nullable Duration interval) {
        return EventUtils.createOrUpdateEventWithInterval(
                client,
                resource,
                type,
                reason,
                message,
                component,
                e -> eventListenerFlinkResource.accept(resource, e),
                messageKey,
                interval);
    }

    public boolean triggerEventOnce(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component,
            String messageKey,
            KubernetesClient client) {
        return EventUtils.createIfNotExists(
                client,
                resource,
                type,
                reason,
                message,
                component,
                e -> eventListenerFlinkResource.accept(resource, e),
                messageKey);
    }

    /**
     * @param interval Interval for dedupe. Null mean no dedupe.
     * @param dedupePredicate Predicate for dedupe algorithm..
     * @param labels Labels to store in meta data for dedupe. Do nothing if null.
     * @return
     */
    public boolean triggerEventWithLabels(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component,
            @Nullable String messageKey,
            KubernetesClient client,
            @Nullable Duration interval,
            @Nullable Predicate<Map<String, String>> dedupePredicate,
            @Nullable Map<String, String> labels) {
        return EventUtils.createOrUpdateEventWithLabels(
                client,
                resource,
                type,
                reason,
                message,
                component,
                e -> eventListenerFlinkResource.accept(resource, e),
                messageKey,
                interval,
                dedupePredicate,
                labels);
    }

    public boolean triggerEvent(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component,
            KubernetesClient client) {
        return triggerEvent(resource, type, reason, message, component, null, client);
    }

    public static EventRecorder create(
            KubernetesClient client, Collection<FlinkResourceListener> listeners) {

        BiConsumer<AbstractFlinkResource<?, ?>, Event> biConsumerFlinkResource =
                (resource, event) -> {
                    var ctx =
                            new FlinkResourceListener.ResourceEventContext() {
                                @Override
                                public Event getEvent() {
                                    return event;
                                }

                                @Override
                                public AbstractFlinkResource<?, ?> getFlinkResource() {
                                    return resource;
                                }

                                @Override
                                public KubernetesClient getKubernetesClient() {
                                    return client;
                                }
                            };
                    listeners.forEach(
                            listener -> {
                                if (resource instanceof FlinkDeployment) {
                                    listener.onDeploymentEvent(ctx);
                                } else {
                                    listener.onSessionJobEvent(ctx);
                                }
                            });
                    AuditUtils.logContext(ctx);
                };

        BiConsumer<FlinkStateSnapshot, Event> biConsumerFlinkStateSnapshot =
                (resource, event) -> {
                    var ctx =
                            new FlinkResourceListener.FlinkStateSnapshotEventContext() {
                                @Override
                                public Event getEvent() {
                                    return event;
                                }

                                @Override
                                public FlinkStateSnapshot getStateSnapshot() {
                                    return resource;
                                }

                                @Override
                                public KubernetesClient getKubernetesClient() {
                                    return client;
                                }
                            };
                    listeners.forEach(listener -> listener.onStateSnapshotEvent(ctx));
                    AuditUtils.logContext(ctx);
                };

        return new EventRecorder(biConsumerFlinkResource, biConsumerFlinkStateSnapshot);
    }

    /** The type of the events. */
    public enum Type {
        Normal,
        Warning
    }

    /** The component of events. */
    public enum Component {
        Operator,
        JobManagerDeployment,
        Job,
        Snapshot
    }

    /** The reason codes of events. */
    public enum Reason {
        Suspended,
        SpecChanged,
        Rollback,
        Submit,
        JobStatusChanged,
        SavepointError,
        CheckpointError,
        Cleanup,
        CleanupFailed,
        Missing,
        ValidationError,
        RecoverDeployment,
        RestartUnhealthyJob,
        ScalingReport,
        IneffectiveScaling,
        MemoryPressure,
        ResourceQuotaReached,
        AutoscalerError,
        Scaling,
        UnsupportedFlinkVersion,
        SnapshotError,
        SnapshotAbandoned
    }
}
