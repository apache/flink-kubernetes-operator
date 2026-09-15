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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.listener.AuditUtils;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/** Helper class for creating Kubernetes events for Flink resources. */
public class EventRecorder {

    private static final Logger LOG = LoggerFactory.getLogger(EventRecorder.class);

    private final BiConsumer<AbstractFlinkResource<?, ?>, Event> eventListenerFlinkResource;
    private final BiConsumer<FlinkStateSnapshot, Event> eventListenerFlinkStateSnapshot;

    /**
     * Labels added to the metadata of every event created by this recorder.
     *
     * <p>Paths that do not set any dedupe labels themselves must pass this map as is, never null:
     * labels are replaced on every update, so passing the (possibly empty) map is what clears the
     * dedupe labels of a previous update. The autoscaler relies on this to re-emit a scaling report
     * within the dedupe interval after switching back to scaling enabled mode.
     */
    private final Map<String, String> commonLabels;

    public EventRecorder(
            BiConsumer<AbstractFlinkResource<?, ?>, Event> eventListenerFlinkResource,
            BiConsumer<FlinkStateSnapshot, Event> eventListenerFlinkStateSnapshot) {
        this(eventListenerFlinkResource, eventListenerFlinkStateSnapshot, Map.of());
    }

    public EventRecorder(
            BiConsumer<AbstractFlinkResource<?, ?>, Event> eventListenerFlinkResource,
            BiConsumer<FlinkStateSnapshot, Event> eventListenerFlinkStateSnapshot,
            Map<String, String> commonLabels) {
        this.eventListenerFlinkResource = eventListenerFlinkResource;
        this.eventListenerFlinkStateSnapshot = eventListenerFlinkStateSnapshot;
        this.commonLabels = Map.copyOf(commonLabels);
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
                null,
                commonLabels);
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
                null,
                commonLabels);
    }

    /**
     * @param resource The resource
     * @param type The type
     * @param reason the reason
     * @param message the message
     * @param component the component
     * @param messageKey the message key
     * @param client the client
     * @param interval Interval for dedupe. Null mean no dedupe.
     */
    public void triggerEventWithInterval(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component,
            String messageKey,
            KubernetesClient client,
            @Nullable Duration interval) {
        EventUtils.createOrUpdateEventWithInterval(
                client,
                resource,
                type,
                reason,
                message,
                component,
                e -> eventListenerFlinkResource.accept(resource, e),
                messageKey,
                interval,
                commonLabels);
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
                messageKey,
                mergeWithCommonLabels(null));
    }

    public boolean triggerEventWithAnnotations(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            Reason reason,
            String message,
            Component component,
            String messageKey,
            KubernetesClient client,
            Map<String, String> annotations) {
        return EventUtils.createWithAnnotations(
                client,
                resource,
                type,
                reason.toString(),
                message,
                component,
                e -> eventListenerFlinkResource.accept(resource, e),
                messageKey,
                annotations,
                mergeWithCommonLabels(null));
    }

    /**
     * @param resource The resource
     * @param type The type
     * @param reason the reason
     * @param message the message
     * @param component the component
     * @param messageKey the message key
     * @param client the client
     * @param interval Interval for dedupe. Null mean no dedupe.
     * @param dedupePredicate Predicate for dedupe algorithm..
     * @param labels Labels to store in meta data for dedupe. Do nothing if null.
     */
    public void triggerEventWithLabels(
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
        EventUtils.createOrUpdateEventWithLabels(
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
                mergeWithCommonLabels(labels));
    }

    /**
     * Merge the caller provided labels with the globally configured ones. Caller provided labels
     * take precedence as they may carry functional information such as dedupe state.
     *
     * @return the merged labels or null if there is nothing to set.
     */
    @Nullable
    private Map<String, String> mergeWithCommonLabels(@Nullable Map<String, String> labels) {
        if (commonLabels.isEmpty()) {
            return labels;
        }
        if (labels == null || labels.isEmpty()) {
            return commonLabels;
        }
        var merged = new HashMap<>(commonLabels);
        merged.putAll(labels);
        return merged;
    }

    /**
     * Drop label entries that Kubernetes would reject. A single invalid entry coming from the
     * operator config would otherwise fail the creation of every event.
     */
    @VisibleForTesting
    static Map<String, String> validateLabels(Map<String, String> labels) {
        var valid = new HashMap<String, String>();
        labels.forEach(
                (key, value) -> {
                    // Label keys follow the same syntax as annotation keys
                    if (!K8sAnnotationsSanitizer.isValidAnnotationKey(key)) {
                        LOG.warn("Ignoring event label with invalid key: {}", key);
                    } else if (!K8sAnnotationsSanitizer.isValidLabelValue(value)) {
                        LOG.warn("Ignoring event label {} with invalid value: {}", key, value);
                    } else {
                        valid.put(key, value);
                    }
                });
        return valid;
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
        return create(client, listeners, new Configuration());
    }

    public static EventRecorder create(
            KubernetesClient client,
            Collection<FlinkResourceListener> listeners,
            Configuration operatorConfig) {

        var commonLabels =
                validateLabels(
                        operatorConfig.get(KubernetesOperatorConfigOptions.OPERATOR_EVENT_LABELS));
        if (!commonLabels.isEmpty()) {
            LOG.info("Adding labels {} to all operator generated events", commonLabels);
        }

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

        return new EventRecorder(
                biConsumerFlinkResource, biConsumerFlinkStateSnapshot, commonLabels);
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
        SnapshotAbandoned,
        JobException,
        Error
    }
}
