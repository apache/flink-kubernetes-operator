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
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.listener.AuditUtils;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.function.BiConsumer;

/** Helper class for creating Kubernetes events for Flink resources. */
public class EventRecorder {

    private final KubernetesClient client;
    private final BiConsumer<AbstractFlinkResource<?, ?>, Event> eventListener;

    public EventRecorder(
            KubernetesClient client, BiConsumer<AbstractFlinkResource<?, ?>, Event> eventListener) {
        this.client = client;
        this.eventListener = eventListener;
    }

    public boolean triggerEvent(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            Reason reason,
            Component component,
            String message) {
        return triggerEvent(resource, type, reason, component, message, null);
    }

    public boolean triggerEventOnce(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            Reason reason,
            Component component,
            String message,
            String messageKey) {
        return triggerEventOnce(resource, type, reason.toString(), message, component, messageKey);
    }

    public boolean triggerEvent(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            Reason reason,
            Component component,
            String message,
            @Nullable String messageKey) {
        return triggerEvent(resource, type, reason.toString(), message, component, messageKey);
    }

    public boolean triggerEvent(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component,
            String messageKey) {
        return EventUtils.createOrUpdateEvent(
                client,
                resource,
                type,
                reason,
                message,
                component,
                e -> eventListener.accept(resource, e),
                messageKey);
    }

    public boolean triggerEventOnce(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component,
            String messageKey) {
        return EventUtils.createIfNotExists(
                client,
                resource,
                type,
                reason,
                message,
                component,
                e -> eventListener.accept(resource, e),
                messageKey);
    }

    public boolean triggerEvent(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component) {
        return triggerEvent(resource, type, reason, message, component, null);
    }

    public static EventRecorder create(
            KubernetesClient client, Collection<FlinkResourceListener> listeners) {

        BiConsumer<AbstractFlinkResource<?, ?>, Event> biConsumer =
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

        return new EventRecorder(client, biConsumer);
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
        Job
    }

    /** The reason codes of events. */
    public enum Reason {
        Suspended,
        SpecChanged,
        Rollback,
        Submit,
        JobStatusChanged,
        SavepointError,
        Cleanup,
        CleanupFailed,
        Missing,
        ValidationError,
        RecoverDeployment,
        RestartUnhealthyJob,
        ScalingReport,
        Scaling
    }
}
