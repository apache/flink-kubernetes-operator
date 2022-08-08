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

import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.listener.FlinkResourceListener;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;

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
        return triggerEvent(resource, type, reason.toString(), message, component);
    }

    public boolean triggerEvent(
            AbstractFlinkResource<?, ?> resource,
            Type type,
            String reason,
            String message,
            Component component) {
        return EventUtils.createOrUpdateEvent(
                client,
                resource,
                type,
                reason,
                message,
                component,
                e -> eventListener.accept(resource, e));
    }

    public static EventRecorder create(
            KubernetesClient client, Collection<FlinkResourceListener> listeners) {

        BiConsumer<AbstractFlinkResource<?, ?>, Event> biConsumer =
                (resource, event) ->
                        listeners.forEach(
                                listener -> {
                                    var ctx =
                                            new FlinkResourceListener.ResourceEventContext() {
                                                @Override
                                                public Event getEvent() {
                                                    return event;
                                                }

                                                @Override
                                                public AbstractFlinkResource<?, ?>
                                                        getFlinkResource() {
                                                    return resource;
                                                }

                                                @Override
                                                public KubernetesClient getKubernetesClient() {
                                                    return client;
                                                }
                                            };

                                    if (resource instanceof FlinkDeployment) {
                                        listener.onDeploymentEvent(ctx);
                                    } else {
                                        listener.onSessionJobEvent(ctx);
                                    }
                                });
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
        StatusChanged,
        SavepointError,
        Cleanup,
        Missing,
        ValidationError
    }
}
