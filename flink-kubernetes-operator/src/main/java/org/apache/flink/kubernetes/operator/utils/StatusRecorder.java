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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/** Helper class for status management and updates. */
public class StatusRecorder<STATUS extends CommonStatus<?>> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusRecorder.class);

    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected final ConcurrentHashMap<Tuple2<String, String>, ObjectNode> statusCache =
            new ConcurrentHashMap<>();

    private final KubernetesClient client;
    private final MetricManager<AbstractFlinkResource<?, STATUS>> metricManager;
    private final BiConsumer<AbstractFlinkResource<?, STATUS>, STATUS> statusUpdateListener;

    public StatusRecorder(
            KubernetesClient client,
            MetricManager<AbstractFlinkResource<?, STATUS>> metricManager,
            BiConsumer<AbstractFlinkResource<?, STATUS>, STATUS> statusUpdateListener) {
        this.client = client;
        this.statusUpdateListener = statusUpdateListener;
        this.metricManager = metricManager;
    }

    /**
     * Update the status of the provided kubernetes resource on the k8s cluster. We use patch
     * together with null resourceVersion to try to guarantee that the status update succeeds even
     * if the underlying resource spec was update in the meantime. This is necessary for the correct
     * operator behavior.
     *
     * @param resource Resource for which status update should be performed
     * @param <T> Resource type.
     */
    @SneakyThrows
    public <T extends AbstractFlinkResource<?, STATUS>> void patchAndCacheStatus(T resource) {
        Class<T> resourceClass = (Class<T>) resource.getClass();
        String namespace = resource.getMetadata().getNamespace();
        String name = resource.getMetadata().getName();

        // This is necessary so the client wouldn't fail of the underlying resource spec was updated
        // in the meantime
        resource.getMetadata().setResourceVersion(null);

        ObjectNode newStatusNode =
                objectMapper.convertValue(resource.getStatus(), ObjectNode.class);
        ObjectNode previousStatusNode = statusCache.put(getKey(resource), newStatusNode);

        if (newStatusNode.equals(previousStatusNode)) {
            LOG.debug("No status change.");
            return;
        }

        var statusClass =
                (resource instanceof FlinkDeployment)
                        ? FlinkDeploymentStatus.class
                        : FlinkSessionJobStatus.class;
        var prevStatus = (STATUS) objectMapper.convertValue(previousStatusNode, statusClass);

        Exception err = null;
        for (int i = 0; i < 3; i++) {
            // In any case we retry the status update 3 times to avoid some intermittent
            // connectivity errors if any
            try {
                client.resources(resourceClass)
                        .inNamespace(namespace)
                        .withName(name)
                        .patchStatus(resource);
                statusUpdateListener.accept(resource, prevStatus);
                metricManager.onUpdate(resource);
                return;
            } catch (Exception e) {
                LOG.error("Error while patching status, retrying {}/3...", (i + 1), e);
                Thread.sleep(1000);
                err = e;
            }
        }
        throw err;
    }

    /**
     * Update the custom resource status based on the in-memory cached to ensure that any status
     * updates that we made previously are always visible in the reconciliation loop. This is
     * required due to our custom status patching logic.
     *
     * <p>If the cache doesn't have a status stored, we do no update. This happens when the operator
     * reconciles a resource for the first time after a restart.
     *
     * @param resource Resource for which the status should be updated from the cache
     * @param <T> Custom resource type.
     */
    public <T extends AbstractFlinkResource<?, STATUS>> void updateStatusFromCache(T resource) {
        var key = getKey(resource);
        var cachedStatus = statusCache.get(key);
        if (cachedStatus != null) {
            resource.setStatus(
                    (STATUS)
                            objectMapper.convertValue(
                                    cachedStatus, resource.getStatus().getClass()));
        } else {
            // Initialize cache with current status copy
            statusCache.put(key, objectMapper.convertValue(resource.getStatus(), ObjectNode.class));
        }
        metricManager.onUpdate(resource);
    }

    /**
     * Remove cached status for Flink resource.
     *
     * @param resource Flink resource.
     * @param <T> Resource type.
     */
    public <T extends AbstractFlinkResource<?, STATUS>> void removeCachedStatus(T resource) {
        statusCache.remove(getKey(resource));
        metricManager.onRemove(resource);
    }

    protected static Tuple2<String, String> getKey(HasMetadata resource) {
        return Tuple2.of(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    public static <S extends CommonStatus<?>> StatusRecorder<S> create(
            KubernetesClient kubernetesClient,
            MetricManager<AbstractFlinkResource<?, S>> metricManager,
            Collection<FlinkResourceListener> listeners) {
        BiConsumer<AbstractFlinkResource<?, S>, S> consumer =
                (resource, previousStatus) -> {
                    var ctx =
                            new FlinkResourceListener.StatusUpdateContext() {
                                @Override
                                public S getPreviousStatus() {
                                    return previousStatus;
                                }

                                @Override
                                public AbstractFlinkResource<?, S> getFlinkResource() {
                                    return resource;
                                }

                                @Override
                                public KubernetesClient getKubernetesClient() {
                                    return kubernetesClient;
                                }
                            };

                    listeners.forEach(
                            listener -> {
                                if (resource instanceof FlinkDeployment) {
                                    listener.onDeploymentStatusUpdate(ctx);
                                } else {
                                    listener.onSessionJobStatusUpdate(ctx);
                                }
                            });
                };
        return new StatusRecorder<>(kubernetesClient, metricManager, consumer);
    }
}
