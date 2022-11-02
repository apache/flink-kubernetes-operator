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
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.listener.AuditUtils;
import org.apache.flink.kubernetes.operator.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/** Helper class for status management and updates. */
public class StatusRecorder<
        CR extends AbstractFlinkResource<?, STATUS>, STATUS extends CommonStatus<?>> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusRecorder.class);

    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected final ConcurrentHashMap<Tuple2<String, String>, ObjectNode> statusCache =
            new ConcurrentHashMap<>();

    private final KubernetesClient client;
    private final MetricManager<CR> metricManager;
    private final BiConsumer<CR, STATUS> statusUpdateListener;

    public StatusRecorder(
            KubernetesClient client,
            MetricManager<CR> metricManager,
            BiConsumer<CR, STATUS> statusUpdateListener) {
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
     */
    @SneakyThrows
    public void patchAndCacheStatus(CR resource) {
        Class<CR> resourceClass = (Class<CR>) resource.getClass();
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
     */
    public void updateStatusFromCache(CR resource) {
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
            if (ResourceLifecycleState.CREATED.equals(resource.getStatus().getLifecycleState())) {
                statusUpdateListener.accept(resource, resource.getStatus());
            }
        }
        metricManager.onUpdate(resource);
    }

    /**
     * Remove cached status for Flink resource.
     *
     * @param resource Flink resource.
     */
    public void removeCachedStatus(CR resource) {
        statusCache.remove(getKey(resource));
        metricManager.onRemove(resource);
    }

    protected static Tuple2<String, String> getKey(HasMetadata resource) {
        return Tuple2.of(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    public static <S extends CommonStatus<?>, CR extends AbstractFlinkResource<?, S>>
            StatusRecorder<CR, S> create(
                    KubernetesClient kubernetesClient,
                    MetricManager<CR> metricManager,
                    Collection<FlinkResourceListener> listeners) {
        BiConsumer<CR, S> consumer =
                (resource, previousStatus) -> {
                    var now = Instant.now();
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

                                @Override
                                public Instant getTimestamp() {
                                    return now;
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
                    AuditUtils.logContext(ctx);
                };

        return new StatusRecorder<>(kubernetesClient, metricManager, consumer);
    }
}
