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
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.exception.StatusConflictException;
import org.apache.flink.kubernetes.operator.listener.AuditUtils;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/** Helper class for status management and updates. */
public class StatusRecorder<CR extends CustomResource<?, STATUS>, STATUS> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusRecorder.class);

    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected final ConcurrentHashMap<ResourceID, ObjectNode> statusCache =
            new ConcurrentHashMap<>();

    private final MetricManager<CR> metricManager;
    private final BiConsumer<CR, STATUS> statusUpdateListener;

    public StatusRecorder(
            MetricManager<CR> metricManager, BiConsumer<CR, STATUS> statusUpdateListener) {
        this.statusUpdateListener = statusUpdateListener;
        this.metricManager = metricManager;
    }

    /**
     * Notifies status update listeners of changes made to a resource.
     *
     * @param resource resource that has been updated
     * @param prevStatus previous status of resource
     */
    public void notifyListeners(CR resource, STATUS prevStatus) {
        statusUpdateListener.accept(resource, prevStatus);
    }

    /**
     * Update the status of the provided kubernetes resource on the k8s cluster. We use patch
     * together with null resourceVersion to try to guarantee that the status update succeeds even
     * if the underlying resource spec was update in the meantime. This is necessary for the correct
     * operator behavior.
     *
     * @param resource Resource for which status update should be performed
     * @param client Kubernetes client to use for the update
     */
    @SneakyThrows
    public void patchAndCacheStatus(CR resource, KubernetesClient client) {
        ObjectNode newStatusNode =
                objectMapper.convertValue(resource.getStatus(), ObjectNode.class);
        var resourceId = ResourceID.fromResource(resource);
        ObjectNode previousStatusNode = statusCache.get(resourceId);

        if (newStatusNode.equals(previousStatusNode)) {
            LOG.debug("No status change.");
            return;
        }

        var prevStatus = convertPreviousStatus(resource, previousStatusNode);

        Exception err = null;
        for (int i = 0; i < 3; i++) {
            // We retry the status update 3 times to avoid some intermittent connectivity errors
            try {
                replaceStatus(resource, prevStatus, client);
                err = null;
            } catch (KubernetesClientException e) {
                LOG.error("Error while patching status, retrying {}/3...", (i + 1), e);
                Thread.sleep(1000);
                err = e;
            }
        }

        if (err != null) {
            throw err;
        }

        statusCache.put(resourceId, newStatusNode);
        statusUpdateListener.accept(resource, prevStatus);
        metricManager.onUpdate(resource);
    }

    private STATUS convertPreviousStatus(CR resource, ObjectNode previousStatusNode) {
        Class<?> statusClass;
        if (resource instanceof FlinkDeployment) {
            statusClass = FlinkDeploymentStatus.class;
        } else if (resource instanceof FlinkSessionJob) {
            statusClass = FlinkSessionJobStatus.class;
        } else if (resource instanceof FlinkStateSnapshot) {
            statusClass = FlinkStateSnapshotStatus.class;
        } else {
            throw new RuntimeException(
                    String.format("Resource is unknown class: %s", resource.getClass()));
        }
        return (STATUS) objectMapper.convertValue(previousStatusNode, statusClass);
    }

    private void replaceStatus(CR resource, STATUS prevStatus, KubernetesClient client)
            throws JsonProcessingException {
        int retries = 0;
        while (true) {
            try {
                var updated = client.resource(resource).lockResourceVersion().updateStatus();

                // If we successfully replaced the status, update the resource version so we know
                // what to lock next in the same reconciliation loop
                resource.getMetadata()
                        .setResourceVersion(updated.getMetadata().getResourceVersion());
                return;
            } catch (KubernetesClientException kce) {
                // 409 is the error code for conflicts resulting from the locking
                if (kce.getCode() == 409) {
                    handleLockingError(resource, prevStatus, client, retries, kce);
                    ++retries;
                } else {
                    // We simply throw non-conflict errors, to trigger retry with delay
                    throw kce;
                }
            }
        }
    }

    @VisibleForTesting
    void handleLockingError(
            CR resource,
            STATUS prevStatus,
            KubernetesClient client,
            int retries,
            KubernetesClientException kce)
            throws JsonProcessingException {

        var currentVersion = resource.getMetadata().getResourceVersion();
        LOG.debug("Could not apply status update for resource version {}", currentVersion);

        var latest = client.resource(resource).get();
        if (latest == null || latest.getMetadata() == null) {
            // This can happen occasionally, we throw the error to retry with delay.
            throw new KubernetesClientException(
                    String.format(
                            "Failed to retrieve latest %s",
                            latest == null ? "resource" : "metadata"),
                    kce);
        }

        var latestVersion = latest.getMetadata().getResourceVersion();
        if (currentVersion.equals(latestVersion)) {
            // This should not happen as long as the client works consistently
            LOG.error("Unable to fetch latest resource version");
            throw kce;
        }

        if (latest.getStatus().equals(prevStatus)) {
            if (retries < 3) {
                LOG.debug("Retrying status update for latest version {}", latestVersion);
                resource.getMetadata().setResourceVersion(latestVersion);
            } else {
                // If we cannot get the latest version in 3 tries we throw the error to
                // retry with delay
                throw kce;
            }
        } else {
            throw new StatusConflictException(
                    "Status have been modified externally in version "
                            + latestVersion
                            + " Previous: "
                            + objectMapper.writeValueAsString(prevStatus)
                            + " Latest: "
                            + objectMapper.writeValueAsString(latest.getStatus()));
        }
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
        var key = ResourceID.fromResource(resource);
        var cachedStatus = statusCache.get(key);
        if (cachedStatus != null) {
            resource.setStatus(
                    (STATUS)
                            objectMapper.convertValue(
                                    cachedStatus, resource.getStatus().getClass()));
        } else {
            // Initialize cache with current status copy
            statusCache.put(key, objectMapper.convertValue(resource.getStatus(), ObjectNode.class));
            if (resource.getStatus() instanceof CommonStatus<?>) {
                if (ResourceLifecycleState.CREATED.equals(
                        ((CommonStatus<?>) resource.getStatus()).getLifecycleState())) {
                    statusUpdateListener.accept(resource, resource.getStatus());
                }
            }
        }
        metricManager.onUpdate(resource);
    }

    /**
     * Clean up resource after deletion and send a last status update.
     *
     * @param resource Flink resource.
     */
    public void cleanupForDeletion(CR resource) {
        var prevJson = statusCache.remove(ResourceID.fromResource(resource));
        var prevStatus = convertPreviousStatus(resource, prevJson);
        metricManager.onRemove(resource);
        statusUpdateListener.accept(resource, prevStatus);
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

        return new StatusRecorder<>(metricManager, consumer);
    }

    public static StatusRecorder<FlinkStateSnapshot, FlinkStateSnapshotStatus>
            createForFlinkStateSnapshot(
                    KubernetesClient kubernetesClient,
                    MetricManager<FlinkStateSnapshot> metricManager,
                    Collection<FlinkResourceListener> listeners) {
        BiConsumer<FlinkStateSnapshot, FlinkStateSnapshotStatus> consumer =
                (resource, previousStatus) -> {
                    var now = Instant.now();
                    var ctx =
                            new FlinkResourceListener.FlinkStateSnapshotStatusUpdateContext() {
                                @Override
                                public FlinkStateSnapshotStatus getPreviousStatus() {
                                    return previousStatus;
                                }

                                @Override
                                public KubernetesClient getKubernetesClient() {
                                    return kubernetesClient;
                                }

                                @Override
                                public FlinkStateSnapshot getStateSnapshot() {
                                    return resource;
                                }

                                @Override
                                public Instant getTimestamp() {
                                    return now;
                                }
                            };

                    listeners.forEach(listener -> listener.onStateSnapshotStatusUpdate(ctx));
                    AuditUtils.logContext(ctx);
                };

        return new StatusRecorder<>(metricManager, consumer);
    }
}
