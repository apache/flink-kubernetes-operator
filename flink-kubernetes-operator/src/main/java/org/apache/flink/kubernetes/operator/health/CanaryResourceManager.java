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

package org.apache.flink.kubernetes.operator.health;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks the canary resources of one kind, labeled with {@link #CANARY_LABEL}, to verify that the
 * operator still picks up and reconciles spec changes. Each canary spec is periodically updated by
 * bumping its restart nonce, and the canary is reported unhealthy when the update is not reconciled
 * within the canary resource timeout, or when the update itself keeps failing. Canaries are never
 * deployed, their reconciliation stops at this class.
 */
@RequiredArgsConstructor
public class CanaryResourceManager<CR extends AbstractFlinkResource<?, ?>>
        implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CanaryResourceManager.class);

    public static final String CANARY_LABEL = "flink.apache.org/canary";

    @VisibleForTesting static final int MAX_FAILED_UPDATES = 3;

    private final ConcurrentHashMap<ResourceID, CanaryResourceState> canaryResources =
            new ConcurrentHashMap<>();

    @VisibleForTesting
    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    private final FlinkConfigManager configManager;

    public boolean allCanariesHealthy() {
        return canaryResources.values().stream().allMatch(cr -> cr.isHealthy);
    }

    public boolean handleCanaryResourceReconciliation(CR resource, KubernetesClient client) {
        var resourceId = ResourceID.fromResource(resource);
        if (!isCanaryResource(resource)) {
            if (canaryResources.remove(resourceId) != null) {
                LOG.info(
                        "Reconciled {} without the canary label. Stopping health checks",
                        resourceId);
            }
            return false;
        }

        LOG.info("Reconciling canary resource");

        var firstReconcile = new AtomicBoolean(false);
        var state =
                canaryResources.compute(
                        resourceId,
                        (id, previousState) -> {
                            if (previousState == null) {
                                firstReconcile.set(true);
                                previousState = new CanaryResourceState();
                                previousState.bumpedGeneration =
                                        resource.getMetadata().getGeneration();
                            }
                            previousState.onReconcile(resource);
                            return previousState;
                        });

        if (firstReconcile.get()) {
            updateSpecAndScheduleHealthCheck(resourceId, state, client);
        }

        return true;
    }

    public boolean handleCanaryResourceDeletion(CR resource) {
        if (!isCanaryResource(resource)) {
            return false;
        }

        var resourceId = ResourceID.fromResource(resource);
        LOG.info("Deleting canary resource");
        canaryResources.remove(resourceId);
        return true;
    }

    public void retainNamespaces(Set<String> namespaces) {
        // The operator switched to watching all namespaces
        if (namespaces.contains(Constants.WATCH_ALL_NAMESPACES)) {
            return;
        }

        for (var resourceId : canaryResources.keySet()) {
            var watched = resourceId.getNamespace().map(namespaces::contains).orElse(true);
            if (!watched && canaryResources.remove(resourceId) != null) {
                LOG.info(
                        "Namespace of canary resource {} is no longer watched. Stopping health checks",
                        resourceId);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void updateSpecAndScheduleHealthCheck(
            ResourceID resourceID, CanaryResourceState crs, KubernetesClient client) {
        var canaryTimeout =
                configManager
                        .getDefaultConfig()
                        .get(KubernetesOperatorConfigOptions.CANARY_RESOURCE_TIMEOUT);

        Long newGeneration = null;
        var deleted = false;
        var labelRemoved = false;
        try {
            var canaryResource =
                    client.resources((Class<CR>) crs.resource.getClass())
                            .inNamespace(resourceID.getNamespace().orElse(null))
                            .withName(resourceID.getName());
            var latest = canaryResource.get();
            if (latest == null) {
                deleted = true;
            } else if (!isCanaryResource(latest)) {
                labelRemoved = true;
            } else {
                var restartNonce = latest.getSpec().getRestartNonce();
                latest.getSpec().setRestartNonce(restartNonce == null ? 1L : restartNonce + 1);
                newGeneration = client.resource(latest).update().getMetadata().getGeneration();
            }
        } catch (KubernetesClientException e) {
            if (e.getCode() == 404) {
                deleted = true;
            } else {
                LOG.warn("Could not bump canary resource {}, will retry", resourceID, e);
            }
        } catch (Exception e) {
            LOG.warn("Could not bump canary resource {}, will retry", resourceID, e);
        }

        if (deleted) {
            LOG.info(
                    "Canary resource {} was deleted without cleanup. Stopping health checks",
                    resourceID);
            canaryResources.remove(resourceID, crs);
            return;
        }

        if (labelRemoved) {
            LOG.info(
                    "Canary label removed from {}, skipping the spec bump. Stopping health checks",
                    resourceID);
            canaryResources.remove(resourceID, crs);
            return;
        }

        if (newGeneration == null) {
            crs.failedBumps++;
        } else {
            crs.bumpedGeneration = newGeneration;
            crs.failedBumps = 0;
        }

        LOG.info("Scheduling canary check for {} in {}s", resourceID, canaryTimeout.toSeconds());

        executorService.schedule(
                () -> checkHealth(resourceID, client),
                canaryTimeout.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    protected void checkHealth(ResourceID resourceID, KubernetesClient client) {
        CanaryResourceState crs = canaryResources.get(resourceID);
        if (crs == null) {
            LOG.info("Canary resource {} not found. Stopping health checks", resourceID);
            return;
        }

        if (crs.failedBumps >= MAX_FAILED_UPDATES) {
            LOG.error(
                    "Canary deployment {} spec could not be updated in {} consecutive attempts",
                    resourceID,
                    crs.failedBumps);
            crs.isHealthy = false;
        } else if (crs.canaryReconciledSinceUpdate() && crs.failedBumps > 0) {
            LOG.warn(
                    "Canary deployment {} healthy, verified against its last successful spec update. The last {} spec update attempts failed",
                    resourceID,
                    crs.failedBumps);
            crs.isHealthy = true;
        } else if (crs.canaryReconciledSinceUpdate()) {
            LOG.info("Canary deployment healthy");
            crs.isHealthy = true;
        } else {
            // We did not reconcile since the last spec update within deadline
            LOG.error(
                    "Canary deployment {} latest spec not reconciled. Expected generation at least {}, received {}",
                    resourceID,
                    crs.bumpedGeneration,
                    crs.resource.getMetadata().getGeneration());
            crs.isHealthy = false;
        }

        updateSpecAndScheduleHealthCheck(resourceID, crs, client);
    }

    @VisibleForTesting
    public int getNumberOfActiveCanaries() {
        return canaryResources.size();
    }

    public static boolean isCanaryResource(HasMetadata resource) {
        var labels = resource.getMetadata().getLabels();
        if (labels == null) {
            return false;
        }
        return "true".equalsIgnoreCase(labels.getOrDefault(CANARY_LABEL, "false"));
    }

    /** Stops the scheduled canary health checks. */
    @Override
    public void close() {
        executorService.shutdownNow();
    }

    private class CanaryResourceState {
        CR resource;
        long bumpedGeneration;
        int failedBumps;
        boolean isHealthy = true;

        void onReconcile(CR cr) {
            resource = cr;
        }

        boolean canaryReconciledSinceUpdate() {
            return resource.getMetadata().getGeneration() >= bumpedGeneration;
        }
    }
}
