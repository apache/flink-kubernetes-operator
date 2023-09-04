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
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Logic encapsulating canary tests. */
@RequiredArgsConstructor
public class CanaryResourceManager<CR extends AbstractFlinkResource<?, ?>> {

    private static final Logger LOG = LoggerFactory.getLogger(CanaryResourceManager.class);

    public static final String CANARY_LABEL = "flink.apache.org/canary";

    private final ConcurrentHashMap<ResourceID, CanaryResourceState> canaryResources =
            new ConcurrentHashMap<>();

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

    private final FlinkConfigManager configManager;

    public boolean allCanariesHealthy() {
        return canaryResources.values().stream().allMatch(cr -> cr.isHealthy);
    }

    public boolean handleCanaryResourceReconciliation(CR resource, KubernetesClient client) {
        if (!isCanaryResource(resource)) {
            return false;
        }

        var resourceId = ResourceID.fromResource(resource);

        LOG.info("Reconciling canary resource");

        canaryResources.compute(
                resourceId,
                (id, previousState) -> {
                    boolean firstReconcile = false;
                    if (previousState == null) {
                        firstReconcile = true;
                        previousState = new CanaryResourceState();
                    }
                    previousState.onReconcile(resource);
                    if (firstReconcile) {
                        updateSpecAndScheduleHealthCheck(resourceId, previousState, client);
                    }
                    return previousState;
                });

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

    private void updateSpecAndScheduleHealthCheck(
            ResourceID resourceID, CanaryResourceState crs, KubernetesClient client) {
        var canaryTimeout =
                configManager
                        .getDefaultConfig()
                        .get(KubernetesOperatorConfigOptions.CANARY_RESOURCE_TIMEOUT);

        Long restartNonce = crs.resource.getSpec().getRestartNonce();
        crs.resource.getSpec().setRestartNonce(restartNonce == null ? 1L : restartNonce + 1);
        crs.previousGeneration = crs.resource.getMetadata().getGeneration();

        LOG.info("Scheduling canary check for {} in {}s", resourceID, canaryTimeout.toSeconds());

        try {
            client.resource(ReconciliationUtils.clone(crs.resource)).replace();
        } catch (Throwable t) {
            LOG.warn("Could not bump canary deployment, it may have been deleted", t);
        }

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

        // We did not reconcile since the last spec update within deadline
        if (crs.canaryReconciledSinceUpdate()) {
            LOG.info("Canary deployment healthy");
            crs.isHealthy = true;
        } else {
            LOG.error(
                    "Canary deployment {} latest spec not reconciled. Expected generation larger than {}, received {}",
                    resourceID,
                    crs.previousGeneration,
                    crs.resource.getMetadata().getGeneration());
            crs.isHealthy = false;
        }

        // Update spec and reschedule health check
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

    private class CanaryResourceState {
        CR resource;

        long previousGeneration;

        boolean isHealthy = true;

        void onReconcile(CR cr) {
            resource = cr;
        }

        boolean canaryReconciledSinceUpdate() {
            return resource.getMetadata().getGeneration() > previousGeneration;
        }
    }
}
