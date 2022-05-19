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
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/** Helper class for status management and updates. */
public class StatusHelper<STATUS extends CommonStatus<?>> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusHelper.class);

    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected final ConcurrentHashMap<Tuple2<String, String>, ObjectNode> statusCache =
            new ConcurrentHashMap<>();

    private final KubernetesClient client;

    public StatusHelper(KubernetesClient client) {
        this.client = client;
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
    public <T extends CustomResource<?, STATUS>> void patchAndCacheStatus(T resource) {

        Class<T> resourceClass = (Class<T>) resource.getClass();
        String namespace = resource.getMetadata().getNamespace();
        String name = resource.getMetadata().getName();

        // This is necessary so the client wouldn't fail of the underlying resource spec was updated
        // in the meantime
        resource.getMetadata().setResourceVersion(null);

        ObjectNode newStatus = objectMapper.convertValue(resource.getStatus(), ObjectNode.class);
        ObjectNode previousStatus = statusCache.put(getKey(resource), newStatus);

        if (newStatus.equals(previousStatus)) {
            LOG.debug("No status change.");
            return;
        }

        Exception err = null;
        for (int i = 0; i < 3; i++) {
            // In any case we retry the status update 3 times to avoid some intermittent
            // connectivity errors if any
            try {
                client.resources(resourceClass)
                        .inNamespace(namespace)
                        .withName(name)
                        .patchStatus(resource);
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
    public <T extends CustomResource<?, STATUS>> void updateStatusFromCache(T resource) {
        var cachedStatus = statusCache.get(getKey(resource));
        if (cachedStatus != null) {
            resource.setStatus(
                    (STATUS)
                            objectMapper.convertValue(
                                    cachedStatus, resource.getStatus().getClass()));
        }
    }

    /** Remove cached status for Flink resource. */
    public <T extends CustomResource<?, STATUS>> void removeCachedStatus(T resource) {
        statusCache.remove(getKey(resource));
    }

    protected static Tuple2<String, String> getKey(HasMetadata resource) {
        return Tuple2.of(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }
}
