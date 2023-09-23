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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** The ConfigMapStore persists state in Kubernetes ConfigMaps. */
public class ConfigMapStore {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesAutoScalerStateStore.class);

    private static final String LABEL_COMPONENT_AUTOSCALER = "autoscaler";

    private final KubernetesClient kubernetesClient;

    // The cache for each resourceId may be in three situations:
    // 1. The resourceId isn't exist : ConfigMap isn't loaded from kubernetes, or it's removed.
    // 2. The resourceId is exist, and value is the Optional.empty() : We have loaded the ConfigMap
    // from kubernetes, but the ConfigMap isn't created at kubernetes side.
    // 3. The resourceId is exist, and the Optional isn't empty : We have loaded the ConfigMap from
    // kubernetes, it may be not same with kubernetes side due to it's not flushed after updating.
    private final ConcurrentHashMap<ResourceID, Optional<ConfigMap>> cache =
            new ConcurrentHashMap<>();

    public ConfigMapStore(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    protected void putSerializedState(
            KubernetesJobAutoScalerContext jobContext, String key, String value) {
        getOrCreateState(jobContext).put(key, value);
    }

    protected Optional<String> getSerializedState(
            KubernetesJobAutoScalerContext jobContext, String key) {
        return getConfigMap(jobContext).map(configMap -> configMap.getData().get(key));
    }

    protected void removeSerializedState(KubernetesJobAutoScalerContext jobContext, String key) {
        getConfigMap(jobContext)
                .ifPresentOrElse(
                        configMap -> configMap.getData().remove(key),
                        () -> {
                            throw new IllegalStateException(
                                    "The configMap isn't created, so the remove is unavailable.");
                        });
    }

    public void flush(KubernetesJobAutoScalerContext jobContext) {
        Optional<ConfigMap> configMapOpt = cache.get(jobContext.getJobKey());
        if (configMapOpt == null || configMapOpt.isEmpty()) {
            LOG.debug("The configMap isn't updated, so skip the flush.");
            return;
        }
        try {
            cache.put(
                    jobContext.getJobKey(),
                    Optional.of(kubernetesClient.resource(configMapOpt.get()).update()));
        } catch (Exception e) {
            LOG.error(
                    "Error while updating autoscaler info configmap, invalidating to clear the cache",
                    e);
            removeInfoFromCache(jobContext.getJobKey());
            throw e;
        }
    }

    public void removeInfoFromCache(ResourceID resourceID) {
        cache.remove(resourceID);
    }

    private Optional<ConfigMap> getConfigMap(KubernetesJobAutoScalerContext jobContext) {
        return cache.computeIfAbsent(
                jobContext.getJobKey(), (id) -> getConfigMapFromKubernetes(jobContext));
    }

    private Map<String, String> getOrCreateState(KubernetesJobAutoScalerContext jobContext) {
        return cache.compute(
                        jobContext.getJobKey(),
                        (id, configMapOpt) -> {
                            // If in the cache and valid simply return
                            if (configMapOpt != null && configMapOpt.isPresent()) {
                                return configMapOpt;
                            }
                            // Otherwise get or create
                            return Optional.of(getOrCreateConfigMapFromKubernetes(jobContext));
                        })
                .get()
                .getData();
    }

    @VisibleForTesting
    protected Optional<ConfigMap> getConfigMapFromKubernetes(
            KubernetesJobAutoScalerContext jobContext) {
        HasMetadata cr = jobContext.getResource();
        var meta = createCmObjectMeta(ResourceID.fromResource(cr));
        return getScalingInfoConfigMap(meta);
    }

    @Nonnull
    private ConfigMap getOrCreateConfigMapFromKubernetes(
            KubernetesJobAutoScalerContext jobContext) {
        HasMetadata cr = jobContext.getResource();
        var meta = createCmObjectMeta(ResourceID.fromResource(cr));
        return getScalingInfoConfigMap(meta).orElseGet(() -> createConfigMap(cr, meta));
    }

    private ConfigMap createConfigMap(HasMetadata cr, ObjectMeta meta) {
        LOG.info("Creating scaling info config map");
        var cm = new ConfigMap();
        cm.setMetadata(meta);
        cm.addOwnerReference(cr);
        cm.setData(new HashMap<>());
        return kubernetesClient.resource(cm).create();
    }

    private ObjectMeta createCmObjectMeta(ResourceID uid) {
        var objectMeta = new ObjectMeta();
        objectMeta.setName("autoscaler-" + uid.getName());
        uid.getNamespace().ifPresent(objectMeta::setNamespace);
        objectMeta.setLabels(
                Map.of(
                        Constants.LABEL_COMPONENT_KEY,
                        LABEL_COMPONENT_AUTOSCALER,
                        Constants.LABEL_APP_KEY,
                        uid.getName()));
        return objectMeta;
    }

    private Optional<ConfigMap> getScalingInfoConfigMap(ObjectMeta objectMeta) {
        return Optional.ofNullable(
                kubernetesClient
                        .configMaps()
                        .inNamespace(objectMeta.getNamespace())
                        .withName(objectMeta.getName())
                        .get());
    }

    @VisibleForTesting
    protected ConcurrentHashMap<ResourceID, Optional<ConfigMap>> getCache() {
        return cache;
    }
}
