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

package org.apache.flink.kubernetes.operator.autoscaler.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** The ConfigMapStore persists state in Kubernetes ConfigMaps. */
public class ConfigMapStore {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesAutoScalerStateStore.class);

    public static final String LABEL_COMPONENT_AUTOSCALER = "autoscaler";

    private final KubernetesClient kubernetesClient;

    /**
     * Cache for Kubernetes ConfigMaps which reflects the latest state of a ConfigMap for a
     * ResourceId. Any changes to the ConfigMap are only reflected in Kubernetes once the flush()
     * method is called.
     */
    private final ConcurrentHashMap<ResourceID, ConfigMapView> cache = new ConcurrentHashMap<>();

    public ConfigMapStore(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    protected void putSerializedState(
            KubernetesJobAutoScalerContext jobContext, String key, String value) {
        getConfigMap(jobContext).put(key, value);
    }

    protected Optional<String> getSerializedState(
            KubernetesJobAutoScalerContext jobContext, String key) {
        return Optional.ofNullable(getConfigMap(jobContext).get(key));
    }

    protected void removeSerializedState(KubernetesJobAutoScalerContext jobContext, String key) {
        getConfigMap(jobContext).removeKey(key);
    }

    public void clearAll(KubernetesJobAutoScalerContext jobContext) {
        getConfigMap(jobContext).clear();
    }

    public void flush(KubernetesJobAutoScalerContext jobContext) {
        ConfigMapView configMapView = cache.get(jobContext.getJobKey());
        if (configMapView == null) {
            LOG.debug("The configMap doesn't exist, so skip the flush.");
            return;
        }
        try {
            configMapView.flush();
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

    private ConfigMapView getConfigMap(KubernetesJobAutoScalerContext jobContext) {
        return cache.computeIfAbsent(
                jobContext.getJobKey(), (id) -> getConfigMapFromKubernetes(jobContext));
    }

    @VisibleForTesting
    ConfigMapView getConfigMapFromKubernetes(KubernetesJobAutoScalerContext jobContext) {
        HasMetadata cr = jobContext.getResource();
        var meta = createCmObjectMeta(ResourceID.fromResource(cr));
        var configMapSkeleton = buildConfigMap(cr, meta);
        return new ConfigMapView(configMapSkeleton, kubernetesClient::resource);
    }

    private ObjectMeta createCmObjectMeta(ResourceID uid) {
        var objectMeta = new ObjectMeta();
        objectMeta.setName("autoscaler-" + uid.getName());
        uid.getNamespace().ifPresent(objectMeta::setNamespace);
        objectMeta.setLabels(getAutoscalerCmLabels(uid));
        return objectMeta;
    }

    @VisibleForTesting
    public static Map<String, String> getAutoscalerCmLabels(ResourceID uid) {
        return Map.of(
                Constants.LABEL_COMPONENT_KEY,
                LABEL_COMPONENT_AUTOSCALER,
                Constants.LABEL_APP_KEY,
                uid.getName());
    }

    private ConfigMap buildConfigMap(HasMetadata cr, ObjectMeta meta) {
        var cm = new ConfigMap();
        cm.setMetadata(meta);
        cm.addOwnerReference(cr);
        cm.setData(new HashMap<>());
        return cm;
    }

    @VisibleForTesting
    protected ConcurrentHashMap<ResourceID, ConfigMapView> getCache() {
        return cache;
    }
}
