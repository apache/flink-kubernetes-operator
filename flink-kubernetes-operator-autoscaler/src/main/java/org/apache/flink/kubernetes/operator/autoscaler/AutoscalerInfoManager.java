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
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
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

/** Class responsible to managing the creation and retrieval of {@link AutoScalerInfo} objects. */
public class AutoscalerInfoManager {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerInfoManager.class);
    private static final String LABEL_COMPONENT_AUTOSCALER = "autoscaler";

    private final ConcurrentHashMap<ResourceID, Optional<AutoScalerInfo>> cache =
            new ConcurrentHashMap<>();
    private final KubernetesClient kubernetesClient;

    public AutoscalerInfoManager(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    public AutoScalerInfo getOrCreateInfo(AbstractFlinkResource<?, ?> cr) {
        return cache.compute(
                        ResourceID.fromResource(cr),
                        (id, infOpt) -> {
                            // If in the cache and valid simply return
                            if (infOpt != null
                                    && infOpt.map(AutoScalerInfo::isValid).orElse(false)) {
                                return infOpt;
                            }
                            // Otherwise get or create
                            return Optional.of(getOrCreateInternal(cr));
                        })
                .get();
    }

    public Optional<AutoScalerInfo> getInfo(AbstractFlinkResource<?, ?> cr) {
        return cache.compute(
                ResourceID.fromResource(cr),
                (id, infOpt) -> {
                    // If in the cache and empty or valid simply return
                    if (infOpt != null && (infOpt.isEmpty() || infOpt.get().isValid())) {
                        return infOpt;
                    }

                    // Otherwise get
                    return getScalingInfoConfigMap(createCmObjectMeta(ResourceID.fromResource(cr)))
                            .map(AutoScalerInfo::new);
                });
    }

    public void removeInfoFromCache(AbstractFlinkResource<?, ?> cr) {
        // We don't need to remove from Kubernetes, that is handled through the owner reference
        cache.remove(ResourceID.fromResource(cr));
    }

    @VisibleForTesting
    protected Optional<ConfigMap> getInfoFromKubernetes(AbstractFlinkResource<?, ?> cr) {
        return getScalingInfoConfigMap(createCmObjectMeta(ResourceID.fromResource(cr)));
    }

    private AutoScalerInfo getOrCreateInternal(HasMetadata cr) {
        var meta = createCmObjectMeta(ResourceID.fromResource(cr));
        var info = getScalingInfoConfigMap(meta).orElseGet(() -> createConfigMap(cr, meta));

        return new AutoScalerInfo(info);
    }

    private ConfigMap createConfigMap(HasMetadata cr, ObjectMeta meta) {
        LOG.info("Creating scaling info config map");
        var cm = new ConfigMap();
        cm.setMetadata(meta);
        cm.addOwnerReference(cr);
        cm.setData(new HashMap<>());
        return kubernetesClient.resource(cm).create();
    }

    private static ObjectMeta createCmObjectMeta(ResourceID uid) {
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
}
