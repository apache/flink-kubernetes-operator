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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.AutoScalerStateStoreFactory;
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

/** The factory of kubernetes auto scaler state store. */
public class KubernetesAutoScalerStateStoreFactory implements AutoScalerStateStoreFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesAutoScalerStateStoreFactory.class);

    private static final String LABEL_COMPONENT_AUTOSCALER = "autoscaler";

    private final KubernetesClient kubernetesClient;

    private final AbstractFlinkResource<?, ?> cr;

    public KubernetesAutoScalerStateStoreFactory(
            KubernetesClient kubernetesClient, AbstractFlinkResource<?, ?> cr) {
        this.kubernetesClient = kubernetesClient;
        this.cr = cr;
    }

    @Override
    public Optional<AutoScalerStateStore> get() {
        return getScalingInfoConfigMap(createCmObjectMeta(ResourceID.fromResource(cr)))
                .map(configMap -> new KubernetesAutoScalerStateStore(kubernetesClient, configMap));
    }

    @Override
    public AutoScalerStateStore getOrCreate() {
        var meta = createCmObjectMeta(ResourceID.fromResource(cr));
        var info = getScalingInfoConfigMap(meta).orElseGet(() -> createConfigMap(cr, meta));

        return new KubernetesAutoScalerStateStore(kubernetesClient, info);
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
