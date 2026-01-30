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

package org.apache.flink.kubernetes.operator.api.bluegreen;

import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/** Simple Kubernetes service proxy for Gate operations. */
public class GateKubernetesService implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(GateKubernetesService.class);

    @Getter private final KubernetesClient kubernetesClient;

    private final String namespace;
    private final String configMapName;

    public GateKubernetesService(String namespace, String configMapName) {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(configMapName);

        try {
            kubernetesClient = new KubernetesClientBuilder().build();
        } catch (Exception e) {
            logger.error("Error instantiating Kubernetes Client", e);
            throw e;
        }

        this.namespace = namespace;
        this.configMapName = configMapName;
    }

    public void setInformers(ResourceEventHandler<ConfigMap> resourceEventHandler) {
        kubernetesClient
                .configMaps()
                .inNamespace(namespace)
                .withName(configMapName)
                .inform(resourceEventHandler, 0);
    }

    public void updateConfigMapEntries(Map<String, String> kvps) {
        var configMap = parseConfigMap();

        kvps.forEach((key, value) -> configMap.getData().put(key, value));

        try {
            kubernetesClient.configMaps().inNamespace(namespace).resource(configMap).update();
        } catch (Exception e) {
            logger.error("Failed to UPDATE the ConfigMap", e);
            throw e;
        }
    }

    public ConfigMap parseConfigMap() {
        try {
            return kubernetesClient
                    .configMaps()
                    .inNamespace(namespace)
                    .withName(configMapName)
                    .get();
        } catch (Exception e) {
            logger.error("Failed to GET the ConfigMap", e);
            throw e;
        }
    }
}
