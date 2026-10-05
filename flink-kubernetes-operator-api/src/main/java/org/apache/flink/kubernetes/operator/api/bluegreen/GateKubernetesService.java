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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.ACTIVE_DEPLOYMENT_TYPE;

/** Simple Kubernetes service proxy for Gate operations. */
public class GateKubernetesService implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(GateKubernetesService.class);

    // A round of concurrent writers ends with one success; the others find it on their next read.
    private static final int COMPARE_AND_SET_ATTEMPTS = 10;

    @Getter private final KubernetesClient kubernetesClient;

    private final String namespace;
    private final String configMapName;

    public GateKubernetesService(String namespace, String configMapName) {
        this(buildClient(), namespace, configMapName);
    }

    @VisibleForTesting
    public GateKubernetesService(
            KubernetesClient kubernetesClient, String namespace, String configMapName) {
        Preconditions.checkNotNull(namespace);
        Preconditions.checkNotNull(configMapName);

        this.kubernetesClient = kubernetesClient;
        this.namespace = namespace;
        this.configMapName = configMapName;
    }

    private static KubernetesClient buildClient() {
        try {
            return new KubernetesClientBuilder().build();
        } catch (Exception e) {
            logger.error("Error instantiating Kubernetes Client", e);
            throw e;
        }
    }

    public void setInformers(ResourceEventHandler<ConfigMap> resourceEventHandler) {
        kubernetesClient
                .configMaps()
                .inNamespace(namespace)
                .withName(configMapName)
                .inform(resourceEventHandler, 0);
    }

    /**
     * Writes the entries only while the ConfigMap still belongs to the caller's transition (its
     * active deployment type is {@code activeDeploymentType}) and {@code key} still holds {@code
     * expected} ({@code null}: absent). Returns the value {@code key} holds afterwards, or empty if
     * the ConfigMap belongs to another transition or {@code key} is absent.
     *
     * <p>Each attempt updates with the resourceVersion it read, so Kubernetes rejects it (409) if
     * anyone wrote in between, and the next attempt reads again. Concurrent callers therefore agree
     * on the first value written: the others find it on their next read and leave it in place.
     */
    public Optional<String> compareAndSet(
            BlueGreenDeploymentType activeDeploymentType,
            String key,
            String expected,
            Map<String, String> entries) {
        for (int attempt = 1; attempt <= COMPARE_AND_SET_ATTEMPTS; attempt++) {
            var configMap = parseConfigMap();
            if (configMap == null
                    || !activeDeploymentType
                            .toString()
                            .equals(configMap.getData().get(ACTIVE_DEPLOYMENT_TYPE.getLabel()))) {
                return Optional.empty();
            }
            var current = configMap.getData().get(key);
            if (!Objects.equals(current, expected)) {
                return Optional.ofNullable(current);
            }
            configMap.getData().putAll(entries);
            try {
                kubernetesClient.configMaps().inNamespace(namespace).resource(configMap).update();
                return Optional.ofNullable(entries.get(key));
            } catch (KubernetesClientException e) {
                if (e.getCode() != HttpURLConnection.HTTP_CONFLICT) {
                    logger.error("Failed to UPDATE the ConfigMap", e);
                    throw e;
                }
                logger.info("ConfigMap changed while writing {}, reading it again", key);
            }
        }
        throw new IllegalStateException(
                "ConfigMap "
                        + configMapName
                        + " kept changing while writing "
                        + key
                        + " ("
                        + COMPARE_AND_SET_ATTEMPTS
                        + " attempts)");
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
