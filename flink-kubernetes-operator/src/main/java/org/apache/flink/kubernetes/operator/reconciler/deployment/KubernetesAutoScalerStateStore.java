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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** The kubernetes auto scaler state store, it's based on the config map. */
public class KubernetesAutoScalerStateStore implements AutoScalerStateStore {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesAutoScalerStateStore.class);

    private final KubernetesClient kubernetesClient;

    private ConfigMap configMap;

    public KubernetesAutoScalerStateStore(KubernetesClient kubernetesClient, ConfigMap configMap) {
        this.kubernetesClient = kubernetesClient;
        this.configMap = configMap;
    }

    @Override
    public Optional<String> get(String key) {
        return Optional.ofNullable(configMap.getData().get(key));
    }

    @Override
    public void put(String key, String value) {
        configMap.getData().put(key, value);
    }

    @Override
    public void remove(String key) {
        configMap.getData().remove(key);
    }

    @Override
    public void flush() {
        try {
            configMap = kubernetesClient.resource(configMap).update();
        } catch (Exception e) {
            LOG.error(
                    "Error while updating autoscaler info configmap, invalidating to clear the cache",
                    e);
            configMap = null;
            throw e;
        }
    }

    @Override
    public boolean isValid() {
        return configMap != null;
    }
}
