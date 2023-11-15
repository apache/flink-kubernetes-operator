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
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

class ConfigMapView {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigMapView.class);

    enum State {
        /** ConfigMap is only stored locally, not created in Kubernetes yet. */
        NEEDS_CREATE,
        /** ConfigMap exists in Kubernetes but there are newer local changes. */
        NEEDS_UPDATE,
        /** ConfigMap view reflects the actual contents of Kubernetes ConfigMap. */
        UP_TO_DATE
    }

    private State state;

    private ConfigMap configMap;

    private final Function<ConfigMap, Resource<ConfigMap>> resourceRetriever;

    public ConfigMapView(
            ConfigMap configMapSkeleton,
            Function<ConfigMap, Resource<ConfigMap>> resourceRetriever) {
        var existingConfigMap = resourceRetriever.apply(configMapSkeleton).get();
        if (existingConfigMap != null) {
            refreshConfigMap(existingConfigMap);
        } else {
            this.configMap = configMapSkeleton;
            this.state = State.NEEDS_CREATE;
        }
        this.resourceRetriever = resourceRetriever;
    }

    public String get(String key) {
        return configMap.getData().get(key);
    }

    public void put(String key, String value) {
        configMap.getData().put(key, value);
        requireUpdate();
    }

    public void removeKey(String key) {
        var oldKey = configMap.getData().remove(key);
        if (oldKey != null) {
            requireUpdate();
        }
    }

    public void clear() {
        if (configMap.getData().isEmpty()) {
            return;
        }
        configMap.getData().clear();
        requireUpdate();
    }

    public void flush() {
        if (state == State.UP_TO_DATE) {
            return;
        }
        Resource<ConfigMap> resource = resourceRetriever.apply(configMap);
        if (state == State.NEEDS_UPDATE) {
            refreshConfigMap(resource.update());
        } else if (state == State.NEEDS_CREATE) {
            LOG.info("Creating config map {}", configMap.getMetadata().getName());
            refreshConfigMap(resource.create());
        }
    }

    private void refreshConfigMap(ConfigMap configMap) {
        Preconditions.checkNotNull(configMap);
        this.configMap = configMap;
        this.state = State.UP_TO_DATE;
    }

    private void requireUpdate() {
        if (state != State.NEEDS_CREATE) {
            state = State.NEEDS_UPDATE;
        }
    }

    @VisibleForTesting
    public Map<String, String> getDataReadOnly() {
        return Collections.unmodifiableMap(configMap.getData());
    }
}
