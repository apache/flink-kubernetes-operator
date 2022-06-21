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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.KubernetesDeploymentMode;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The factory to create the FlinkService based on the {@link FlinkDeployment} mode. */
public class FlinkServiceFactory {

    private final KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager;
    private final Map<KubernetesDeploymentMode, FlinkService> serviceMap;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkServiceFactory.class);

    public FlinkServiceFactory(
            KubernetesClient kubernetesClient, FlinkConfigManager configManager) {
        this.kubernetesClient = kubernetesClient;
        this.configManager = configManager;
        this.serviceMap = new ConcurrentHashMap<>();
    }

    public FlinkService getOrCreate(KubernetesDeploymentMode deploymentMode) {
        return serviceMap.computeIfAbsent(
                deploymentMode,
                mode -> {
                    switch (mode) {
                        case NATIVE:
                            LOG.info("Using NativeFlinkService");
                            return new NativeFlinkService(kubernetesClient, configManager);
                        case STANDALONE:
                            LOG.info("Using StandaloneFlinkService");
                            return new StandaloneFlinkService(kubernetesClient, configManager);
                        default:
                            throw new UnsupportedOperationException(
                                    String.format("Unsupported deployment mode: %s", mode));
                    }
                });
    }

    public FlinkService getOrCreate(FlinkDeployment deployment) {
        LOG.info("Getting service for {}", deployment.getMetadata().getName());
        return getOrCreate(getDeploymentMode(deployment));
    }

    private KubernetesDeploymentMode getDeploymentMode(FlinkDeployment deployment) {
        return KubernetesDeploymentMode.getDeploymentMode(deployment);
    }
}
