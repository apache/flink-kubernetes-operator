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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.ClusterScalingContext;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentContext;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.controller.FlinkSessionJobContext;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.metrics.KubernetesResourceMetricGroup;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Factory for creating the {@link FlinkResourceContext}. */
public class FlinkResourceContextFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkResourceContextFactory.class);

    private final KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager;
    private final KubernetesOperatorMetricGroup operatorMetricGroup;
    private final ClusterScalingContext clusterScalingContext;
    private final Map<KubernetesDeploymentMode, FlinkService> serviceMap;

    private final Map<Tuple2<Class<?>, ResourceID>, KubernetesResourceMetricGroup>
            resourceMetricGroups = new ConcurrentHashMap<>();

    public FlinkResourceContextFactory(
            KubernetesClient kubernetesClient,
            FlinkConfigManager configManager,
            KubernetesOperatorMetricGroup operatorMetricGroup,
            ClusterScalingContext clusterScalingContext) {
        this.kubernetesClient = kubernetesClient;
        this.configManager = configManager;
        this.operatorMetricGroup = operatorMetricGroup;
        this.clusterScalingContext = clusterScalingContext;
        this.serviceMap = new ConcurrentHashMap<>();
    }

    public <CR extends AbstractFlinkResource<?, ?>> FlinkResourceContext<CR> getResourceContext(
            CR resource, Context josdkContext) {
        var resMg =
                resourceMetricGroups.computeIfAbsent(
                        Tuple2.of(resource.getClass(), ResourceID.fromResource(resource)),
                        r ->
                                OperatorMetricUtils.createResourceMetricGroup(
                                        operatorMetricGroup, configManager, resource));

        if (resource instanceof FlinkDeployment) {
            var flinkDep = (FlinkDeployment) resource;
            return (FlinkResourceContext<CR>)
                    new FlinkDeploymentContext(
                            flinkDep,
                            josdkContext,
                            resMg,
                            getOrCreateFlinkService(flinkDep),
                            configManager,
                            clusterScalingContext);
        } else if (resource instanceof FlinkSessionJob) {
            return (FlinkResourceContext<CR>)
                    new FlinkSessionJobContext(
                            (FlinkSessionJob) resource,
                            josdkContext,
                            resMg,
                            this,
                            configManager,
                            clusterScalingContext);
        } else {
            throw new IllegalArgumentException(
                    "Unknown resource type " + resource.getClass().getSimpleName());
        }
    }

    private FlinkService getOrCreateFlinkService(KubernetesDeploymentMode deploymentMode) {
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

    @VisibleForTesting
    protected FlinkService getOrCreateFlinkService(FlinkDeployment deployment) {
        LOG.info("Getting service for {}", deployment.getMetadata().getName());
        return getOrCreateFlinkService(getDeploymentMode(deployment));
    }

    private KubernetesDeploymentMode getDeploymentMode(FlinkDeployment deployment) {
        return KubernetesDeploymentMode.getDeploymentMode(deployment);
    }
}
