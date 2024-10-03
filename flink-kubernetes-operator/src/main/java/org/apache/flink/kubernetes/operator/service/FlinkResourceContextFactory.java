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
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.artifact.ArtifactManager;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentContext;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.controller.FlinkSessionJobContext;
import org.apache.flink.kubernetes.operator.controller.FlinkStateSnapshotContext;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.metrics.KubernetesResourceMetricGroup;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Factory for creating the {@link FlinkResourceContext}. */
public class FlinkResourceContextFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkResourceContextFactory.class);

    private final FlinkConfigManager configManager;
    private final ArtifactManager artifactManager;
    private final ExecutorService clientExecutorService;
    private final KubernetesOperatorMetricGroup operatorMetricGroup;
    private final EventRecorder eventRecorder;

    protected final Map<Tuple2<Class<?>, ResourceID>, KubernetesResourceMetricGroup>
            resourceMetricGroups = new ConcurrentHashMap<>();

    public FlinkResourceContextFactory(
            FlinkConfigManager configManager,
            KubernetesOperatorMetricGroup operatorMetricGroup,
            EventRecorder eventRecorder) {
        this.configManager = configManager;
        this.operatorMetricGroup = operatorMetricGroup;
        this.eventRecorder = eventRecorder;
        this.artifactManager = new ArtifactManager(configManager);
        this.clientExecutorService =
                Executors.newFixedThreadPool(
                        configManager.getOperatorConfiguration().getReconcilerMaxParallelism(),
                        new ExecutorThreadFactory("Flink-RestClusterClient-IO"));
    }

    public FlinkStateSnapshotContext getFlinkStateSnapshotContext(
            FlinkStateSnapshot savepoint, Context<FlinkStateSnapshot> josdkContext) {
        return new FlinkStateSnapshotContext(
                savepoint,
                savepoint.getStatus().toBuilder().build(),
                ImmutableMap.copyOf(savepoint.getMetadata().getLabels()),
                josdkContext,
                configManager);
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
                            flinkDep, josdkContext, resMg, configManager, this::getFlinkService);
        } else if (resource instanceof FlinkSessionJob) {
            return (FlinkResourceContext<CR>)
                    new FlinkSessionJobContext(
                            (FlinkSessionJob) resource,
                            josdkContext,
                            resMg,
                            configManager,
                            this::getFlinkService);
        } else {
            throw new IllegalArgumentException(
                    "Unknown resource type " + resource.getClass().getSimpleName());
        }
    }

    @VisibleForTesting
    protected FlinkService getFlinkService(FlinkResourceContext<?> ctx) {
        var deploymentMode = ctx.getDeploymentMode();
        switch (deploymentMode) {
            case NATIVE:
                return new NativeFlinkService(
                        ctx.getKubernetesClient(),
                        artifactManager,
                        clientExecutorService,
                        ctx.getOperatorConfig(),
                        eventRecorder);
            case STANDALONE:
                return new StandaloneFlinkService(
                        ctx.getKubernetesClient(),
                        artifactManager,
                        clientExecutorService,
                        ctx.getOperatorConfig());
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported deployment mode: %s", deploymentMode));
        }
    }

    public <CR extends AbstractFlinkResource<?, ?>> void cleanup(CR flinkApp) {
        var resourceMetricGroup =
                resourceMetricGroups.remove(
                        Tuple2.of(flinkApp.getClass(), ResourceID.fromResource(flinkApp)));
        if (resourceMetricGroup != null) {
            resourceMetricGroup.close();
        } else {
            LOG.warn("Unknown resource metric group for {}", flinkApp);
        }
    }
}
