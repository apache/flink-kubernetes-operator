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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkMiniClusterSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.metrics.KubernetesResourceMetricGroup;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.service.minicluster.MiniClusterFlinkService;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.function.Function;

/** Context for reconciling a {@link FlinkMiniCluster}. */
public class FlinkMiniClusterContext extends FlinkResourceContext<FlinkMiniCluster> {

    public FlinkMiniClusterContext(
            FlinkMiniCluster resource,
            Context<?> josdkContext,
            KubernetesResourceMetricGroup resourceMetricGroup,
            FlinkConfigManager configManager,
            Function<FlinkResourceContext<?>, MiniClusterFlinkService> flinkServiceFactory,
            FlinkResourceContextFactory.ExceptionCacheEntry exceptionCacheEntry) {
        super(
                resource,
                josdkContext,
                resourceMetricGroup,
                configManager,
                flinkServiceFactory,
                exceptionCacheEntry);
    }

    @Override
    public Configuration getDeployConfig(AbstractFlinkSpec spec) {
        return configManager.getDeployConfig(
                getResource().getMetadata(), (FlinkMiniClusterSpec) spec);
    }

    @Override
    protected Configuration createObserveConfig() {
        return configManager.getObserveConfig(getResource());
    }

    /**
     * A MiniCluster isn't really native or standalone.
     *
     * <p>This is only used to compare specs, where native is the right choice: a MiniCluster cannot
     * be rescaled in place, so a change of parallelism has to upgrade the job, as it does for a
     * native deployment.
     */
    @Override
    public KubernetesDeploymentMode getDeploymentMode() {
        return KubernetesDeploymentMode.NATIVE;
    }

    /** The factory this context is created with only creates a {@link MiniClusterFlinkService}. */
    @Override
    public MiniClusterFlinkService getFlinkService() {
        return (MiniClusterFlinkService) super.getFlinkService();
    }

    @Override
    public FlinkVersion getFlinkVersion() {
        return getResource().getSpec().getFlinkVersion();
    }
}
