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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.metrics.KubernetesResourceMetricGroup;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;

/** Context for reconciling a Flink resource. * */
public class FlinkDeploymentContext extends FlinkResourceContext<FlinkDeployment> {

    private final FlinkConfigManager configManager;
    @Getter private final FlinkService flinkService;

    public FlinkDeploymentContext(
            FlinkDeployment resource,
            Context<?> josdkContext,
            KubernetesResourceMetricGroup resourceMetricGroup,
            FlinkService flinkService,
            FlinkConfigManager configManager,
            ClusterScalingContext clusterScalingContext) {
        super(resource, josdkContext, resourceMetricGroup, clusterScalingContext);
        this.configManager = configManager;
        this.flinkService = flinkService;
    }

    @Override
    public Configuration getDeployConfig(AbstractFlinkSpec spec) {
        return configManager.getDeployConfig(
                getResource().getMetadata(), (FlinkDeploymentSpec) spec);
    }

    @Override
    protected Configuration createObserveConfig() {
        return configManager.getObserveConfig(getResource());
    }
}
