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
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.metrics.KubernetesResourceMetricGroup;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import static org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler.sessionClusterReady;

/** Context for reconciling a Flink resource. * */
public class FlinkSessionJobContext extends FlinkResourceContext<FlinkSessionJob> {

    private final FlinkResourceContextFactory flinkResourceContextFactory;
    private final FlinkConfigManager configManager;
    private FlinkService flinkService;

    public FlinkSessionJobContext(
            FlinkSessionJob resource,
            Context<?> josdkContext,
            KubernetesResourceMetricGroup resourceMetricGroup,
            FlinkResourceContextFactory flinkResourceContextFactory,
            FlinkConfigManager configManager) {
        super(resource, josdkContext, resourceMetricGroup);
        this.flinkResourceContextFactory = flinkResourceContextFactory;
        this.configManager = configManager;
    }

    @Override
    public Configuration getDeployConfig(AbstractFlinkSpec spec) {
        var session = getJosdkContext().getSecondaryResource(FlinkDeployment.class);

        if (sessionClusterReady(session)) {
            return configManager.getSessionJobConfig(session.get(), (FlinkSessionJobSpec) spec);
        }
        return null;
    }

    @Override
    public FlinkService getFlinkService() {
        if (flinkService != null) {
            return flinkService;
        }
        var session = getJosdkContext().getSecondaryResource(FlinkDeployment.class);
        return flinkService =
                sessionClusterReady(session)
                        ? flinkResourceContextFactory
                                .getResourceContext(session.get(), getJosdkContext())
                                .getFlinkService()
                        : null;
    }

    @Override
    protected Configuration createObserveConfig() {
        return getDeployConfig(getResource().getSpec());
    }

    @Override
    public KubernetesDeploymentMode getDeploymentMode() {
        return KubernetesDeploymentMode.getDeploymentMode(
                getJosdkContext().getSecondaryResource(FlinkDeployment.class).get());
    }
}
