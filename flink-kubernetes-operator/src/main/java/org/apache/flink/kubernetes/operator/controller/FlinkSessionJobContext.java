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
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.metrics.KubernetesResourceMetricGroup;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.function.Function;

import static org.apache.flink.kubernetes.operator.reconciler.sessionjob.SessionJobReconciler.sessionClusterReady;

/** Context for reconciling a Flink resource. */
public class FlinkSessionJobContext extends FlinkResourceContext<FlinkSessionJob> {

    private FlinkVersion flinkVersion;

    public FlinkSessionJobContext(
            FlinkSessionJob resource,
            Context<?> josdkContext,
            KubernetesResourceMetricGroup resourceMetricGroup,
            FlinkConfigManager configManager,
            Function<FlinkResourceContext<?>, FlinkService> flinkServiceFactory) {
        super(resource, josdkContext, resourceMetricGroup, configManager, flinkServiceFactory);
    }

    @Override
    public Configuration getDeployConfig(AbstractFlinkSpec spec) {
        var session = getJosdkContext().getSecondaryResource(FlinkDeployment.class);

        if (sessionClusterReady(session)) {
            return configManager.getSessionJobConfig(
                    getResource().getMetadata().getName(),
                    session.get(),
                    (FlinkSessionJobSpec) spec);
        }
        return null;
    }

    @Override
    protected FlinkService createFlinkService() {
        var session = getJosdkContext().getSecondaryResource(FlinkDeployment.class);
        return sessionClusterReady(session) ? super.createFlinkService() : null;
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

    @Override
    public FlinkVersion getFlinkVersion() {
        if (flinkVersion != null) {
            return flinkVersion;
        }
        flinkVersion =
                getJosdkContext()
                        .getSecondaryResource(FlinkDeployment.class)
                        .map(fd -> fd.getSpec().getFlinkVersion())
                        .orElse(null);
        return flinkVersion;
    }
}
