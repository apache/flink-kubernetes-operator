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
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.JobKind;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Optional;

/** Context for reconciling a snapshot. */
@Getter
@RequiredArgsConstructor
public class FlinkStateSnapshotContext {

    private final FlinkStateSnapshot resource;
    private final FlinkStateSnapshotStatus originalStatus;
    private final Map<String, String> originalLabels;
    private final Context<FlinkStateSnapshot> josdkContext;
    private final FlinkConfigManager configManager;

    @Getter(lazy = true)
    private final FlinkOperatorConfiguration operatorConfig = operatorConfig();

    @Getter(lazy = true)
    private final Configuration referencedJobObserveConfig = referencedJobObserveConfig();

    @Getter(lazy = true)
    private final FlinkDeployment referencedJobFlinkDeployment = referencedJobFlinkDeployment();

    /**
     * @return Operator configuration for this resource.
     */
    private FlinkOperatorConfiguration operatorConfig() {
        return getConfigManager()
                .getOperatorConfiguration(getResource().getMetadata().getNamespace(), null);
    }

    private Configuration referencedJobObserveConfig() {
        return getConfigManager().getObserveConfig(getReferencedJobFlinkDeployment());
    }

    private FlinkDeployment referencedJobFlinkDeployment() {
        return getJosdkContext()
                .getSecondaryResource(FlinkDeployment.class)
                .orElseThrow(
                        () ->
                                new RuntimeException(
                                        String.format(
                                                "Failed to find FlinkDeployment from job reference: %s",
                                                getResource()
                                                        .getSpec()
                                                        .getJobReference()
                                                        .getName())));
    }

    public Optional<AbstractFlinkResource<?, ?>> getSecondaryResource() {
        var jobKind =
                Optional.ofNullable(getResource().getSpec().getJobReference())
                        .map(JobReference::getKind)
                        .orElse(null);

        if (JobKind.FLINK_DEPLOYMENT.equals(jobKind)) {
            return getJosdkContext().getSecondaryResource(FlinkDeployment.class).map(r -> r);
        }

        if (JobKind.FLINK_SESSION_JOB.equals(jobKind)) {
            return getJosdkContext().getSecondaryResource(FlinkSessionJob.class).map(r -> r);
        }

        return Optional.empty();
    }

    /**
     * Get KubernetesClient available from JOSDK Context.
     *
     * @return KubernetesClient.
     */
    public KubernetesClient getKubernetesClient() {
        return getJosdkContext().getClient();
    }
}
