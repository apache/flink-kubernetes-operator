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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

/** Context for reconciling a snapshot. */
@Getter
@RequiredArgsConstructor
public class FlinkStateSnapshotContext {

    private final FlinkStateSnapshot resource;
    private final Context<FlinkStateSnapshot> josdkContext;
    private final FlinkConfigManager configManager;

    private FlinkOperatorConfiguration operatorConfig;
    private Configuration referencedJobObserveConfig;
    private FlinkDeployment referencedJobFlinkDeployment;

    /**
     * @return Operator configuration for this resource.
     */
    public FlinkOperatorConfiguration getOperatorConfig() {
        if (operatorConfig != null) {
            return operatorConfig;
        }
        return operatorConfig =
                configManager.getOperatorConfiguration(
                        getResource().getMetadata().getNamespace(), null);
    }

    public Optional<AbstractFlinkResource<?, ?>> getSecondaryResource() {
        var jobRef = getResource().getSpec().getJobReference();
        if (JobKind.FLINK_DEPLOYMENT.equals(jobRef.getKind())) {
            return getJosdkContext().getSecondaryResource(FlinkDeployment.class).map(r -> r);
        } else if (JobKind.FLINK_SESSION_JOB.equals(jobRef.getKind())) {
            return getJosdkContext().getSecondaryResource(FlinkSessionJob.class).map(r -> r);
        } else {
            return Optional.empty();
        }
    }

    public Configuration getReferencedJobObserveConfig() {
        if (referencedJobObserveConfig != null) {
            return referencedJobObserveConfig;
        }
        return referencedJobObserveConfig =
                configManager.getObserveConfig(getReferencedJobFlinkDeployment());
    }

    public FlinkDeployment getReferencedJobFlinkDeployment() {
        if (referencedJobFlinkDeployment != null) {
            return referencedJobFlinkDeployment;
        }
        return referencedJobFlinkDeployment =
                getJosdkContext()
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

    /**
     * Get KubernetesClient available from JOSDK Context.
     *
     * @return KubernetesClient.
     */
    public KubernetesClient getKubernetesClient() {
        return getJosdkContext().getClient();
    }
}
