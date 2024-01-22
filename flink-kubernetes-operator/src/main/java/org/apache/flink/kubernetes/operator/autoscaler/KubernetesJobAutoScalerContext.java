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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;

import javax.annotation.Nullable;

import java.util.Optional;

/** An implementation of JobAutoscalerContext for Kubernetes. */
public class KubernetesJobAutoScalerContext extends JobAutoScalerContext<ResourceID> {

    @Getter private final FlinkResourceContext<?> resourceContext;

    public KubernetesJobAutoScalerContext(
            @Nullable JobID jobID,
            @Nullable JobStatus jobStatus,
            Configuration configuration,
            MetricGroup metricGroup,
            SupplierWithException<RestClusterClient<String>, Exception> restClientSupplier,
            FlinkResourceContext<?> resourceContext) {
        super(
                ResourceID.fromResource(resourceContext.getResource()),
                jobID,
                jobStatus,
                configuration,
                metricGroup,
                restClientSupplier);
        this.resourceContext = resourceContext;
    }

    @Override
    public Optional<Double> getTaskManagerCpu() {
        return Optional.ofNullable(
                getConfiguration().get(KubernetesConfigOptions.TASK_MANAGER_CPU));
    }

    @Override
    public Optional<MemorySize> getTaskManagerMemory() {
        return Optional.ofNullable(getConfiguration().get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Override
    public Optional<MemorySize> getTaskManagerMemoryFromSpec() {
        return getJobDeployment()
                .map(
                        flinkDeployment ->
                                MemorySize.parse(
                                        flinkDeployment
                                                .getSpec()
                                                .getTaskManager()
                                                .getResource()
                                                .getMemory()));
    }

    public AbstractFlinkResource<?, ?> getResource() {
        return resourceContext.getResource();
    }

    public KubernetesClient getKubernetesClient() {
        return resourceContext.getKubernetesClient();
    }

    private Optional<FlinkDeployment> getJobDeployment() {
        AbstractFlinkResource<?, ?> resource = resourceContext.getResource();
        if (resource instanceof FlinkDeployment) {
            return Optional.of((FlinkDeployment) resource);
        }
        return Optional.empty();
    }
}
