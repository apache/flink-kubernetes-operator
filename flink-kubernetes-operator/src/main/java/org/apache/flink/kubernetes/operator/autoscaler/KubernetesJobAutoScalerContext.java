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
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import javax.annotation.Nullable;

/** An implementation of JobAutoscalerContext for Kubernetes. */
public class KubernetesJobAutoScalerContext extends JobAutoScalerContext<ResourceID> {

    private final AbstractFlinkResource<?, ?> resource;

    private final KubernetesClient kubernetesClient;

    public KubernetesJobAutoScalerContext(
            @Nullable JobID jobID,
            @Nullable JobStatus jobStatus,
            Configuration configuration,
            MetricGroup metricGroup,
            SupplierWithException<RestClusterClient<String>, Exception> restClientSupplier,
            AbstractFlinkResource<?, ?> resource,
            KubernetesClient kubernetesClient) {
        super(
                ResourceID.fromResource(resource),
                jobID,
                jobStatus,
                configuration,
                metricGroup,
                restClientSupplier);
        this.resource = resource;
        this.kubernetesClient = kubernetesClient;
    }

    public AbstractFlinkResource<?, ?> getResource() {
        return resource;
    }

    public KubernetesClient getKubernetesClient() {
        return kubernetesClient;
    }
}
