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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.service.FlinkService.CancelResult;
import org.apache.flink.kubernetes.operator.service.SuspendMode;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.javaoperatorsdk.operator.processing.event.ResourceID;

import java.util.Map;

/** Reconciler Flink Application deployments. */
public class ApplicationReconciler
        extends AbstractClusterJobReconciler<
                FlinkDeployment, FlinkDeploymentSpec, FlinkDeploymentStatus> {

    public ApplicationReconciler(
            EventRecorder eventRecorder,
            StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder,
            JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> autoscaler) {
        super(eventRecorder, statusRecorder, autoscaler);
    }

    @Override
    protected void submitCluster(
            FlinkResourceContext<FlinkDeployment> ctx,
            FlinkDeploymentSpec spec,
            Configuration deployConfig,
            boolean requireHaMetadata)
            throws Exception {
        ctx.getFlinkService()
                .submitApplicationCluster(spec.getJob(), deployConfig, requireHaMetadata);
    }

    @Override
    protected void deleteCluster(
            FlinkResourceContext<FlinkDeployment> ctx, Configuration conf, boolean deleteHaData) {
        var deployment = ctx.getResource();
        ctx.getFlinkService()
                .deleteClusterDeployment(
                        deployment.getMetadata(), deployment.getStatus(), conf, deleteHaData);
    }

    @Override
    protected CancelResult cancelClusterJob(
            FlinkResourceContext<FlinkDeployment> ctx, SuspendMode suspendMode) throws Exception {
        return ctx.getFlinkService()
                .cancelJob(ctx.getResource(), suspendMode, ctx.getObserveConfig());
    }

    @Override
    protected Map<String, String> getClusterInfo(FlinkDeploymentStatus status) {
        return status.getClusterInfo();
    }

    @Override
    protected IngressSpec getIngress(FlinkDeploymentSpec spec) {
        return spec.getIngress();
    }
}
