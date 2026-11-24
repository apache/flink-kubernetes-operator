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

import org.apache.flink.autoscaler.NoopJobAutoscaler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.spec.FlinkMiniClusterSpec;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkMiniClusterStatus;
import org.apache.flink.kubernetes.operator.config.MiniClusterResourceConfigUtils;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.service.FlinkService.CancelResult;
import org.apache.flink.kubernetes.operator.service.SuspendMode;
import org.apache.flink.kubernetes.operator.service.minicluster.MiniClusterFlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import java.util.Map;

/**
 * Reconciler for a {@link FlinkMiniCluster}.
 *
 * <p>Upgrade modes, suspending and resuming, savepoint redeployment, restart nonces and rollbacks
 * are shared with the application {@code FlinkDeployment} through {@link
 * AbstractClusterJobReconciler}.
 *
 * <p>Every deployment of the job is given a fixed JobID, which is kept when the job is restored
 * from its last state. This is the JobID the launcher submits the job with, so that when the pod
 * restarts under high availability, the launcher submits the same job that the MiniCluster has
 * recovered from its HA metadata, and the job continues from its checkpoints.
 */
public class MiniClusterReconciler
        extends AbstractClusterJobReconciler<
                FlinkMiniCluster, FlinkMiniClusterSpec, FlinkMiniClusterStatus> {

    public MiniClusterReconciler(
            EventRecorder eventRecorder,
            StatusRecorder<FlinkMiniCluster, FlinkMiniClusterStatus> statusRecorder) {
        // A MiniCluster cannot be rescaled, so there is nothing for the autoscaler to do
        super(eventRecorder, statusRecorder, new NoopJobAutoscaler<>());
    }

    @Override
    protected void submitCluster(
            FlinkResourceContext<FlinkMiniCluster> ctx,
            FlinkMiniClusterSpec spec,
            Configuration deployConfig,
            boolean requireHaMetadata)
            throws Exception {
        warnIfInsufficientMemory(ctx, spec, deployConfig);
        // The launcher reads the restore path from the config file (rather than from the command
        // line), so the savepoint path set in the deploy config is the one the job restores from
        MiniClusterFlinkService.of(ctx)
                .submitMiniCluster(ctx.getResource(), spec, deployConfig, requireHaMetadata);
    }

    /**
     * Warns the user if the memory of the MiniCluster container doesn't fit the memory budget
     * derived from it. For a FlinkDeployment, Flink fails to start a JobManager or TaskManager
     * without enough memory, but a MiniCluster runs with the memory it is given, so it could be
     * OOM-killed long after it starts. It isn't rejected, as jobs that don't use managed memory can
     * run in less.
     */
    private void warnIfInsufficientMemory(
            FlinkResourceContext<FlinkMiniCluster> ctx,
            FlinkMiniClusterSpec spec,
            Configuration deployConfig) {
        MiniClusterResourceConfigUtils.checkMemoryBudget(
                        deployConfig, spec.getResources(), spec.getPodTemplate())
                .ifPresent(
                        message ->
                                eventRecorder.triggerEvent(
                                        ctx.getResource(),
                                        EventRecorder.Type.Warning,
                                        EventRecorder.Reason.InsufficientMemory,
                                        message,
                                        EventRecorder.Component.JobManagerDeployment,
                                        // One event, updated on each deploy that doesn't fit
                                        EventRecorder.Reason.InsufficientMemory.name(),
                                        ctx.getKubernetesClient()));
    }

    @Override
    protected void deleteCluster(
            FlinkResourceContext<FlinkMiniCluster> ctx, Configuration conf, boolean deleteHaData) {
        var miniCluster = ctx.getResource();
        MiniClusterFlinkService.of(ctx)
                .deleteMiniCluster(
                        miniCluster.getMetadata(), miniCluster.getStatus(), conf, deleteHaData);
    }

    @Override
    protected CancelResult cancelClusterJob(
            FlinkResourceContext<FlinkMiniCluster> ctx, SuspendMode suspendMode) throws Exception {
        return MiniClusterFlinkService.of(ctx)
                .cancelMiniClusterJob(ctx.getResource(), suspendMode, ctx.getObserveConfig());
    }

    @Override
    protected Map<String, String> getClusterInfo(FlinkMiniClusterStatus status) {
        return status.getClusterInfo();
    }

    @Override
    protected IngressSpec getIngress(FlinkMiniClusterSpec spec) {
        return spec.getIngress();
    }

    @Override
    protected boolean flinkVersionChanged(
            FlinkMiniClusterSpec oldSpec, FlinkMiniClusterSpec newSpec) {
        return oldSpec.getFlinkVersion() != newSpec.getFlinkVersion();
    }
}
