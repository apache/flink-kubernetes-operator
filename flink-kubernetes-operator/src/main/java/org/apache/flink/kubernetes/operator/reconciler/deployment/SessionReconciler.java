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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reconciler responsible for handling the session cluster lifecycle according to the desired and
 * current states.
 */
public class SessionReconciler
        extends AbstractFlinkResourceReconciler<
                FlinkDeployment, FlinkDeploymentSpec, FlinkDeploymentStatus> {

    private static final Logger LOG = LoggerFactory.getLogger(SessionReconciler.class);
    private final FlinkConfigManager configManager;

    public SessionReconciler(
            KubernetesClient kubernetesClient,
            EventRecorder eventRecorder,
            StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder,
            FlinkConfigManager configManager) {
        super(kubernetesClient, eventRecorder, statusRecorder, new NoopJobAutoscalerFactory());
        this.configManager = configManager;
    }

    @Override
    protected boolean readyToReconcile(FlinkResourceContext<FlinkDeployment> ctx) {
        return true;
    }

    @Override
    protected boolean reconcileSpecChange(
            FlinkResourceContext<FlinkDeployment> ctx, Configuration deployConfig)
            throws Exception {
        var deployment = ctx.getResource();
        deleteSessionCluster(ctx);

        // We record the target spec into an upgrading state before deploying
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(deployment, deployConfig, clock);
        statusRecorder.patchAndCacheStatus(deployment);

        deploy(ctx, deployment.getSpec(), deployConfig, Optional.empty(), false);
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, deployConfig, clock);
        return true;
    }

    private void deleteSessionCluster(FlinkResourceContext<FlinkDeployment> ctx) {
        var deployment = ctx.getResource();
        var conf = ctx.getDeployConfig(ctx.getResource().getSpec());
        ctx.getFlinkService()
                .deleteClusterDeployment(
                        deployment.getMetadata(), deployment.getStatus(), conf, false);
        ctx.getFlinkService().waitForClusterShutdown(ctx.getObserveConfig());
    }

    @Override
    public void deploy(
            FlinkResourceContext<FlinkDeployment> ctx,
            FlinkDeploymentSpec spec,
            Configuration deployConfig,
            Optional<String> savepoint,
            boolean requireHaMetadata)
            throws Exception {
        var cr = ctx.getResource();
        setOwnerReference(cr, deployConfig);
        ctx.getFlinkService().submitSessionCluster(deployConfig);
        cr.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
        IngressUtils.updateIngressRules(cr.getMetadata(), spec, deployConfig, kubernetesClient);
    }

    @Override
    public boolean reconcileOtherChanges(FlinkResourceContext<FlinkDeployment> ctx)
            throws Exception {
        if (shouldRecoverDeployment(ctx.getObserveConfig(), ctx.getResource())) {
            recoverSession(ctx);
            return true;
        }
        return false;
    }

    private void recoverSession(FlinkResourceContext<FlinkDeployment> ctx) throws Exception {
        ctx.getFlinkService().submitSessionCluster(ctx.getObserveConfig());
        ctx.getResource()
                .getStatus()
                .setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
    }

    @Override
    public DeleteControl cleanupInternal(FlinkResourceContext<FlinkDeployment> ctx) {
        Set<FlinkSessionJob> sessionJobs =
                ctx.getJosdkContext().getSecondaryResources(FlinkSessionJob.class);
        var deployment = ctx.getResource();
        if (!sessionJobs.isEmpty()) {
            var error =
                    String.format(
                            "The session jobs %s should be deleted first",
                            sessionJobs.stream()
                                    .map(job -> job.getMetadata().getName())
                                    .collect(Collectors.toList()));
            if (eventRecorder.triggerEvent(
                    deployment,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.CleanupFailed,
                    EventRecorder.Component.Operator,
                    error)) {
                LOG.warn(error);
            }
            return DeleteControl.noFinalizerRemoval()
                    .rescheduleAfter(
                            configManager
                                    .getOperatorConfiguration()
                                    .getReconcileInterval()
                                    .toMillis());
        } else {
            LOG.info("Stopping session cluster");
            var conf = ctx.getDeployConfig(ctx.getResource().getSpec());
            ctx.getFlinkService()
                    .deleteClusterDeployment(
                            deployment.getMetadata(), deployment.getStatus(), conf, true);
            return DeleteControl.defaultDelete();
        }
    }
}
