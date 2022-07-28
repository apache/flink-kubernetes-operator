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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.diff.DiffType;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
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

    protected final FlinkService flinkService;

    private static final Logger LOG = LoggerFactory.getLogger(SessionReconciler.class);

    public SessionReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder,
            StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder) {
        super(kubernetesClient, configManager, eventRecorder, statusRecorder);
        this.flinkService = flinkService;
    }

    @Override
    protected FlinkService getFlinkService(FlinkDeployment resource, Context<?> context) {
        return flinkService;
    }

    @Override
    protected Configuration getDeployConfig(
            ObjectMeta meta, FlinkDeploymentSpec spec, Context<?> ctx) {
        return configManager.getDeployConfig(meta, spec);
    }

    @Override
    protected Configuration getObserveConfig(FlinkDeployment resource, Context<?> context) {
        return configManager.getObserveConfig(resource);
    }

    @Override
    protected boolean readyToReconcile(
            FlinkDeployment deployment, Context<?> ctx, Configuration deployConfig) {
        return true;
    }

    @Override
    protected void reconcileSpecChange(
            FlinkDeployment deployment,
            Context<?> ctx,
            Configuration observeConfig,
            Configuration deployConfig,
            DiffType type)
            throws Exception {
        deleteSessionCluster(deployment, observeConfig);

        // We record the target spec into an upgrading state before deploying
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(deployment, deployConfig);
        statusRecorder.patchAndCacheStatus(deployment);

        deploy(
                deployment,
                deployment.getSpec(),
                deployment.getStatus(),
                ctx,
                deployConfig,
                Optional.empty(),
                false);
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, deployConfig);
    }

    private void deleteSessionCluster(FlinkDeployment deployment, Configuration effectiveConfig) {
        flinkService.deleteClusterDeployment(
                deployment.getMetadata(), deployment.getStatus(), false);
        flinkService.waitForClusterShutdown(effectiveConfig);
    }

    @Override
    protected void deploy(
            FlinkDeployment cr,
            FlinkDeploymentSpec spec,
            FlinkDeploymentStatus status,
            Context<?> ctx,
            Configuration deployConfig,
            Optional<String> savepoint,
            boolean requireHaMetadata)
            throws Exception {
        flinkService.submitSessionCluster(deployConfig);
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
        IngressUtils.updateIngressRules(cr.getMetadata(), spec, deployConfig, kubernetesClient);
    }

    @Override
    protected void rollback(FlinkDeployment deployment, Context<?> ctx, Configuration observeConfig)
            throws Exception {
        FlinkDeploymentStatus status = deployment.getStatus();
        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                status.getReconciliationStatus();
        FlinkDeploymentSpec rollbackSpec = reconciliationStatus.deserializeLastStableSpec();
        Configuration rollbackConfig =
                configManager.getDeployConfig(deployment.getMetadata(), rollbackSpec);

        deleteSessionCluster(deployment, observeConfig);
        deploy(
                deployment,
                rollbackSpec,
                deployment.getStatus(),
                ctx,
                rollbackConfig,
                Optional.empty(),
                false);

        reconciliationStatus.setState(ReconciliationState.ROLLED_BACK);
    }

    @Override
    public boolean reconcileOtherChanges(
            FlinkDeployment flinkApp, Context<?> ctx, Configuration observeConfig)
            throws Exception {
        if (shouldRecoverDeployment(observeConfig, flinkApp)) {
            recoverSession(flinkApp, observeConfig);
            return true;
        }
        return false;
    }

    private void recoverSession(FlinkDeployment deployment, Configuration effectiveConfig)
            throws Exception {
        flinkService.submitSessionCluster(effectiveConfig);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
    }

    @Override
    public DeleteControl cleanupInternal(FlinkDeployment deployment, Context<?> context) {
        Set<FlinkSessionJob> sessionJobs = context.getSecondaryResources(FlinkSessionJob.class);
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
                    EventRecorder.Reason.Cleanup,
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
            flinkService.deleteClusterDeployment(
                    deployment.getMetadata(), deployment.getStatus(), true);
            return DeleteControl.defaultDelete();
        }
    }
}
