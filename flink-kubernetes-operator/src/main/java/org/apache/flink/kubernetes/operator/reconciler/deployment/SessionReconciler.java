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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.NoopJobAutoscaler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;

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

    public SessionReconciler(
            EventRecorder eventRecorder,
            StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder) {
        super(eventRecorder, statusRecorder, new NoopJobAutoscaler<>());
    }

    @Override
    protected boolean readyToReconcile(FlinkResourceContext<FlinkDeployment> ctx) {
        return true;
    }

    @Override
    protected boolean reconcileSpecChange(
            DiffType diffType,
            FlinkResourceContext<FlinkDeployment> ctx,
            Configuration deployConfig,
            FlinkDeploymentSpec lastReconciledSpec)
            throws Exception {
        var deployment = ctx.getResource();
        deleteSessionCluster(ctx);

        // We record the target spec into an upgrading state before deploying
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(deployment, deployConfig, clock);
        statusRecorder.patchAndCacheStatus(deployment, ctx.getKubernetesClient());

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
        IngressUtils.updateIngressRules(
                cr.getMetadata(), spec, deployConfig, ctx.getKubernetesClient());
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

    // Detects jobs which are not in globally terminated states
    @VisibleForTesting
    Set<JobID> getNonTerminalJobs(FlinkResourceContext<FlinkDeployment> ctx) {
        LOG.debug("Starting nonTerminal jobs detection for session cluster");
        try {
            // Get all jobs running in the Flink cluster
            var flinkService = ctx.getFlinkService();
            var clusterClient = flinkService.getClusterClient(ctx.getObserveConfig());
            var allJobs =
                    clusterClient
                            .sendRequest(
                                    JobsOverviewHeaders.getInstance(),
                                    EmptyMessageParameters.getInstance(),
                                    EmptyRequestBody.getInstance())
                            .get()
                            .getJobs();

            // running job Ids
            Set<JobID> nonTerminalJobIds =
                    allJobs.stream()
                            .filter(job -> !job.getStatus().isGloballyTerminalState())
                            .map(JobDetails::getJobId)
                            .collect(Collectors.toSet());

            return nonTerminalJobIds;
        } catch (Exception e) {
            LOG.warn("Failed to detect nonTerminal jobs in session cluster", e);
            return Set.of();
        }
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
                    error,
                    ctx.getKubernetesClient())) {
                LOG.warn(error);
            }
            return DeleteControl.noFinalizerRemoval()
                    .rescheduleAfter(ctx.getOperatorConfig().getReconcileInterval().toMillis());
        }

        // Check for non-terminated jobs if the option is enabled (Enabled by default) , after
        // sessionJobs are deleted
        boolean blockOnUnmanagedJobs =
                ctx.getObserveConfig()
                        .getBoolean(KubernetesOperatorConfigOptions.BLOCK_ON_UNMANAGED_JOBS);
        if (blockOnUnmanagedJobs) {
            Set<JobID> nonTerminalJobs = getNonTerminalJobs(ctx);
            if (!nonTerminalJobs.isEmpty()) {
                var error =
                        String.format(
                                "The session cluster has non terminated jobs %s that should be cancelled first",
                                nonTerminalJobs.stream()
                                        .map(JobID::toHexString)
                                        .collect(Collectors.toList()));
                eventRecorder.triggerEvent(
                        deployment,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.CleanupFailed,
                        EventRecorder.Component.Operator,
                        error,
                        ctx.getKubernetesClient());
                return DeleteControl.noFinalizerRemoval()
                        .rescheduleAfter(ctx.getOperatorConfig().getReconcileInterval().toMillis());
            }
        }

        LOG.info("Stopping session cluster");
        var conf = ctx.getObserveConfig();
        ctx.getFlinkService()
                .deleteClusterDeployment(
                        deployment.getMetadata(), deployment.getStatus(), conf, true);
        return DeleteControl.defaultDelete();
    }
}
