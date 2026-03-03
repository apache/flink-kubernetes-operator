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

package org.apache.flink.kubernetes.operator.reconciler.sessionjob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractJobReconciler;
import org.apache.flink.kubernetes.operator.service.SuspendMode;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

/** The reconciler for the {@link FlinkSessionJob}. */
public class SessionJobReconciler
        extends AbstractJobReconciler<FlinkSessionJob, FlinkSessionJobSpec, FlinkSessionJobStatus> {

    private static final Logger LOG = LoggerFactory.getLogger(SessionJobReconciler.class);

    public SessionJobReconciler(
            EventRecorder eventRecorder,
            StatusRecorder<FlinkSessionJob, FlinkSessionJobStatus> statusRecorder,
            JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> autoscaler) {
        super(eventRecorder, statusRecorder, autoscaler);
    }

    @Override
    public boolean readyToReconcile(FlinkResourceContext<FlinkSessionJob> ctx) {
        return sessionClusterReady(
                        ctx.getJosdkContext().getSecondaryResource(FlinkDeployment.class))
                && super.readyToReconcile(ctx);
    }

    @Override
    public void deploy(
            FlinkResourceContext<FlinkSessionJob> ctx,
            FlinkSessionJobSpec sessionJobSpec,
            Configuration deployConfig,
            Optional<String> savepoint,
            boolean requireHaMetadata)
            throws Exception {

        eventRecorder.triggerEvent(
                ctx.getResource(),
                EventRecorder.Type.Normal,
                EventRecorder.Reason.Submit,
                EventRecorder.Component.Job,
                MSG_SUBMIT,
                ctx.getKubernetesClient());

        var jobStatus = ctx.getResource().getStatus().getJobStatus();

        String existingJobIdStr = jobStatus.getJobId();
        JobID jobId;
        var jobState = jobStatus.getState();
        var reuseJobId = jobState == JobStatus.RECONCILING;
        if (existingJobIdStr != null && reuseJobId) {
            jobId = JobID.fromHexString(existingJobIdStr);
            LOG.info("Reusing existing job ID {} for deployment retry", jobId);
        } else {
            jobId = JobID.generate();
            LOG.info("Generated new job ID {} for deployment", jobId);
            jobStatus.setJobId(jobId.toHexString());
            jobStatus.setState(org.apache.flink.api.common.JobStatus.RECONCILING);
            statusRecorder.patchAndCacheStatus(ctx.getResource(), ctx.getKubernetesClient());
        }

        ctx.getFlinkService()
                .submitJobToSessionCluster(
                        ctx.getResource().getMetadata(),
                        sessionJobSpec,
                        jobId,
                        deployConfig,
                        savepoint.orElse(null));
    }

    @Override
    protected boolean cancelJob(FlinkResourceContext<FlinkSessionJob> ctx, SuspendMode suspendMode)
            throws Exception {
        var cancelTs = Instant.now();
        var result =
                ctx.getFlinkService()
                        .cancelSessionJob(ctx.getResource(), suspendMode, ctx.getObserveConfig());
        result.getSavepointPath()
                .ifPresent(location -> setUpgradeSavepointPath(ctx, location, cancelTs));
        return result.isPending();
    }

    @Override
    protected void cleanupAfterFailedJob(FlinkResourceContext<FlinkSessionJob> ctx) {
        // The job has already stopped, nothing to clean up.
    }

    @Override
    public DeleteControl cleanupInternal(FlinkResourceContext<FlinkSessionJob> ctx) {
        var status = ctx.getResource().getStatus();
        var rescheduleDelete =
                DeleteControl.noFinalizerRemoval()
                        .rescheduleAfter(
                                ctx.getOperatorConfig().getProgressCheckInterval().toMillis());
        var jobID = ctx.getResource().getStatus().getJobStatus().getJobId();

        if (status.getReconciliationStatus().isBeforeFirstDeployment()
                || ReconciliationUtils.isJobInTerminalState(status)
                || status.getReconciliationStatus()
                                .deserializeLastReconciledSpec()
                                .getJob()
                                .getState()
                        == JobState.SUSPENDED
                || JobStatusObserver.JOB_NOT_FOUND_ERR.equals(status.getError())
                || jobID == null) {
            // Job is not running, nothing to do...
            return DeleteControl.defaultDelete();
        }

        var flinkDepOptional = ctx.getJosdkContext().getSecondaryResource(FlinkDeployment.class);
        if (flinkDepOptional.isEmpty()) {
            LOG.info("Session cluster deployment not available");
            return DeleteControl.defaultDelete();
        }

        var flinkDep = flinkDepOptional.get();

        // If the session cluster is being deleted, the job will not survive regardless,
        // so there is no need to explicitly cancel it.
        var sessionLifecycleState = flinkDep.getStatus().getLifecycleState();
        if (sessionLifecycleState == ResourceLifecycleState.DELETING
                || sessionLifecycleState == ResourceLifecycleState.DELETED) {
            LOG.info("Session cluster is being deleted, skipping job cancellation");
            return DeleteControl.defaultDelete();
        }

        if (!sessionClusterReady(flinkDepOptional)) {
            // If the session cluster is not healthy and HA is not enabled, the job state
            // will not survive a cluster restart, so we can safely delete without
            // explicit cancellation.
            var sessionConf = flinkDep.getSpec().getFlinkConfiguration().asConfiguration();
            if (!FlinkUtils.isKubernetesHAActivated(sessionConf)
                    && !FlinkUtils.isZookeeperHAActivated(sessionConf)) {
                LOG.info(
                        "Session cluster is not healthy and HA is not enabled, skipping job cancellation");
                return DeleteControl.defaultDelete();
            }
            LOG.info("Session cluster is not healthy, waiting before attempting job cancellation");
            return rescheduleDelete;
        }

        // Only check for pending cancellation once the cluster is ready, so that other deletion
        // paths (cluster missing, deleting, unhealthy without HA) are not blocked.
        if (ReconciliationUtils.isJobCancelling(status)) {
            LOG.info("Waiting for pending cancellation");
            return rescheduleDelete;
        }

        try {
            var observeConfig = ctx.getObserveConfig();
            var suspendMode =
                    observeConfig.getBoolean(KubernetesOperatorConfigOptions.SAVEPOINT_ON_DELETION)
                            ? SuspendMode.SAVEPOINT
                            : SuspendMode.STATELESS;
            if (cancelJob(ctx, suspendMode)) {
                LOG.info("Waiting for pending cancellation");
                return rescheduleDelete;
            }
            return DeleteControl.defaultDelete();
        } catch (Exception e) {
            LOG.error("Failed to cancel job, rescheduling deletion.", e);
            return rescheduleDelete;
        }
    }

    public static boolean sessionClusterReady(Optional<FlinkDeployment> flinkDeploymentOpt) {
        if (flinkDeploymentOpt.isPresent()) {
            var flinkdep = flinkDeploymentOpt.get();
            var jobmanagerDeploymentStatus = flinkdep.getStatus().getJobManagerDeploymentStatus();
            if (jobmanagerDeploymentStatus != JobManagerDeploymentStatus.READY) {
                LOG.info(
                        "Session cluster deployment is in {} status, not ready for serve",
                        jobmanagerDeploymentStatus);
                return false;
            } else {
                return true;
            }
        } else {
            LOG.warn("Session cluster deployment is not found");
            return false;
        }
    }
}
