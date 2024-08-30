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
import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractJobReconciler;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

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

        // Generate job id and record in status for durability
        var jobId = JobID.generate();
        ctx.getResource().getStatus().getJobStatus().setJobId(jobId.toHexString());
        statusRecorder.patchAndCacheStatus(ctx.getResource(), ctx.getKubernetesClient());

        ctx.getFlinkService()
                .submitJobToSessionCluster(
                        ctx.getResource().getMetadata(),
                        sessionJobSpec,
                        jobId,
                        deployConfig,
                        savepoint.orElse(null));

        var status = ctx.getResource().getStatus();
        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.RECONCILING.name());
    }

    @Override
    protected void cancelJob(FlinkResourceContext<FlinkSessionJob> ctx, UpgradeMode upgradeMode)
            throws Exception {
        var conf = ObjectUtils.firstNonNull(ctx.getObserveConfig(), new Configuration());

        ctx.getFlinkService()
                .cancelSessionJob(ctx.getResource(), upgradeMode, conf)
                .ifPresent(location -> setUpgradeSavepointPath(ctx, location));
        ctx.getResource().getStatus().getJobStatus().setJobId(null);
    }

    @Override
    protected void cleanupAfterFailedJob(FlinkResourceContext<FlinkSessionJob> ctx) {
        // The job has already stopped, nothing to clean up.
    }

    @Override
    public DeleteControl cleanupInternal(FlinkResourceContext<FlinkSessionJob> ctx) {
        Optional<FlinkDeployment> flinkDepOptional =
                ctx.getJosdkContext().getSecondaryResource(FlinkDeployment.class);

        if (flinkDepOptional.isPresent()) {
            String jobID = ctx.getResource().getStatus().getJobStatus().getJobId();
            if (jobID != null) {
                try {
                    var observeConfig = ctx.getObserveConfig();
                    UpgradeMode upgradeMode =
                            observeConfig.getBoolean(
                                            KubernetesOperatorConfigOptions.SAVEPOINT_ON_DELETION)
                                    ? UpgradeMode.SAVEPOINT
                                    : UpgradeMode.STATELESS;
                    cancelJob(ctx, upgradeMode);
                } catch (ExecutionException e) {
                    if (AbstractFlinkService.isJobMissingOrTerminated(e)) {
                        return DeleteControl.defaultDelete();
                    }
                    long delay = ctx.getOperatorConfig().getProgressCheckInterval().toMillis();
                    LOG.error(
                            "Failed to cancel job {}, will reschedule after {} milliseconds.",
                            jobID,
                            delay,
                            e);
                    return DeleteControl.noFinalizerRemoval().rescheduleAfter(delay);
                } catch (Exception e) {
                    LOG.error("Failed to cancel job {}.", jobID, e);
                }
            }
        } else {
            LOG.info("Session cluster deployment not available");
        }
        return DeleteControl.defaultDelete();
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
