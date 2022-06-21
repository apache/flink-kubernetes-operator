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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractJobReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** The reconciler for the {@link FlinkSessionJob}. */
public class SessionJobReconciler
        extends AbstractJobReconciler<FlinkSessionJob, FlinkSessionJobSpec, FlinkSessionJobStatus> {

    private static final Logger LOG = LoggerFactory.getLogger(SessionJobReconciler.class);

    public SessionJobReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder) {
        super(kubernetesClient, flinkService, configManager, eventRecorder);
    }

    @Override
    protected Configuration getObserveConfig(FlinkSessionJob sessionJob, Context context) {
        return getDeployConfig(sessionJob.getMetadata(), sessionJob.getSpec(), context);
    }

    @Override
    protected Configuration getDeployConfig(
            ObjectMeta deployMeta, FlinkSessionJobSpec currentDeploySpec, Context context) {
        Optional<FlinkDeployment> deploymentOpt =
                context.getSecondaryResource(FlinkDeployment.class);

        if (!sessionClusterReady(deploymentOpt)) {
            return null;
        }
        return configManager.getSessionJobConfig(deploymentOpt.get(), currentDeploySpec);
    }

    @Override
    public boolean readyToReconcile(
            FlinkSessionJob flinkSessionJob, Context context, Configuration deployConfig) {
        return sessionClusterReady(context.getSecondaryResource(FlinkDeployment.class))
                && super.readyToReconcile(flinkSessionJob, context, deployConfig);
    }

    @Override
    protected void deploy(
            ObjectMeta meta,
            FlinkSessionJobSpec sessionJobSpec,
            FlinkSessionJobStatus status,
            Configuration deployConfig,
            Optional<String> savepoint,
            boolean requireHaMetadata)
            throws Exception {
        var jobID =
                flinkService.submitJobToSessionCluster(
                        meta, sessionJobSpec, deployConfig, savepoint.orElse(null));
        status.setJobStatus(
                new JobStatus()
                        .toBuilder()
                        .jobId(jobID.toHexString())
                        .state(org.apache.flink.api.common.JobStatus.RECONCILING.name())
                        .build());
    }

    @Override
    protected void cancelJob(
            FlinkSessionJob resource, UpgradeMode upgradeMode, Configuration observeConfig)
            throws Exception {
        flinkService.cancelSessionJob(resource, upgradeMode, observeConfig);
    }

    @Override
    public DeleteControl cleanupInternal(FlinkSessionJob sessionJob, Context context) {
        Optional<FlinkDeployment> flinkDepOptional =
                context.getSecondaryResource(FlinkDeployment.class);

        if (flinkDepOptional.isPresent()) {
            String jobID = sessionJob.getStatus().getJobStatus().getJobId();
            if (jobID != null) {
                try {
                    cancelJob(
                            sessionJob,
                            UpgradeMode.STATELESS,
                            getObserveConfig(sessionJob, context));
                } catch (Exception e) {
                    LOG.error("Failed to cancel job.", e);
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
