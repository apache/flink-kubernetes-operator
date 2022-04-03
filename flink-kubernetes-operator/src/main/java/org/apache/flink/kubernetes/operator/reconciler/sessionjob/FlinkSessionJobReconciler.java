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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ApplicationReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** The reconciler for the {@link FlinkSessionJob}. */
public class FlinkSessionJobReconciler implements Reconciler<FlinkSessionJob> {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationReconciler.class);

    private final FlinkOperatorConfiguration operatorConfiguration;
    private final KubernetesClient kubernetesClient;
    private final FlinkService flinkService;

    public FlinkSessionJobReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkOperatorConfiguration operatorConfiguration) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
        this.operatorConfiguration = operatorConfiguration;
    }

    @Override
    public void reconcile(
            FlinkSessionJob flinkSessionJob, Context context, Configuration defaultConfig)
            throws Exception {

        FlinkSessionJobSpec lastReconciledSpec =
                flinkSessionJob.getStatus().getReconciliationStatus().getLastReconciledSpec();

        if (lastReconciledSpec == null) {
            submitFlinkJob(flinkSessionJob, context, defaultConfig);
            return;
        }

        boolean specChanged = !flinkSessionJob.getSpec().equals(lastReconciledSpec);

        if (specChanged) {
            // TODO reconcile other spec change.
            LOG.info("Other spec change have not supported");
        }
    }

    @Override
    public DeleteControl cleanup(
            FlinkSessionJob sessionJob, Context context, Configuration defaultConfig) {
        Optional<FlinkDeployment> flinkDepOptional =
                OperatorUtils.getSecondaryResource(sessionJob, context, operatorConfiguration);

        if (flinkDepOptional.isPresent()) {
            Configuration effectiveConfig =
                    FlinkUtils.getEffectiveConfig(flinkDepOptional.get(), defaultConfig);
            String jobID = sessionJob.getStatus().getJobStatus().getJobId();
            if (jobID != null) {
                try {
                    flinkService.cancelSessionJob(JobID.fromHexString(jobID), effectiveConfig);
                } catch (Exception e) {
                    LOG.error("Failed to cancel job.", e);
                }
            }
        } else {
            LOG.info("Session cluster deployment not available");
        }
        return DeleteControl.defaultDelete();
    }

    private void submitFlinkJob(
            FlinkSessionJob sessionJob, Context context, Configuration defaultConfig)
            throws Exception {
        Optional<FlinkDeployment> flinkDepOptional =
                OperatorUtils.getSecondaryResource(sessionJob, context, operatorConfiguration);
        if (flinkDepOptional.isPresent()) {
            var flinkdep = flinkDepOptional.get();
            var jobDeploymentStatus = flinkdep.getStatus().getJobManagerDeploymentStatus();
            if (jobDeploymentStatus == JobManagerDeploymentStatus.READY) {
                Configuration effectiveConfig =
                        FlinkUtils.getEffectiveConfig(flinkdep, defaultConfig);
                flinkService.submitJobToSessionCluster(sessionJob, effectiveConfig);
                ReconciliationUtils.updateForSpecReconciliationSuccess(sessionJob);
            } else {
                LOG.info(
                        "Session cluster deployment is in {} status, not ready for serve",
                        jobDeploymentStatus);
            }
        } else {
            LOG.info("Session cluster deployment is not found");
        }
    }
}
