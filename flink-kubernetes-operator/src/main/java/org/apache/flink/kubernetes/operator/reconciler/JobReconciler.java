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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Reconciler responsible for handling the job lifecycle according to the desired and current
 * states.
 */
public class JobReconciler extends BaseReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(JobReconciler.class);

    public JobReconciler(KubernetesClient kubernetesClient, FlinkService flinkService) {
        super(kubernetesClient, flinkService);
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(
            String operatorNamespace,
            FlinkDeployment flinkApp,
            Context context,
            Configuration effectiveConfig)
            throws Exception {
        FlinkDeploymentSpec lastReconciledSpec =
                flinkApp.getStatus().getReconciliationStatus().getLastReconciledSpec();
        JobSpec jobSpec = flinkApp.getSpec().getJob();
        if (lastReconciledSpec == null) {
            deployFlinkJob(
                    flinkApp,
                    effectiveConfig,
                    Optional.ofNullable(jobSpec.getInitialSavepointPath()));
            IngressUtils.updateIngressRules(
                    flinkApp, effectiveConfig, operatorNamespace, kubernetesClient, false);
            return JobManagerDeploymentStatus.DEPLOYING.toUpdateControl(flinkApp);
        }

        // TODO: following assumes that current job is running
        // What if it never enters running state due to bad deployment?
        boolean specChanged = !flinkApp.getSpec().equals(lastReconciledSpec);
        if (specChanged) {
            JobState currentJobState = lastReconciledSpec.getJob().getState();
            JobState desiredJobState = jobSpec.getState();

            UpgradeMode upgradeMode = jobSpec.getUpgradeMode();
            if (currentJobState == JobState.RUNNING) {
                if (desiredJobState == JobState.RUNNING) {
                    upgradeFlinkJob(flinkApp, effectiveConfig);
                }
                if (desiredJobState.equals(JobState.SUSPENDED)) {
                    printCancelLogs(upgradeMode, flinkApp.getMetadata().getName());
                    cancelJob(flinkApp, upgradeMode, effectiveConfig);
                }
            }
            if (currentJobState == JobState.SUSPENDED && desiredJobState == JobState.RUNNING) {
                if (upgradeMode == UpgradeMode.STATELESS) {
                    deployFlinkJob(flinkApp, effectiveConfig, Optional.empty());
                } else if (upgradeMode == UpgradeMode.SAVEPOINT) {
                    restoreFromLastSavepoint(flinkApp, effectiveConfig);
                } else if (upgradeMode == UpgradeMode.LAST_STATE) {
                    final String savepointLocation =
                            flinkApp.getStatus().getJobStatus().getSavepointLocation();
                    // Upgrade mode changes from savepoint -> last-state
                    deployFlinkJob(
                            flinkApp, effectiveConfig, Optional.ofNullable(savepointLocation));
                }
            }
        }

        return UpdateControl.updateStatus(flinkApp)
                .rescheduleAfter(REFRESH_SECONDS, TimeUnit.SECONDS);
    }

    private void deployFlinkJob(
            FlinkDeployment flinkApp, Configuration effectiveConfig, Optional<String> savepoint)
            throws Exception {
        if (savepoint.isPresent()) {
            effectiveConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, savepoint.get());
        } else {
            effectiveConfig.removeConfig(SavepointConfigOptions.SAVEPOINT_PATH);
        }
        flinkService.submitApplicationCluster(flinkApp, effectiveConfig);
    }

    private void upgradeFlinkJob(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        LOG.info("Upgrading running job");
        final Optional<String> savepoint =
                cancelJob(flinkApp, flinkApp.getSpec().getJob().getUpgradeMode(), effectiveConfig);
        deployFlinkJob(flinkApp, effectiveConfig, savepoint);
    }

    private void restoreFromLastSavepoint(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        JobStatus jobStatus = flinkApp.getStatus().getJobStatus();
        deployFlinkJob(flinkApp, effectiveConfig, Optional.of(jobStatus.getSavepointLocation()));
    }

    private void printCancelLogs(UpgradeMode upgradeMode, String name) {
        switch (upgradeMode) {
            case STATELESS:
                LOG.info("Cancelling {}", name);
                break;
            case SAVEPOINT:
                LOG.info("Suspending {}", name);
                break;
            case LAST_STATE:
                LOG.info("Cancelling {} with last state retained", name);
                break;
            default:
                throw new RuntimeException("Unsupported upgrade mode " + upgradeMode);
        }
    }

    private Optional<String> cancelJob(
            FlinkDeployment flinkApp, UpgradeMode upgradeMode, Configuration effectiveConfig)
            throws Exception {
        Optional<String> savepointOpt =
                flinkService.cancelJob(
                        JobID.fromHexString(flinkApp.getStatus().getJobStatus().getJobId()),
                        upgradeMode,
                        effectiveConfig);
        JobStatus jobStatus = flinkApp.getStatus().getJobStatus();
        jobStatus.setState("suspended");
        flinkApp.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        savepointOpt.ifPresent(jobStatus::setSavepointLocation);
        return savepointOpt;
    }

    @Override
    protected void shutdown(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        if (org.apache.flink.api.common.JobStatus.RUNNING
                .name()
                .equalsIgnoreCase(flinkApp.getStatus().getJobStatus().getState())) {
            LOG.info("Job is running, attempting graceful shutdown.");
            try {
                flinkService.cancelJob(
                        JobID.fromHexString(flinkApp.getStatus().getJobStatus().getJobId()),
                        UpgradeMode.STATELESS,
                        effectiveConfig);
                return;
            } catch (Exception e) {
                LOG.error("Could not shut down cluster gracefully, deleting...", e);
            }
        }

        FlinkUtils.deleteCluster(flinkApp, kubernetesClient, true);
    }
}
