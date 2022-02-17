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
import org.apache.flink.kubernetes.operator.exception.InvalidDeploymentException;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Reconciler responsible for handling the job lifecycle according to the desired and current
 * states.
 */
public class JobReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(JobReconciler.class);

    private final KubernetesClient kubernetesClient;
    private final FlinkService flinkService;

    public JobReconciler(KubernetesClient kubernetesClient, FlinkService flinkService) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
    }

    public void reconcile(
            String operatorNamespace, FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {

        FlinkDeploymentSpec lastReconciledSpec =
                flinkApp.getStatus().getReconciliationStatus().getLastReconciledSpec();
        JobSpec jobSpec = flinkApp.getSpec().getJob();
        if (lastReconciledSpec == null) {
            if (!jobSpec.getState().equals(JobState.RUNNING)) {
                throw new InvalidDeploymentException("Job must start in running state");
            }
            deployFlinkJob(
                    flinkApp,
                    effectiveConfig,
                    Optional.ofNullable(jobSpec.getInitialSavepointPath()));
            IngressUtils.updateIngressRules(
                    flinkApp, effectiveConfig, operatorNamespace, kubernetesClient, false);
            return;
        }

        boolean specChanged = !flinkApp.getSpec().equals(lastReconciledSpec);
        if (specChanged) {
            if (lastReconciledSpec.getJob() == null) {
                throw new InvalidDeploymentException("Cannot switch from session to job cluster");
            }
            JobState currentJobState = lastReconciledSpec.getJob().getState();
            JobState desiredJobState = jobSpec.getState();

            UpgradeMode upgradeMode = jobSpec.getUpgradeMode();
            if (currentJobState == JobState.RUNNING) {
                if (desiredJobState == JobState.RUNNING) {
                    upgradeFlinkJob(flinkApp, effectiveConfig);
                }
                if (desiredJobState.equals(JobState.SUSPENDED)) {
                    if (upgradeMode == UpgradeMode.STATELESS) {
                        cancelJob(flinkApp, effectiveConfig);
                    } else {
                        suspendJob(flinkApp, effectiveConfig);
                    }
                }
            }
            if (currentJobState == JobState.SUSPENDED) {
                if (desiredJobState == JobState.RUNNING) {
                    if (upgradeMode == UpgradeMode.STATELESS) {
                        deployFlinkJob(flinkApp, effectiveConfig, Optional.empty());
                    } else if (upgradeMode == UpgradeMode.SAVEPOINT) {
                        restoreFromLastSavepoint(flinkApp, effectiveConfig);
                    } else {
                        throw new InvalidDeploymentException(
                                "Only savepoint and stateless strategies are supported at the moment.");
                    }
                }
            }
        }
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
        Optional<String> savepoint = cancelJob(flinkApp, effectiveConfig);
        deployFlinkJob(flinkApp, effectiveConfig, savepoint);
    }

    private void restoreFromLastSavepoint(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        JobStatus jobStatus = flinkApp.getStatus().getJobStatus();

        String savepointLocation = jobStatus.getSavepointLocation();
        if (savepointLocation == null) {
            throw new InvalidDeploymentException(
                    "Cannot perform stateful restore without a valid savepoint");
        }
        deployFlinkJob(flinkApp, effectiveConfig, Optional.of(savepointLocation));
    }

    private Optional<String> suspendJob(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        LOG.info("Suspending {}", flinkApp.getMetadata().getName());
        return cancelJob(flinkApp, UpgradeMode.SAVEPOINT, effectiveConfig);
    }

    private Optional<String> cancelJob(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        LOG.info("Cancelling {}", flinkApp.getMetadata().getName());
        UpgradeMode upgradeMode = flinkApp.getSpec().getJob().getUpgradeMode();
        return cancelJob(flinkApp, upgradeMode, effectiveConfig);
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
        savepointOpt.ifPresent(jobStatus::setSavepointLocation);
        return savepointOpt;
    }
}
