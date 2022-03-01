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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Observes the actual state of the running jobs on the Flink cluster. */
public class Observer {

    private static final Logger LOG = LoggerFactory.getLogger(Observer.class);

    private final FlinkService flinkService;
    private final FlinkOperatorConfiguration operatorConfiguration;

    public Observer(FlinkService flinkService, FlinkOperatorConfiguration operatorConfiguration) {
        this.flinkService = flinkService;
        this.operatorConfiguration = operatorConfiguration;
    }

    public boolean observe(
            FlinkDeployment flinkApp, Context context, Configuration effectiveConfig) {
        observeJmDeployment(flinkApp, context, effectiveConfig);
        return isReadyToReconcile(flinkApp, effectiveConfig);
    }

    private void observeJmDeployment(
            FlinkDeployment flinkApp, Context context, Configuration effectiveConfig) {
        FlinkDeploymentStatus deploymentStatus = flinkApp.getStatus();
        JobManagerDeploymentStatus previousJmStatus =
                deploymentStatus.getJobManagerDeploymentStatus();

        if (JobManagerDeploymentStatus.READY == previousJmStatus) {
            return;
        }

        if (JobManagerDeploymentStatus.DEPLOYED_NOT_READY == previousJmStatus) {
            deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
            return;
        }

        Optional<Deployment> deployment = context.getSecondaryResource(Deployment.class);
        if (deployment.isPresent()) {
            DeploymentStatus status = deployment.get().getStatus();
            DeploymentSpec spec = deployment.get().getSpec();
            if (status != null
                    && status.getAvailableReplicas() != null
                    && spec.getReplicas().intValue() == status.getReplicas()
                    && spec.getReplicas().intValue() == status.getAvailableReplicas()
                    && flinkService.isJobManagerPortReady(effectiveConfig)) {

                // typically it takes a few seconds for the REST server to be ready
                LOG.info(
                        "JobManager deployment {} in namespace {} port ready, waiting for the REST API...",
                        flinkApp.getMetadata().getName(),
                        flinkApp.getMetadata().getNamespace());
                deploymentStatus.setJobManagerDeploymentStatus(
                        JobManagerDeploymentStatus.DEPLOYED_NOT_READY);
                return;
            }
            LOG.info(
                    "JobManager deployment {} in namespace {} exists but not ready yet, status {}",
                    flinkApp.getMetadata().getName(),
                    flinkApp.getMetadata().getNamespace(),
                    status);

            deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
            return;
        }

        deploymentStatus.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
    }

    private boolean observeFlinkJobStatus(FlinkDeployment flinkApp, Configuration effectiveConfig) {

        // No need to observe job status for session clusters
        if (flinkApp.getSpec().getJob() == null) {
            return true;
        }

        LOG.info("Getting job statuses for {}", flinkApp.getMetadata().getName());
        FlinkDeploymentStatus flinkAppStatus = flinkApp.getStatus();

        Collection<JobStatusMessage> clusterJobStatuses;
        try {
            clusterJobStatuses = flinkService.listJobs(effectiveConfig);
        } catch (Exception e) {
            LOG.error("Exception while listing jobs", e);
            flinkAppStatus.getJobStatus().setState("UNKNOWN");
            return false;
        }
        if (clusterJobStatuses.isEmpty()) {
            LOG.info("No jobs found on {} yet", flinkApp.getMetadata().getName());
            return false;
        } else {
            updateJobStatus(flinkAppStatus.getJobStatus(), new ArrayList<>(clusterJobStatuses));
            LOG.info("Job statuses updated for {}", flinkApp.getMetadata().getName());
            return true;
        }
    }

    private boolean observeSavepointStatus(
            FlinkDeployment flinkApp, Configuration effectiveConfig) {
        SavepointInfo savepointInfo = flinkApp.getStatus().getJobStatus().getSavepointInfo();
        if (savepointInfo.getTriggerId() == null) {
            LOG.debug("Checkpointing not in progress");
            return true;
        }
        SavepointFetchResult savepointFetchResult;
        try {
            savepointFetchResult = flinkService.fetchSavepointInfo(flinkApp, effectiveConfig);
        } catch (Exception e) {
            LOG.error("Exception while fetching savepoint info", e);
            return false;
        }

        if (!savepointFetchResult.isTriggered()) {
            String error = savepointFetchResult.getError();
            if (error != null
                    || SavepointUtils.gracePeriodEnded(operatorConfiguration, savepointInfo)) {
                String errorMsg = error != null ? error : "Savepoint status unknown";
                LOG.error(errorMsg);
                savepointInfo.setTriggerId(null);
                ReconciliationUtils.updateForReconciliationError(flinkApp, errorMsg);
                return false;
            }
            LOG.info("Savepoint operation not running, waiting within grace period");
        }
        if (savepointFetchResult.getSavepoint() == null) {
            LOG.info("Savepoint not completed yet");
            return false;
        }

        savepointInfo.setLastSavepoint(savepointFetchResult.getSavepoint());
        savepointInfo.setTriggerId(null);
        return true;
    }

    private boolean isReadyToReconcile(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        JobManagerDeploymentStatus jmDeploymentStatus =
                flinkApp.getStatus().getJobManagerDeploymentStatus();

        switch (jmDeploymentStatus) {
            case READY:
                return observeFlinkJobStatus(flinkApp, effectiveConfig)
                        && observeSavepointStatus(flinkApp, effectiveConfig);
            case MISSING:
                return true;
            case DEPLOYING:
            case DEPLOYED_NOT_READY:
                return false;
            default:
                throw new RuntimeException("Unknown status: " + jmDeploymentStatus);
        }
    }

    /** Update previous job status based on the job list from the cluster. */
    private void updateJobStatus(JobStatus status, List<JobStatusMessage> clusterJobStatuses) {
        Collections.sort(
                clusterJobStatuses, (j1, j2) -> Long.compare(j2.getStartTime(), j1.getStartTime()));
        JobStatusMessage newJob = clusterJobStatuses.get(0);

        status.setState(newJob.getJobState().name());
        status.setJobName(newJob.getJobName());
        status.setJobId(newJob.getJobId().toHexString());
        // track the start time, changing timestamp would cause busy reconciliation
        status.setUpdateTime(String.valueOf(newJob.getStartTime()));
    }
}
