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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Observes the actual state of the running jobs on the Flink cluster. */
public class JobStatusObserver {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatusObserver.class);

    private final FlinkService flinkService;

    public JobStatusObserver(FlinkService flinkService) {
        this.flinkService = flinkService;
    }

    public boolean observeFlinkJobStatus(FlinkDeployment flinkApp, Configuration effectiveConfig)
            throws Exception {
        FlinkDeploymentSpec lastReconciledSpec =
                flinkApp.getStatus().getReconciliationStatus().getLastReconciledSpec();

        if (lastReconciledSpec == null) {
            // This is the first run, nothing to observe
            return true;
        }

        JobSpec jobSpec = lastReconciledSpec.getJob();

        if (jobSpec == null) {
            // This is a session cluster, nothing to observe
            return true;
        }

        if (!jobSpec.getState().equals(JobState.RUNNING)) {
            // The job is not running, nothing to observe
            return true;
        }
        LOG.info("Getting job statuses for {}", flinkApp.getMetadata().getName());
        FlinkDeploymentStatus flinkAppStatus = flinkApp.getStatus();

        Collection<JobStatusMessage> clusterJobStatuses = flinkService.listJobs(effectiveConfig);
        if (clusterJobStatuses.isEmpty()) {
            LOG.info("No jobs found on {} yet", flinkApp.getMetadata().getName());
            return false;
        } else {
            flinkAppStatus.setJobStatus(
                    mergeJobStatus(
                            flinkAppStatus.getJobStatus(), new ArrayList<>(clusterJobStatuses)));
            LOG.info("Job statuses updated for {}", flinkApp.getMetadata().getName());
            return true;
        }
    }

    /** Merge previous job status with the new one from the flink job cluster. */
    private JobStatus mergeJobStatus(
            JobStatus oldStatus, List<JobStatusMessage> clusterJobStatuses) {
        JobStatus newStatus = oldStatus;
        Collections.sort(
                clusterJobStatuses,
                (j1, j2) -> -1 * Long.compare(j1.getStartTime(), j2.getStartTime()));
        JobStatusMessage newJob = clusterJobStatuses.get(0);

        if (newStatus == null) {
            newStatus = createJobStatus(newJob);
        } else {
            newStatus.setState(JobState.valueOf(newJob.getJobState().name()));
            newStatus.setJobName(newJob.getJobName());
            newStatus.setJobId(newJob.getJobId().toHexString());
        }
        return newStatus;
    }

    public static JobStatus createJobStatus(JobStatusMessage message) {
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobId(message.getJobId().toHexString());
        jobStatus.setJobName(message.getJobName());
        jobStatus.setState(JobState.valueOf(message.getJobState().name()));
        return jobStatus;
    }
}
