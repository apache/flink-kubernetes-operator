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

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;

import org.slf4j.Logger;

import java.util.Optional;

/** A tool for session job management condition checker. */
public class SessionJobHelper {

    private final Logger logger;
    private final FlinkSessionJob sessionJob;
    private final FlinkSessionJobSpec lastReconciledSpec;

    public SessionJobHelper(FlinkSessionJob sessionJob, Logger logger) {
        this.sessionJob = sessionJob;
        this.lastReconciledSpec =
                sessionJob.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        this.logger = logger;
    }

    public boolean savepointInProgress() {
        return SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus());
    }

    public boolean shouldTriggerSavepoint() {
        if (lastReconciledSpec == null) {
            return false;
        }
        if (savepointInProgress()) {
            return false;
        }
        return savepointTriggerNonce() != null
                && !savepointTriggerNonce()
                        .equals(lastReconciledSpec.getJob().getSavepointTriggerNonce());
    }

    private Long savepointTriggerNonce() {
        return sessionJob.getSpec().getJob().getSavepointTriggerNonce();
    }

    public boolean specChanged(FlinkSessionJobSpec lastReconciledSpec) {
        return !sessionJob.getSpec().equals(lastReconciledSpec);
    }

    public boolean isJobRunning(FlinkDeployment deployment) {
        FlinkDeploymentStatus status = deployment.getStatus();
        JobManagerDeploymentStatus deploymentStatus = status.getJobManagerDeploymentStatus();
        return deploymentStatus == JobManagerDeploymentStatus.READY
                && org.apache.flink.api.common.JobStatus.RUNNING
                        .name()
                        .equals(sessionJob.getStatus().getJobStatus().getState());
    }

    public boolean sessionClusterReady(Optional<FlinkDeployment> flinkDeploymentOpt) {
        if (flinkDeploymentOpt.isPresent()) {
            var flinkdep = flinkDeploymentOpt.get();
            var jobmanagerDeploymentStatus = flinkdep.getStatus().getJobManagerDeploymentStatus();
            if (jobmanagerDeploymentStatus != JobManagerDeploymentStatus.READY) {
                logger.info(
                        "Session cluster deployment is in {} status, not ready for serve",
                        jobmanagerDeploymentStatus);
                return false;
            } else {
                return true;
            }
        } else {
            logger.info("Session cluster deployment is not found");
            return false;
        }
    }
}
