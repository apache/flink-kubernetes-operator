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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;

import org.apache.commons.lang3.StringUtils;

import java.time.Duration;

/** Savepoint utilities. */
public class SavepointUtils {

    public static boolean savepointInProgress(JobStatus jobStatus) {
        return StringUtils.isNotEmpty(jobStatus.getSavepointInfo().getTriggerId());
    }

    public static boolean shouldTriggerSavepoint(JobSpec jobSpec, FlinkDeploymentStatus status) {
        if (savepointInProgress(status.getJobStatus())) {
            return false;
        }
        return jobSpec.getSavepointTriggerNonce() != null
                && !jobSpec.getSavepointTriggerNonce()
                        .equals(
                                status.getReconciliationStatus()
                                        .deserializeLastReconciledSpec()
                                        .getJob()
                                        .getSavepointTriggerNonce());
    }

    public static boolean gracePeriodEnded(
            FlinkOperatorConfiguration configuration, SavepointInfo savepointInfo) {
        Duration gracePeriod = configuration.getSavepointTriggerGracePeriod();
        long triggerTimestamp = savepointInfo.getTriggerTimestamp();
        return (System.currentTimeMillis() - triggerTimestamp) > gracePeriod.toMillis();
    }
}
