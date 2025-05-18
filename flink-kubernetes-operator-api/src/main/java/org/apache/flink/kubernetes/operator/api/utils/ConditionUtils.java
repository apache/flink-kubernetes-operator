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

package org.apache.flink.kubernetes.operator.api.utils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;

import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus.READY;

/** Creates a condition object with the type, status, message and reason. */
public class ConditionUtils {
    public static final String CONDITION_TYPE_RUNNING = "Running";

    public static Condition getCondition(FlinkDeploymentStatus flinkDeploymentStatus) {
        org.apache.flink.kubernetes.operator.api.status.JobStatus status =
                flinkDeploymentStatus.getJobStatus();
        Condition conditionToAdd = null;
        if (status != null) {

            JobStatus jobStatus = status.getState();

            conditionToAdd =
                    jobStatus == null
                            ? getSessionModeCondition(
                                    flinkDeploymentStatus.getJobManagerDeploymentStatus())
                            : getApplicationModeCondition(jobStatus);
        }

        return conditionToAdd;
    }

    public static void updateLastTransitionTime(List<Condition> conditions, Condition condition) {
        if (condition == null) {
            return;
        }
        Condition existingCondition = conditions.isEmpty() ? null : conditions.get(0);
        if (isLastTransactionTimeStampUpdateRequired(existingCondition, condition)) {
            condition.setLastTransitionTime(
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
        } else {
            condition.setLastTransitionTime(existingCondition.getLastTransitionTime());
        }
    }

    private static Condition getApplicationModeCondition(JobStatus jobStatus) {
        return new ConditionBuilder()
                .withType(CONDITION_TYPE_RUNNING)
                .withStatus(jobStatus == RUNNING ? "True" : "False")
                .withReason(toCamelCase(jobStatus.name()))
                .withMessage("Job state " + jobStatus.name())
                .build();
    }

    private static Condition getSessionModeCondition(JobManagerDeploymentStatus jmStatus) {
        return new ConditionBuilder()
                .withType(CONDITION_TYPE_RUNNING)
                .withStatus(jmStatus == READY ? "True" : "False")
                .withReason(jmStatus.getReason())
                .withMessage(jmStatus.getMessage())
                .build();
    }

    private static String toCamelCase(String reason) {
        reason = reason.toLowerCase();
        return reason.substring(0, 1).toUpperCase() + reason.substring(1);
    }

    private static boolean isLastTransactionTimeStampUpdateRequired(
            Condition existingCondition, Condition newCondition) {
        return existingCondition == null
                || !existingCondition.getStatus().equals(newCondition.getStatus());
    }
}
