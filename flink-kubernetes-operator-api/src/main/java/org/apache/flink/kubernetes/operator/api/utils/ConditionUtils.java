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
import org.apache.flink.kubernetes.operator.api.Mode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentReconciliationStatus;
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

    /**
     * Creates a List of Condition object based on the provided FlinkDeploymentStatus.
     *
     * @param flinkDeploymentStatus the FlinkDeploymentStatus object containing job status
     *     information
     * @return a list of Condition object representing the current status of the Flink deployment
     */
    public static List<Condition> createConditionFromStatus(
            FlinkDeploymentStatus flinkDeploymentStatus) {

        FlinkDeploymentReconciliationStatus reconciliationStatus =
                flinkDeploymentStatus.getReconciliationStatus();
        Condition conditionToAdd = null;

        if (reconciliationStatus != null) {
            FlinkDeploymentSpec deploymentSpec =
                    reconciliationStatus.deserializeLastReconciledSpec();

            if (deploymentSpec != null) {
                switch (Mode.getMode(deploymentSpec)) {
                    case APPLICATION:
                        conditionToAdd =
                                getApplicationModeCondition(
                                        flinkDeploymentStatus.getJobStatus().getState());
                        break;
                    case SESSION:
                        conditionToAdd =
                                getSessionModeCondition(
                                        flinkDeploymentStatus.getJobManagerDeploymentStatus());
                }
                updateLastTransitionTime(flinkDeploymentStatus.getConditions(), conditionToAdd);
            }
        }
        return conditionToAdd == null ? List.of() : List.of(conditionToAdd);
    }

    private static void updateLastTransitionTime(List<Condition> conditions, Condition condition) {
        if (condition == null) {
            return;
        }
        Condition existingCondition = conditions.isEmpty() ? null : conditions.get(0);
        condition.setLastTransitionTime(getLastTransitionTimeStamp(existingCondition, condition));
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

    /**
     * Reason in the condition object should be a CamelCase string, so need to convert JobStatus as
     * all the keywords are one noun, so we only need to upper case the first letter.
     *
     * @return CamelCase reason as String
     */
    private static String toCamelCase(String reason) {
        reason = reason.toLowerCase();
        return reason.substring(0, 1).toUpperCase() + reason.substring(1);
    }

    private static boolean isLastTransitionTimeStampUpdateRequired(
            Condition existingCondition, Condition newCondition) {
        return existingCondition == null
                || !existingCondition.getStatus().equals(newCondition.getStatus());
    }

    /**
     * get the last transition time for the condition , returns the current time if there is no
     * existing condition or if the condition status has changed, otherwise returns existing
     * condition LastTransitionTime.
     *
     * @param existingCondition The current condition object, may be null.
     * @param condition The new condition object to compare against the existing one.
     * @return A string representing the last transition time in the format
     *     "yyyy-MM-dd'T'HH:mm:ss'Z'". Returns a new timestamp if the existing condition is null or
     *     the status has changed, otherwise returns the last transition time of the existing
     *     condition.
     */
    private static String getLastTransitionTimeStamp(
            Condition existingCondition, Condition condition) {
        String lastTransitionTime;
        if (existingCondition == null
                || !existingCondition.getStatus().equals(condition.getStatus())) {
            lastTransitionTime =
                    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date());
        } else {
            lastTransitionTime = existingCondition.getLastTransitionTime();
        }
        return lastTransitionTime;
    }
}
