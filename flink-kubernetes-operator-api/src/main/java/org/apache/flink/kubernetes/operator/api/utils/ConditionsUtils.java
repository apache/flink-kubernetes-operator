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

import java.time.Instant;
import java.util.List;

import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus.READY;

/** Utility methods for working with Kubernetes {@link Condition} objects. */
public class ConditionsUtils {

    /**
     * Updates the last transition time of the given condition based on the existing conditions
     * list. If a condition of the same type already exists and has the same status, the existing
     * transition time is preserved; otherwise a new timestamp is set.
     *
     * @param conditions the current list of conditions
     * @param condition the new condition whose last transition time should be updated
     */
    public static void updateLastTransitionTime(List<Condition> conditions, Condition condition) {
        if (condition == null) {
            return;
        }
        Condition existingCondition =
                conditions.stream()
                        .filter(c -> c.getType().equals(condition.getType()))
                        .findFirst()
                        .orElse(null);
        condition.setLastTransitionTime(getLastTransitionTimeStamp(existingCondition, condition));
    }

    /**
     * Creates a Running condition for application mode based on the given {@link JobStatus}.
     *
     * @param jobStatus the current job status
     * @return a {@link Condition} reflecting whether the job is running
     */
    public static Condition createApplicationModeCondition(JobStatus jobStatus) {
        return new ConditionBuilder()
                .withType(FlinkDeploymentStatus.CONDITION_TYPE_RUNNING)
                .withStatus(jobStatus == RUNNING ? "True" : "False")
                .withReason(toCamelCase(jobStatus.name()))
                .withMessage("Job status " + jobStatus.name())
                .build();
    }

    /**
     * Creates a Running condition for session mode based on the given {@link
     * JobManagerDeploymentStatus}.
     *
     * @param jmStatus the current JobManager deployment status
     * @return a {@link Condition} reflecting whether the session cluster is running
     */
    public static Condition createSessionModeCondition(JobManagerDeploymentStatus jmStatus) {
        return new ConditionBuilder()
                .withType(FlinkDeploymentStatus.CONDITION_TYPE_RUNNING)
                .withStatus(jmStatus == READY ? "True" : "False")
                .withReason(jmStatus.getReason())
                .withMessage(jmStatus.getMessage())
                .build();
    }

    /**
     * Converts a string to CamelCase by lower-casing it and upper-casing the first letter. Reason
     * in the condition object should be a CamelCase string, so need to convert JobStatus as all the
     * keywords are one noun, so we only need to upper case the first letter.
     *
     * @param reason the string to convert
     * @return CamelCase reason as String
     */
    private static String toCamelCase(String reason) {
        reason = reason.toLowerCase();
        return reason.substring(0, 1).toUpperCase() + reason.substring(1);
    }

    /**
     * Gets the last transition time for the condition. Returns the current time if there is no
     * existing condition or if the condition status has changed, otherwise returns the existing
     * condition's LastTransitionTime.
     *
     * @param existingCondition the current condition object, may be null
     * @param condition the new condition object to compare against the existing one
     * @return a string representing the last transition time in ISO 8601 format with nanosecond
     *     precision (e.g., "2025-10-30T07:35:35.189752790Z"). Returns a new timestamp if the
     *     existing condition is null or the status has changed, otherwise returns the last
     *     transition time of the existing condition.
     */
    private static String getLastTransitionTimeStamp(
            Condition existingCondition, Condition condition) {
        String lastTransitionTime;
        if (existingCondition == null
                || !existingCondition.getStatus().equals(condition.getStatus())) {
            lastTransitionTime = Instant.now().toString();
        } else {
            lastTransitionTime = existingCondition.getLastTransitionTime();
        }
        return lastTransitionTime;
    }
}
