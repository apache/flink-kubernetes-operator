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

package org.apache.flink.kubernetes.operator.api.status;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.utils.ConditionUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.kubernetes.api.model.Condition;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.api.utils.ConditionUtils.CONDITION_TYPE_RUNNING;

/** Last observed status of the Flink deployment. */
@Experimental
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@SuperBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkDeploymentStatus extends CommonStatus<FlinkDeploymentSpec> {

    /** Information from running clusters. */
    private Map<String, String> clusterInfo = new HashMap<>();

    /** Last observed status of the JobManager deployment. */
    private JobManagerDeploymentStatus jobManagerDeploymentStatus =
            JobManagerDeploymentStatus.MISSING;

    /** Status of the last reconcile operation. */
    private FlinkDeploymentReconciliationStatus reconciliationStatus =
            new FlinkDeploymentReconciliationStatus();

    /** Information about the TaskManagers for the scale subresource. */
    private TaskManagerInfo taskManager;

    /** Condition of the CR . */
    private List<Condition> conditions = new ArrayList<>();

    public List<Condition> getConditions() {
        if (getJobStatus() != null && getJobStatus().getState() == null) {
            // Populate conditions for SessionMode deployment
            switch (jobManagerDeploymentStatus) {
                case READY:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.SESSION_MODE_CONDITION.get(
                                            JobManagerDeploymentStatus.READY.name())));
                    break;
                case MISSING:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.SESSION_MODE_CONDITION.get(
                                            JobManagerDeploymentStatus.MISSING.name())));
                    break;
                case DEPLOYING:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.SESSION_MODE_CONDITION.get(
                                            JobManagerDeploymentStatus.DEPLOYING.name())));
                    break;
                case DEPLOYED_NOT_READY:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.SESSION_MODE_CONDITION.get(
                                            JobManagerDeploymentStatus.DEPLOYED_NOT_READY.name())));
                    break;
                case ERROR:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.SESSION_MODE_CONDITION.get(
                                            JobManagerDeploymentStatus.ERROR.name())));
            }
        } else if (getJobStatus() != null && getJobStatus().getState() != null) {
            // Populate conditions for ApplicationMode deployment
            switch (getJobStatus().getState()) {
                case RECONCILING:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.APPLICATION_MODE_CONDITION.get(
                                            JobStatus.RECONCILING.name())));
                    break;
                case CREATED:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.APPLICATION_MODE_CONDITION.get(
                                            JobStatus.CREATED.name())));
                    break;
                case RUNNING:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.APPLICATION_MODE_CONDITION.get(
                                            JobStatus.RUNNING.name())));
                    break;
                case FAILING:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.APPLICATION_MODE_CONDITION.get(
                                            JobStatus.FAILING.name())));
                    break;
                case RESTARTING:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.APPLICATION_MODE_CONDITION.get(
                                            JobStatus.RESTARTING.name())));
                    break;
                case FAILED:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.APPLICATION_MODE_CONDITION.get(
                                            JobStatus.FAILED.name())));
                    break;
                case FINISHED:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.APPLICATION_MODE_CONDITION.get(
                                            JobStatus.FINISHED.name())));
                    break;

                case CANCELED:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.APPLICATION_MODE_CONDITION.get(
                                            JobStatus.CANCELED.name())));
                    break;
                case SUSPENDED:
                    updateCondition(
                            conditions,
                            ConditionUtils.crCondition(
                                    ConditionUtils.APPLICATION_MODE_CONDITION.get(
                                            JobStatus.SUSPENDED.name())));
                    break;
            }
        }
        return conditions;
    }

    private static void updateCondition(List<Condition> conditions, Condition newCondition) {
        if (newCondition.getType().equals(CONDITION_TYPE_RUNNING)) {
            Optional<Condition> existingCondition =
                    conditions.stream()
                            .filter(
                                    c ->
                                            c.getType().equals(CONDITION_TYPE_RUNNING)
                                                    && c.getReason()
                                                            .equals(newCondition.getReason())
                                                    && c.getMessage()
                                                            .equals(newCondition.getMessage()))
                            .findFirst();
            // Until there is a condition change which reflects the latest state, no need to add
            // condition to list.
            if (existingCondition.isPresent()) {
                return;
            }
            // Remove existing Condition with type running and then add a new condition that
            // reflects the current state.
            conditions.removeIf(
                    c ->
                            c.getType().equals(CONDITION_TYPE_RUNNING)
                                    && !c.getMessage().equals(newCondition.getMessage())
                                    && !c.getReason().equals(newCondition.getReason()));
        }
        conditions.add(newCondition);
    }
}
