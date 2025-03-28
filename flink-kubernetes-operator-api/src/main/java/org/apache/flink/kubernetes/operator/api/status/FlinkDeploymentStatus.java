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

    private String phase;

    public List<Condition> getConditions() {
        if (reconciliationStatus != null
                && reconciliationStatus.deserializeLastReconciledSpec() != null
                && reconciliationStatus.deserializeLastReconciledSpec().getJob() == null) {
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

    public String getPhase() {
        if (reconciliationStatus != null
                && reconciliationStatus.deserializeLastReconciledSpec() != null
                && reconciliationStatus.deserializeLastReconciledSpec().getJob() == null) {
            // populate phase for SessionMode deployment
            switch (jobManagerDeploymentStatus) {
                case READY:
                    phase = "Running";
                    break;
                case MISSING:
                case DEPLOYING:
                    phase = "Pending";
                    break;
                case ERROR:
                    phase = "Failed";
                    break;
            }
        } else if (getJobStatus() != null && getJobStatus().getState() != null) {
            // populate phase for ApplicationMode deployment
            switch (getJobStatus().getState()) {
                case RECONCILING:
                    phase = "Pending";
                    break;
                case CREATED:
                    phase = JobStatus.CREATED.name();
                    break;
                case RUNNING:
                    phase = JobStatus.RUNNING.name();
                    break;
                case FAILING:
                    phase = JobStatus.FAILING.name();
                    break;
                case RESTARTING:
                    phase = JobStatus.RESTARTING.name();
                    break;
                case FAILED:
                    phase = JobStatus.FAILED.name();
                    break;
                case FINISHED:
                    phase = JobStatus.FINISHED.name();
                    break;
                case CANCELED:
                    phase = JobStatus.CANCELED.name();
                    break;
                case SUSPENDED:
                    phase = JobStatus.SUSPENDED.name();
                    break;
            }
        }
        return phase;
    }

    private static void updateCondition(List<Condition> conditions, Condition condition) {
        if (conditions.isEmpty()) {
            conditions.add(condition);
            return;
        }
        // If new condition is same as last condition, ignore
        Condition existingCondition = conditions.get(conditions.size() - 1);
        if (existingCondition.getType().equals(condition.getType())
                && existingCondition.getMessage().equals(condition.getMessage())) {
            return;
        }
        conditions.add(condition);
    }
}
