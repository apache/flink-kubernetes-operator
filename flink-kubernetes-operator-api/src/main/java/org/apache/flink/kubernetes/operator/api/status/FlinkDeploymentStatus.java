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
            switch (jobManagerDeploymentStatus) {
                case READY:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningTrue(
                                    "JobManager is running and ready to receive REST API call",
                                    "JobManager is running and ready to receive REST API call"));
                    break;
                case MISSING:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    "JobManager deployment not found ",
                                    "JobManager deployment not found "));
                    break;
                case DEPLOYING:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    "JobManager process is starting up",
                                    "JobManager process is starting up"));
                    break;
                case DEPLOYED_NOT_READY:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    "JobManager is running but not ready yet to receive REST API calls",
                                    "JobManager is running but not ready yet to receive REST API calls"));
                    break;
                case ERROR:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    "Deployment in terminal error, requires spec change for reconciliation to continue",
                                    "JobManager deployment failed"));
            }
        } else if (getJobStatus() != null && getJobStatus().getState() != null) {
            switch (getJobStatus().getState()) {
                case RECONCILING:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    JobStatus.RECONCILING.name(), "Job is currently reconciling"));
                    break;
                case CREATED:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    JobStatus.CREATED.name(), "Job is created"));
                    break;
                case RUNNING:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningTrue(JobStatus.RUNNING.name(), "Job is running"));
                    break;
                case FAILING:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    JobStatus.FAILING.name(), "Job has failed"));
                    break;
                case RESTARTING:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    JobStatus.RESTARTING.name(),
                                    "The job is currently undergoing a restarting"));
                    break;
                case FAILED:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    JobStatus.FAILED.name(),
                                    "The job has failed with a non-recoverable task failure"));
                    break;
                case FINISHED:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    JobStatus.FINISHED.name(),
                                    "Job's tasks have successfully finished"));
                    break;

                case CANCELED:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    JobStatus.CANCELED.name(), "Job has been cancelled"));
                    break;
                case SUSPENDED:
                    updateCondition(
                            conditions,
                            ConditionUtils.runningFalse(
                                    JobStatus.SUSPENDED.name(), "The job has been suspended"));
                    break;
            }
        }
        return conditions;
    }

    public String getPhase() {
        if (reconciliationStatus != null
                && reconciliationStatus.deserializeLastReconciledSpec() != null
                && reconciliationStatus.deserializeLastReconciledSpec().getJob() == null) {
            switch (jobManagerDeploymentStatus) {
                case READY:
                    phase = "Running";
                    break;
                case MISSING:
                case ERROR:
                case DEPLOYING:
                    phase = "Pending";
                    break;
            }
        } else if (getJobStatus() != null && getJobStatus().getState() != null) {
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
