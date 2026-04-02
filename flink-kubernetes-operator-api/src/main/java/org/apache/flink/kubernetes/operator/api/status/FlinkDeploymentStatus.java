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
import org.apache.flink.kubernetes.operator.api.Mode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.utils.ConditionsUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
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

    public static final String CONDITION_TYPE_RUNNING = "Running";

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

    @JsonIgnore
    @Override
    public boolean isJobCancellable() {
        return super.isJobCancellable() && jobManagerDeploymentStatus.isRestApiAvailable();
    }

    /**
     * Update the conditions with a RUNNING condition if required.
     *
     * @return a list of Conditions that will be either empty or containing a running condition.
     */
    public List<Condition> setRunningConditionIfRequired() {
        Condition conditionToAdd = null;

        if (reconciliationStatus != null) {
            FlinkDeploymentSpec deploymentSpec =
                    reconciliationStatus.deserializeLastReconciledSpec();

            if (deploymentSpec != null) {
                switch (Mode.getMode(deploymentSpec)) {
                    case APPLICATION:
                        conditionToAdd =
                                ConditionsUtils.createApplicationModeCondition(
                                        getJobStatus().getState());
                        break;
                    case SESSION:
                        conditionToAdd =
                                ConditionsUtils.createSessionModeCondition(
                                        jobManagerDeploymentStatus);
                }
                ConditionsUtils.updateLastTransitionTime(conditions, conditionToAdd);
            }
        }
        List<Condition> newConditions = new ArrayList<>();
        if (conditionToAdd != null) {
            newConditions = new ArrayList<>(List.of(conditionToAdd));
        }
        setConditions(newConditions);
        return newConditions;
    }
}
