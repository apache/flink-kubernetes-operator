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
import org.apache.flink.annotation.Internal;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.utils.ConditionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.model.annotation.PrinterColumn;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/** Last observed common status of the Flink deployment/Flink SessionJob. */
@Experimental
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public abstract class CommonStatus<SPEC extends AbstractFlinkSpec> {

    /** Last observed status of the Flink job on Application/Session cluster. */
    private JobStatus jobStatus = new JobStatus();

    /** Error information about the FlinkDeployment/FlinkSessionJob. */
    private String error;

    /** Lifecycle state of the Flink resource (including being rolled back, failed etc.). */
    @PrinterColumn(name = "Lifecycle State")
    // Calculated from the status, requires no setter. The purpose of this is to expose as a printer
    // column.
    private ResourceLifecycleState lifecycleState;

    private List<Condition> conditions = new ArrayList<>();

    /**
     * Current reconciliation status of this resource.
     *
     * @return Current {@link ReconciliationStatus}.
     */
    public abstract ReconciliationStatus<SPEC> getReconciliationStatus();

    public ResourceLifecycleState getLifecycleState() {
        var reconciliationStatus = getReconciliationStatus();

        if (reconciliationStatus.isBeforeFirstDeployment()) {
            return StringUtils.isEmpty(error)
                    ? ResourceLifecycleState.CREATED
                    : ResourceLifecycleState.FAILED;
        }

        switch (reconciliationStatus.getState()) {
            case UPGRADING:
                return ResourceLifecycleState.UPGRADING;
            case ROLLING_BACK:
                return ResourceLifecycleState.ROLLING_BACK;
        }

        var lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();
        if (lastReconciledSpec.getJob() != null
                && lastReconciledSpec.getJob().getState() == JobState.SUSPENDED) {
            return ResourceLifecycleState.SUSPENDED;
        }

        var jobState = getJobStatus().getState();
        if (jobState != null
                && org.apache.flink.api.common.JobStatus.valueOf(jobState)
                        .equals(org.apache.flink.api.common.JobStatus.FAILED)) {
            return ResourceLifecycleState.FAILED;
        }

        if (reconciliationStatus.getState() == ReconciliationState.ROLLED_BACK) {
            return ResourceLifecycleState.ROLLED_BACK;
        } else if (reconciliationStatus.isLastReconciledSpecStable()) {
            return ResourceLifecycleState.STABLE;
        }

        return ResourceLifecycleState.DEPLOYED;
    }

    /**
     * Internal flag to signal that due to some condition we need to schedule a new reconciliation
     * loop immediately. For example autoscaler overrides have changed and we need to apply them.
     */
    @JsonIgnore @Internal private boolean immediateReconciliationNeeded = false;

    public List<Condition> getConditions() {
        switch (getLifecycleState()) {
            case CREATED:
                updateConditionIfNotExist(
                        conditions,
                        ConditionUtils.notReady(
                                "The resource was created in Kubernetes but not yet handled by the operator"));
                break;
            case SUSPENDED:
                updateConditionIfNotExist(
                        conditions,
                        ConditionUtils.notReady("The resource (job) has been suspended"));
                break;
            case UPGRADING:
                updateConditionIfNotExist(
                        conditions, ConditionUtils.notReady("The resource is being upgraded"));
                break;
            case DEPLOYED:
                updateConditionIfNotExist(
                        conditions,
                        ConditionUtils.ready(
                                "The resource is deployed, but it’s not yet considered to be stable and might be rolled back in the future"));
                break;
            case ROLLING_BACK:
                updateConditionIfNotExist(
                        conditions,
                        ConditionUtils.notReady(
                                "The resource is being rolled back to the last stable spec"));
                break;
            case ROLLED_BACK:
                updateConditionIfNotExist(
                        conditions,
                        ConditionUtils.ready("The resource is deployed with the last stable spec"));
                break;
            case FAILED:
                updateConditionIfNotExist(conditions, ConditionUtils.error("failed"));
                break;
            case STABLE:
                updateConditionIfNotExist(
                        conditions,
                        ConditionUtils.ready(
                                "The resource deployment is considered to be stable and won’t be rolled back"));
                break;
        }

        return conditions;
    }

    private void updateConditionIfNotExist(List<Condition> conditions, Condition newCondition) {
        if (conditions.isEmpty()) {
            conditions.add(newCondition);
        }
        if (conditions.stream()
                .noneMatch(condition -> condition.getType().equals(newCondition.getType()))) {
            conditions.add(newCondition);
        } else if (conditions.removeIf(
                condition ->
                        !(condition.getReason().equals(newCondition.getReason())
                                && condition.getMessage().equals(newCondition.getMessage())))) {
            conditions.add(newCondition);
        }
    }
}
