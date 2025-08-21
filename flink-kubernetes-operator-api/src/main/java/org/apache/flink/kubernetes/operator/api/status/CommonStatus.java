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
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;

import io.fabric8.crd.generator.annotation.PrinterColumn;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

/** Last observed common status of the Flink deployment/Flink SessionJob. */
@Experimental
@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public abstract class CommonStatus<SPEC extends AbstractFlinkSpec> {

    // Frequent error message constants for resource failure reporting
    public static final String MSG_JOB_FINISHED_OR_CONFIGMAPS_DELETED =
            "It is possible that the job has finished or terminally failed, or the configmaps have been deleted.";
    public static final String MSG_HA_METADATA_NOT_AVAILABLE = "HA metadata is not available";
    public static final String MSG_MANUAL_RESTORE_REQUIRED = "Manual restore required.";

    /** Last observed status of the Flink job on Application/Session cluster. */
    private JobStatus jobStatus = new JobStatus();

    /** Error information about the FlinkDeployment/FlinkSessionJob. */
    private String error;

    /** Last observed generation of the FlinkDeployment/FlinkSessionJob. */
    private Long observedGeneration;

    /** Lifecycle state of the Flink resource (including being rolled back, failed etc.). */
    @PrinterColumn(name = "Lifecycle State")
    // Calculated from the status, requires no setter. The purpose of this is to expose as a printer
    // column.
    private ResourceLifecycleState lifecycleState;

    /**
     * Current reconciliation status of this resource.
     *
     * @return Current {@link ReconciliationStatus}.
     */
    public abstract ReconciliationStatus<SPEC> getReconciliationStatus();

    public ResourceLifecycleState getLifecycleState() {
        if (ResourceLifecycleState.DELETING == lifecycleState
                || ResourceLifecycleState.DELETED == lifecycleState) {
            return lifecycleState;
        }

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

        if (getJobStatus().getState() == org.apache.flink.api.common.JobStatus.FAILED) {
            return ResourceLifecycleState.FAILED;
        }

        // Check for unrecoverable deployments that should be marked as FAILED if the error contains
        // the following substrings
        if (this instanceof FlinkDeploymentStatus) {
            FlinkDeploymentStatus deploymentStatus = (FlinkDeploymentStatus) this;
            var jmDeployStatus = deploymentStatus.getJobManagerDeploymentStatus();

            // ERROR/MISSING deployments are in terminal error state and should always be FAILED
            if ((jmDeployStatus == JobManagerDeploymentStatus.MISSING
                            || jmDeployStatus == JobManagerDeploymentStatus.ERROR)
                    && error != null
                    && (error.contains(MSG_MANUAL_RESTORE_REQUIRED)
                            || error.contains(MSG_JOB_FINISHED_OR_CONFIGMAPS_DELETED)
                            || error.contains(MSG_HA_METADATA_NOT_AVAILABLE))) {
                return ResourceLifecycleState.FAILED;
            }
        }

        if (reconciliationStatus.getState() == ReconciliationState.ROLLED_BACK) {
            return ResourceLifecycleState.ROLLED_BACK;
        } else if (reconciliationStatus.isLastReconciledSpecStable()) {
            return ResourceLifecycleState.STABLE;
        }

        return ResourceLifecycleState.DEPLOYED;
    }
}
