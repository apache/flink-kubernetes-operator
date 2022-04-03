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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobReconciliationStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

import java.time.Duration;
import java.util.Objects;

/** Reconciliation utilities. */
public class ReconciliationUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void updateForSpecReconciliationSuccess(
            FlinkDeployment flinkApp, JobState stateAfterReconcile) {
        ReconciliationStatus reconciliationStatus = flinkApp.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(true);
        reconciliationStatus.setError(null);
        FlinkDeploymentSpec clonedSpec = clone(flinkApp.getSpec());
        if (reconciliationStatus.getLastReconciledSpec() != null
                && reconciliationStatus.getLastReconciledSpec().getJob() != null) {
            Long oldSavepointTriggerNonce =
                    reconciliationStatus
                            .getLastReconciledSpec()
                            .getJob()
                            .getSavepointTriggerNonce();
            clonedSpec.getJob().setSavepointTriggerNonce(oldSavepointTriggerNonce);
            clonedSpec.getJob().setState(stateAfterReconcile);
        }
        reconciliationStatus.setLastReconciledSpec(clonedSpec);
    }

    public static void updateSavepointReconciliationSuccess(FlinkDeployment flinkApp) {
        ReconciliationStatus reconciliationStatus = flinkApp.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(true);
        reconciliationStatus.setError(null);
        reconciliationStatus
                .getLastReconciledSpec()
                .getJob()
                .setSavepointTriggerNonce(flinkApp.getSpec().getJob().getSavepointTriggerNonce());
    }

    public static void updateForReconciliationError(FlinkDeployment flinkApp, String err) {
        ReconciliationStatus reconciliationStatus = flinkApp.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(false);
        reconciliationStatus.setError(err);
    }

    public static void updateForSpecReconciliationSuccess(FlinkSessionJob sessionJob) {
        FlinkSessionJobReconciliationStatus reconciliationStatus =
                sessionJob.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(true);
        reconciliationStatus.setError(null);
        FlinkSessionJobSpec clonedSpec = clone(sessionJob.getSpec());
        reconciliationStatus.setLastReconciledSpec(clonedSpec);
    }

    public static void updateForReconciliationError(FlinkSessionJob flinkSessionJob, String err) {
        FlinkSessionJobReconciliationStatus reconciliationStatus =
                flinkSessionJob.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(false);
        reconciliationStatus.setError(err);
    }

    public static <T> T clone(T object) {
        if (object == null) {
            return null;
        }
        try {
            return (T)
                    objectMapper.readValue(
                            objectMapper.writeValueAsString(object), object.getClass());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static <CR extends CustomResource> UpdateControl<CR> toUpdateControl(
            CR originalCopy, CR current) {

        UpdateControl<CR> updateControl;
        if (!Objects.equals(originalCopy.getSpec(), current.getSpec())) {
            throw new UnsupportedOperationException(
                    "Detected spec change after reconcile, this probably indicates a bug.");
        }

        boolean statusChanged = !Objects.equals(originalCopy.getStatus(), current.getStatus());

        if (statusChanged) {
            updateControl = UpdateControl.updateStatus(current);
        } else {
            updateControl = UpdateControl.noUpdate();
        }

        return updateControl;
    }

    public static UpdateControl<FlinkDeployment> toUpdateControl(
            FlinkOperatorConfiguration operatorConfiguration,
            FlinkDeployment originalCopy,
            FlinkDeployment current,
            boolean reschedule) {
        UpdateControl<FlinkDeployment> updateControl = toUpdateControl(originalCopy, current);

        if (!reschedule) {
            return updateControl;
        }

        if (isJobUpgradeInProgress(current)) {
            return updateControl.rescheduleAfter(0);
        }

        Duration rescheduleAfter =
                current.getStatus()
                        .getJobManagerDeploymentStatus()
                        .rescheduleAfter(current, operatorConfiguration);

        return updateControl.rescheduleAfter(rescheduleAfter.toMillis());
    }

    public static boolean isUpgradeModeChangedToLastStateAndHADisabledPreviously(
            FlinkDeployment flinkApp) {
        final FlinkDeploymentSpec lastReconciledSpec =
                Preconditions.checkNotNull(
                        flinkApp.getStatus().getReconciliationStatus().getLastReconciledSpec());
        final UpgradeMode previousUpgradeMode = lastReconciledSpec.getJob().getUpgradeMode();
        final UpgradeMode currentUpgradeMode = flinkApp.getSpec().getJob().getUpgradeMode();

        final Configuration lastReconciledFlinkConfig =
                Configuration.fromMap(lastReconciledSpec.getFlinkConfiguration());

        return previousUpgradeMode != UpgradeMode.LAST_STATE
                && currentUpgradeMode == UpgradeMode.LAST_STATE
                && !FlinkUtils.isKubernetesHAActivated(lastReconciledFlinkConfig);
    }

    private static boolean isJobUpgradeInProgress(FlinkDeployment current) {
        ReconciliationStatus reconciliationStatus = current.getStatus().getReconciliationStatus();

        if (reconciliationStatus == null || current.getSpec().getJob() == null) {
            return false;
        }

        return current.getSpec().getJob().getState() == JobState.RUNNING
                && reconciliationStatus.isSuccess()
                && reconciliationStatus.getLastReconciledSpec().getJob().getState()
                        == JobState.SUSPENDED;
    }
}
