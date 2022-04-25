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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.CrdConstants;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Reconciliation utilities. */
public class ReconciliationUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <SPEC extends AbstractFlinkSpec, STATUS extends CommonStatus<SPEC>>
            void updateForSpecReconciliationSuccess(
                    CustomResource<SPEC, STATUS> target, JobState stateAfterReconcile) {
        var commonStatus = target.getStatus();
        var spec = target.getSpec();

        ReconciliationStatus<SPEC> reconciliationStatus = commonStatus.getReconciliationStatus();
        commonStatus.setError(null);

        var clonedSpec = ReconciliationUtils.clone(spec);
        AbstractFlinkSpec lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();
        if (lastReconciledSpec != null && lastReconciledSpec.getJob() != null) {
            Long oldSavepointTriggerNonce = lastReconciledSpec.getJob().getSavepointTriggerNonce();
            clonedSpec.getJob().setSavepointTriggerNonce(oldSavepointTriggerNonce);
            clonedSpec.getJob().setState(stateAfterReconcile);
        }
        reconciliationStatus.serializeAndSetLastReconciledSpec(clonedSpec);
        reconciliationStatus.setReconciliationTimestamp(System.currentTimeMillis());
        reconciliationStatus.setState(ReconciliationState.DEPLOYED);

        if (spec.getJob() != null && spec.getJob().getState() == JobState.SUSPENDED) {
            // When a job is suspended by the user it is automatically marked stable
            reconciliationStatus.markReconciledSpecAsStable();
        }
    }

    public static <SPEC extends AbstractFlinkSpec, STATUS extends CommonStatus<SPEC>>
            void updateSavepointReconciliationSuccess(CustomResource<SPEC, STATUS> target) {
        var commonStatus = target.getStatus();
        var spec = target.getSpec();
        ReconciliationStatus<SPEC> reconciliationStatus = commonStatus.getReconciliationStatus();
        commonStatus.setError(null);
        SPEC lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();
        lastReconciledSpec
                .getJob()
                .setSavepointTriggerNonce(spec.getJob().getSavepointTriggerNonce());
        reconciliationStatus.serializeAndSetLastReconciledSpec(lastReconciledSpec);
        reconciliationStatus.setReconciliationTimestamp(System.currentTimeMillis());
    }

    public static <SPEC extends AbstractFlinkSpec, STATUS extends CommonStatus<SPEC>>
            void updateForReconciliationError(CustomResource<SPEC, STATUS> target, String error) {
        target.getStatus().setError(error);
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
            FlinkDeployment flinkApp, FlinkConfigManager configManager) {

        FlinkDeploymentSpec deployedSpec = getDeployedSpec(flinkApp);
        UpgradeMode previousUpgradeMode = deployedSpec.getJob().getUpgradeMode();
        UpgradeMode currentUpgradeMode = flinkApp.getSpec().getJob().getUpgradeMode();

        return previousUpgradeMode != UpgradeMode.LAST_STATE
                && currentUpgradeMode == UpgradeMode.LAST_STATE
                && !FlinkUtils.isKubernetesHAActivated(configManager.getObserveConfig(flinkApp));
    }

    public static FlinkDeploymentSpec getDeployedSpec(FlinkDeployment deployment) {
        var reconciliationStatus = deployment.getStatus().getReconciliationStatus();
        if (reconciliationStatus.getState() == ReconciliationState.DEPLOYED) {
            var lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();
            if (lastReconciledSpec == null) {
                // No deployment submitted, use the provided one
                return deployment.getSpec();
            } else {
                return lastReconciledSpec;
            }
        } else {
            return reconciliationStatus.deserializeLastStableSpec();
        }
    }

    private static boolean isJobUpgradeInProgress(FlinkDeployment current) {
        ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus =
                current.getStatus().getReconciliationStatus();

        if (reconciliationStatus == null) {
            return false;
        }

        if (reconciliationStatus.getState() == ReconciliationState.ROLLING_BACK) {
            return true;
        }

        if (current.getSpec().getJob() == null
                || reconciliationStatus.getLastReconciledSpec() == null) {
            return false;
        }

        return current.getSpec().getJob().getState() == JobState.RUNNING
                && current.getStatus().getError() == null
                && reconciliationStatus.deserializeLastReconciledSpec().getJob().getState()
                        == JobState.SUSPENDED;
    }

    public static <T> T deserializedSpecWithVersion(
            @Nullable String specString, Class<T> specClass) {
        if (specString == null) {
            return null;
        }

        try {
            ObjectNode objectNode = (ObjectNode) objectMapper.readTree(specString);
            objectNode.remove("apiVersion");
            return objectMapper.treeToValue(objectNode, specClass);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not deserialize spec, this indicates a bug...", e);
        }
    }

    public static String writeSpecWithCurrentVersion(Object spec) {
        ObjectNode objectNode = objectMapper.valueToTree(Preconditions.checkNotNull(spec));
        objectNode.set("apiVersion", new TextNode(CrdConstants.API_VERSION));
        try {
            return objectMapper.writeValueAsString(objectNode);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not serialize spec, this indicates a bug...", e);
        }
    }

    public static boolean shouldRollBack(
            ReconciliationStatus<FlinkDeploymentSpec> reconciliationStatus,
            Configuration configuration) {

        if (reconciliationStatus.getState() == ReconciliationState.ROLLING_BACK) {
            return true;
        }

        if (!configuration.get(KubernetesOperatorConfigOptions.DEPLOYMENT_ROLLBACK_ENABLED)
                || reconciliationStatus.getState() == ReconciliationState.ROLLED_BACK
                || reconciliationStatus.isLastReconciledSpecStable()) {
            return false;
        }

        FlinkDeploymentSpec lastStableSpec = reconciliationStatus.deserializeLastStableSpec();
        if (lastStableSpec != null
                && lastStableSpec.getJob() != null
                && lastStableSpec.getJob().getState() == JobState.SUSPENDED) {
            // Should not roll back to suspended state
            return false;
        }

        Duration readinessTimeout =
                configuration.get(KubernetesOperatorConfigOptions.DEPLOYMENT_READINESS_TIMEOUT);
        return Instant.now()
                .minus(readinessTimeout)
                .isAfter(Instant.ofEpochMilli(reconciliationStatus.getReconciliationTimestamp()));
    }
}
