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
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.CrdConstants;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.CommonStatus;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.StatusHelper;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/** Reconciliation utilities. */
public class ReconciliationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ReconciliationUtils.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <SPEC extends AbstractFlinkSpec> void updateForSpecReconciliationSuccess(
            AbstractFlinkResource<SPEC, ?> target, JobState stateAfterReconcile) {
        var commonStatus = target.getStatus();
        var spec = target.getSpec();

        ReconciliationStatus<SPEC> reconciliationStatus = commonStatus.getReconciliationStatus();
        commonStatus.setError(null);

        var clonedSpec = ReconciliationUtils.clone(spec);
        AbstractFlinkSpec lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();
        boolean upgrading = false;
        if (lastReconciledSpec != null && lastReconciledSpec.getJob() != null) {
            Long oldSavepointTriggerNonce = lastReconciledSpec.getJob().getSavepointTriggerNonce();
            clonedSpec.getJob().setSavepointTriggerNonce(oldSavepointTriggerNonce);
            if (clonedSpec.getJob().getState() == JobState.RUNNING
                    && stateAfterReconcile == JobState.SUSPENDED) {
                upgrading = true;
            }
            clonedSpec.getJob().setState(stateAfterReconcile);
        }
        reconciliationStatus.serializeAndSetLastReconciledSpec(clonedSpec);
        reconciliationStatus.setReconciliationTimestamp(System.currentTimeMillis());
        reconciliationStatus.setState(
                upgrading ? ReconciliationState.UPGRADING : ReconciliationState.DEPLOYED);

        if (spec.getJob() != null && spec.getJob().getState() == JobState.SUSPENDED) {
            // When a job is suspended by the user it is automatically marked stable
            reconciliationStatus.markReconciledSpecAsStable();
        }
    }

    public static <SPEC extends AbstractFlinkSpec> void updateSavepointReconciliationSuccess(
            AbstractFlinkResource<SPEC, ?> target) {
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

    public static void updateForReconciliationError(
            AbstractFlinkResource<?, ?> target, String error) {
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

    public static <
                    SPEC extends AbstractFlinkSpec,
                    STATUS extends CommonStatus<SPEC>,
                    R extends CustomResource<SPEC, STATUS>>
            UpdateControl<R> toUpdateControl(
                    FlinkOperatorConfiguration operatorConfiguration,
                    R current,
                    R previous,
                    boolean reschedule) {

        STATUS status = current.getStatus();

        // Status update is handled manually independently, we only use UpdateControl to reschedule
        // reconciliation
        UpdateControl<R> updateControl = UpdateControl.noUpdate();

        if (!reschedule) {
            return updateControl;
        }

        if (upgradeStarted(
                status.getReconciliationStatus().getState(),
                previous.getStatus().getReconciliationStatus().getState())) {
            return updateControl.rescheduleAfter(0);
        }

        if (status instanceof FlinkDeploymentStatus) {
            return updateControl.rescheduleAfter(
                    ((FlinkDeploymentStatus) status)
                            .getJobManagerDeploymentStatus()
                            .rescheduleAfter((FlinkDeployment) current, operatorConfiguration)
                            .toMillis());
        } else {
            return updateControl.rescheduleAfter(
                    operatorConfiguration.getReconcileInterval().toMillis());
        }
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
        var reconciliationState = reconciliationStatus.getState();
        if (reconciliationState != ReconciliationState.ROLLED_BACK) {
            var lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();
            if (lastReconciledSpec == null) {
                return null;
            } else {
                return lastReconciledSpec;
            }
        } else {
            return reconciliationStatus.deserializeLastStableSpec();
        }
    }

    private static boolean upgradeStarted(
            ReconciliationState currentReconState, ReconciliationState previousReconState) {
        if (currentReconState == previousReconState) {
            return false;
        }
        return currentReconState == ReconciliationState.ROLLING_BACK
                || currentReconState == ReconciliationState.UPGRADING;
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
            FlinkService flinkService,
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
        if (!Instant.now()
                .minus(readinessTimeout)
                .isAfter(Instant.ofEpochMilli(reconciliationStatus.getReconciliationTimestamp()))) {
            return false;
        }

        var haDataAvailable = flinkService.isHaMetadataAvailable(configuration);
        if (!haDataAvailable) {
            LOG.warn("Rollback is not possible due to missing HA metadata");
        }
        return haDataAvailable;
    }

    public static boolean shouldRecoverDeployment(Configuration conf, FlinkDeployment deployment) {

        if (!ReconciliationUtils.jmMissingForRunningDeployment(deployment)
                || !conf.get(
                        KubernetesOperatorConfigOptions.OPERATOR_RECOVER_JM_DEPLOYMENT_ENABLED)) {
            return false;
        }

        if (!FlinkUtils.isKubernetesHAActivated(conf)) {
            LOG.warn("Could not recover lost deployment without HA enabled");
            return false;
        }
        return true;
    }

    private static boolean jmMissingForRunningDeployment(FlinkDeployment deployment) {
        var deployedJob = getDeployedSpec(deployment).getJob();
        return (deployedJob == null || deployedJob.getState() == JobState.RUNNING)
                && (deployment.getStatus().getJobManagerDeploymentStatus()
                        == JobManagerDeploymentStatus.MISSING);
    }

    public static boolean isJobInTerminalState(FlinkDeploymentStatus status) {
        var jobState = status.getJobStatus().getState();
        return org.apache.flink.api.common.JobStatus.valueOf(jobState).isGloballyTerminalState();
    }

    public static boolean isJobRunning(FlinkDeploymentStatus status) {
        JobManagerDeploymentStatus deploymentStatus = status.getJobManagerDeploymentStatus();
        return deploymentStatus == JobManagerDeploymentStatus.READY
                && org.apache.flink.api.common.JobStatus.RUNNING
                        .name()
                        .equals(status.getJobStatus().getState());
    }

    /**
     * In case of validation errors we need to (temporarily) reset the old spec so that we can
     * reconcile other outstanding changes, instead of simply blocking.
     *
     * <p>This is only possible if we have a previously reconciled spec.
     *
     * <p>For in-flight application upgrades we need extra logic to set the desired job state to
     * running
     *
     * @param deployment The current deployment to be reconciled
     * @param validationError Validation error encountered for the current spec
     * @return True if the spec was reset and reconciliation can continue. False if nothing to
     *     reconcile.
     */
    public static <SPEC extends AbstractFlinkSpec> boolean applyValidationErrorAndResetSpec(
            AbstractFlinkResource<SPEC, ?> deployment, String validationError) {

        var status = deployment.getStatus();
        if (!validationError.equals(status.getError())) {
            LOG.error("Validation failed: " + validationError);
            ReconciliationUtils.updateForReconciliationError(deployment, validationError);
        }

        var lastReconciledSpec = status.getReconciliationStatus().deserializeLastReconciledSpec();
        if (lastReconciledSpec == null) {
            // Validation failed before anything was deployed, nothing to do
            return false;
        } else {
            // We need to observe/reconcile using the last version of the deployment spec
            deployment.setSpec(lastReconciledSpec);
            if (status.getReconciliationStatus().getState() == ReconciliationState.UPGRADING) {
                // We were in the middle of an application upgrade, must set desired state to
                // running
                deployment.getSpec().getJob().setState(JobState.RUNNING);
            }
            return true;
        }
    }

    /**
     * Update the resource error status and metrics when the operator encountered an exception
     * during reconciliation.
     *
     * @param resource Flink Resource to be updated
     * @param retryInfo Current RetryInformation
     * @param e Exception that caused the retry
     * @param metricManager Metric manager to be updated
     * @param statusHelper StatusHelper object for patching status
     * @return This always returns Empty optional currently, due to the status update logic
     */
    public static <STATUS extends CommonStatus<?>, R extends AbstractFlinkResource<?, STATUS>>
            Optional<R> updateErrorStatus(
                    R resource,
                    RetryInfo retryInfo,
                    RuntimeException e,
                    MetricManager<R> metricManager,
                    StatusHelper<STATUS> statusHelper) {
        LOG.warn(
                "Attempt count: {}, last attempt: {}",
                retryInfo.getAttemptCount(),
                retryInfo.isLastAttempt());
        statusHelper.updateStatusFromCache(resource);
        ReconciliationUtils.updateForReconciliationError(
                resource,
                (e instanceof ReconciliationException) ? e.getCause().toString() : e.toString());
        metricManager.onUpdate(resource);
        statusHelper.patchAndCacheStatus(resource);

        // Status was updated already, no need to return anything
        return Optional.empty();
    }
}
