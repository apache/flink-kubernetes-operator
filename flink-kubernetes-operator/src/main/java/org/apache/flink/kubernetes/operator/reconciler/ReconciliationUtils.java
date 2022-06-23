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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
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
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.crd.status.TaskManagerInfo;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
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

    public static final String INTERNAL_METADATA_JSON_KEY = "resource_metadata";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static <SPEC extends AbstractFlinkSpec> void updateForSpecReconciliationSuccess(
            AbstractFlinkResource<SPEC, ?> target,
            JobState stateAfterReconcile,
            Configuration conf) {
        var status = target.getStatus();
        var spec = target.getSpec();

        ReconciliationStatus<SPEC> reconciliationStatus = status.getReconciliationStatus();
        status.setError(null);

        // For application deployments we update the taskmanager info
        if (target instanceof FlinkDeployment && spec.getJob() != null) {
            ((FlinkDeploymentStatus) status)
                    .setTaskManager(
                            getTaskManagerInfo(
                                    target.getMetadata().getName(), conf, stateAfterReconcile));
        }

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
        reconciliationStatus.serializeAndSetLastReconciledSpec(clonedSpec, target);
        reconciliationStatus.setReconciliationTimestamp(System.currentTimeMillis());
        reconciliationStatus.setState(
                upgrading ? ReconciliationState.UPGRADING : ReconciliationState.DEPLOYED);

        if (spec.getJob() != null && spec.getJob().getState() == JobState.SUSPENDED) {
            // When a job is suspended by the user it is automatically marked stable
            reconciliationStatus.markReconciledSpecAsStable();
        }
    }

    public static <SPEC extends AbstractFlinkSpec> void updateLastReconciledSavepointTriggerNonce(
            SavepointInfo savepointInfo, AbstractFlinkResource<SPEC, ?> target) {

        // We only need to update for MANUAL triggers
        if (savepointInfo.getTriggerType() != SavepointTriggerType.MANUAL) {
            return;
        }

        var commonStatus = target.getStatus();
        var spec = target.getSpec();
        var reconciliationStatus = commonStatus.getReconciliationStatus();
        var lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();

        lastReconciledSpec
                .getJob()
                .setSavepointTriggerNonce(spec.getJob().getSavepointTriggerNonce());

        reconciliationStatus.serializeAndSetLastReconciledSpec(lastReconciledSpec, target);
        reconciliationStatus.setReconciliationTimestamp(System.currentTimeMillis());
    }

    private static TaskManagerInfo getTaskManagerInfo(
            String name, Configuration conf, JobState jobState) {
        var labelSelector = "component=taskmanager,app=" + name;
        if (jobState == JobState.RUNNING) {
            return new TaskManagerInfo(labelSelector, FlinkUtils.getNumTaskManagers(conf));
        } else {
            return new TaskManagerInfo("", 0);
        }
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
            return reconciliationStatus.deserializeLastReconciledSpec();
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

    public static <T> Tuple2<T, ObjectNode> deserializeSpecWithMeta(
            @Nullable String specWithMetaString, Class<T> specClass) {
        if (specWithMetaString == null) {
            return null;
        }

        try {
            ObjectNode wrapper = (ObjectNode) objectMapper.readTree(specWithMetaString);
            ObjectNode internalMeta = (ObjectNode) wrapper.remove(INTERNAL_METADATA_JSON_KEY);

            if (internalMeta == null) {
                // migrating from old format
                wrapper.remove("apiVersion");
                return Tuple2.of(objectMapper.treeToValue(wrapper, specClass), internalMeta);
            } else {
                return Tuple2.of(
                        objectMapper.treeToValue(wrapper.get("spec"), specClass), internalMeta);
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not deserialize spec, this indicates a bug...", e);
        }
    }

    public static String writeSpecWithMeta(
            Object spec, AbstractFlinkResource<?, ?> relatedResource) {
        ObjectNode wrapper = objectMapper.createObjectNode();
        wrapper.set("spec", objectMapper.valueToTree(Preconditions.checkNotNull(spec)));

        ObjectNode internalMeta = wrapper.putObject(INTERNAL_METADATA_JSON_KEY);
        internalMeta.put("apiVersion", relatedResource.getApiVersion());
        ObjectNode metadata = internalMeta.putObject("metadata");
        metadata.put("generation", relatedResource.getMetadata().getGeneration());

        try {
            return objectMapper.writeValueAsString(wrapper);
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

        var lastStableSpec = reconciliationStatus.deserializeLastStableSpec();
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
                        KubernetesOperatorConfigOptions.OPERATOR_JM_DEPLOYMENT_RECOVERY_ENABLED)) {
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

    public static boolean isJobInTerminalState(CommonStatus<?> status) {
        var jobState = status.getJobStatus().getState();
        return org.apache.flink.api.common.JobStatus.valueOf(jobState).isGloballyTerminalState();
    }

    public static boolean isJobRunning(CommonStatus<?> status) {
        return org.apache.flink.api.common.JobStatus.RUNNING
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
     * @param statusRecorder statusRecorder object for patching status
     * @return This always returns Empty optional currently, due to the status update logic
     */
    public static <STATUS extends CommonStatus<?>, R extends AbstractFlinkResource<?, STATUS>>
            ErrorStatusUpdateControl<R> toErrorStatusUpdateControl(
                    R resource,
                    Optional<RetryInfo> retryInfo,
                    Exception e,
                    MetricManager<R> metricManager,
                    StatusRecorder<STATUS> statusRecorder) {

        retryInfo.ifPresent(
                r -> {
                    LOG.warn(
                            "Attempt count: {}, last attempt: {}",
                            r.getAttemptCount(),
                            r.isLastAttempt());
                });

        statusRecorder.updateStatusFromCache(resource);
        ReconciliationUtils.updateForReconciliationError(
                resource,
                (e instanceof ReconciliationException) ? e.getCause().toString() : e.toString());
        metricManager.onUpdate(resource);
        statusRecorder.patchAndCacheStatus(resource);

        // Status was updated already, no need to return anything
        return ErrorStatusUpdateControl.noStatusUpdate();
    }

    public static Long getUpgradeTargetGeneration(FlinkDeployment deployment) {
        var lastSpecWithMeta =
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpecWithMeta();

        if (lastSpecWithMeta == null || lastSpecWithMeta.f1 == null) {
            // For first deployments and when migrating from before this feature simply return
            // current generation
            return deployment.getMetadata().getGeneration();
        }

        return lastSpecWithMeta.f1.get("metadata").get("generation").asLong(-1L);
    }
}
