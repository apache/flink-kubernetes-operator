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
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.api.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.api.status.TaskManagerInfo;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.exception.ValidationException;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.kubernetes.operator.utils.FlinkResourceExceptionUtils.updateFlinkResourceException;

/** Reconciliation utilities. */
public class ReconciliationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ReconciliationUtils.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();
    /**
     * Update status after successful deployment of a new resource spec. Existing reconciliation
     * errors will be cleared, lastReconciled spec will be updated and for suspended jobs it will
     * also be marked stable.
     *
     * <p>For Application deployments TaskManager info will also be updated.
     *
     * @param target Target Flink resource.
     * @param conf Deployment configuration.
     * @param <SPEC> Spec type.
     */
    public static <SPEC extends AbstractFlinkSpec> void updateStatusForDeployedSpec(
            AbstractFlinkResource<SPEC, ?> target, Configuration conf) {
        var job = target.getSpec().getJob();
        updateStatusForSpecReconciliation(target, job != null ? job.getState() : null, conf, false);
    }

    /**
     * Update status before deployment attempt of a new resource spec. Existing reconciliation
     * errors will be cleared, lastReconciled spec will be updated and reconciliation status marked
     * UPGRADING.
     *
     * <p>For Application deployments TaskManager info will also be updated.
     *
     * @param target Target Flink resource.
     * @param conf Deployment configuration.
     * @param <SPEC> Spec type.
     */
    public static <SPEC extends AbstractFlinkSpec> void updateStatusBeforeDeploymentAttempt(
            AbstractFlinkResource<SPEC, ?> target, Configuration conf) {
        updateStatusForSpecReconciliation(target, JobState.SUSPENDED, conf, true);
    }

    private static <SPEC extends AbstractFlinkSpec> void updateStatusForSpecReconciliation(
            AbstractFlinkResource<SPEC, ?> target,
            JobState stateAfterReconcile,
            Configuration conf,
            boolean upgrading) {

        var status = target.getStatus();
        var spec = target.getSpec();
        var reconciliationStatus = status.getReconciliationStatus();

        // Clear errors
        status.setError(null);
        reconciliationStatus.setReconciliationTimestamp(System.currentTimeMillis());
        reconciliationStatus.setState(
                upgrading ? ReconciliationState.UPGRADING : ReconciliationState.DEPLOYED);

        if (spec.getJob() != null) {
            // For jobs we have to adjust the reconciled spec
            var clonedSpec = ReconciliationUtils.clone(spec);
            var job = clonedSpec.getJob();
            job.setState(stateAfterReconcile);

            var lastSpec = reconciliationStatus.deserializeLastReconciledSpec();
            if (lastSpec != null) {
                // We preserve the last savepoint trigger to not lose new triggers during upgrade
                job.setSavepointTriggerNonce(lastSpec.getJob().getSavepointTriggerNonce());
            }

            if (target instanceof FlinkDeployment) {
                // For application deployments we update the taskmanager info
                ((FlinkDeploymentStatus) status)
                        .setTaskManager(
                                getTaskManagerInfo(
                                        target.getMetadata().getName(), conf, stateAfterReconcile));
            }
            reconciliationStatus.serializeAndSetLastReconciledSpec(clonedSpec, target);
            if (spec.getJob().getState() == JobState.SUSPENDED) {
                // When a job is suspended by the user it is automatically marked stable
                reconciliationStatus.markReconciledSpecAsStable();
            }
        } else {
            reconciliationStatus.serializeAndSetLastReconciledSpec(spec, target);
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
            AbstractFlinkResource<?, ?> target, Throwable error, FlinkOperatorConfiguration conf) {
        updateFlinkResourceException(error, target, conf);
    }

    public static <T> T clone(T object) {
        return SpecUtils.clone(object);
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
                    rescheduleAfter(
                                    ((FlinkDeploymentStatus) status)
                                            .getJobManagerDeploymentStatus(),
                                    (FlinkDeployment) current,
                                    operatorConfiguration)
                            .toMillis());
        } else {
            return updateControl.rescheduleAfter(
                    operatorConfiguration.getReconcileInterval().toMillis());
        }
    }

    public static Duration rescheduleAfter(
            JobManagerDeploymentStatus status,
            FlinkDeployment flinkDeployment,
            FlinkOperatorConfiguration operatorConfiguration) {
        Duration rescheduleAfter;
        switch (status) {
            case DEPLOYING:
                rescheduleAfter = operatorConfiguration.getProgressCheckInterval();
                break;
            case READY:
                rescheduleAfter =
                        savepointInProgress(flinkDeployment.getStatus().getJobStatus())
                                ? operatorConfiguration.getProgressCheckInterval()
                                : operatorConfiguration.getReconcileInterval();
                break;
            case MISSING:
            case ERROR:
                rescheduleAfter = operatorConfiguration.getReconcileInterval();
                break;
            case DEPLOYED_NOT_READY:
                rescheduleAfter = operatorConfiguration.getRestApiReadyDelay();
                break;
            default:
                throw new RuntimeException("Unknown status: " + status);
        }
        return rescheduleAfter;
    }

    private static boolean savepointInProgress(JobStatus jobStatus) {
        return StringUtils.isNotEmpty(jobStatus.getSavepointInfo().getTriggerId());
    }

    public static boolean isUpgradeModeChangedToLastStateAndHADisabledPreviously(
            AbstractFlinkResource<?, ?> flinkApp, Configuration observeConfig) {

        var deployedSpec = getDeployedSpec(flinkApp);
        UpgradeMode previousUpgradeMode = deployedSpec.getJob().getUpgradeMode();
        UpgradeMode currentUpgradeMode = flinkApp.getSpec().getJob().getUpgradeMode();

        return previousUpgradeMode != UpgradeMode.LAST_STATE
                && currentUpgradeMode == UpgradeMode.LAST_STATE
                && !FlinkUtils.isKubernetesHAActivated(observeConfig);
    }

    public static <SPEC extends AbstractFlinkSpec> SPEC getDeployedSpec(
            AbstractFlinkResource<SPEC, ?> deployment) {
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
     * @param <SPEC> Spec type.
     * @return True if the spec was reset and reconciliation can continue. False if nothing to
     *     reconcile.
     */
    public static <SPEC extends AbstractFlinkSpec> boolean applyValidationErrorAndResetSpec(
            AbstractFlinkResource<SPEC, ?> deployment,
            String validationError,
            FlinkOperatorConfiguration conf) {

        var status = deployment.getStatus();
        if (!validationError.equals(status.getError())) {
            LOG.error("Validation failed: " + validationError);
            ReconciliationUtils.updateForReconciliationError(
                    deployment, new ValidationException(validationError), conf);
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
     * @param <STATUS> Status type.
     * @param <R> Resource type.
     * @param resource Flink Resource to be updated
     * @param retryInfo Current RetryInformation
     * @param e Exception that caused the retry
     * @param statusRecorder statusRecorder object for patching status
     * @param operatorConfiguration
     * @return This always returns Empty optional currently, due to the status update logic
     */
    public static <STATUS extends CommonStatus<?>, R extends AbstractFlinkResource<?, STATUS>>
            ErrorStatusUpdateControl<R> toErrorStatusUpdateControl(
                    R resource,
                    Optional<RetryInfo> retryInfo,
                    Exception e,
                    StatusRecorder<R, STATUS> statusRecorder,
                    FlinkOperatorConfiguration operatorConfiguration) {

        retryInfo.ifPresent(
                r ->
                        LOG.warn(
                                "Attempt count: {}, last attempt: {}",
                                r.getAttemptCount(),
                                r.isLastAttempt()));

        statusRecorder.updateStatusFromCache(resource);
        ReconciliationUtils.updateForReconciliationError(resource, e, operatorConfiguration);
        statusRecorder.patchAndCacheStatus(resource);

        // Status was updated already, no need to return anything
        return ErrorStatusUpdateControl.noStatusUpdate();
    }

    /**
     * Get spec generation for the current in progress upgrade.
     *
     * @param resource Flink resource.
     * @return The spec generation for the upgrade.
     */
    public static Long getUpgradeTargetGeneration(AbstractFlinkResource<?, ?> resource) {
        var lastSpecWithMeta =
                resource.getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpecWithMeta();

        if (lastSpecWithMeta.getMeta() == null) {
            return -1L;
        }

        return lastSpecWithMeta.getMeta().getMetadata().getGeneration();
    }

    /**
     * Clear last reconciled spec if that corresponds to the first deployment. This is important in
     * cases where the first deployment fails.
     *
     * @param resource Flink resource.
     */
    public static void clearLastReconciledSpecIfFirstDeploy(AbstractFlinkResource<?, ?> resource) {
        var reconStatus = resource.getStatus().getReconciliationStatus();
        var lastSpecWithMeta = reconStatus.deserializeLastReconciledSpecWithMeta();

        if (lastSpecWithMeta.getMeta() == null) {
            return;
        }

        if (lastSpecWithMeta.getMeta().isFirstDeployment()) {
            reconStatus.setLastReconciledSpec(null);
            reconStatus.setState(ReconciliationState.UPGRADING);
        }
    }

    /**
     * Checks the status and if the corresponding Flink job/application is in stable running state,
     * it updates the last stable spec.
     *
     * @param status Status to be updated.
     */
    public static void checkAndUpdateStableSpec(CommonStatus<?> status) {
        var flinkJobStatus =
                org.apache.flink.api.common.JobStatus.valueOf(status.getJobStatus().getState());

        if (status.getReconciliationStatus().getState() != ReconciliationState.DEPLOYED) {
            return;
        }

        if (flinkJobStatus == RUNNING) {
            // Running jobs are currently always marked stable
            status.getReconciliationStatus().markReconciledSpecAsStable();
            return;
        }

        var reconciledJobState =
                status.getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState();

        if (reconciledJobState == JobState.RUNNING && flinkJobStatus == FINISHED) {
            // If the job finished on its own, it's marked stable
            status.getReconciliationStatus().markReconciledSpecAsStable();
        }
    }

    /**
     * Updates status in cases where a previously successful deployment wasn't recorded for any
     * reason. We simply change the job status from SUSPENDED to RUNNING and ReconciliationState to
     * DEPLOYED while keeping the metadata.
     *
     * @param resource Flink resource to be updated.
     * @param <SPEC> Spec type.
     */
    public static <SPEC extends AbstractFlinkSpec> void updateStatusForAlreadyUpgraded(
            AbstractFlinkResource<SPEC, ?> resource) {
        var reconciliationStatus = resource.getStatus().getReconciliationStatus();
        var lastSpecWithMeta = reconciliationStatus.deserializeLastReconciledSpecWithMeta();
        var lastJobSpec = lastSpecWithMeta.getSpec().getJob();
        if (lastJobSpec != null) {
            lastJobSpec.setState(JobState.RUNNING);
        }
        reconciliationStatus.setState(ReconciliationState.DEPLOYED);
        reconciliationStatus.setLastReconciledSpec(
                SpecUtils.writeSpecWithMeta(
                        lastSpecWithMeta.getSpec(), lastSpecWithMeta.getMeta()));
    }
}
