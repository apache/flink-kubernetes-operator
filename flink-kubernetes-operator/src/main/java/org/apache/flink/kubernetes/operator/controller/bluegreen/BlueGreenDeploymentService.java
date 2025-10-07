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

package org.apache.flink.kubernetes.operator.controller.bluegreen;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDiffType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.deleteFlinkDeployment;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.deployCluster;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.isFlinkDeploymentReady;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.suspendFlinkDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.fetchSavepointInfo;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getReconciliationReschedInterval;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getSpecDiff;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.hasSpecChanged;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.instantStrToMillis;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.isSavepointRequired;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.millisToInstantStr;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.prepareFlinkDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.setLastReconciledSpec;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.triggerSavepoint;

/** Consolidated service for all Blue/Green deployment operations. */
public class BlueGreenDeploymentService {

    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenDeploymentService.class);
    private static final long RETRY_DELAY_MS = 500;

    // ==================== Deployment Initiation Methods ====================

    /**
     * Initiates a new Blue/Green deployment.
     *
     * @param context the transition context
     * @param nextBlueGreenDeploymentType the type of deployment to create
     * @param nextState the next state to transition to
     * @param lastCheckpoint the checkpoint to restore from (can be null)
     * @param isFirstDeployment whether this is the first deployment
     * @return UpdateControl for the deployment
     */
    public UpdateControl<FlinkBlueGreenDeployment> initiateDeployment(
            BlueGreenContext context,
            BlueGreenDeploymentType nextBlueGreenDeploymentType,
            FlinkBlueGreenDeploymentState nextState,
            Savepoint lastCheckpoint,
            boolean isFirstDeployment) {
        ObjectMeta bgMeta = context.getBgDeployment().getMetadata();

        FlinkDeployment flinkDeployment =
                prepareFlinkDeployment(
                        context,
                        nextBlueGreenDeploymentType,
                        lastCheckpoint,
                        isFirstDeployment,
                        bgMeta);

        deployCluster(context, flinkDeployment);

        BlueGreenUtils.setAbortTimestamp(context);

        return patchStatusUpdateControl(context, nextState, JobStatus.RECONCILING, null)
                .rescheduleAfter(BlueGreenUtils.getReconciliationReschedInterval(context));
    }

    /**
     * Checks if a full transition can be initiated and initiates it if conditions are met.
     *
     * @param context the transition context
     * @param currentBlueGreenDeploymentType the current deployment type
     * @return UpdateControl for the deployment
     */
    public UpdateControl<FlinkBlueGreenDeployment> checkAndInitiateDeployment(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {
        BlueGreenDiffType specDiff = getSpecDiff(context);

        if (specDiff != BlueGreenDiffType.IGNORE) {
            FlinkDeployment currentFlinkDeployment =
                    context.getDeploymentByType(currentBlueGreenDeploymentType);

            if (isFlinkDeploymentReady(currentFlinkDeployment)) {
                if (specDiff == BlueGreenDiffType.TRANSITION) {
                    boolean savepointTriggered = false;
                    try {
                        savepointTriggered = handleSavepoint(context, currentFlinkDeployment);
                    } catch (Exception e) {
                        var error = "Could not trigger Savepoint. Details: " + e.getMessage();
                        return markDeploymentFailing(context, error);
                    }

                    if (savepointTriggered) {
                        // Spec is intentionally not marked as reconciled here to allow
                        // reprocessing the TRANSITION once savepoint creation completes
                        var savepointingState =
                                calculateSavepointingState(currentBlueGreenDeploymentType);
                        return patchStatusUpdateControl(context, savepointingState, null, null)
                                .rescheduleAfter(getReconciliationReschedInterval(context));
                    }

                    setLastReconciledSpec(context);
                    try {
                        return startTransition(
                                context, currentBlueGreenDeploymentType, currentFlinkDeployment);
                    } catch (Exception e) {
                        var error = "Could not start Transition. Details: " + e.getMessage();
                        context.getDeploymentStatus().setSavepointTriggerId(null);
                        return markDeploymentFailing(context, error);
                    }
                } else {
                    setLastReconciledSpec(context);
                    LOG.info(
                            "Patching FlinkDeployment '{}' during checkAndInitiateDeployment",
                            currentFlinkDeployment.getMetadata().getName());
                    return patchFlinkDeployment(context, currentBlueGreenDeploymentType);
                }
            } else {
                if (context.getDeploymentStatus().getJobStatus().getState() != JobStatus.FAILING) {
                    setLastReconciledSpec(context);
                    var error =
                            String.format(
                                    "Transition to %s not possible, current Flink Deployment '%s' is not READY. FAILING '%s'",
                                    calculateTransition(currentBlueGreenDeploymentType)
                                            .nextBlueGreenDeploymentType,
                                    currentFlinkDeployment.getMetadata().getName(),
                                    context.getBgDeployment().getMetadata().getName());
                    return markDeploymentFailing(context, error);
                }
            }
        }

        return UpdateControl.noUpdate();
    }

    private UpdateControl<FlinkBlueGreenDeployment> patchFlinkDeployment(
            BlueGreenContext context, BlueGreenDeploymentType blueGreenDeploymentTypeToPatch) {

        String childDeploymentName =
                context.getBgDeployment().getMetadata().getName()
                        + "-"
                        + blueGreenDeploymentTypeToPatch.toString().toLowerCase();

        // We want to patch, therefore the transition should point to the existing deployment
        // details
        var patchingState = calculatePatchingState(blueGreenDeploymentTypeToPatch);

        // If we're not transitioning between deployments, mark as a single deployment to have it
        // not wait for synchronization
        var isFirstDeployment = context.getDeployments().getNumberOfDeployments() != 2;

        // TODO: if the resource failed right after being deployed with an initialSavepointPath,
        //  will it be used by this patching? otherwise this is unnecessary, keep lastSavepoint =
        // null.
        Savepoint lastSavepoint =
                carryOverSavepoint(context, blueGreenDeploymentTypeToPatch, childDeploymentName);

        return initiateDeployment(
                context,
                blueGreenDeploymentTypeToPatch,
                patchingState,
                lastSavepoint,
                isFirstDeployment);
    }

    @Nullable
    private static Savepoint carryOverSavepoint(
            BlueGreenContext context,
            BlueGreenDeploymentType blueGreenDeploymentTypeToPatch,
            String childDeploymentName) {
        var deploymentToPatch = context.getDeploymentByType(blueGreenDeploymentTypeToPatch);
        var initialSavepointPath = deploymentToPatch.getSpec().getJob().getInitialSavepointPath();

        if (initialSavepointPath == null || initialSavepointPath.isEmpty()) {
            initialSavepointPath =
                    deploymentToPatch.getStatus().getJobStatus().getUpgradeSavepointPath();
        }

        Savepoint lastSavepoint = null;
        if (initialSavepointPath != null && !initialSavepointPath.isEmpty()) {
            var ctx =
                    context.getCtxFactory()
                            .getResourceContext(deploymentToPatch, context.getJosdkContext());

            lastSavepoint = getSavepointObject(ctx, initialSavepointPath);

            LOG.info(
                    "Patching FlinkDeployment '{}', carrying over Savepoint at: '{}'",
                    childDeploymentName,
                    initialSavepointPath);
        } else {
            LOG.info("Patching FlinkDeployment '{}'", childDeploymentName);
        }

        return lastSavepoint;
    }

    private UpdateControl<FlinkBlueGreenDeployment> startTransition(
            BlueGreenContext context,
            BlueGreenDeploymentType currentBlueGreenDeploymentType,
            FlinkDeployment currentFlinkDeployment) {
        DeploymentTransition transition = calculateTransition(currentBlueGreenDeploymentType);

        Savepoint lastCheckpoint = configureInitialSavepoint(context, currentFlinkDeployment);

        return initiateDeployment(
                context,
                transition.nextBlueGreenDeploymentType,
                transition.nextState,
                lastCheckpoint,
                false);
    }

    private DeploymentTransition calculateTransition(BlueGreenDeploymentType currentType) {
        if (BlueGreenDeploymentType.BLUE == currentType) {
            return new DeploymentTransition(
                    BlueGreenDeploymentType.GREEN,
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN);
        } else {
            return new DeploymentTransition(
                    BlueGreenDeploymentType.BLUE,
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE);
        }
    }

    private FlinkBlueGreenDeploymentState calculatePatchingState(
            BlueGreenDeploymentType currentType) {
        if (BlueGreenDeploymentType.BLUE == currentType) {
            return FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
        } else {
            return FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
        }
    }

    // ==================== Savepointing Methods ====================

    public boolean monitorSavepoint(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {

        FlinkResourceContext<FlinkDeployment> ctx =
                context.getCtxFactory()
                        .getResourceContext(
                                context.getDeploymentByType(currentBlueGreenDeploymentType),
                                context.getJosdkContext());

        String savepointTriggerId = context.getDeploymentStatus().getSavepointTriggerId();
        var savepointFetchResult = fetchSavepointInfo(ctx, savepointTriggerId);

        return !savepointFetchResult.isPending();
    }

    private Savepoint configureInitialSavepoint(
            BlueGreenContext context, FlinkDeployment currentFlinkDeployment) {
        // Create savepoint for all upgrade modes except STATELESS
        // (originally only SAVEPOINT mode required savepoints)
        if (isSavepointRequired(context)) {
            FlinkResourceContext<FlinkDeployment> ctx =
                    context.getCtxFactory()
                            .getResourceContext(currentFlinkDeployment, context.getJosdkContext());

            String triggerId = context.getDeploymentStatus().getSavepointTriggerId();
            var savepointFetchResult = fetchSavepointInfo(ctx, triggerId);

            if (savepointFetchResult.getError() != null
                    && !savepointFetchResult.getError().isEmpty()) {
                throw new RuntimeException(
                        String.format(
                                "Could not fetch savepoint with triggerId: %s. Error: %s",
                                triggerId, savepointFetchResult.getError()));
            }

            return getSavepointObject(ctx, savepointFetchResult.getLocation());
        }

        // Currently not using last checkpoint recovery for LAST_STATE upgrade mode
        // This could be re-enabled in the future by uncommenting the logic below
        return null;

        //        if (!lookForCheckpoint(context)) {
        //            return null;
        //        }
        //
        //        return getLastCheckpoint(ctx);
    }

    @NotNull
    private static Savepoint getSavepointObject(
            FlinkResourceContext<FlinkDeployment> ctx, String savepointLocation) {
        org.apache.flink.core.execution.SavepointFormatType coreSavepointFormatType =
                ctx.getObserveConfig()
                        .get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);

        var savepointFormatType = SavepointFormatType.valueOf(coreSavepointFormatType.toString());

        return Savepoint.of(savepointLocation, SnapshotTriggerType.MANUAL, savepointFormatType);
    }

    private boolean handleSavepoint(
            BlueGreenContext context, FlinkDeployment currentFlinkDeployment) throws Exception {

        if (!isSavepointRequired(context)) {
            return false;
        }

        FlinkResourceContext<FlinkDeployment> ctx =
                context.getCtxFactory()
                        .getResourceContext(currentFlinkDeployment, context.getJosdkContext());

        String savepointTriggerId = context.getDeploymentStatus().getSavepointTriggerId();

        if (savepointTriggerId == null || savepointTriggerId.isEmpty()) {
            String triggerId = triggerSavepoint(ctx);
            LOG.info("Savepoint requested (triggerId: {}", triggerId);
            context.getDeploymentStatus().setSavepointTriggerId(triggerId);
            return true;
        }

        LOG.info("Savepoint previously requested (triggerId: {})", savepointTriggerId);
        return false;
    }

    private FlinkBlueGreenDeploymentState calculateSavepointingState(
            BlueGreenDeploymentType currentType) {
        if (BlueGreenDeploymentType.BLUE == currentType) {
            return FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE;
        } else {
            return FlinkBlueGreenDeploymentState.SAVEPOINTING_GREEN;
        }
    }

    // ==================== Transition Monitoring Methods ====================

    /**
     * Monitors an ongoing Blue/Green deployment transition.
     *
     * @param context the transition context
     * @param currentBlueGreenDeploymentType the current deployment type being transitioned from
     * @return UpdateControl for the transition
     */
    public UpdateControl<FlinkBlueGreenDeployment> monitorTransition(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {

        var updateControl =
                handleSpecChangesDuringTransition(context, currentBlueGreenDeploymentType);

        if (updateControl != null) {
            return updateControl;
        }

        TransitionState transitionState =
                determineTransitionState(context, currentBlueGreenDeploymentType);

        if (isFlinkDeploymentReady(transitionState.nextDeployment)) {
            return shouldWeDelete(
                    context,
                    transitionState.currentDeployment,
                    transitionState.nextDeployment,
                    transitionState.nextState);
        } else {
            return shouldWeAbort(
                    context, transitionState.nextDeployment, transitionState.nextState);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> handleSpecChangesDuringTransition(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {
        if (hasSpecChanged(context)) {
            BlueGreenDiffType diffType = getSpecDiff(context);

            if (diffType != BlueGreenDiffType.IGNORE) {
                setLastReconciledSpec(context);
                var oppositeDeploymentType =
                        context.getOppositeDeploymentType(currentBlueGreenDeploymentType);
                LOG.info(
                        "Patching FlinkDeployment '{}' during handleSpecChangesDuringTransition",
                        context.getDeploymentByType(oppositeDeploymentType)
                                .getMetadata()
                                .getName());
                return patchFlinkDeployment(context, oppositeDeploymentType);
            }
        }

        return null;
    }

    private TransitionState determineTransitionState(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {
        TransitionState transitionState;

        if (BlueGreenDeploymentType.BLUE == currentBlueGreenDeploymentType) {
            transitionState =
                    new TransitionState(
                            context.getBlueDeployment(), // currentDeployment
                            context.getGreenDeployment(), // nextDeployment
                            FlinkBlueGreenDeploymentState.ACTIVE_GREEN); // next State
        } else {
            transitionState =
                    new TransitionState(
                            context.getGreenDeployment(), // currentDeployment
                            context.getBlueDeployment(), // nextDeployment
                            FlinkBlueGreenDeploymentState.ACTIVE_BLUE); // next State
        }

        Preconditions.checkNotNull(
                transitionState.nextDeployment,
                "Target Dependent Deployment resource not found. Blue/Green deployment name: "
                        + context.getDeploymentName()
                        + ", current deployment type: "
                        + currentBlueGreenDeploymentType);

        return transitionState;
    }

    // ==================== Deployment Deletion Methods ====================

    private UpdateControl<FlinkBlueGreenDeployment> shouldWeDelete(
            BlueGreenContext context,
            FlinkDeployment currentDeployment,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState) {

        var deploymentStatus = context.getDeploymentStatus();

        if (currentDeployment == null) {
            deploymentStatus.setDeploymentReadyTimestamp(Instant.now().toString());
            return finalizeBlueGreenDeployment(context, nextState);
        }

        long deploymentDeletionDelayMs = BlueGreenUtils.getDeploymentDeletionDelay(context);
        long deploymentReadyTimestamp =
                instantStrToMillis(deploymentStatus.getDeploymentReadyTimestamp());

        if (deploymentReadyTimestamp == 0) {
            LOG.info(
                    "FlinkDeployment '{}' marked ready, rescheduling reconciliation in {} seconds.",
                    nextDeployment.getMetadata().getName(),
                    deploymentDeletionDelayMs / 1000);

            deploymentStatus.setDeploymentReadyTimestamp(Instant.now().toString());
            return patchStatusUpdateControl(context, null, null, null)
                    .rescheduleAfter(deploymentDeletionDelayMs);
        }

        long deletionTimestamp = deploymentReadyTimestamp + deploymentDeletionDelayMs;

        if (deletionTimestamp < System.currentTimeMillis()) {
            return deleteDeployment(currentDeployment, context);
        } else {
            return waitBeforeDeleting(currentDeployment, deletionTimestamp);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> waitBeforeDeleting(
            FlinkDeployment currentDeployment, long deletionTimestamp) {

        long delay = deletionTimestamp - System.currentTimeMillis();
        LOG.info(
                "Awaiting deletion delay for FlinkDeployment '{}', rescheduling reconciliation in {} seconds.",
                currentDeployment.getMetadata().getName(),
                delay / 1000);

        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(delay);
    }

    private UpdateControl<FlinkBlueGreenDeployment> deleteDeployment(
            FlinkDeployment currentDeployment, BlueGreenContext context) {

        boolean deleted = deleteFlinkDeployment(currentDeployment, context);

        if (!deleted) {
            LOG.info("FlinkDeployment '{}' not deleted, will retry", currentDeployment);
        } else {
            LOG.info("FlinkDeployment '{}' deleted!", currentDeployment);
        }

        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(RETRY_DELAY_MS);
    }

    // ==================== Abort and Retry Methods ====================

    private UpdateControl<FlinkBlueGreenDeployment> shouldWeAbort(
            BlueGreenContext context,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState) {

        String deploymentName = nextDeployment.getMetadata().getName();
        long abortTimestamp = instantStrToMillis(context.getDeploymentStatus().getAbortTimestamp());

        if (abortTimestamp == 0) {
            throw new IllegalStateException("Unexpected abortTimestamp == 0");
        }

        if (abortTimestamp < System.currentTimeMillis()) {
            return abortDeployment(context, nextDeployment, nextState, deploymentName);
        } else {
            return retryDeployment(context, deploymentName);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> retryDeployment(
            BlueGreenContext context, String deploymentName) {

        long delay = getReconciliationReschedInterval(context);

        LOG.info(
                "FlinkDeployment '{}' not ready yet, retrying in {} seconds.",
                deploymentName,
                delay / 1000);

        return patchStatusUpdateControl(context, null, null, null).rescheduleAfter(delay);
    }

    private UpdateControl<FlinkBlueGreenDeployment> abortDeployment(
            BlueGreenContext context,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState,
            String deploymentName) {

        suspendFlinkDeployment(context, nextDeployment);

        FlinkBlueGreenDeploymentState previousState =
                getPreviousState(nextState, context.getDeployments());
        context.getDeploymentStatus().setBlueGreenState(previousState);

        var error =
                String.format(
                        "Aborting deployment '%s', rolling B/G deployment back to %s",
                        deploymentName, previousState);
        return markDeploymentFailing(context, error);
    }

    @NotNull
    private static UpdateControl<FlinkBlueGreenDeployment> markDeploymentFailing(
            BlueGreenContext context, String error) {
        LOG.error(error);
        return patchStatusUpdateControl(context, null, JobStatus.FAILING, error);
    }

    private static FlinkBlueGreenDeploymentState getPreviousState(
            FlinkBlueGreenDeploymentState nextState, FlinkBlueGreenDeployments deployments) {
        FlinkBlueGreenDeploymentState previousState;
        if (deployments.getNumberOfDeployments() == 1) {
            previousState = FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
        } else if (deployments.getNumberOfDeployments() == 2) {
            previousState =
                    nextState == FlinkBlueGreenDeploymentState.ACTIVE_BLUE
                            ? FlinkBlueGreenDeploymentState.ACTIVE_GREEN
                            : FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
        } else {
            throw new IllegalStateException("No blue/green FlinkDeployments found!");
        }
        return previousState;
    }

    // ==================== Finalization Methods ====================

    /**
     * Finalizes a Blue/Green deployment transition.
     *
     * @param context the transition context
     * @param nextState the next state to transition to
     * @return UpdateControl for finalization
     */
    public UpdateControl<FlinkBlueGreenDeployment> finalizeBlueGreenDeployment(
            BlueGreenContext context, FlinkBlueGreenDeploymentState nextState) {

        LOG.info("Finalizing deployment '{}' to {} state", context.getDeploymentName(), nextState);

        context.getDeploymentStatus().setDeploymentReadyTimestamp(millisToInstantStr(0));
        context.getDeploymentStatus().setAbortTimestamp(millisToInstantStr(0));
        context.getDeploymentStatus().setSavepointTriggerId(null);

        return patchStatusUpdateControl(context, nextState, JobStatus.RUNNING, null);
    }

    // ==================== Common Utility Methods ====================

    public static UpdateControl<FlinkBlueGreenDeployment> patchStatusUpdateControl(
            BlueGreenContext context,
            FlinkBlueGreenDeploymentState deploymentState,
            JobStatus jobState,
            String error) {

        var deploymentStatus = context.getDeploymentStatus();
        var flinkBlueGreenDeployment = context.getBgDeployment();

        if (deploymentState != null) {
            deploymentStatus.setBlueGreenState(deploymentState);
        }

        if (jobState != null) {
            deploymentStatus.getJobStatus().setState(jobState);
        }

        if (jobState == JobStatus.FAILING) {
            deploymentStatus.setError(error);
        }

        if (jobState == JobStatus.RECONCILING || jobState == JobStatus.RUNNING) {
            deploymentStatus.setError(null);
        }

        deploymentStatus.setLastReconciledTimestamp(java.time.Instant.now().toString());
        flinkBlueGreenDeployment.setStatus(deploymentStatus);
        return UpdateControl.patchStatus(flinkBlueGreenDeployment);
    }

    // ==================== DTO/Result Classes ====================

    @Getter
    @AllArgsConstructor
    private static class DeploymentTransition {
        final BlueGreenDeploymentType nextBlueGreenDeploymentType;
        final FlinkBlueGreenDeploymentState nextState;
    }

    @Getter
    @AllArgsConstructor
    private static class TransitionState {
        final FlinkDeployment currentDeployment;
        final FlinkDeployment nextDeployment;
        final FlinkBlueGreenDeploymentState nextState;
    }
}
