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
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDiffType;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.deleteFlinkDeployment;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.deployCluster;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.isFlinkDeploymentReady;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.suspendFlinkDeployment;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.updateFlinkDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.fetchSavepointInfo;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.getLastCheckpoint;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.isSavepointRequired;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.lookForCheckpoint;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.prepareFlinkDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.triggerSavepoint;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getReconciliationReschedInterval;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.instantStrToMillis;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.millisToInstantStr;

/** Consolidated service for all Blue/Green deployment operations. */
public class BlueGreenDeploymentService {

    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenDeploymentService.class);
    private static final long RETRY_DELAY_MS = 500;

    // ==================== Deployment Initiation Methods ====================

    /**
     * Initiates a new Blue/Green deployment.
     *
     * @param context the transition context
     * @param nextDeploymentType the type of deployment to create
     * @param nextState the next state to transition to
     * @param lastCheckpoint the checkpoint to restore from (can be null)
     * @param isFirstDeployment whether this is the first deployment
     * @return UpdateControl for the deployment
     */
    public UpdateControl<FlinkBlueGreenDeployment> initiateDeployment(
            BlueGreenContext context,
            DeploymentType nextDeploymentType,
            FlinkBlueGreenDeploymentState nextState,
            Savepoint lastCheckpoint,
            boolean isFirstDeployment) {
        ObjectMeta bgMeta = context.getBgDeployment().getMetadata();

        FlinkDeployment flinkDeployment =
                prepareFlinkDeployment(
                        context.getBgDeployment(),
                        nextDeploymentType,
                        lastCheckpoint,
                        isFirstDeployment,
                        bgMeta);

        deployCluster(context, flinkDeployment);

        BlueGreenUtils.setAbortTimestamp(context);

        return patchStatusUpdateControl(context, nextState, JobStatus.RECONCILING)
                .rescheduleAfter(BlueGreenUtils.getReconciliationReschedInterval(context));
    }

    /**
     * Checks if a full transition can be initiated and initiates it if conditions are met.
     *
     * @param context the transition context
     * @param currentDeploymentType the current deployment type
     * @return UpdateControl for the deployment
     */
    public UpdateControl<FlinkBlueGreenDeployment> checkAndInitiateDeployment(
            BlueGreenContext context, DeploymentType currentDeploymentType) {
        BlueGreenDiffType specDiff = BlueGreenSpecUtils.getSpecDiff(context);

        if (specDiff != BlueGreenDiffType.IGNORE) {
            FlinkDeployment currentFlinkDeployment =
                    context.getDeploymentByType(currentDeploymentType);

            if (isFlinkDeploymentReady(currentFlinkDeployment)) {
                if (specDiff == BlueGreenDiffType.TRANSITION) {
                    if (handleSavepoint(context, currentFlinkDeployment)) {
                        // This is the only portion where the last reconciled spec is not set,
                        // so we can reprocess TRANSITION after the savepoint is done
                        var savepointingState = calculateSavepointingState(currentDeploymentType);
                        return patchStatusUpdateControl(context, savepointingState, null)
                                .rescheduleAfter(getReconciliationReschedInterval(context));
                    }

                    BlueGreenSpecUtils.setLastReconciledSpec(context);
                    return startTransition(context, currentDeploymentType, currentFlinkDeployment);
                } else {
                    BlueGreenSpecUtils.setLastReconciledSpec(context);
                    return patchFlinkDeployment(context, currentDeploymentType, specDiff);
                }
            } else {
                if (context.getDeploymentStatus().getJobStatus().getState() != JobStatus.FAILING) {
                    BlueGreenSpecUtils.setLastReconciledSpec(context);
                    return patchStatusUpdateControl(context, null, JobStatus.FAILING);
                }
            }
        }

        return UpdateControl.noUpdate();
    }

    private UpdateControl<FlinkBlueGreenDeployment> patchFlinkDeployment(
            BlueGreenContext context,
            DeploymentType currentDeploymentType,
            BlueGreenDiffType specDiff) {

        if (specDiff == BlueGreenDiffType.PATCH_CHILD) {
            FlinkDeployment nextFlinkDeployment =
                    context.getDeploymentByType(currentDeploymentType);

            nextFlinkDeployment.setSpec(
                    context.getBgDeployment().getSpec().getTemplate().getSpec());

            updateFlinkDeployment(nextFlinkDeployment, context);
        }

        return patchStatusUpdateControl(
                        context,
                        calculatePatchingState(currentDeploymentType),
                        JobStatus.RECONCILING)
                .rescheduleAfter(BlueGreenUtils.getReconciliationReschedInterval(context));
    }

    private UpdateControl<FlinkBlueGreenDeployment> startTransition(
            BlueGreenContext context,
            DeploymentType currentDeploymentType,
            FlinkDeployment currentFlinkDeployment) {
        DeploymentTransition transition = calculateTransition(currentDeploymentType);

        Savepoint lastCheckpoint = configureInitialSavepoint(context, currentFlinkDeployment);

        return initiateDeployment(
                context,
                transition.nextDeploymentType,
                transition.nextState,
                lastCheckpoint,
                false);
    }

    private DeploymentTransition calculateTransition(DeploymentType currentType) {
        if (DeploymentType.BLUE == currentType) {
            return new DeploymentTransition(
                    DeploymentType.GREEN, FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN);
        } else {
            return new DeploymentTransition(
                    DeploymentType.BLUE, FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE);
        }
    }

    private FlinkBlueGreenDeploymentState calculatePatchingState(DeploymentType currentType) {
        if (DeploymentType.BLUE == currentType) {
            return FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
        } else {
            return FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
        }
    }

    // ==================== Savepointing Methods ====================

    public boolean monitorSavepoint(
            BlueGreenContext context, DeploymentType currentDeploymentType) {

        FlinkResourceContext<FlinkDeployment> ctx =
                context.getCtxFactory()
                        .getResourceContext(
                                context.getDeploymentByType(currentDeploymentType),
                                context.getJosdkContext());

        String savepointTriggerId = context.getDeploymentStatus().getSavepointTriggerId();
        var savepointFetchResult = fetchSavepointInfo(ctx, savepointTriggerId);

        return !savepointFetchResult.isPending();
    }

    private Savepoint configureInitialSavepoint(
            BlueGreenContext context, FlinkDeployment currentFlinkDeployment) {

        FlinkResourceContext<FlinkDeployment> ctx =
                context.getCtxFactory()
                        .getResourceContext(currentFlinkDeployment, context.getJosdkContext());

        // If a savepoint is required we fetch it, should be ready by this point
        if (isSavepointRequired(context)) {
            String savepointTriggerId = context.getDeploymentStatus().getSavepointTriggerId();
            var savepointFetchResult = fetchSavepointInfo(ctx, savepointTriggerId);

            org.apache.flink.core.execution.SavepointFormatType coreSavepointFormatType =
                    ctx.getObserveConfig()
                            .get(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE);

            var savepointFormatType =
                    SavepointFormatType.valueOf(coreSavepointFormatType.toString());

            return Savepoint.of(
                    savepointFetchResult.getLocation(),
                    SnapshotTriggerType.MANUAL,
                    savepointFormatType);
        }

        // Else we start looking for the last checkpoint if needed

        if (!lookForCheckpoint(context)) {
            return null;
        }

        return getLastCheckpoint(ctx);
    }

    private boolean handleSavepoint(
            BlueGreenContext context, FlinkDeployment currentFlinkDeployment) {

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

        LOG.debug("Savepoint previously requested (triggerId: {})", savepointTriggerId);
        return false;
    }

    private FlinkBlueGreenDeploymentState calculateSavepointingState(DeploymentType currentType) {
        if (DeploymentType.BLUE == currentType) {
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
     * @param currentDeploymentType the current deployment type being transitioned from
     * @return UpdateControl for the transition
     */
    public UpdateControl<FlinkBlueGreenDeployment> monitorTransition(
            BlueGreenContext context, DeploymentType currentDeploymentType) {

        handleSpecChangesDuringTransition(context);

        TransitionState transitionState = determineTransitionState(context, currentDeploymentType);

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

    private void handleSpecChangesDuringTransition(BlueGreenContext context) {
        if (BlueGreenSpecUtils.hasSpecChanged(context)) {
            BlueGreenSpecUtils.revertToLastSpec(context);
            LOG.warn(
                    "Blue/Green Spec change detected during transition, ignored and reverted to the last reconciled spec");
        }
    }

    private TransitionState determineTransitionState(
            BlueGreenContext context, DeploymentType currentDeploymentType) {
        TransitionState transitionState;

        if (DeploymentType.BLUE == currentDeploymentType) {
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
                        + currentDeploymentType);

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
            return patchStatusUpdateControl(context, null, null)
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

        return patchStatusUpdateControl(context, null, null).rescheduleAfter(delay);
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

        LOG.warn(
                "Aborting deployment '{}', rolling B/G deployment back to {}",
                deploymentName,
                previousState);

        return patchStatusUpdateControl(context, null, JobStatus.FAILING);
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

        return patchStatusUpdateControl(context, nextState, JobStatus.RUNNING);
    }

    // ==================== Common Utility Methods ====================

    public static UpdateControl<FlinkBlueGreenDeployment> patchStatusUpdateControl(
            BlueGreenContext context,
            FlinkBlueGreenDeploymentState deploymentState,
            JobStatus jobState) {

        var deploymentStatus = context.getDeploymentStatus();
        var flinkBlueGreenDeployment = context.getBgDeployment();

        if (deploymentState != null) {
            deploymentStatus.setBlueGreenState(deploymentState);
        }

        if (jobState != null) {
            deploymentStatus.getJobStatus().setState(jobState);
        }

        deploymentStatus.setLastReconciledTimestamp(java.time.Instant.now().toString());
        flinkBlueGreenDeployment.setStatus(deploymentStatus);
        return UpdateControl.patchStatus(flinkBlueGreenDeployment);
    }

    // ==================== DTO/Result Classes ====================

    @Getter
    @AllArgsConstructor
    private static class DeploymentTransition {
        final DeploymentType nextDeploymentType;
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
