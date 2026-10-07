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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDiffType;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionMode;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.EventUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenTransitionUtils;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_GATE_TIMEOUT;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.deleteFlinkDeployment;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.deployCluster;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.isFlinkDeploymentReady;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.suspendFlinkDeployment;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.updateFlinkDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenTransitionUtils.updateTransitionStageFromJobStatus;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenTransitionUtils.validateAdvancedModeConfig;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.fetchSavepointInfo;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getDeploymentDeletionDelay;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getGateTimeout;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getReconciliationReschedInterval;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getSpecDiff;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.hasSpecChanged;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.instantStrToMillis;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.isSavepointRequired;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.millisToInstantStr;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.prepareFlinkDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.revertToLastSpec;
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
        context.getDeploymentStatus().setError(null);

        FlinkDeployment flinkDeployment =
                prepareFlinkDeployment(
                        context,
                        nextBlueGreenDeploymentType,
                        lastCheckpoint,
                        isFirstDeployment,
                        bgMeta);

        Optional<String> configError = validateAdvancedModeConfig(context);
        if (configError.isPresent()) {
            return rejectWithValidationError(context, configError.get());
        }
        if (isFirstDeployment) {
            setLastReconciledSpec(context);
        }
        BlueGreenTransitionUtils.prepareTransitionMetadata(
                context, nextBlueGreenDeploymentType, flinkDeployment, isFirstDeployment);

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

        if (specDiff == BlueGreenDiffType.IMMUTABLE_FIELD_CHANGED) {
            return rejectImmutableFieldChange(context);
        }

        if (specDiff != BlueGreenDiffType.IGNORE) {
            FlinkDeployment currentFlinkDeployment =
                    context.getDeploymentByType(currentBlueGreenDeploymentType);

            if (specDiff == BlueGreenDiffType.SUSPEND && currentFlinkDeployment != null) {
                setLastReconciledSpec(context);
                LOG.info(
                        "In-place suspension for '{}'",
                        currentFlinkDeployment.getMetadata().getName());
                return patchFlinkDeployment(context, currentBlueGreenDeploymentType);
            }

            if (specDiff == BlueGreenDiffType.RESUME && currentFlinkDeployment != null) {
                setLastReconciledSpec(context);
                LOG.info(
                        "In-place resume for '{}'", currentFlinkDeployment.getMetadata().getName());
                return patchFlinkDeployment(context, currentBlueGreenDeploymentType);
            }

            // Check if child is currently suspended - if so, just patch specs without restart
            if (isChildSuspended(currentFlinkDeployment)) {
                setLastReconciledSpec(context);
                LOG.info(
                        "Spec change while suspended for '{}'",
                        currentFlinkDeployment.getMetadata().getName());
                return patchFlinkDeployment(context, currentBlueGreenDeploymentType);
            }

            if (currentFlinkDeployment != null && isFlinkDeploymentReady(currentFlinkDeployment)) {
                if (specDiff == BlueGreenDiffType.TRANSITION) {
                    Optional<String> configError = validateAdvancedModeConfig(context);
                    if (configError.isEmpty() && isSavepointRequired(context)) {
                        configError =
                                BlueGreenUtils.validateTransitionStateBackend(
                                        context.getCtxFactory()
                                                .getResourceContext(
                                                        currentFlinkDeployment,
                                                        context.getJosdkContext())
                                                .getObserveConfig(),
                                        BlueGreenUtils.templateConfig(context));
                    }
                    if (configError.isPresent()) {
                        return rejectWithValidationError(context, configError.get());
                    }

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

                } else if (specDiff == BlueGreenDiffType.SAVEPOINT_REDEPLOY) {
                    // Savepoint redeploy: skip taking a new savepoint, use initialSavepointPath
                    var jobSpec =
                            context.getBgDeployment().getSpec().getTemplate().getSpec().getJob();
                    LOG.info(
                            "Savepoint redeploy triggered for '{}', using initialSavepointPath: {}",
                            context.getBgDeployment().getMetadata().getName(),
                            Objects.toString(jobSpec.getInitialSavepointPath(), "<none>"));
                    setLastReconciledSpec(context);
                    try {
                        return startSavepointRedeployTransition(
                                context, currentBlueGreenDeploymentType);
                    } catch (Exception e) {
                        var error =
                                "Could not start Savepoint Redeploy Transition. Details: "
                                        + e.getMessage();
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
                    var childName =
                            currentFlinkDeployment != null
                                    ? currentFlinkDeployment.getMetadata().getName()
                                    : "null";
                    var error =
                            String.format(
                                    "Transition to %s not possible, current Flink Deployment '%s' is not READY. FAILING '%s'",
                                    calculateTransition(currentBlueGreenDeploymentType)
                                            .nextBlueGreenDeploymentType,
                                    childName,
                                    context.getBgDeployment().getMetadata().getName());
                    return markDeploymentFailing(context, error);
                }
            }
        }

        return UpdateControl.noUpdate();
    }

    private boolean isChildSuspended(FlinkDeployment deployment) {
        if (deployment == null || deployment.getSpec() == null) {
            return false;
        }
        var job = deployment.getSpec().getJob();
        return job != null
                && job.getState()
                        == org.apache.flink.kubernetes.operator.api.spec.JobState.SUSPENDED;
    }

    private UpdateControl<FlinkBlueGreenDeployment> patchFlinkDeployment(
            BlueGreenContext context, BlueGreenDeploymentType blueGreenDeploymentTypeToPatch) {
        return patchFlinkDeployment(context, blueGreenDeploymentTypeToPatch, true);
    }

    private UpdateControl<FlinkBlueGreenDeployment> patchFlinkDeployment(
            BlueGreenContext context,
            BlueGreenDeploymentType blueGreenDeploymentTypeToPatch,
            boolean carryOverSavepointInPatch) {

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
                carryOverSavepointInPatch
                        ? carryOverSavepoint(
                                context, blueGreenDeploymentTypeToPatch, childDeploymentName)
                        : null;

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

    /**
     * Starts a transition for savepoint redeploy scenario. Unlike normal transitions, this does not
     * take a new savepoint - it uses the initialSavepointPath specified in the spec.
     *
     * @param context the transition context
     * @param currentBlueGreenDeploymentType the current deployment type
     * @return UpdateControl for the deployment
     */
    private UpdateControl<FlinkBlueGreenDeployment> startSavepointRedeployTransition(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {
        DeploymentTransition transition = calculateTransition(currentBlueGreenDeploymentType);

        return initiateDeployment(
                context,
                transition.nextBlueGreenDeploymentType,
                transition.nextState,
                null, // Use initialSavepointPath from spec
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
                                "Could not fetch savepoint with triggerId: %s. Error: %s%s",
                                triggerId,
                                savepointFetchResult.getError(),
                                BlueGreenUtils.savepointFailureHint(
                                        ctx.getFlinkVersion(),
                                        ctx.getObserveConfig()
                                                .get(
                                                        KubernetesOperatorConfigOptions
                                                                .OPERATOR_SAVEPOINT_FORMAT_TYPE))));
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

        if (isChildSuspended(transitionState.nextDeployment)) {
            if (transitionState.nextDeployment.getStatus().getLifecycleState()
                    == ResourceLifecycleState.SUSPENDED) {
                return finalizeSuspendedDeployment(context, transitionState.nextState);
            } else {
                return shouldWeAbort(
                        context, transitionState.nextDeployment, transitionState.nextState);
            }
        }

        if (isFlinkDeploymentReady(transitionState.nextDeployment)) {
            if (BlueGreenTransitionUtils.moveToFirstTransitionStage(context)) {
                // The gate phase starts now and gets its own deadline
                BlueGreenUtils.setGateAbortTimestamp(context);
                return patchStatusUpdateControl(context, null, null, null)
                        .rescheduleAfter(getReconciliationReschedInterval(context));
            }
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

    private UpdateControl<FlinkBlueGreenDeployment> finalizeSuspendedDeployment(
            BlueGreenContext context, FlinkBlueGreenDeploymentState nextState) {

        LOG.info(
                "Finalizing suspended deployment '{}' to {} state",
                context.getDeploymentName(),
                nextState);

        resetTransitionMarkers(context.getDeploymentStatus());

        return patchStatusUpdateControl(context, nextState, JobStatus.SUSPENDED, null)
                .rescheduleAfter(0);
    }

    private UpdateControl<FlinkBlueGreenDeployment> handleSpecChangesDuringTransition(
            BlueGreenContext context, BlueGreenDeploymentType currentBlueGreenDeploymentType) {
        if (hasSpecChanged(context)) {
            BlueGreenDiffType diffType = getSpecDiff(context);

            if (diffType == BlueGreenDiffType.IMMUTABLE_FIELD_CHANGED) {
                return rejectImmutableFieldChange(context);
            }

            // Block SUSPEND during transition
            if (diffType == BlueGreenDiffType.SUSPEND) {
                LOG.info(
                        "Suspend requested during transition for '{}'. "
                                + "Waiting for transition to complete before processing suspend.",
                        context.getBgDeployment().getMetadata().getName());
                return null;
            }

            if (diffType != BlueGreenDiffType.IGNORE) {
                setLastReconciledSpec(context);
                var oppositeDeploymentType =
                        context.getOppositeDeploymentType(currentBlueGreenDeploymentType);
                LOG.info(
                        "Patching FlinkDeployment '{}' during handleSpecChangesDuringTransition",
                        context.getDeploymentByType(oppositeDeploymentType)
                                .getMetadata()
                                .getName());
                return patchFlinkDeployment(
                        context,
                        oppositeDeploymentType,
                        diffType != BlueGreenDiffType.SAVEPOINT_REDEPLOY);
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

        // The standby is only stopped once the gate has handed over, and its gate metrics go with
        // it: checking them again would wait until the gate timeout aborts the transition
        if (currentDeployment != null
                && currentDeployment.getSpec().getJob().getState() == JobState.SUSPENDED) {
            return stopWithSavepointAndDelete(currentDeployment, context, nextState);
        }

        if (!BlueGreenTransitionUtils.isClearToTeardown(context)) {
            // Wait until CLEAR_TO_TEARDOWN is set by the client
            return waitForGate(
                    context,
                    nextDeployment,
                    nextState,
                    String.format("the gate did not reach %s", TransitionStage.CLEAR_TO_TEARDOWN));
        }

        // The first standby subtask past the toggle sets CLEAR_TO_TEARDOWN, the others may still
        // owe records: their watermarks pass the toggle separately unless the gate is behind a
        // shuffle
        if (currentDeployment != null
                && !BlueGreenTransitionUtils.isHandOverDone(context, currentDeployment)) {
            return waitForGate(
                    context,
                    nextDeployment,
                    nextState,
                    String.format(
                            "not every gate subtask of '%s' finished the hand-over",
                            currentDeployment.getMetadata().getName()));
        }

        var deploymentStatus = context.getDeploymentStatus();

        if (currentDeployment == null) {
            deploymentStatus.setDeploymentReadyTimestamp(Instant.now().toString());
            return finalizeBlueGreenDeployment(context, nextState);
        }

        long deploymentDeletionDelayMs = getDeploymentDeletionDelay(context);
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
            return TransitionMode.ADVANCED == BlueGreenTransitionUtils.getTransitionMode(context)
                    ? stopWithSavepointAndDelete(currentDeployment, context, nextState)
                    : deleteDeployment(currentDeployment, context, nextState);
        } else {
            return waitBeforeDeleting(currentDeployment, deletionTimestamp);
        }
    }

    /** Waits for the gate until its deadline, then aborts the transition. */
    private UpdateControl<FlinkBlueGreenDeployment> waitForGate(
            BlueGreenContext context,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState,
            String pending) {
        long gateDeadline = instantStrToMillis(context.getDeploymentStatus().getAbortTimestamp());
        if (gateDeadline > 0 && gateDeadline < System.currentTimeMillis()) {
            var reason =
                    String.format(
                            "Aborting deployment '%s': %s within %d ms (%s)",
                            nextDeployment.getMetadata().getName(),
                            pending,
                            getGateTimeout(context),
                            BLUEGREEN_GATE_TIMEOUT.key());
            return abortDeployment(context, nextDeployment, nextState, reason);
        }
        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate()
                .rescheduleAfter(getReconciliationReschedInterval(context));
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

    /**
     * Stops the standby with a savepoint, then deletes it. Its gate no longer emits, and the
     * savepoint makes sinks that commit on checkpoints (Iceberg, Kafka exactly-once, files) commit
     * what it wrote since its last checkpoint. Deleting a running job cancels it and discards that
     * output, and the new deployment does not write those records again.
     */
    private UpdateControl<FlinkBlueGreenDeployment> stopWithSavepointAndDelete(
            FlinkDeployment standby,
            BlueGreenContext context,
            FlinkBlueGreenDeploymentState nextState) {
        var name = standby.getMetadata().getName();
        if (standby.getStatus().getLifecycleState() == ResourceLifecycleState.SUSPENDED
                || ReconciliationUtils.isJobInTerminalState(standby.getStatus())) {
            return deleteDeployment(standby, context, nextState);
        }
        if (standby.getSpec().getJob().getState() != JobState.SUSPENDED) {
            var ctx =
                    context.getCtxFactory().getResourceContext(standby, context.getJosdkContext());
            if (StringUtils.isNullOrWhitespaceOnly(
                    ctx.getObserveConfig().get(CheckpointingOptions.SAVEPOINT_DIRECTORY))) {
                LOG.warn(
                        "No savepoint directory for '{}', deleting it without a savepoint: sinks"
                                + " that commit on checkpoints lose what it wrote since its last one",
                        name);
                return deleteDeployment(standby, context, nextState);
            }
            LOG.info("Stopping '{}' with a savepoint before deleting it", name);
            standby.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
            standby.getSpec().getJob().setState(JobState.SUSPENDED);
            updateFlinkDeployment(standby, context);
        }
        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate()
                .rescheduleAfter(getReconciliationReschedInterval(context));
    }

    private UpdateControl<FlinkBlueGreenDeployment> deleteDeployment(
            FlinkDeployment currentDeployment,
            BlueGreenContext context,
            FlinkBlueGreenDeploymentState nextState) {

        boolean deleted = deleteFlinkDeployment(currentDeployment, context);

        if (!deleted) {
            LOG.info("FlinkDeployment '{}' not deleted, will retry", currentDeployment);
            return UpdateControl.<FlinkBlueGreenDeployment>noUpdate()
                    .rescheduleAfter(RETRY_DELAY_MS);
        } else {
            LOG.info("FlinkDeployment '{}' deleted!", currentDeployment);
            return finalizeBlueGreenDeployment(context, nextState);
        }
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
            return abortDeployment(
                    context,
                    nextDeployment,
                    nextState,
                    String.format("Aborting deployment '%s'", deploymentName));
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
            String reason) {

        suspendFlinkDeployment(context, nextDeployment);

        FlinkBlueGreenDeploymentState previousState =
                getPreviousState(nextState, context.getDeployments());
        // Before resetGateOnAbort below drops the cutover point from the ConfigMap
        var redeployed = redeployIfCutoverWasSet(context, nextDeployment, previousState);
        context.getDeploymentStatus().setBlueGreenState(previousState);
        resetTransitionMarkers(context.getDeploymentStatus());

        var error =
                String.format(
                        "%s, rolling B/G deployment back to %s%s",
                        reason, previousState, redeployed);
        var updateControl = markDeploymentFailing(context, error);
        // Must follow the stage write in markDeploymentFailing, see resetGateOnAbort
        BlueGreenTransitionUtils.resetGateOnAbort(context, previousState);
        return updateControl;
    }

    /**
     * Once the cutover point is set, the surviving deployment's gate skips the records from it on
     * and leaves them to the aborted one, which may not have written or committed them yet. The
     * survivor is then redeployed from the savepoint the aborted deployment started from, so those
     * records are written again (along with duplicates of what was written since) instead of lost.
     * Returns what was done, for the abort message.
     */
    private static String redeployIfCutoverWasSet(
            BlueGreenContext context,
            FlinkDeployment aborted,
            FlinkBlueGreenDeploymentState previousState) {
        if (TransitionMode.ADVANCED != BlueGreenTransitionUtils.getTransitionMode(context)
                || previousState == FlinkBlueGreenDeploymentState.INITIALIZING_BLUE
                || !BlueGreenTransitionUtils.isCutoverSet(context)) {
            return "";
        }
        var survivor =
                context.getDeploymentByType(
                        previousState.name().contains("BLUE")
                                ? BlueGreenDeploymentType.BLUE
                                : BlueGreenDeploymentType.GREEN);
        var savepoint = aborted.getSpec().getJob().getInitialSavepointPath();
        if (survivor == null || StringUtils.isNullOrWhitespaceOnly(savepoint)) {
            LOG.error(
                    "The cutover point was set, but there is no savepoint to redeploy from: the"
                            + " records from it on that '{}' did not write are lost",
                    aborted.getMetadata().getName());
            return ". The cutover point was set and there is no transition savepoint, so the"
                    + " records from it on that the aborted deployment did not write are lost";
        }
        var job = survivor.getSpec().getJob();
        job.setInitialSavepointPath(savepoint);
        job.setSavepointRedeployNonce(
                job.getSavepointRedeployNonce() == null ? 1L : job.getSavepointRedeployNonce() + 1);
        updateFlinkDeployment(survivor, context);
        return String.format(
                ". The cutover point was set, so '%s' is redeployed from the transition savepoint"
                        + " %s, and records written since may be written twice",
                survivor.getMetadata().getName(), savepoint);
    }

    @NotNull
    private static UpdateControl<FlinkBlueGreenDeployment> markDeploymentFailing(
            BlueGreenContext context, String error) {
        LOG.error(error);
        return patchStatusUpdateControl(context, null, JobStatus.FAILING, error);
    }

    private UpdateControl<FlinkBlueGreenDeployment> rejectWithValidationError(
            BlueGreenContext context, String error) {
        LOG.warn(error);
        revertToLastSpec(context);
        EventUtils.createOrUpdateEventWithInterval(
                context.getJosdkContext().getClient(),
                context.getBgDeployment(),
                EventRecorder.Type.Warning,
                EventRecorder.Reason.ValidationError.toString(),
                error,
                EventRecorder.Component.Operator,
                e -> {},
                null,
                null);
        context.getDeploymentStatus().setError(error);
        return patchStatusUpdateControl(context, null, null, null);
    }

    private UpdateControl<FlinkBlueGreenDeployment> rejectImmutableFieldChange(
            BlueGreenContext context) {
        String error =
                String.format(
                        "transitionMode cannot be changed after initial deployment for '%s'. Reverting spec.",
                        context.getBgDeployment().getMetadata().getName());
        return rejectWithValidationError(context, error);
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

        resetTransitionMarkers(context.getDeploymentStatus());

        updateBlueGreenIngress(context, nextState);

        // Finalize status and reschedule immediately so any pending spec changes
        // (e.g., suspend requested during transition) are picked up on next reconcile
        return patchStatusUpdateControl(context, nextState, JobStatus.RUNNING, null)
                .rescheduleAfter(0);
    }

    /**
     * Reconciles ingress for the active deployment in ACTIVE states. This handles ingress spec
     * changes that occur while the deployment is stable (not transitioning).
     *
     * @param context the Blue/Green context
     * @param activeDeploymentType which deployment (BLUE or GREEN) is currently active
     */
    public void reconcileIngressForActiveDeployment(
            BlueGreenContext context, BlueGreenDeploymentType activeDeploymentType) {
        FlinkDeployment activeDeployment = context.getDeploymentByType(activeDeploymentType);
        if (activeDeployment == null) {
            return;
        }

        var flinkResourceContext =
                context.getCtxFactory()
                        .getResourceContext(activeDeployment, context.getJosdkContext());

        if (!flinkResourceContext.getOperatorConfig().isManageIngress()) {
            return;
        }

        IngressUtils.reconcileBlueGreenIngress(
                context,
                true,
                activeDeployment,
                flinkResourceContext.getDeployConfig(activeDeployment.getSpec()),
                context.getJosdkContext());

        LOG.info(
                "Successfully reconciled ingress for active deployment: {}",
                activeDeployment.getMetadata().getName());
    }

    /**
     * Updates the ingress for Blue/Green deployment during transitions, pointing to the newly
     * active deployment.
     *
     * @param blueGreenContext the Blue/Green context
     * @param nextState which deployment (ACTIVE_BLUE or ACTIVE_GREEN) is becoming active
     */
    public void updateBlueGreenIngress(
            BlueGreenContext blueGreenContext, FlinkBlueGreenDeploymentState nextState) {
        FlinkDeployment activeDeployment;
        switch (nextState) {
            case ACTIVE_BLUE:
                activeDeployment = blueGreenContext.getBlueDeployment();
                break;
            case ACTIVE_GREEN:
                activeDeployment = blueGreenContext.getGreenDeployment();
                break;
            default:
                LOG.info("Skipping ingress reconciliation for non-active state: {}", nextState);
                return;
        }

        // Create a FlinkResourceContext for the active deployment to get proper config
        var flinkResourceContext =
                blueGreenContext
                        .getCtxFactory()
                        .getResourceContext(activeDeployment, blueGreenContext.getJosdkContext());

        IngressUtils.reconcileBlueGreenIngress(
                blueGreenContext,
                flinkResourceContext.getOperatorConfig().isManageIngress(),
                activeDeployment,
                flinkResourceContext.getDeployConfig(activeDeployment.getSpec()),
                blueGreenContext.getJosdkContext());
    }

    // ==================== Common Utility Methods ====================

    private static void resetTransitionMarkers(FlinkBlueGreenDeploymentStatus status) {
        status.setDeploymentReadyTimestamp(millisToInstantStr(0));
        status.setAbortTimestamp(millisToInstantStr(0));
        status.setSavepointTriggerId(null);
    }

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
            updateTransitionStageFromJobStatus(context, jobState);
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
