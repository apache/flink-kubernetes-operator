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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenKubernetesUtils;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils;
import org.apache.flink.util.Preconditions;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenKubernetesUtils.isDeploymentReady;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.configureSavepoint;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.getPreviousState;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.instantStrToMillis;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.millisToInstantStr;

/**
 * State machine handler for managing Blue/Green deployment state transitions. Encapsulates the
 * logic for processing different states in the Blue/Green deployment lifecycle.
 */
public class BlueGreenStateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenStateMachine.class);

    /**
     * Simplified context object containing all the necessary state and dependencies for Blue/Green
     * deployment state transitions. This reduces parameter passing without wrapping utility
     * functions.
     */
    @Getter
    @RequiredArgsConstructor
    public static class BlueGreenTransitionContext {
        private final FlinkBlueGreenDeployment bgDeployment;
        private final FlinkBlueGreenDeploymentStatus deploymentStatus;
        private final Context<FlinkBlueGreenDeployment> josdkContext;
        private final FlinkBlueGreenDeployments deployments;
        private final FlinkResourceContextFactory ctxFactory;

        // Only keep convenience methods that add real value
        public String getDeploymentName() {
            return bgDeployment.getMetadata().getName();
        }

        public FlinkDeployment getBlueDeployment() {
            return deployments != null ? deployments.getFlinkDeploymentBlue() : null;
        }

        public FlinkDeployment getGreenDeployment() {
            return deployments != null ? deployments.getFlinkDeploymentGreen() : null;
        }

        public FlinkDeployment getDeploymentByType(DeploymentType type) {
            return type == DeploymentType.BLUE ? getBlueDeployment() : getGreenDeployment();
        }
    }

    public BlueGreenStateMachine() { }

    /**
     * Processes the current state and returns the appropriate update control.
     *
     * @param context the BlueGreen transition context containing all necessary dependencies
     * @return UpdateControl indicating the next action
     */
    public UpdateControl<FlinkBlueGreenDeployment> processState(
            BlueGreenTransitionContext context) {

        switch (context.getDeploymentStatus().getBlueGreenState()) {
            case INITIALIZING_BLUE:
                return checkFirstDeployment(context);
            case ACTIVE_BLUE:
                return checkAndInitiateDeployment(context, DeploymentType.BLUE);
            case ACTIVE_GREEN:
                return checkAndInitiateDeployment(context, DeploymentType.GREEN);
            case TRANSITIONING_TO_BLUE:
                return monitorTransition(context, DeploymentType.GREEN);
            case TRANSITIONING_TO_GREEN:
                return monitorTransition(context, DeploymentType.BLUE);
            default:
                return UpdateControl.noUpdate();
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> checkFirstDeployment(
            BlueGreenTransitionContext context) {

        FlinkBlueGreenDeploymentStatus deploymentStatus = context.getDeploymentStatus();

        // We only allow a deployment if it's indeed the first (null last spec)
        // or if we're recovering (failing status) and the spec has changed
        if (deploymentStatus.getLastReconciledSpec() == null
                || (deploymentStatus.getJobStatus().getState().equals(JobStatus.FAILING)
                        && BlueGreenSpecUtils.hasSpecChanged(context))) {
            // Ack the change in the spec (setLastReconciledSpec)
            BlueGreenSpecUtils.setLastReconciledSpec(context);
            return initiateDeployment(
                    context,
                    DeploymentType.BLUE,
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                    null,
                    true);
        } else {
            LOG.warn(
                    "Ignoring initial deployment. Last Reconciled Spec null: {}. BG Status: {}.",
                    deploymentStatus.getLastReconciledSpec() == null,
                    deploymentStatus.getJobStatus().getState());
            return UpdateControl.noUpdate();
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> monitorTransition(
            BlueGreenTransitionContext context, DeploymentType currentDeploymentType) {

        if (BlueGreenSpecUtils.hasSpecChanged(context)) {
            // this means the spec was changed during transition,
            //  ignore the new change, revert the spec and log as warning
            BlueGreenSpecUtils.revertToLastSpec(context);
            LOG.warn(
                    "Blue/Green Spec change detected during transition, ignored and reverted to the last reconciled spec");
        }

        var nextState = FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
        FlinkDeployment currentDeployment;
        FlinkDeployment nextDeployment;

        if (DeploymentType.BLUE == currentDeploymentType) {
            nextState = FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
            currentDeployment = context.getBlueDeployment();
            nextDeployment = context.getGreenDeployment();
        } else {
            currentDeployment = context.getGreenDeployment();
            nextDeployment = context.getBlueDeployment();
        }

        Preconditions.checkNotNull(
                nextDeployment,
                "Target Dependent Deployment resource not found. Blue/Green deployment name: "
                        + context.getDeploymentName()
                        + ", current deployment type: "
                        + currentDeploymentType);

        if (isDeploymentReady(nextDeployment)) {
            return shouldWeDelete(context, currentDeployment, nextDeployment, nextState);
        } else {
            return shouldWeAbort(context, nextDeployment, nextState);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> shouldWeDelete(
            BlueGreenTransitionContext context,
            FlinkDeployment currentDeployment,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState) {

        FlinkBlueGreenDeploymentStatus deploymentStatus = context.getDeploymentStatus();

        // currentDeployment will be null in case:
        //  - of first time deployments
        //  - the previous deployment has been successfully deleted
        // therefore, finalize right away
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

        var deletionTs = deploymentReadyTimestamp + deploymentDeletionDelayMs;

        if (deletionTs < System.currentTimeMillis()) {
            return deleteDeployment(currentDeployment, context);
        } else {
            return waitBeforeDeleting(currentDeployment, deletionTs);
        }
    }

    private static UpdateControl<FlinkBlueGreenDeployment> waitBeforeDeleting(
            FlinkDeployment currentDeployment, long deletionTs) {
        long delay = deletionTs - System.currentTimeMillis();
        LOG.info(
                "Awaiting deletion delay for FlinkDeployment '{}', rescheduling reconciliation in {} seconds.",
                currentDeployment.getMetadata().getName(),
                delay / 1000);
        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(delay);
    }

    private UpdateControl<FlinkBlueGreenDeployment> shouldWeAbort(
            BlueGreenTransitionContext context,
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
            return retryDeployment(context, abortTimestamp, deploymentName);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> retryDeployment(
            BlueGreenTransitionContext context, long abortTimestamp, String deploymentName) {
        var delay = abortTimestamp - System.currentTimeMillis();
        LOG.info(
                "FlinkDeployment '{}' not ready yet, retrying in {} seconds.",
                deploymentName,
                delay / 1000);
        return patchStatusUpdateControl(context, null, null).rescheduleAfter(delay);
    }

    private UpdateControl<FlinkBlueGreenDeployment> abortDeployment(
            BlueGreenTransitionContext context,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState,
            String deploymentName) {

        BlueGreenKubernetesUtils.suspendDeployment(context, nextDeployment);

        // We indicate this Blue/Green deployment is no longer Transitioning
        //  and rollback the state
        FlinkBlueGreenDeploymentState previousState =
                getPreviousState(nextState, context.getDeployments());
        context.getDeploymentStatus().setBlueGreenState(previousState);

        LOG.warn(
                "Aborting deployment '{}', rolling B/G deployment back to {}",
                deploymentName,
                previousState);

        // If the current running FlinkDeployment is not in RUNNING/STABLE,
        // we flag this Blue/Green as FAILING
        return patchStatusUpdateControl(context, null, JobStatus.FAILING);
    }

    private UpdateControl<FlinkBlueGreenDeployment> finalizeBlueGreenDeployment(
            BlueGreenTransitionContext context, FlinkBlueGreenDeploymentState nextState) {

        LOG.info("Finalizing deployment '{}' to {} state", context.getDeploymentName(), nextState);
        context.getDeploymentStatus().setDeploymentReadyTimestamp(millisToInstantStr(0));
        context.getDeploymentStatus().setAbortTimestamp(millisToInstantStr(0));
        return patchStatusUpdateControl(context, nextState, JobStatus.RUNNING);
    }

    private UpdateControl<FlinkBlueGreenDeployment> checkAndInitiateDeployment(
            BlueGreenTransitionContext context, DeploymentType currentDeploymentType) {

        if (BlueGreenSpecUtils.hasSpecChanged(context)) {

            // Ack the change in the spec (setLastReconciledSpec)
            BlueGreenSpecUtils.setLastReconciledSpec(context);

            FlinkDeployment currentFlinkDeployment =
                    context.getDeploymentByType(currentDeploymentType);

            if (isDeploymentReady(currentFlinkDeployment)) {

                DeploymentType nextDeploymentType = DeploymentType.BLUE;
                FlinkBlueGreenDeploymentState nextState =
                        FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
                FlinkResourceContext<FlinkDeployment> resourceContext =
                        context.getCtxFactory()
                                .getResourceContext(
                                        currentFlinkDeployment, context.getJosdkContext());

                // Updating status
                if (DeploymentType.BLUE == currentDeploymentType) {
                    nextState = FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
                    nextDeploymentType = DeploymentType.GREEN;
                }

                Savepoint lastCheckpoint = configureSavepoint(resourceContext);

                return initiateDeployment(
                        context, nextDeploymentType, nextState, lastCheckpoint, false);
            } else {
                // If the current running FlinkDeployment is not in RUNNING/STABLE,
                // we flag this Blue/Green as FAILING
                if (context.getDeploymentStatus().getJobStatus().getState() != JobStatus.FAILING) {
                    return patchStatusUpdateControl(context, null, JobStatus.FAILING);
                }
            }
        }

        return UpdateControl.noUpdate();
    }

    private UpdateControl<FlinkBlueGreenDeployment> initiateDeployment(
            BlueGreenTransitionContext context,
            DeploymentType nextDeploymentType,
            FlinkBlueGreenDeploymentState nextState,
            Savepoint lastCheckpoint,
            boolean isFirstDeployment) {

        BlueGreenKubernetesUtils.deployCluster(
                context, nextDeploymentType, lastCheckpoint, isFirstDeployment);

        BlueGreenUtils.setAbortTimestamp(context);

        return patchStatusUpdateControl(context, nextState, JobStatus.RECONCILING)
                .rescheduleAfter(BlueGreenUtils.getReconciliationReschedInterval(context));
    }

    // TODO: factor this method out to KubernetesUtils?
    public UpdateControl<FlinkBlueGreenDeployment> patchStatusUpdateControl(
            BlueGreenTransitionContext context,
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

        deploymentStatus.setLastReconciledTimestamp(Instant.now().toString());
        flinkBlueGreenDeployment.setStatus(deploymentStatus);
        return UpdateControl.patchStatus(flinkBlueGreenDeployment);
    }

    private static UpdateControl<FlinkBlueGreenDeployment> deleteDeployment(
            FlinkDeployment currentDeployment, BlueGreenTransitionContext context) {
        var deleted =
                BlueGreenKubernetesUtils.deleteKubernetesDeployment(currentDeployment, context);

        if (!deleted) {
            LOG.info("FlinkDeployment '{}' not deleted, will retry", currentDeployment);
        } else {
            LOG.info("FlinkDeployment '{}' deleted!", currentDeployment);
        }

        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(500);
    }
}
