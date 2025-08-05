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
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils;
import org.apache.flink.util.Preconditions;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.isFlinkDeploymentReady;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.configureSavepoint;
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

        BlueGreenKubernetesService.deployCluster(
                context, nextDeploymentType, lastCheckpoint, isFirstDeployment);

        BlueGreenUtils.setAbortTimestamp(context);

        return patchStatusUpdateControl(context, nextState, JobStatus.RECONCILING)
                .rescheduleAfter(BlueGreenUtils.getReconciliationReschedInterval(context));
    }

    /**
     * Checks if a deployment can be initiated and initiates it if conditions are met.
     *
     * @param context the transition context
     * @param currentDeploymentType the current deployment type
     * @return UpdateControl for the deployment
     */
    public UpdateControl<FlinkBlueGreenDeployment> checkAndInitiateDeployment(
            BlueGreenContext context, DeploymentType currentDeploymentType) {

        if (BlueGreenSpecUtils.hasSpecChanged(context)) {
            BlueGreenSpecUtils.setLastReconciledSpec(context);

            FlinkDeployment currentFlinkDeployment =
                    context.getDeploymentByType(currentDeploymentType);

            if (BlueGreenKubernetesService.isFlinkDeploymentReady(currentFlinkDeployment)) {
                DeploymentTransition transition = calculateTransition(currentDeploymentType);

                FlinkResourceContext<FlinkDeployment> resourceContext =
                        context.getCtxFactory()
                                .getResourceContext(
                                        currentFlinkDeployment, context.getJosdkContext());

                Savepoint lastCheckpoint = configureSavepoint(resourceContext);

                return initiateDeployment(
                        context,
                        transition.nextDeploymentType,
                        transition.nextState,
                        lastCheckpoint,
                        false);
            } else {
                if (context.getDeploymentStatus().getJobStatus().getState()
                        != JobStatus.FAILING) {
                    return patchStatusUpdateControl(context, null, JobStatus.FAILING);
                }
            }
        }

        return UpdateControl.noUpdate();
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
            transitionState = new TransitionState(
                    context.getBlueDeployment(),                    // currentDeployment
                    context.getGreenDeployment(),                   // nextDeployment
                    FlinkBlueGreenDeploymentState.ACTIVE_GREEN);    // next State
        } else {
            transitionState = new TransitionState(
                    context.getGreenDeployment(),               // currentDeployment
                    context.getBlueDeployment(),                // nextDeployment
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

        boolean deleted =
                BlueGreenKubernetesService.deleteFlinkDeployment(currentDeployment, context);

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
        long abortTimestamp =
                instantStrToMillis(context.getDeploymentStatus().getAbortTimestamp());

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
            BlueGreenContext context, long abortTimestamp, String deploymentName) {

        long delay = abortTimestamp - System.currentTimeMillis();
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

        BlueGreenKubernetesService.suspendFlinkDeployment(context, nextDeployment);

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

        LOG.info(
                "Finalizing deployment '{}' to {} state",
                context.getDeploymentName(),
                nextState);

        context.getDeploymentStatus().setDeploymentReadyTimestamp(millisToInstantStr(0));
        context.getDeploymentStatus().setAbortTimestamp(millisToInstantStr(0));

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

    // ==================== Inner Classes ====================

    private static class DeploymentTransition {
        final DeploymentType nextDeploymentType;
        final FlinkBlueGreenDeploymentState nextState;

        DeploymentTransition(
                DeploymentType nextDeploymentType, FlinkBlueGreenDeploymentState nextState) {
            this.nextDeploymentType = nextDeploymentType;
            this.nextState = nextState;
        }
    }

    private static class TransitionState {
        final FlinkDeployment currentDeployment;
        final FlinkDeployment nextDeployment;
        final FlinkBlueGreenDeploymentState nextState;

        TransitionState(
                FlinkDeployment currentDeployment,
                FlinkDeployment nextDeployment,
                FlinkBlueGreenDeploymentState nextState) {
            this.currentDeployment = currentDeployment;
            this.nextDeployment = nextDeployment;
            this.nextState = nextState;
        }
    }
}