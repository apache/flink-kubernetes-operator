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
import org.apache.flink.util.Preconditions;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenKubernetesUtils.deleteKubernetesDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenKubernetesUtils.deployCluster;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenKubernetesUtils.isDeploymentReady;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenKubernetesUtils.suspendDeployment;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.configureSavepoint;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.getPreviousState;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.hasSpecChanged;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.revertToLastSpec;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenSpecUtils.setLastReconciledSpec;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getDeploymentDeletionDelay;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getReconciliationReschedInterval;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.instantStrToMillis;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.millisToInstantStr;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.setAbortTimestamp;

/**
 * State machine handler for managing Blue/Green deployment state transitions. Encapsulates the
 * logic for processing different states in the Blue/Green deployment lifecycle.
 */
public class BlueGreenStateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenStateMachine.class);

    private final FlinkResourceContextFactory ctxFactory;

    public BlueGreenStateMachine(FlinkResourceContextFactory ctxFactory) {
        this.ctxFactory = ctxFactory;
    }

    /**
     * Processes the current state and returns the appropriate update control.
     *
     * @param bgDeployment the Blue/Green deployment resource
     * @param josdkContext the JOSDK context
     * @param deploymentStatus the current deployment status
     * @return UpdateControl indicating the next action
     */
    public UpdateControl<FlinkBlueGreenDeployment> processState(
            FlinkBlueGreenDeployment bgDeployment,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkBlueGreenDeploymentStatus deploymentStatus) {

        switch (deploymentStatus.getBlueGreenState()) {
            case INITIALIZING_BLUE:
                return checkFirstDeployment(bgDeployment, josdkContext, deploymentStatus);
            case ACTIVE_BLUE:
                return checkAndInitiateDeployment(
                        bgDeployment,
                        FlinkBlueGreenDeployments.fromSecondaryResources(josdkContext),
                        deploymentStatus,
                        DeploymentType.BLUE,
                        josdkContext);
            case ACTIVE_GREEN:
                return checkAndInitiateDeployment(
                        bgDeployment,
                        FlinkBlueGreenDeployments.fromSecondaryResources(josdkContext),
                        deploymentStatus,
                        DeploymentType.GREEN,
                        josdkContext);
            case TRANSITIONING_TO_BLUE:
                return monitorTransition(
                        bgDeployment,
                        FlinkBlueGreenDeployments.fromSecondaryResources(josdkContext),
                        deploymentStatus,
                        DeploymentType.GREEN,
                        josdkContext);
            case TRANSITIONING_TO_GREEN:
                return monitorTransition(
                        bgDeployment,
                        FlinkBlueGreenDeployments.fromSecondaryResources(josdkContext),
                        deploymentStatus,
                        DeploymentType.BLUE,
                        josdkContext);
            default:
                return UpdateControl.noUpdate();
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> checkFirstDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkBlueGreenDeploymentStatus deploymentStatus) {
        // We only allow a deployment if it's indeed the first (null last spec)
        // or if we're recovering (failing status) and the spec has changed
        if (deploymentStatus.getLastReconciledSpec() == null
                || (deploymentStatus.getJobStatus().getState().equals(JobStatus.FAILING)
                        && hasSpecChanged(bgDeployment.getSpec(), deploymentStatus))) {
            // Ack the change in the spec (setLastReconciledSpec)
            setLastReconciledSpec(bgDeployment, deploymentStatus);
            return initiateDeployment(
                    bgDeployment,
                    deploymentStatus,
                    DeploymentType.BLUE,
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                    null,
                    josdkContext,
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
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeployments deployments,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType currentDeploymentType,
            Context<FlinkBlueGreenDeployment> josdkContext) {

        if (hasSpecChanged(bgDeployment.getSpec(), deploymentStatus)) {
            // this means the spec was changed during transition,
            //  ignore the new change, revert the spec and log as warning
            revertToLastSpec(bgDeployment, deploymentStatus, josdkContext);
            LOG.warn(
                    "Blue/Green Spec change detected during transition, ignored and reverted to the last reconciled spec");
        }

        var nextState = FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
        FlinkDeployment currentDeployment;
        FlinkDeployment nextDeployment;

        if (DeploymentType.BLUE == currentDeploymentType) {
            nextState = FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
            currentDeployment = deployments.getFlinkDeploymentBlue();
            nextDeployment = deployments.getFlinkDeploymentGreen();
        } else {
            currentDeployment = deployments.getFlinkDeploymentGreen();
            nextDeployment = deployments.getFlinkDeploymentBlue();
        }

        Preconditions.checkNotNull(
                nextDeployment,
                "Target Dependent Deployment resource not found. Blue/Green deployment name: "
                        + bgDeployment.getMetadata().getName()
                        + ", current deployment type: "
                        + currentDeploymentType);

        if (isDeploymentReady(nextDeployment)) {
            return shouldWeDelete(
                    bgDeployment,
                    deploymentStatus,
                    josdkContext,
                    currentDeployment,
                    nextDeployment,
                    nextState);
        } else {
            return shouldWeAbort(
                    bgDeployment,
                    deploymentStatus,
                    josdkContext,
                    nextDeployment,
                    nextState,
                    deployments);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> shouldWeDelete(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkDeployment currentDeployment,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState) {
        // currentDeployment will be null in case:
        //  - of first time deployments
        //  - the previous deployment has been successfully deleted
        // therefore, finalize right away
        if (currentDeployment == null) {
            deploymentStatus.setDeploymentReadyTimestamp(Instant.now().toString());
            return finalizeBlueGreenDeployment(bgDeployment, deploymentStatus, nextState);
        }

        long deploymentDeletionDelayMs = getDeploymentDeletionDelay(bgDeployment);

        long deploymentReadyTimestamp =
                instantStrToMillis(deploymentStatus.getDeploymentReadyTimestamp());

        if (deploymentReadyTimestamp == 0) {
            LOG.info(
                    "FlinkDeployment '{}' marked ready, rescheduling reconciliation in {} seconds.",
                    nextDeployment.getMetadata().getName(),
                    deploymentDeletionDelayMs / 1000);
            deploymentStatus.setDeploymentReadyTimestamp(Instant.now().toString());
            return patchStatusUpdateControl(bgDeployment, deploymentStatus, null, null)
                    .rescheduleAfter(deploymentDeletionDelayMs);
        }

        var deletionTs = deploymentReadyTimestamp + deploymentDeletionDelayMs;

        if (deletionTs < System.currentTimeMillis()) {
            return deleteDeployment(currentDeployment, josdkContext);
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
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState,
            FlinkBlueGreenDeployments deployments) {

        String deploymentName = nextDeployment.getMetadata().getName();
        long abortTimestamp = instantStrToMillis(deploymentStatus.getAbortTimestamp());

        if (abortTimestamp == 0) {
            throw new IllegalStateException("Unexpected abortTimestamp == 0");
        }

        if (abortTimestamp < System.currentTimeMillis()) {
            return abortDeployment(
                    bgDeployment,
                    deploymentStatus,
                    josdkContext,
                    nextDeployment,
                    nextState,
                    deployments,
                    deploymentName);
        } else {
            return retryDeployment(bgDeployment, deploymentStatus, abortTimestamp, deploymentName);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> retryDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            long abortTimestamp,
            String deploymentName) {
        var delay = abortTimestamp - System.currentTimeMillis();
        LOG.info(
                "FlinkDeployment '{}' not ready yet, retrying in {} seconds.",
                deploymentName,
                delay / 1000);
        return patchStatusUpdateControl(bgDeployment, deploymentStatus, null, null)
                .rescheduleAfter(delay);
    }

    private UpdateControl<FlinkBlueGreenDeployment> abortDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkDeployment nextDeployment,
            FlinkBlueGreenDeploymentState nextState,
            FlinkBlueGreenDeployments deployments,
            String deploymentName) {
        suspendDeployment(josdkContext, nextDeployment);

        // We indicate this Blue/Green deployment is no longer Transitioning
        //  and rollback the state
        FlinkBlueGreenDeploymentState previousState = getPreviousState(nextState, deployments);
        deploymentStatus.setBlueGreenState(previousState);

        LOG.warn(
                "Aborting deployment '{}', rolling B/G deployment back to {}",
                deploymentName,
                previousState);

        // If the current running FlinkDeployment is not in RUNNING/STABLE,
        // we flag this Blue/Green as FAILING
        return patchStatusUpdateControl(bgDeployment, deploymentStatus, null, JobStatus.FAILING);
    }

    private UpdateControl<FlinkBlueGreenDeployment> finalizeBlueGreenDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            FlinkBlueGreenDeploymentState nextState) {

        LOG.info(
                "Finalizing deployment '{}' to {} state",
                bgDeployment.getMetadata().getName(),
                nextState);
        deploymentStatus.setDeploymentReadyTimestamp(millisToInstantStr(0));
        deploymentStatus.setAbortTimestamp(millisToInstantStr(0));
        return patchStatusUpdateControl(
                bgDeployment, deploymentStatus, nextState, JobStatus.RUNNING);
    }

    private UpdateControl<FlinkBlueGreenDeployment> checkAndInitiateDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeployments deployments,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType currentDeploymentType,
            Context<FlinkBlueGreenDeployment> josdkContext) {

        if (hasSpecChanged(bgDeployment.getSpec(), deploymentStatus)) {

            // Ack the change in the spec (setLastReconciledSpec)
            setLastReconciledSpec(bgDeployment, deploymentStatus);

            FlinkDeployment currentFlinkDeployment =
                    DeploymentType.BLUE == currentDeploymentType
                            ? deployments.getFlinkDeploymentBlue()
                            : deployments.getFlinkDeploymentGreen();

            if (isDeploymentReady(currentFlinkDeployment)) {

                DeploymentType nextDeploymentType = DeploymentType.BLUE;
                FlinkBlueGreenDeploymentState nextState =
                        FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
                FlinkResourceContext<FlinkDeployment> resourceContext =
                        ctxFactory.getResourceContext(currentFlinkDeployment, josdkContext);

                // Updating status
                if (DeploymentType.BLUE == currentDeploymentType) {
                    nextState = FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
                    nextDeploymentType = DeploymentType.GREEN;
                }

                Savepoint lastCheckpoint = configureSavepoint(resourceContext);

                return initiateDeployment(
                        bgDeployment,
                        deploymentStatus,
                        nextDeploymentType,
                        nextState,
                        lastCheckpoint,
                        josdkContext,
                        false);
            } else {
                // If the current running FlinkDeployment is not in RUNNING/STABLE,
                // we flag this Blue/Green as FAILING
                if (deploymentStatus.getJobStatus().getState() != JobStatus.FAILING) {
                    return patchStatusUpdateControl(
                            bgDeployment, deploymentStatus, null, JobStatus.FAILING);
                }
            }
        }

        return UpdateControl.noUpdate();
    }

    private UpdateControl<FlinkBlueGreenDeployment> initiateDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType nextDeploymentType,
            FlinkBlueGreenDeploymentState nextState,
            Savepoint lastCheckpoint,
            Context<FlinkBlueGreenDeployment> josdkContext,
            boolean isFirstDeployment) {

        deployCluster(
                bgDeployment, nextDeploymentType, lastCheckpoint, josdkContext, isFirstDeployment);

        setAbortTimestamp(bgDeployment, deploymentStatus);

        return patchStatusUpdateControl(
                        bgDeployment, deploymentStatus, nextState, JobStatus.RECONCILING)
                .rescheduleAfter(getReconciliationReschedInterval(bgDeployment));
    }

    // TODO: factor this method out to KubernetesUtils?
    public UpdateControl<FlinkBlueGreenDeployment> patchStatusUpdateControl(
            FlinkBlueGreenDeployment flinkBlueGreenDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            FlinkBlueGreenDeploymentState deploymentState,
            JobStatus jobState) {
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
            FlinkDeployment currentDeployment, Context<FlinkBlueGreenDeployment> josdkContext) {
        var deleted = deleteKubernetesDeployment(currentDeployment, josdkContext);

        if (!deleted) {
            LOG.info("FlinkDeployment '{}' not deleted, will retry", currentDeployment);
        } else {
            LOG.info("FlinkDeployment '{}' deleted!", currentDeployment);
        }

        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(500);
    }
}
