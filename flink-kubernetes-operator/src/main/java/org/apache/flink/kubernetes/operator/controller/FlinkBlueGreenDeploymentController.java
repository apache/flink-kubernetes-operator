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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.ABORT_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.RECONCILIATION_RESCHEDULING_INTERVAL;
import static org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeploymentUtils.getConfigOption;
import static org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeploymentUtils.instantStrToMillis;
import static org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeploymentUtils.millisToInstantStr;

/** Controller that runs the main reconcile loop for Flink Blue/Green deployments. */
@ControllerConfiguration
public class FlinkBlueGreenDeploymentController implements Reconciler<FlinkBlueGreenDeployment> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkDeploymentController.class);

    private final FlinkResourceContextFactory ctxFactory;

    public static long minimumAbortGracePeriodMs = ABORT_GRACE_PERIOD.defaultValue().toMillis();

    public FlinkBlueGreenDeploymentController(FlinkResourceContextFactory ctxFactory) {
        this.ctxFactory = ctxFactory;
    }

    @Override
    public List<EventSource<?, FlinkBlueGreenDeployment>> prepareEventSources(
            EventSourceContext<FlinkBlueGreenDeployment> context) {
        List<EventSource<?, FlinkBlueGreenDeployment>> eventSources = new ArrayList<>();

        InformerEventSourceConfiguration<FlinkDeployment> config =
                InformerEventSourceConfiguration.from(
                                FlinkDeployment.class, FlinkBlueGreenDeployment.class)
                        .withSecondaryToPrimaryMapper(
                                Mappers.fromOwnerReferences(context.getPrimaryResourceClass()))
                        .withNamespacesInheritedFromController()
                        .withFollowControllerNamespacesChanges(true)
                        .build();

        eventSources.add(new InformerEventSource<>(config, context));

        return eventSources;
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> reconcile(
            FlinkBlueGreenDeployment bgDeployment, Context<FlinkBlueGreenDeployment> josdkContext)
            throws Exception {

        FlinkBlueGreenDeploymentStatus deploymentStatus = bgDeployment.getStatus();

        if (deploymentStatus == null) {
            deploymentStatus = new FlinkBlueGreenDeploymentStatus();
            return patchStatusUpdateControl(
                            bgDeployment,
                            deploymentStatus,
                            FlinkBlueGreenDeploymentState.INITIALIZING_BLUE,
                            null)
                    .rescheduleAfter(100);
        } else {
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
    }

    private UpdateControl<FlinkBlueGreenDeployment> checkFirstDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkBlueGreenDeploymentStatus deploymentStatus) {
        if (deploymentStatus.getLastReconciledSpec() == null
                || hasSpecChanged(bgDeployment.getSpec(), deploymentStatus)) {
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
            return UpdateControl.noUpdate();
        }
    }

    private static void setAbortTimestamp(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus) {
        deploymentStatus.setAbortTimestamp(
                millisToInstantStr(System.currentTimeMillis() + getAbortGracePeriod(bgDeployment)));
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
            bgDeployment.setSpec(
                    SpecUtils.readSpecFromJSON(
                            deploymentStatus.getLastReconciledSpec(),
                            "spec",
                            FlinkBlueGreenDeploymentSpec.class));
            josdkContext.getClient().resource(bgDeployment).replace();
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
            return canDelete(
                    bgDeployment,
                    deploymentStatus,
                    josdkContext,
                    currentDeployment,
                    nextDeployment,
                    nextState);
        } else {
            return shouldAbort(
                    bgDeployment,
                    deploymentStatus,
                    josdkContext,
                    nextDeployment,
                    nextState,
                    deployments);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> canDelete(
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
            long delay = deletionTs - System.currentTimeMillis();
            LOG.info(
                    "Awaiting deletion delay for FlinkDeployment '{}', rescheduling reconciliation in {} seconds.",
                    currentDeployment.getMetadata().getName(),
                    delay / 1000);
            return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(delay);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> shouldAbort(
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
            // ABORT
            // Suspend the nextDeployment (FlinkDeployment)
            nextDeployment.getSpec().getJob().setState(JobState.SUSPENDED);
            josdkContext.getClient().resource(nextDeployment).update();

            // We indicate this Blue/Green deployment is no longer Transitioning
            //  and rollback the state value
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

            deploymentStatus.setBlueGreenState(previousState);

            LOG.warn(
                    "Aborting deployment '{}', rolling B/G deployment back to {}",
                    deploymentName,
                    previousState);

            // If the current running FlinkDeployment is not in RUNNING/STABLE,
            // we flag this Blue/Green as FAILING
            return patchStatusUpdateControl(
                    bgDeployment, deploymentStatus, null, JobStatus.FAILING);
        } else {
            // RETRY
            var delay = abortTimestamp - System.currentTimeMillis();
            LOG.info(
                    "FlinkDeployment '{}' not ready yet, retrying in {} seconds.",
                    deploymentName,
                    delay / 1000);
            return patchStatusUpdateControl(bgDeployment, deploymentStatus, null, null)
                    .rescheduleAfter(delay);
        }
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

    private static void setLastReconciledSpec(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus) {
        deploymentStatus.setLastReconciledSpec(
                SpecUtils.writeSpecAsJSON(bgDeployment.getSpec(), "spec"));
        deploymentStatus.setLastReconciledTimestamp(Instant.now().toString());
    }

    private static Savepoint configureSavepoint(
            FlinkResourceContext<FlinkDeployment> resourceContext) {
        // TODO: if the user specified an initialSavepointPath, use it and skip this?
        Optional<Savepoint> lastCheckpoint =
                resourceContext
                        .getFlinkService()
                        .getLastCheckpoint(
                                JobID.fromHexString(
                                        resourceContext
                                                .getResource()
                                                .getStatus()
                                                .getJobStatus()
                                                .getJobId()),
                                resourceContext.getObserveConfig());

        // Alternative action if no checkpoint is available?
        if (lastCheckpoint.isEmpty()) {
            throw new IllegalStateException(
                    "Last Checkpoint for Job "
                            + resourceContext.getResource().getMetadata().getName()
                            + " not found!");
        }
        return lastCheckpoint.get();
    }

    private UpdateControl<FlinkBlueGreenDeployment> initiateDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType nextDeploymentType,
            FlinkBlueGreenDeploymentState nextState,
            Savepoint lastCheckpoint,
            Context<FlinkBlueGreenDeployment> josdkContext,
            boolean isFirstDeployment) {

        deploy(bgDeployment, nextDeploymentType, lastCheckpoint, josdkContext, isFirstDeployment);

        setAbortTimestamp(bgDeployment, deploymentStatus);

        return patchStatusUpdateControl(
                        bgDeployment, deploymentStatus, nextState, JobStatus.RECONCILING)
                .rescheduleAfter(getReconciliationReschedInterval(bgDeployment));
    }

    private boolean isDeploymentReady(FlinkDeployment deployment) {
        return ResourceLifecycleState.STABLE == deployment.getStatus().getLifecycleState()
                && JobStatus.RUNNING == deployment.getStatus().getJobStatus().getState();
    }

    private boolean hasSpecChanged(
            FlinkBlueGreenDeploymentSpec newSpec, FlinkBlueGreenDeploymentStatus deploymentStatus) {

        String lastReconciledSpec = deploymentStatus.getLastReconciledSpec();
        String newSpecSerialized = SpecUtils.writeSpecAsJSON(newSpec, "spec");

        return !lastReconciledSpec.equals(newSpecSerialized);
    }

    private UpdateControl<FlinkBlueGreenDeployment> patchStatusUpdateControl(
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

    private void deploy(
            FlinkBlueGreenDeployment bgDeployment,
            DeploymentType deploymentType,
            Savepoint lastCheckpoint,
            Context<FlinkBlueGreenDeployment> josdkContext,
            boolean isFirstDeployment) {
        ObjectMeta bgMeta = bgDeployment.getMetadata();

        // Deployment
        FlinkDeployment flinkDeployment = new FlinkDeployment();
        FlinkBlueGreenDeploymentSpec spec = bgDeployment.getSpec();

        String childDeploymentName =
                bgMeta.getName() + "-" + deploymentType.toString().toLowerCase();

        FlinkBlueGreenDeploymentSpec adjustedSpec =
                FlinkBlueGreenDeploymentUtils.adjustNameReferences(
                        spec,
                        bgMeta.getName(),
                        childDeploymentName,
                        "spec",
                        FlinkBlueGreenDeploymentSpec.class);

        // The B/G initialSavepointPath is only used in first time deployments
        if (isFirstDeployment) {
            String initialSavepointPath =
                    adjustedSpec.getTemplate().getSpec().getJob().getInitialSavepointPath();
            if (initialSavepointPath != null && !initialSavepointPath.isEmpty()) {
                LOG.info("Using initialSavepointPath: " + initialSavepointPath);
                adjustedSpec
                        .getTemplate()
                        .getSpec()
                        .getJob()
                        .setInitialSavepointPath(initialSavepointPath);
            } else {
                LOG.info("Clean start up, no checkpoint/savepoint");
            }
        } else if (lastCheckpoint != null) {
            String location = lastCheckpoint.getLocation().replace("file:", "");
            LOG.info("Using B/G checkpoint: " + location);
            adjustedSpec.getTemplate().getSpec().getJob().setInitialSavepointPath(location);
        }

        flinkDeployment.setSpec(adjustedSpec.getTemplate().getSpec());

        // Deployment metadata
        ObjectMeta flinkDeploymentMeta =
                FlinkBlueGreenDeploymentUtils.getDependentObjectMeta(bgDeployment);
        flinkDeploymentMeta.setName(childDeploymentName);
        flinkDeploymentMeta.setLabels(Map.of(DeploymentType.LABEL_KEY, deploymentType.toString()));
        flinkDeployment.setMetadata(flinkDeploymentMeta);

        // Deploy
        josdkContext.getClient().resource(flinkDeployment).createOrReplace();
    }

    private static UpdateControl<FlinkBlueGreenDeployment> deleteDeployment(
            FlinkDeployment currentDeployment, Context<FlinkBlueGreenDeployment> josdkContext) {
        String deploymentName = currentDeployment.getMetadata().getName();
        List<StatusDetails> deletedStatus =
                josdkContext
                        .getClient()
                        .resources(FlinkDeployment.class)
                        .inNamespace(currentDeployment.getMetadata().getNamespace())
                        .withName(deploymentName)
                        .delete();

        boolean deleted =
                deletedStatus.size() == 1
                        && deletedStatus.get(0).getKind().equals("FlinkDeployment");

        if (!deleted) {
            LOG.info("FlinkDeployment '{}' not deleted, will retry", deploymentName);
        } else {
            LOG.info("Deployment '{}' deleted!", deploymentName);
        }

        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(500);
    }

    private long getReconciliationReschedInterval(FlinkBlueGreenDeployment bgDeployment) {
        return Math.max(
                getConfigOption(bgDeployment, RECONCILIATION_RESCHEDULING_INTERVAL).toMillis(), 0);
    }

    private long getDeploymentDeletionDelay(FlinkBlueGreenDeployment bgDeployment) {
        return Math.max(getConfigOption(bgDeployment, DEPLOYMENT_DELETION_DELAY).toMillis(), 0);
    }

    private static long getAbortGracePeriod(FlinkBlueGreenDeployment bgDeployment) {
        long abortGracePeriod = getConfigOption(bgDeployment, ABORT_GRACE_PERIOD).toMillis();
        return Math.max(abortGracePeriod, minimumAbortGracePeriodMs);
    }

    public static void logAndThrow(String message) {
        throw new RuntimeException(message);
    }
}
