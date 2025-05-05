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
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionMode;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.OperationNotSupportedException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Controller that runs the main reconcile loop for Flink Blue/Green deployments. */
@ControllerConfiguration
public class FlinkBlueGreenDeploymentController
        implements Reconciler<FlinkBlueGreenDeployment>,
                EventSourceInitializer<FlinkBlueGreenDeployment> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkDeploymentController.class);
    private static final int DEFAULT_RECONCILIATION_RESCHEDULING_INTERVAL_MS = 15000; // 15 secs

    private final FlinkResourceContextFactory ctxFactory;

    public static int minimumAbortGracePeriodMs = 120000; // 2 mins

    public FlinkBlueGreenDeploymentController(FlinkResourceContextFactory ctxFactory) {
        this.ctxFactory = ctxFactory;
    }

    @Override
    public Map<String, EventSource> prepareEventSources(
            EventSourceContext<FlinkBlueGreenDeployment> eventSourceContext) {
        InformerConfiguration<FlinkDeployment> flinkDeploymentInformerConfig =
                InformerConfiguration.from(FlinkDeployment.class, eventSourceContext)
                        .withSecondaryToPrimaryMapper(Mappers.fromOwnerReference())
                        .withNamespacesInheritedFromController(eventSourceContext)
                        .followNamespaceChanges(true)
                        .build();

        return EventSourceInitializer.nameEventSources(
                new InformerEventSource<>(flinkDeploymentInformerConfig, eventSourceContext));
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> reconcile(
            FlinkBlueGreenDeployment bgDeployment, Context<FlinkBlueGreenDeployment> josdkContext)
            throws Exception {

        // TODO: this verification is only for FLIP-503, remove later.
        if (bgDeployment.getSpec().getTemplate().getTransitionMode() != TransitionMode.BASIC) {
            throw new OperationNotSupportedException(
                    "Only TransitionMode == BASIC is currently supported");
        }

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
            FlinkBlueGreenDeploymentStatus deploymentStatus)
            throws JsonProcessingException {
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
        int abortGracePeriod = bgDeployment.getSpec().getTemplate().getAbortGracePeriodMs();
        abortGracePeriod = Math.max(abortGracePeriod, minimumAbortGracePeriodMs);
        deploymentStatus.setAbortTimestamp(System.currentTimeMillis() + abortGracePeriod);
    }

    private UpdateControl<FlinkBlueGreenDeployment> monitorTransition(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeployments deployments,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType currentDeploymentType,
            Context<FlinkBlueGreenDeployment> josdkContext)
            throws JsonProcessingException {

        if (hasSpecChanged(bgDeployment.getSpec(), deploymentStatus)) {
            // this means the spec was changed during transition,
            //  ignore the new change, revert the spec and log as warning
            bgDeployment.setSpec(
                    SpecUtils.deserializeObject(
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

        if (isDeploymentReady(nextDeployment, josdkContext, deploymentStatus)) {
            return canDelete(
                    bgDeployment, deploymentStatus, josdkContext, currentDeployment, nextState);
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
            FlinkBlueGreenDeploymentState nextState) {
        int deploymentDeletionDelayMs =
                Math.max(bgDeployment.getSpec().getTemplate().getDeploymentDeletionDelayMs(), 0);

        if (deploymentStatus.getDeploymentReadyTimestamp() == 0) {
            LOG.info(
                    "Deployment marked ready on "
                            + System.currentTimeMillis()
                            + ", rescheduling reconciliation in "
                            + deploymentDeletionDelayMs
                            + " ms.");
            deploymentStatus.setDeploymentReadyTimestamp(System.currentTimeMillis());
            return patchStatusUpdateControl(bgDeployment, deploymentStatus, null, null)
                    .rescheduleAfter(deploymentDeletionDelayMs);
        }

        var deletionTs = deploymentStatus.getDeploymentReadyTimestamp() + deploymentDeletionDelayMs;

        if (deletionTs < System.currentTimeMillis()) {
            return deleteAndFinalize(
                    bgDeployment, deploymentStatus, josdkContext, currentDeployment, nextState);
        } else {
            long delay = deletionTs - System.currentTimeMillis();
            LOG.info("Rescheduling reconciliation (to delete) in " + delay + " ms.");
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
        long abortTimestamp = deploymentStatus.getAbortTimestamp();

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
                    "Aborting deployment '"
                            + deploymentName
                            + "', rolling B/G deployment back to "
                            + previousState);

            // If the current running FlinkDeployment is not in RUNNING/STABLE,
            // we flag this Blue/Green as FAILING
            return patchStatusUpdateControl(
                    bgDeployment, deploymentStatus, null, JobStatus.FAILING);
        } else {
            // RETRY
            var delay = abortTimestamp - System.currentTimeMillis();
            LOG.info(
                    "Deployment '"
                            + deploymentName
                            + "' not ready yet, retrying in "
                            + delay
                            + " ms");
            return patchStatusUpdateControl(bgDeployment, deploymentStatus, null, null)
                    .rescheduleAfter(delay);
        }
    }

    private static int getReconciliationReschedInterval(FlinkBlueGreenDeployment bgDeployment) {
        int reconciliationReschedInterval =
                bgDeployment.getSpec().getTemplate().getReconciliationReschedulingIntervalMs();
        if (reconciliationReschedInterval <= 0) {
            reconciliationReschedInterval = DEFAULT_RECONCILIATION_RESCHEDULING_INTERVAL_MS;
        }
        return reconciliationReschedInterval;
    }

    private UpdateControl<FlinkBlueGreenDeployment> deleteAndFinalize(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkDeployment currentDeployment,
            FlinkBlueGreenDeploymentState nextState) {

        if (currentDeployment != null) {
            deleteDeployment(currentDeployment, josdkContext);
            return UpdateControl.<FlinkBlueGreenDeployment>noUpdate().rescheduleAfter(500);
        } else {
            LOG.info(
                    "Finalizing deployment '"
                            + bgDeployment.getMetadata().getName()
                            + "' to "
                            + nextState
                            + " state");
            deploymentStatus.setDeploymentReadyTimestamp(0);
            deploymentStatus.setAbortTimestamp(0);
            return patchStatusUpdateControl(
                    bgDeployment, deploymentStatus, nextState, JobStatus.RUNNING);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> checkAndInitiateDeployment(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeployments deployments,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType currentDeploymentType,
            Context<FlinkBlueGreenDeployment> josdkContext)
            throws Exception {

        if (hasSpecChanged(bgDeployment.getSpec(), deploymentStatus)) {

            // Ack the change in the spec (setLastReconciledSpec)
            setLastReconciledSpec(bgDeployment, deploymentStatus);

            FlinkDeployment currentFlinkDeployment =
                    DeploymentType.BLUE == currentDeploymentType
                            ? deployments.getFlinkDeploymentBlue()
                            : deployments.getFlinkDeploymentGreen();

            if (isDeploymentReady(currentFlinkDeployment, josdkContext, deploymentStatus)) {

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
                SpecUtils.serializeObject(bgDeployment.getSpec(), "spec"));
        deploymentStatus.setLastReconciledTimestamp(System.currentTimeMillis());
    }

    public void logPotentialWarnings(
            FlinkDeployment flinkDeployment,
            Context<FlinkBlueGreenDeployment> josdkContext,
            long lastReconciliationTimestamp) {
        // Event reason constants
        // https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/events/event.go
        Set<String> badEventPatterns =
                ImmutableSet.of(
                        "FAIL", "EXCEPTION", "BACKOFF", "ERROR", "EVICTION", "KILL", "EXCEED");
        Set<String> goodPodPhases = ImmutableSet.of("PENDING", "RUNNING");

        Set<String> podPhases =
                getDeploymentPods(josdkContext, flinkDeployment)
                        .map(p -> p.get().getStatus().getPhase().toUpperCase())
                        .collect(Collectors.toSet());

        podPhases.removeAll(goodPodPhases);

        if (!podPhases.isEmpty()) {
            LOG.warn("Deployment not healthy, some Pods have the following status: " + podPhases);
        }

        List<Event> abnormalEvents =
                josdkContext
                        .getClient()
                        .v1()
                        .events()
                        .inNamespace(flinkDeployment.getMetadata().getNamespace())
                        .resources()
                        .map(Resource::item)
                        .filter(e -> !e.getType().equalsIgnoreCase("NORMAL"))
                        .filter(
                                e ->
                                        e.getInvolvedObject()
                                                .getName()
                                                .contains(flinkDeployment.getMetadata().getName()))
                        .filter(
                                e ->
                                        Instant.parse(e.getLastTimestamp()).toEpochMilli()
                                                > lastReconciliationTimestamp)
                        .filter(
                                e ->
                                        badEventPatterns.stream()
                                                .anyMatch(
                                                        p ->
                                                                e.getReason()
                                                                        .toUpperCase()
                                                                        .contains(p)))
                        .collect(Collectors.toList());

        if (!abnormalEvents.isEmpty()) {
            LOG.warn("Abnormal events detected: " + abnormalEvents);
        }
    }

    private static Savepoint configureSavepoint(
            FlinkResourceContext<FlinkDeployment> resourceContext) throws Exception {
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
            boolean isFirstDeployment)
            throws JsonProcessingException {

        deploy(bgDeployment, nextDeploymentType, lastCheckpoint, josdkContext, isFirstDeployment);

        setAbortTimestamp(bgDeployment, deploymentStatus);

        return patchStatusUpdateControl(
                        bgDeployment, deploymentStatus, nextState, JobStatus.RECONCILING)
                .rescheduleAfter(getReconciliationReschedInterval(bgDeployment));
    }

    private boolean isDeploymentReady(
            FlinkDeployment deployment,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkBlueGreenDeploymentStatus deploymentStatus) {
        if (ResourceLifecycleState.STABLE == deployment.getStatus().getLifecycleState()
                && JobStatus.RUNNING == deployment.getStatus().getJobStatus().getState()) {
            // TODO: checking for running pods seems to be redundant, check if this can be removed
            int notRunningPods =
                    (int)
                            getDeploymentPods(josdkContext, deployment)
                                    .filter(
                                            p ->
                                                    !p.get()
                                                            .getStatus()
                                                            .getPhase()
                                                            .equalsIgnoreCase("RUNNING"))
                                    .count();

            if (notRunningPods > 0) {
                LOG.warn("Waiting for " + notRunningPods + " Pods to transition to RUNNING status");
            }

            return notRunningPods == 0;
        }

        logPotentialWarnings(
                deployment, josdkContext, deploymentStatus.getLastReconciledTimestamp());
        return false;
    }

    private static Stream<PodResource> getDeploymentPods(
            Context<FlinkBlueGreenDeployment> josdkContext, FlinkDeployment deployment) {
        var namespace = deployment.getMetadata().getNamespace();
        var deploymentName = deployment.getMetadata().getName();

        return josdkContext
                .getClient()
                .pods()
                .inNamespace(namespace)
                .withLabel("app", deploymentName)
                .resources();
    }

    private boolean hasSpecChanged(
            FlinkBlueGreenDeploymentSpec newSpec, FlinkBlueGreenDeploymentStatus deploymentStatus) {

        String lastReconciledSpec = deploymentStatus.getLastReconciledSpec();
        String newSpecSerialized = SpecUtils.serializeObject(newSpec, "spec");

        // TODO: in FLIP-504 check here the TransitionMode has not been changed

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

        deploymentStatus.setLastReconciledTimestamp(System.currentTimeMillis());
        flinkBlueGreenDeployment.setStatus(deploymentStatus);
        return UpdateControl.patchStatus(flinkBlueGreenDeployment);
    }

    private void deploy(
            FlinkBlueGreenDeployment bgDeployment,
            DeploymentType deploymentType,
            Savepoint lastCheckpoint,
            Context<FlinkBlueGreenDeployment> josdkContext,
            boolean isFirstDeployment)
            throws JsonProcessingException {
        ObjectMeta bgMeta = bgDeployment.getMetadata();

        // Deployment
        FlinkDeployment flinkDeployment = new FlinkDeployment();
        flinkDeployment.setApiVersion("flink.apache.org/v1beta1");
        flinkDeployment.setKind("FlinkDeployment");
        FlinkBlueGreenDeploymentSpec spec = bgDeployment.getSpec();

        String childDeploymentName =
                bgMeta.getName() + "-" + deploymentType.toString().toLowerCase();

        FlinkBlueGreenDeploymentSpec adjustedSpec =
                adjustNameReferences(
                        spec,
                        bgMeta.getName(),
                        childDeploymentName,
                        "spec",
                        FlinkBlueGreenDeploymentSpec.class);

        if (lastCheckpoint != null) {
            String location = lastCheckpoint.getLocation().replace("file:", "");
            LOG.info("Using checkpoint: " + location);
            adjustedSpec.getTemplate().getSpec().getJob().setInitialSavepointPath(location);
        }

        flinkDeployment.setSpec(adjustedSpec.getTemplate().getSpec());

        // Deployment metadata
        ObjectMeta flinkDeploymentMeta = getDependentObjectMeta(bgDeployment);
        flinkDeploymentMeta.setName(childDeploymentName);
        flinkDeploymentMeta.setLabels(
                Map.of(deploymentType.getClass().getSimpleName(), deploymentType.toString()));
        flinkDeployment.setMetadata(flinkDeploymentMeta);

        // Deploy
        josdkContext.getClient().resource(flinkDeployment).createOrReplace();
    }

    private static void deleteDeployment(
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
            LOG.info("Deployment '" + deploymentName + "' not deleted, will retry");
        } else {
            LOG.info("Deployment '" + deploymentName + "' deleted!");
        }
    }

    private ObjectMeta getDependentObjectMeta(FlinkBlueGreenDeployment bgDeployment) {
        ObjectMeta bgMeta = bgDeployment.getMetadata();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setNamespace(bgMeta.getNamespace());
        objectMeta.setOwnerReferences(
                List.of(
                        new OwnerReference(
                                bgDeployment.getApiVersion(),
                                true,
                                false,
                                bgDeployment.getKind(),
                                bgMeta.getName(),
                                bgMeta.getUid())));
        return objectMeta;
    }

    private static <T> T adjustNameReferences(
            T spec,
            String deploymentName,
            String childDeploymentName,
            String wrapperKey,
            Class<T> valueType)
            throws JsonProcessingException {
        String serializedSpec = SpecUtils.serializeObject(spec, wrapperKey);
        String replacedSerializedSpec = serializedSpec.replace(deploymentName, childDeploymentName);
        return SpecUtils.deserializeObject(replacedSerializedSpec, wrapperKey, valueType);
    }

    public static void logAndThrow(String message) {
        LOG.error(message);
        throw new RuntimeException(message);
    }
}
