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
    private static final int DEFAULT_MAX_NUM_RETRIES = 5;
    private static final int DEFAULT_RECONCILIATION_RESCHEDULING_INTERVAL_MS = 15000;

    private final FlinkResourceContextFactory ctxFactory;

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
            FlinkBlueGreenDeployment flinkBlueGreenDeployment,
            Context<FlinkBlueGreenDeployment> josdkContext)
            throws Exception {

        FlinkBlueGreenDeploymentStatus deploymentStatus = flinkBlueGreenDeployment.getStatus();

        if (deploymentStatus == null) {
            deploymentStatus = new FlinkBlueGreenDeploymentStatus();
            deploymentStatus.setLastReconciledSpec(
                    SpecUtils.serializeObject(flinkBlueGreenDeployment.getSpec(), "spec"));
            return initiateDeployment(
                    flinkBlueGreenDeployment,
                    deploymentStatus,
                    DeploymentType.BLUE,
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                    null,
                    josdkContext,
                    true);
        } else {
            FlinkBlueGreenDeployments deployments =
                    FlinkBlueGreenDeployments.fromSecondaryResources(josdkContext);

            // TODO: if a new deployment request comes while in the middle of a transition it's
            //  currently ignored, but the new spec remains changed, should we roll it back?
            // TODO: if we choose to leave a previously failed deployment 'running' for debug
            // purposes,
            //  we should flag it somehow as 'ROLLED_BACK' to signal that it can be overriden by a
            // new deployment attempt.
            switch (deploymentStatus.getBlueGreenState()) {
                case ACTIVE_BLUE:
                    return checkAndInitiateDeployment(
                            flinkBlueGreenDeployment,
                            deployments,
                            deploymentStatus,
                            DeploymentType.BLUE,
                            josdkContext);
                case ACTIVE_GREEN:
                    return checkAndInitiateDeployment(
                            flinkBlueGreenDeployment,
                            deployments,
                            deploymentStatus,
                            DeploymentType.GREEN,
                            josdkContext);
                case TRANSITIONING_TO_BLUE:
                    return monitorTransition(
                            flinkBlueGreenDeployment,
                            deployments,
                            deploymentStatus,
                            DeploymentType.GREEN,
                            josdkContext);
                case TRANSITIONING_TO_GREEN:
                    return monitorTransition(
                            flinkBlueGreenDeployment,
                            deployments,
                            deploymentStatus,
                            DeploymentType.BLUE,
                            josdkContext);
                default:
                    return UpdateControl.noUpdate();
            }
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> monitorTransition(
            FlinkBlueGreenDeployment bgDeployment,
            FlinkBlueGreenDeployments deployments,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType currentDeploymentType,
            Context<FlinkBlueGreenDeployment> josdkContext) {

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
            return deleteAndFinalize(
                    bgDeployment,
                    deploymentStatus,
                    currentDeploymentType,
                    josdkContext,
                    currentDeployment,
                    nextState);
        } else {
            // This phase requires rescheduling the reconciliation because the pod initialization
            // could get stuck
            //  (e.g. waiting for resources)
            // TODO: figure out the course of action for error/failure cases

            int maxNumRetries = bgDeployment.getSpec().getTemplate().getMaxNumRetries();
            if (maxNumRetries <= 0) {
                maxNumRetries = DEFAULT_MAX_NUM_RETRIES;
            }

            if (deploymentStatus.getNumRetries() >= maxNumRetries) {
                // ABORT
                // Suspend the nextDeployment (FlinkDeployment)
                nextDeployment.getStatus().getJobStatus().setState(JobStatus.SUSPENDED);
                josdkContext.getClient().resource(nextDeployment).update();

                // We indicate this Blue/Green deployment is no longer Transitioning
                //  and rollback the state value
                deploymentStatus.setBlueGreenState(
                        nextState == FlinkBlueGreenDeploymentState.ACTIVE_BLUE
                                ? FlinkBlueGreenDeploymentState.ACTIVE_GREEN
                                : FlinkBlueGreenDeploymentState.ACTIVE_BLUE);

                // If the current running FlinkDeployment is not in RUNNING/STABLE,
                // we flag this Blue/Green as FAILING
                return patchStatusUpdateControl(
                        bgDeployment, deploymentStatus, null, JobStatus.FAILING, false);
            } else {
                // RETRY
                deploymentStatus.setNumRetries(deploymentStatus.getNumRetries() + 1);

                LOG.info("Deployment " + nextDeployment.getMetadata().getName() + " not ready yet");
                return patchStatusUpdateControl(bgDeployment, deploymentStatus, null, null, false)
                        .rescheduleAfter(getReconciliationReschedInterval(bgDeployment));
            }
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
            DeploymentType currentDeploymentType,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkDeployment currentDeployment,
            FlinkBlueGreenDeploymentState nextState) {

        if (currentDeployment != null) {
            deleteDeployment(currentDeployment, josdkContext);
            return UpdateControl.noUpdate();
        } else {
            deploymentStatus.setLastReconciledSpec(
                    SpecUtils.serializeObject(bgDeployment.getSpec(), "spec"));

            // TODO: Set the new child job STATUS to RUNNING too

            return patchStatusUpdateControl(
                    bgDeployment, deploymentStatus, nextState, JobStatus.RUNNING, false);
        }
    }

    private UpdateControl<FlinkBlueGreenDeployment> checkAndInitiateDeployment(
            FlinkBlueGreenDeployment flinkBlueGreenDeployment,
            FlinkBlueGreenDeployments deployments,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType currentDeploymentType,
            Context<FlinkBlueGreenDeployment> josdkContext)
            throws Exception {

        if (hasSpecChanged(
                flinkBlueGreenDeployment.getSpec(), deploymentStatus, currentDeploymentType)) {

            FlinkDeployment currentFlinkDeployment =
                    DeploymentType.BLUE == currentDeploymentType
                            ? deployments.getFlinkDeploymentBlue()
                            : deployments.getFlinkDeploymentGreen();

            // spec, report the error and abort
            if (isDeploymentReady(currentFlinkDeployment, josdkContext, deploymentStatus)) {

                DeploymentType nextDeploymentType = DeploymentType.BLUE;
                FlinkBlueGreenDeploymentState nextState =
                        FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
                FlinkResourceContext<FlinkDeployment> resourceContext =
                        ctxFactory.getResourceContext(currentFlinkDeployment, josdkContext);

                // TODO: this operation is already done by hasSpecChanged() above, dedup later
                String serializedSpec =
                        SpecUtils.serializeObject(flinkBlueGreenDeployment.getSpec(), "spec");

                // Updating status
                if (DeploymentType.BLUE == currentDeploymentType) {
                    nextState = FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
                    nextDeploymentType = DeploymentType.GREEN;
                }

                Savepoint lastCheckpoint = configureSavepoint(resourceContext);

                return initiateDeployment(
                        flinkBlueGreenDeployment,
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
                            flinkBlueGreenDeployment,
                            deploymentStatus,
                            null,
                            JobStatus.FAILING,
                            false);
                }
            }
        }

        return UpdateControl.noUpdate();
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

        List<Event> badEvents =
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

        if (!badEvents.isEmpty()) {
            LOG.warn("Bad events detected: " + badEvents);
        }
    }

    private static Savepoint configureSavepoint(
            FlinkResourceContext<FlinkDeployment> resourceContext) throws Exception {
        // TODO: if the user specified an initialSavepointPath, use it and skip this
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

        // TODO 1: check the last CP age with the logic from
        // AbstractJobReconciler.changeLastStateIfCheckpointTooOld
        // TODO 2: if no checkpoint is available, take a savepoint? throw error?
        if (lastCheckpoint.isEmpty()) {
            throw new IllegalStateException(
                    "Last Checkpoint for Job "
                            + resourceContext.getResource().getMetadata().getName()
                            + " not found!");
        }
        return lastCheckpoint.get();
    }

    private UpdateControl<FlinkBlueGreenDeployment> initiateDeployment(
            FlinkBlueGreenDeployment flinkBlueGreenDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType nextDeploymentType,
            FlinkBlueGreenDeploymentState nextState,
            Savepoint lastCheckpoint,
            Context<FlinkBlueGreenDeployment> josdkContext,
            boolean isFirstDeployment)
            throws JsonProcessingException {

        deploy(
                flinkBlueGreenDeployment,
                nextDeploymentType,
                lastCheckpoint,
                josdkContext,
                isFirstDeployment);

        // TODO: set child job status to JobStatus.INITIALIZING

        return patchStatusUpdateControl(
                        flinkBlueGreenDeployment,
                        deploymentStatus,
                        nextState,
                        null,
                        isFirstDeployment)
                .rescheduleAfter(getReconciliationReschedInterval(flinkBlueGreenDeployment));
    }

    private boolean isDeploymentReady(
            FlinkDeployment deployment,
            Context<FlinkBlueGreenDeployment> josdkContext,
            FlinkBlueGreenDeploymentStatus deploymentStatus) {
        if (ResourceLifecycleState.STABLE == deployment.getStatus().getLifecycleState()
                && JobStatus.RUNNING == deployment.getStatus().getJobStatus().getState()) {
            // TODO: verify, e.g. will pods be "pending" after the FlinkDeployment is RUNNING and
            // STABLE?
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
            FlinkBlueGreenDeploymentSpec newSpec,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            DeploymentType deploymentType) {

        String lastReconciledSpec = deploymentStatus.getLastReconciledSpec();

        return !lastReconciledSpec.equals(SpecUtils.serializeObject(newSpec, "spec"));
    }

    private UpdateControl<FlinkBlueGreenDeployment> patchStatusUpdateControl(
            FlinkBlueGreenDeployment flinkBlueGreenDeployment,
            FlinkBlueGreenDeploymentStatus deploymentStatus,
            FlinkBlueGreenDeploymentState deploymentState,
            JobStatus jobState,
            boolean isFirstDeployment) {
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
        // TODO: This gets called multiple times, check to see if it's already in a TERMINATING
        // state
        // (or only execute if RUNNING)
        List<StatusDetails> deletedStatus =
                josdkContext
                        .getClient()
                        .resources(FlinkDeployment.class)
                        .inNamespace(currentDeployment.getMetadata().getNamespace())
                        .withName(currentDeployment.getMetadata().getName())
                        .delete();

        boolean deleted =
                deletedStatus.size() == 1
                        && deletedStatus.get(0).getKind().equals("FlinkDeployment");
        if (!deleted) {
            LOG.info("Deployment not deleted, will retry");
        } else {
            LOG.info("Deployment deleted!");
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
