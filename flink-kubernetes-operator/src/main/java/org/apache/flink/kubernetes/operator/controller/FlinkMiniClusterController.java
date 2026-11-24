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
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.status.FlinkMiniClusterStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.validation.FlinkResourceValidator;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.exception.UpgradeFailureException;
import org.apache.flink.kubernetes.operator.health.CanaryResourceManager;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.service.minicluster.MiniClusterPodFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.EventSourceUtils;
import org.apache.flink.kubernetes.operator.utils.ExceptionUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;

import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Controller that runs the main reconcile loop for Flink MiniClusters. */
@ControllerConfiguration
public class FlinkMiniClusterController
        implements io.javaoperatorsdk.operator.api.reconciler.Reconciler<FlinkMiniCluster>,
                Cleaner<FlinkMiniCluster> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniClusterController.class);

    private final Set<FlinkResourceValidator> validators;
    private final FlinkResourceContextFactory ctxFactory;
    private final Reconciler<FlinkMiniCluster> reconciler;
    private final Observer<FlinkMiniCluster> observer;
    private final StatusRecorder<FlinkMiniCluster, FlinkMiniClusterStatus> statusRecorder;
    private final EventRecorder eventRecorder;
    private final CanaryResourceManager<FlinkMiniCluster> canaryResourceManager;
    private final FlinkConfigManager flinkConfigManager;

    public FlinkMiniClusterController(
            Set<FlinkResourceValidator> validators,
            FlinkResourceContextFactory ctxFactory,
            Reconciler<FlinkMiniCluster> reconciler,
            Observer<FlinkMiniCluster> observer,
            StatusRecorder<FlinkMiniCluster, FlinkMiniClusterStatus> statusRecorder,
            EventRecorder eventRecorder,
            CanaryResourceManager<FlinkMiniCluster> canaryResourceManager,
            FlinkConfigManager flinkConfigManager) {
        this.validators = validators;
        this.ctxFactory = ctxFactory;
        this.reconciler = reconciler;
        this.observer = observer;
        this.statusRecorder = statusRecorder;
        this.eventRecorder = eventRecorder;
        this.canaryResourceManager = canaryResourceManager;
        this.flinkConfigManager = flinkConfigManager;
    }

    @Override
    public DeleteControl cleanup(
            FlinkMiniCluster miniCluster, Context<FlinkMiniCluster> josdkContext) {
        if (canaryResourceManager.handleCanaryResourceDeletion(miniCluster)) {
            return DeleteControl.defaultDelete();
        }
        eventRecorder.triggerEvent(
                miniCluster,
                EventRecorder.Type.Normal,
                EventRecorder.Reason.Cleanup,
                EventRecorder.Component.Operator,
                "Cleaning up FlinkMiniCluster",
                josdkContext.getClient());
        statusRecorder.updateStatusFromCache(miniCluster);
        miniCluster.getStatus().setLifecycleState(ResourceLifecycleState.DELETING);
        var ctx = ctxFactory.getResourceContext(miniCluster, josdkContext);
        try {
            observer.observe(ctx);
        } catch (Exception err) {
            LOG.error("Error while observing for cleanup", err);
        }

        var deleteControl = reconciler.cleanup(ctx);
        if (deleteControl.isRemoveFinalizer()) {
            miniCluster.getStatus().setLifecycleState(ResourceLifecycleState.DELETED);
            statusRecorder.cleanupForDeletion(miniCluster);
            ctxFactory.cleanup(miniCluster);
        } else {
            statusRecorder.patchAndCacheStatus(miniCluster, ctx.getKubernetesClient());
        }
        return deleteControl;
    }

    @Override
    public UpdateControl<FlinkMiniCluster> reconcile(
            FlinkMiniCluster miniCluster, Context<FlinkMiniCluster> josdkContext) throws Exception {

        if (canaryResourceManager.handleCanaryResourceReconciliation(
                miniCluster, josdkContext.getClient())) {
            return UpdateControl.noUpdate();
        }

        LOG.debug("Starting reconciliation");

        statusRecorder.updateStatusFromCache(miniCluster);
        FlinkMiniCluster previousMiniCluster = ReconciliationUtils.clone(miniCluster);
        var ctx = ctxFactory.getResourceContext(miniCluster, josdkContext);

        // If we get an unsupported Flink version, trigger event and exit
        if (!ValidatorUtils.validateSupportedVersion(ctx, eventRecorder)) {
            return UpdateControl.noUpdate();
        }

        try {
            observer.observe(ctx);
            if (!validateMiniCluster(ctx)) {
                statusRecorder.patchAndCacheStatus(miniCluster, ctx.getKubernetesClient());
                return ReconciliationUtils.toUpdateControl(
                        ctx.getOperatorConfig(), miniCluster, previousMiniCluster, false);
            }
            statusRecorder.patchAndCacheStatus(miniCluster, ctx.getKubernetesClient());
            reconciler.reconcile(ctx);
        } catch (UpgradeFailureException ufe) {
            ReconciliationUtils.updateForReconciliationError(ctx, ufe);
            triggerErrorEvent(ctx, ufe, ufe.getReason());
        } catch (DeploymentFailedException dfe) {
            miniCluster.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.ERROR);
            miniCluster.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
            ReconciliationUtils.updateForReconciliationError(ctx, dfe);
            triggerErrorEvent(ctx, dfe, dfe.getReason());
        } catch (Exception e) {
            triggerErrorEvent(ctx, e, EventRecorder.Reason.Error.name());
            throw new ReconciliationException(e);
        }

        miniCluster.getStatus().setRunningConditionIfRequired();
        LOG.debug("End of reconciliation");
        statusRecorder.patchAndCacheStatus(miniCluster, ctx.getKubernetesClient());
        return ReconciliationUtils.toUpdateControl(
                ctx.getOperatorConfig(), miniCluster, previousMiniCluster, true);
    }

    private void triggerErrorEvent(
            FlinkResourceContext<FlinkMiniCluster> ctx, Exception e, String reason) {
        eventRecorder.triggerEvent(
                ctx.getResource(),
                EventRecorder.Type.Warning,
                reason,
                ExceptionUtils.getExceptionMessage(e),
                EventRecorder.Component.JobManagerDeployment,
                ctx.getKubernetesClient());
    }

    @Override
    public List<EventSource<?, FlinkMiniCluster>> prepareEventSources(
            EventSourceContext<FlinkMiniCluster> context) {
        List<EventSource<?, FlinkMiniCluster>> eventSources = new ArrayList<>();
        eventSources.add(
                EventSourceUtils.getDeploymentInformerEventSource(
                        context, MiniClusterPodFactory.LABEL_COMPONENT_MINICLUSTER));
        if (flinkConfigManager.getOperatorConfiguration().isManageIngress()) {
            eventSources.add(EventSourceUtils.getIngressInformerEventSource(context));
        }
        return eventSources;
    }

    @Override
    public ErrorStatusUpdateControl<FlinkMiniCluster> updateErrorStatus(
            FlinkMiniCluster miniCluster, Context<FlinkMiniCluster> context, Exception e) {
        var ctx = ctxFactory.getResourceContext(miniCluster, context);
        return ReconciliationUtils.toErrorStatusUpdateControl(ctx, e, statusRecorder);
    }

    private boolean validateMiniCluster(FlinkResourceContext<FlinkMiniCluster> ctx) {
        var miniCluster = ctx.getResource();
        for (FlinkResourceValidator validator : validators) {
            Optional<String> validationError = validator.validateMiniCluster(miniCluster);
            if (validationError.isPresent()) {
                eventRecorder.triggerEvent(
                        miniCluster,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.ValidationError,
                        EventRecorder.Component.Operator,
                        validationError.get(),
                        ctx.getKubernetesClient());
                return ReconciliationUtils.applyValidationErrorAndResetSpec(
                        ctx, validationError.get());
            }
        }
        return true;
    }
}
