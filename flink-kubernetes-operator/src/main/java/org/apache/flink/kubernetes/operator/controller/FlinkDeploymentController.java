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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.health.CanaryResourceManager;
import org.apache.flink.kubernetes.operator.observer.deployment.FlinkDeploymentObserverFactory;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.EventSourceUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;

import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Controller that runs the main reconcile loop for Flink deployments. */
@ControllerConfiguration()
public class FlinkDeploymentController
        implements Reconciler<FlinkDeployment>,
                ErrorStatusHandler<FlinkDeployment>,
                EventSourceInitializer<FlinkDeployment>,
                Cleaner<FlinkDeployment> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDeploymentController.class);

    private final FlinkConfigManager configManager;

    private final Set<FlinkResourceValidator> validators;
    private final FlinkResourceContextFactory ctxFactory;
    private final ReconcilerFactory reconcilerFactory;
    private final FlinkDeploymentObserverFactory observerFactory;
    private final StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder;
    private final EventRecorder eventRecorder;
    private final CanaryResourceManager<FlinkDeployment> canaryResourceManager;

    public FlinkDeploymentController(
            FlinkConfigManager configManager,
            Set<FlinkResourceValidator> validators,
            FlinkResourceContextFactory ctxFactory,
            ReconcilerFactory reconcilerFactory,
            FlinkDeploymentObserverFactory observerFactory,
            StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder,
            EventRecorder eventRecorder,
            CanaryResourceManager<FlinkDeployment> canaryResourceManager) {
        this.configManager = configManager;
        this.validators = validators;
        this.ctxFactory = ctxFactory;
        this.reconcilerFactory = reconcilerFactory;
        this.observerFactory = observerFactory;
        this.statusRecorder = statusRecorder;
        this.eventRecorder = eventRecorder;
        this.canaryResourceManager = canaryResourceManager;
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkApp, Context josdkContext) {
        if (canaryResourceManager.handleCanaryResourceDeletion(flinkApp)) {
            return DeleteControl.defaultDelete();
        }

        String msg = "Cleaning up " + FlinkDeployment.class.getSimpleName();
        LOG.info(msg);
        eventRecorder.triggerEvent(
                flinkApp,
                EventRecorder.Type.Normal,
                EventRecorder.Reason.Cleanup,
                EventRecorder.Component.Operator,
                msg);
        statusRecorder.updateStatusFromCache(flinkApp);
        var ctx = ctxFactory.getResourceContext(flinkApp, josdkContext);
        try {
            observerFactory.getOrCreate(flinkApp).observe(ctx);
        } catch (DeploymentFailedException dfe) {
            // ignore during cleanup
        }
        statusRecorder.removeCachedStatus(flinkApp);
        ctxFactory.cleanup(flinkApp);
        return reconcilerFactory.getOrCreate(flinkApp).cleanup(ctx);
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(FlinkDeployment flinkApp, Context josdkContext)
            throws Exception {

        if (canaryResourceManager.handleCanaryResourceReconciliation(flinkApp)) {
            return UpdateControl.noUpdate();
        }

        LOG.debug("Starting reconciliation");

        statusRecorder.updateStatusFromCache(flinkApp);
        FlinkDeployment previousDeployment = ReconciliationUtils.clone(flinkApp);
        var ctx = ctxFactory.getResourceContext(flinkApp, josdkContext);
        try {
            observerFactory.getOrCreate(flinkApp).observe(ctx);
            if (!validateDeployment(flinkApp)) {
                statusRecorder.patchAndCacheStatus(flinkApp);
                return ReconciliationUtils.toUpdateControl(
                        configManager.getOperatorConfiguration(),
                        flinkApp,
                        previousDeployment,
                        false);
            }
            // Rely on the last stable spec if rolling back
            if (flinkApp.getStatus().getReconciliationStatus().getState()
                    == ReconciliationState.ROLLING_BACK) {
                flinkApp.setSpec(
                        flinkApp.getStatus().getReconciliationStatus().deserializeLastStableSpec());
            }
            statusRecorder.patchAndCacheStatus(flinkApp);
            reconcilerFactory.getOrCreate(flinkApp).reconcile(ctx);
        } catch (RecoveryFailureException rfe) {
            handleRecoveryFailed(flinkApp, rfe);
        } catch (DeploymentFailedException dfe) {
            handleDeploymentFailed(flinkApp, dfe);
        } catch (Exception e) {
            eventRecorder.triggerEvent(
                    flinkApp,
                    EventRecorder.Type.Warning,
                    "ClusterDeploymentException",
                    e.getMessage(),
                    EventRecorder.Component.JobManagerDeployment);
            throw new ReconciliationException(e);
        }

        LOG.debug("End of reconciliation");
        statusRecorder.patchAndCacheStatus(flinkApp);
        return ReconciliationUtils.toUpdateControl(
                configManager.getOperatorConfiguration(), flinkApp, previousDeployment, true);
    }

    private void handleDeploymentFailed(FlinkDeployment flinkApp, DeploymentFailedException dfe) {
        LOG.error("Flink Deployment failed", dfe);
        flinkApp.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.ERROR);
        flinkApp.getStatus().getJobStatus().setState(JobStatus.RECONCILING.name());
        ReconciliationUtils.updateForReconciliationError(
                flinkApp, dfe, configManager.getOperatorConfiguration());
        eventRecorder.triggerEvent(
                flinkApp,
                EventRecorder.Type.Warning,
                dfe.getReason(),
                dfe.getMessage(),
                EventRecorder.Component.JobManagerDeployment);
    }

    private void handleRecoveryFailed(FlinkDeployment flinkApp, RecoveryFailureException rfe) {
        LOG.error("Flink recovery failed", rfe);
        ReconciliationUtils.updateForReconciliationError(
                flinkApp, rfe, configManager.getOperatorConfiguration());
        eventRecorder.triggerEvent(
                flinkApp,
                EventRecorder.Type.Warning,
                rfe.getReason(),
                rfe.getMessage(),
                EventRecorder.Component.JobManagerDeployment);
    }

    @Override
    public Map<String, EventSource> prepareEventSources(
            EventSourceContext<FlinkDeployment> context) {
        return EventSourceInitializer.nameEventSources(
                EventSourceUtils.getSessionJobInformerEventSource(context),
                EventSourceUtils.getDeploymentInformerEventSource(context));
    }

    @Override
    public ErrorStatusUpdateControl<FlinkDeployment> updateErrorStatus(
            FlinkDeployment flinkDeployment, Context<FlinkDeployment> context, Exception e) {
        return ReconciliationUtils.toErrorStatusUpdateControl(
                flinkDeployment,
                context.getRetryInfo(),
                e,
                statusRecorder,
                configManager.getOperatorConfiguration());
    }

    private boolean validateDeployment(FlinkDeployment deployment) {
        for (FlinkResourceValidator validator : validators) {
            Optional<String> validationError = validator.validateDeployment(deployment);
            if (validationError.isPresent()) {
                eventRecorder.triggerEvent(
                        deployment,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.ValidationError,
                        EventRecorder.Component.Operator,
                        validationError.get());
                return ReconciliationUtils.applyValidationErrorAndResetSpec(
                        deployment,
                        validationError.get(),
                        configManager.getOperatorConfiguration());
            }
        }
        return true;
    }
}
