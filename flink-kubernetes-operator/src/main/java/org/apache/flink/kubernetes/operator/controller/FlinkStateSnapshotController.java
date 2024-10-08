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

import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.observer.snapshot.StateSnapshotObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.snapshot.StateSnapshotReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.EventSourceUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotUtils;
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
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Controller that runs the main reconcile loop for {@link FlinkStateSnapshot}. */
@RequiredArgsConstructor
@ControllerConfiguration
public class FlinkStateSnapshotController
        implements Reconciler<FlinkStateSnapshot>,
                ErrorStatusHandler<FlinkStateSnapshot>,
                EventSourceInitializer<FlinkStateSnapshot>,
                Cleaner<FlinkStateSnapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkStateSnapshotController.class);

    private final Set<FlinkResourceValidator> validators;
    private final FlinkResourceContextFactory ctxFactory;
    private final StateSnapshotReconciler reconciler;
    private final StateSnapshotObserver observer;
    private final EventRecorder eventRecorder;
    private final MetricManager<FlinkStateSnapshot> metricManager;
    private final StatusRecorder<FlinkStateSnapshot, FlinkStateSnapshotStatus> statusRecorder;

    @Override
    public UpdateControl<FlinkStateSnapshot> reconcile(
            FlinkStateSnapshot flinkStateSnapshot, Context<FlinkStateSnapshot> josdkContext) {
        // status might be null here
        flinkStateSnapshot.setStatus(
                Objects.requireNonNullElseGet(
                        flinkStateSnapshot.getStatus(), FlinkStateSnapshotStatus::new));
        var ctx = ctxFactory.getFlinkStateSnapshotContext(flinkStateSnapshot, josdkContext);

        observer.observe(ctx);

        if (validateSnapshot(ctx)) {
            reconciler.reconcile(ctx);
        }

        updateLabels(ctx);

        notifyListenersAndMetricManager(ctx);
        return getUpdateControl(ctx);
    }

    @Override
    public DeleteControl cleanup(
            FlinkStateSnapshot flinkStateSnapshot, Context<FlinkStateSnapshot> josdkContext) {
        var ctx = ctxFactory.getFlinkStateSnapshotContext(flinkStateSnapshot, josdkContext);
        try {
            metricManager.onRemove(flinkStateSnapshot);
            return reconciler.cleanup(ctx);
        } catch (Exception e) {
            eventRecorder.triggerSnapshotEvent(
                    flinkStateSnapshot,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.CleanupFailed,
                    EventRecorder.Component.Snapshot,
                    e.getMessage(),
                    ctx.getKubernetesClient());
            LOG.error(
                    "Error during cleanup of snapshot {}",
                    flinkStateSnapshot.getMetadata().getName(),
                    e);
            return DeleteControl.noFinalizerRemoval()
                    .rescheduleAfter(ctx.getOperatorConfig().getReconcileInterval().toMillis());
        }
    }

    @Override
    public ErrorStatusUpdateControl<FlinkStateSnapshot> updateErrorStatus(
            FlinkStateSnapshot resource, Context<FlinkStateSnapshot> context, Exception e) {
        var ctx = ctxFactory.getFlinkStateSnapshotContext(resource, context);
        ReconciliationUtils.updateForReconciliationError(ctx, e);

        var reason =
                resource.getSpec().isSavepoint()
                        ? EventRecorder.Reason.SavepointError
                        : EventRecorder.Reason.CheckpointError;
        eventRecorder.triggerSnapshotEvent(
                resource,
                EventRecorder.Type.Warning,
                reason,
                EventRecorder.Component.Snapshot,
                resource.getStatus().getError(),
                ctx.getKubernetesClient());

        if (resource.getStatus().getFailures() > resource.getSpec().getBackoffLimit()) {
            LOG.info(
                    "Snapshot {} failed and won't be retried as failure count exceeded the backoff limit",
                    resource.getMetadata().getName());
            notifyListenersAndMetricManager(ctx);
            return ErrorStatusUpdateControl.patchStatus(resource).withNoRetry();
        }

        long retrySeconds = 10L * (1L << resource.getStatus().getFailures() - 1);
        LOG.info(
                "Snapshot {} failed and will be retried in {} seconds...",
                resource.getMetadata().getName(),
                retrySeconds);
        FlinkStateSnapshotUtils.snapshotTriggerPending(resource);

        notifyListenersAndMetricManager(ctx);
        return ErrorStatusUpdateControl.patchStatus(resource)
                .rescheduleAfter(Duration.ofSeconds(retrySeconds));
    }

    @Override
    public Map<String, EventSource> prepareEventSources(
            EventSourceContext<FlinkStateSnapshot> context) {
        return EventSourceInitializer.nameEventSources(
                EventSourceUtils.getFlinkStateSnapshotInformerEventSources(context));
    }

    /**
     * Checks whether status and/or labels were changed on this resource, and returns an
     * UpdateControl instance accordingly. Unless the snapshot state is terminal, the update control
     * will be configured to reschedule the reconciliation.
     *
     * @param ctx snapshot context
     * @return update control
     */
    private UpdateControl<FlinkStateSnapshot> getUpdateControl(FlinkStateSnapshotContext ctx) {
        var resource = ctx.getResource();
        var updateControl = UpdateControl.<FlinkStateSnapshot>noUpdate();

        var labelsChanged = resourceLabelsChanged(ctx);
        var statusChanged = resourceStatusChanged(ctx);

        if (labelsChanged && statusChanged) {
            updateControl = UpdateControl.updateResourceAndPatchStatus(resource);
        } else if (labelsChanged) {
            updateControl = UpdateControl.updateResource(resource);
        } else if (statusChanged) {
            updateControl = UpdateControl.patchStatus(resource);
        }

        switch (resource.getStatus().getState()) {
            case COMPLETED:
            case ABANDONED:
                return updateControl;
            default:
                return updateControl.rescheduleAfter(
                        ctx.getOperatorConfig().getReconcileInterval().toMillis());
        }
    }

    private void notifyListenersAndMetricManager(FlinkStateSnapshotContext ctx) {
        if (resourceStatusChanged(ctx)) {
            statusRecorder.notifyListeners(ctx.getResource(), ctx.getOriginalStatus());
        }
        metricManager.onUpdate(ctx.getResource());
    }

    private boolean validateSnapshot(FlinkStateSnapshotContext ctx) {
        var savepoint = ctx.getResource();
        for (var validator : validators) {
            var validationError =
                    validator.validateStateSnapshot(savepoint, ctx.getSecondaryResource());
            if (validationError.isPresent()) {
                eventRecorder.triggerSnapshotEvent(
                        savepoint,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.ValidationError,
                        EventRecorder.Component.Operator,
                        validationError.get(),
                        ctx.getKubernetesClient());
                return false;
            }
        }
        return true;
    }

    /**
     * Updates FlinkStateSnapshot resource labels with labels that represent its current state and
     * spec.
     *
     * @param ctx snapshot context
     */
    private void updateLabels(FlinkStateSnapshotContext ctx) {
        var labels = new HashMap<>(ctx.getResource().getMetadata().getLabels());
        labels.putAll(
                FlinkStateSnapshotUtils.getSnapshotLabels(
                        ctx.getResource(), ctx.getSecondaryResource()));
        ctx.getResource().getMetadata().setLabels(labels);
    }

    /**
     * Checks if the resource status has changed since the start of reconciliation.
     *
     * @param ctx snapshot context
     * @return true if resource status changed
     */
    private boolean resourceStatusChanged(FlinkStateSnapshotContext ctx) {
        return !ctx.getOriginalStatus().equals(ctx.getResource().getStatus());
    }

    /**
     * Checks if the resource labels have changed since the start of reconciliation.
     *
     * @param ctx snapshot context
     * @return true if resource labels changed
     */
    private boolean resourceLabelsChanged(FlinkStateSnapshotContext ctx) {
        return !ctx.getOriginalLabels().equals(ctx.getResource().getMetadata().getLabels());
    }
}
