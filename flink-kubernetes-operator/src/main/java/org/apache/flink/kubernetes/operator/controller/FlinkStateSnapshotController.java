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

    private UpdateControl<FlinkStateSnapshot> getUpdateControl(FlinkStateSnapshotContext ctx) {
        var resource = ctx.getResource();
        UpdateControl<FlinkStateSnapshot> updateControl;
        if (!ctx.getOriginalStatus().equals(resource.getStatus())) {
            updateControl = UpdateControl.patchStatus(resource);
        } else {
            updateControl = UpdateControl.noUpdate();
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
        if (!ctx.getOriginalStatus().equals(ctx.getResource().getStatus())) {
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
}
