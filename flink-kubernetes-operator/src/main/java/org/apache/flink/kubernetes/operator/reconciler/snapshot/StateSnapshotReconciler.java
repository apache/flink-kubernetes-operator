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

package org.apache.flink.kubernetes.operator.reconciler.snapshot;

import org.apache.flink.autoscaler.utils.DateTimeUtils;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotState;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.controller.FlinkStateSnapshotContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.util.Preconditions;

import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

/** The reconciler for the {@link org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot}. */
@RequiredArgsConstructor
public class StateSnapshotReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(StateSnapshotReconciler.class);

    private final FlinkResourceContextFactory ctxFactory;

    public void reconcile(FlinkStateSnapshotContext ctx) throws Exception {
        var source = ctx.getResource().getSpec().getJobReference();
        var resource = ctx.getResource();

        var savepointState = resource.getStatus().getState();
        if (!FlinkStateSnapshotState.TRIGGER_PENDING.equals(savepointState)) {
            return;
        }

        if (resource.getSpec().isSavepoint()
                && resource.getSpec().getSavepoint().getAlreadyExists()) {
            LOG.info(
                    "Snapshot {} is marked as completed in spec, skipping triggering savepoint.",
                    resource.getMetadata().getName());
            resource.getStatus().setState(FlinkStateSnapshotState.COMPLETED);
            resource.getStatus().setPath(resource.getSpec().getSavepoint().getPath());
            var time = DateTimeUtils.kubernetes(Instant.now());
            resource.getStatus().setTriggerTimestamp(time);
            resource.getStatus().setResultTimestamp(time);
            return;
        }

        var secondaryResource =
                ctx.getSecondaryResource()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                String.format(
                                                        "Secondary resource %s not found",
                                                        source)));
        if (!ReconciliationUtils.isJobRunning(secondaryResource.getStatus())) {
            LOG.warn(
                    "Target job {} for savepoint {} is not running, cannot trigger snapshot.",
                    secondaryResource.getMetadata().getName(),
                    resource.getMetadata().getName());
            return;
        }

        var jobId = secondaryResource.getStatus().getJobStatus().getJobId();
        var ctxFlinkDeployment =
                ctxFactory.getResourceContext(
                        ctx.getReferencedJobFlinkDeployment(), ctx.getJosdkContext());
        var triggerIdOpt =
                triggerCheckpointOrSavepoint(resource.getSpec(), ctxFlinkDeployment, jobId);

        if (triggerIdOpt.isEmpty()) {
            LOG.warn("Failed to trigger snapshot {}", resource.getMetadata().getName());
            return;
        }

        resource.getMetadata()
                .getLabels()
                .putIfAbsent(CrdConstants.LABEL_SNAPSHOT_TYPE, SnapshotTriggerType.MANUAL.name());
        resource.getStatus().setState(FlinkStateSnapshotState.IN_PROGRESS);
        resource.getStatus().setTriggerId(triggerIdOpt.get());
        resource.getStatus().setTriggerTimestamp(DateTimeUtils.kubernetes(Instant.now()));
    }

    public DeleteControl cleanup(FlinkStateSnapshotContext ctx) throws Exception {
        var resource = ctx.getResource();
        var resourceName = resource.getMetadata().getName();
        LOG.info("Cleaning up resource {}...", resourceName);

        if (resource.getSpec().isCheckpoint()) {
            return DeleteControl.defaultDelete();
        }
        if (!resource.getSpec().getSavepoint().getDisposeOnDelete()) {
            return DeleteControl.defaultDelete();
        }

        var josdkContext = ctxFactory.getFlinkStateSnapshotContext(resource, ctx.getJosdkContext());
        var state = resource.getStatus().getState();
        var flinkDeployment = getFlinkDeployment(ctx);

        switch (state) {
            case IN_PROGRESS:
                LOG.info(
                        "Cannot delete resource {} yet as savepoint is still in progress...",
                        resourceName);
                return DeleteControl.noFinalizerRemoval()
                        .rescheduleAfter(
                                josdkContext.getOperatorConfig().getReconcileInterval().toMillis());
            case FAILED:
                LOG.info(
                        "Savepoint was not successful, cleaning up resource {} without disposal...",
                        resourceName);
                return DeleteControl.defaultDelete();
            case TRIGGER_PENDING:
                LOG.info(
                        "Savepoint has not started yet, cleaning up resource {} without disposal...",
                        resourceName);
                return DeleteControl.defaultDelete();
            case COMPLETED:
                return handleSnapshotCleanup(resource, flinkDeployment, josdkContext);
            default:
                LOG.info("Unknown savepoint state for {}: {}", resourceName, state);
                return DeleteControl.defaultDelete();
        }
    }

    private FlinkDeployment getFlinkDeployment(FlinkStateSnapshotContext ctx) {
        var secondaryResourceOpt = ctx.getSecondaryResource();
        if (secondaryResourceOpt.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot dispose of snapshot because referenced Flink job cannot be found!");
        } else if (!ReconciliationUtils.isJobRunning(secondaryResourceOpt.get().getStatus())) {
            throw new IllegalArgumentException(
                    "Cannot dispose of snapshot because referenced Flink job is not running!");
        }

        return ctx.getReferencedJobFlinkDeployment();
    }

    private DeleteControl handleSnapshotCleanup(
            FlinkStateSnapshot flinkStateSnapshot,
            FlinkDeployment flinkDeployment,
            FlinkStateSnapshotContext ctx) {
        var resourceName = flinkStateSnapshot.getMetadata().getName();
        String path = flinkStateSnapshot.getStatus().getPath();

        if (path == null) {
            LOG.info("No path was saved for snapshot {}, no cleanup required.", resourceName);
            return DeleteControl.defaultDelete();
        }

        LOG.info("Disposing of savepoint of {} before deleting the resource...", resourceName);
        var observeConfig = ctx.getReferencedJobObserveConfig();
        var ctxFlinkDeployment =
                ctxFactory.getResourceContext(flinkDeployment, ctx.getJosdkContext());
        var flinkService = ctxFlinkDeployment.getFlinkService();

        try {
            flinkService.disposeSavepoint(path, observeConfig);
            return DeleteControl.defaultDelete();
        } catch (Exception e) {
            LOG.error(
                    "Failed to dispose savepoint {} from deployment {}",
                    path,
                    ctxFlinkDeployment.getResource().getMetadata().getName());
            return DeleteControl.noFinalizerRemoval()
                    .rescheduleAfter(ctx.getOperatorConfig().getReconcileInterval().toMillis());
        }
    }

    private static Optional<String> triggerCheckpointOrSavepoint(
            FlinkStateSnapshotSpec spec,
            FlinkResourceContext<FlinkDeployment> flinkDeploymentContext,
            String jobId)
            throws Exception {
        var flinkService = flinkDeploymentContext.getFlinkService();
        var conf =
                Preconditions.checkNotNull(
                        flinkDeploymentContext.getObserveConfig(),
                        String.format(
                                "Observe config was null for %s",
                                flinkDeploymentContext.getResource().getMetadata().getName()));

        if (spec.isSavepoint()) {
            var path =
                    ObjectUtils.firstNonNull(
                            spec.getSavepoint().getPath(),
                            conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
            if (path == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Either the savepoint path in the spec or configuration %s in the Flink resource has to be supplied",
                                CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));
            }
            return Optional.of(
                    flinkService.triggerSavepoint(
                            jobId,
                            org.apache.flink.core.execution.SavepointFormatType.valueOf(
                                    spec.getSavepoint().getFormatType().name()),
                            path,
                            conf));
        } else {
            return Optional.of(
                    flinkService.triggerCheckpoint(
                            jobId,
                            org.apache.flink.core.execution.CheckpointType.valueOf(
                                    spec.getCheckpoint().getCheckpointType().name()),
                            conf));
        }
    }
}
