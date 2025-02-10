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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.controller.FlinkStateSnapshotContext;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotUtils;
import org.apache.flink.kubernetes.operator.utils.SnapshotUtils;
import org.apache.flink.util.Preconditions;

import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.TRIGGER_PENDING;

/** The reconciler for the {@link org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot}. */
@RequiredArgsConstructor
public class StateSnapshotReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(StateSnapshotReconciler.class);

    private final FlinkResourceContextFactory ctxFactory;
    private final EventRecorder eventRecorder;

    public void reconcile(FlinkStateSnapshotContext ctx) {
        var resource = ctx.getResource();

        var savepointState = resource.getStatus().getState();
        if (!TRIGGER_PENDING.equals(savepointState)) {
            return;
        }

        if (resource.getSpec().isSavepoint()
                && resource.getSpec().getSavepoint().getAlreadyExists()) {
            LOG.info(
                    "Snapshot {} is marked as completed in spec, skipping triggering savepoint.",
                    resource.getMetadata().getName());

            FlinkStateSnapshotUtils.snapshotSuccessful(
                    resource, resource.getSpec().getSavepoint().getPath(), true);
            return;
        }

        if (FlinkStateSnapshotUtils.abandonSnapshotIfJobNotRunning(
                ctx.getKubernetesClient(),
                ctx.getResource(),
                ctx.getSecondaryResource().orElse(null),
                eventRecorder)) {
            return;
        }

        var jobId = ctx.getSecondaryResource().orElseThrow().getStatus().getJobStatus().getJobId();

        Optional<String> triggerIdOpt;
        try {
            triggerIdOpt = triggerCheckpointOrSavepoint(resource.getSpec(), ctx, jobId);
        } catch (Exception e) {
            LOG.error("Failed to trigger snapshot for resource {}", ctx.getResource(), e);
            throw new ReconciliationException(e);
        }

        if (triggerIdOpt.isEmpty()) {
            LOG.warn("Failed to trigger snapshot {}", resource.getMetadata().getName());
            return;
        }

        FlinkStateSnapshotUtils.snapshotInProgress(resource, triggerIdOpt.get());
    }

    public DeleteControl cleanup(FlinkStateSnapshotContext ctx) throws Exception {
        var resource = ctx.getResource();
        var state = resource.getStatus().getState();
        var resourceName = resource.getMetadata().getName();
        LOG.info("Cleaning up resource {}...", resourceName);

        if (resource.getSpec().isCheckpoint()) {
            return DeleteControl.defaultDelete();
        }
        if (!resource.getSpec().getSavepoint().getDisposeOnDelete()) {
            return DeleteControl.defaultDelete();
        }
        if (resource.getSpec().getJobReference() == null
                || resource.getSpec().getJobReference().getName() == null) {
            return DeleteControl.defaultDelete();
        }

        switch (state) {
            case IN_PROGRESS:
                LOG.info(
                        "Cannot delete resource {} yet as savepoint is still in progress...",
                        resourceName);
                return DeleteControl.noFinalizerRemoval()
                        .rescheduleAfter(ctx.getOperatorConfig().getReconcileInterval().toMillis());
            case COMPLETED:
                var flinkDeployment = getFlinkDeployment(ctx);
                return handleSnapshotCleanup(resource, flinkDeployment, ctx);
            case FAILED:
            case TRIGGER_PENDING:
            case ABANDONED:
                LOG.info(
                        "Savepoint state is {}, cleaning up resource {} without disposal...",
                        state.name(),
                        resourceName);
                return DeleteControl.defaultDelete();
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

    private Optional<String> triggerCheckpointOrSavepoint(
            FlinkStateSnapshotSpec spec, FlinkStateSnapshotContext ctx, String jobId)
            throws Exception {
        var flinkDeploymentContext =
                ctxFactory.getResourceContext(
                        ctx.getReferencedJobFlinkDeployment(), ctx.getJosdkContext());
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
        } else if (spec.isCheckpoint()) {
            if (!SnapshotUtils.isSnapshotTriggeringSupported(conf)) {
                throw new IllegalArgumentException(
                        "Manual checkpoint triggering is not supported for this Flink job (requires Flink 1.17+)");
            }
            return Optional.of(flinkService.triggerCheckpoint(jobId, CheckpointType.FULL, conf));
        } else {
            throw new IllegalArgumentException(
                    "Snapshot must specify either savepoint or checkpoint spec");
        }
    }
}
