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

package org.apache.flink.kubernetes.operator.observer.snapshot;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.controller.FlinkStateSnapshotContext;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.observer.CheckpointFetchResult;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotUtils;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.IN_PROGRESS;

/** The observer of {@link org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot}. */
@RequiredArgsConstructor
public class StateSnapshotObserver {

    private static final Logger LOG = LoggerFactory.getLogger(StateSnapshotObserver.class);

    private final FlinkResourceContextFactory ctxFactory;
    private final EventRecorder eventRecorder;

    public void observe(FlinkStateSnapshotContext ctx) {
        var resource = ctx.getResource();
        var savepointState = resource.getStatus().getState();

        if (IN_PROGRESS.equals(savepointState)) {
            observeSnapshotState(ctx);
        }
    }

    private void observeSnapshotState(FlinkStateSnapshotContext ctx) {
        var resource = ctx.getResource();
        var resourceName = resource.getMetadata().getName();
        var triggerId = resource.getStatus().getTriggerId();

        if (StringUtils.isEmpty(triggerId)) {
            return;
        }

        LOG.debug("Observing snapshot state for resource {}...", resourceName);

        if (FlinkStateSnapshotUtils.abandonSnapshotIfJobNotRunning(
                ctx.getKubernetesClient(),
                ctx.getResource(),
                ctx.getSecondaryResource().orElse(null),
                eventRecorder)) {
            return;
        }

        var jobId = ctx.getSecondaryResource().orElseThrow().getStatus().getJobStatus().getJobId();
        var ctxFlinkDeployment =
                ctxFactory.getResourceContext(
                        ctx.getReferencedJobFlinkDeployment(), ctx.getJosdkContext());
        var observeConfig = ctx.getReferencedJobObserveConfig();

        if (resource.getSpec().isSavepoint()) {
            var savepointInfo =
                    ctxFlinkDeployment
                            .getFlinkService()
                            .fetchSavepointInfo(triggerId, jobId, observeConfig);
            handleSavepoint(ctx, savepointInfo);
        } else {
            var checkpointInfo =
                    ctxFlinkDeployment
                            .getFlinkService()
                            .fetchCheckpointInfo(triggerId, jobId, observeConfig);
            handleCheckpoint(ctx, checkpointInfo, ctxFlinkDeployment, jobId);
        }
    }

    private void handleSavepoint(
            FlinkStateSnapshotContext ctx, SavepointFetchResult savepointInfo) {
        var resource = ctx.getResource();
        var resourceName = resource.getMetadata().getName();

        if (savepointInfo.isPending()) {
            LOG.debug(
                    "Savepoint '{}' with ID {} is pending",
                    resourceName,
                    resource.getStatus().getTriggerId());
        } else if (savepointInfo.getError() != null) {
            throw new ReconciliationException(savepointInfo.getError());
        } else {
            LOG.info("Savepoint {} successful: {}", resourceName, savepointInfo.getLocation());
            FlinkStateSnapshotUtils.snapshotSuccessful(
                    resource, savepointInfo.getLocation(), false);
        }
    }

    private void handleCheckpoint(
            FlinkStateSnapshotContext ctx,
            CheckpointFetchResult checkpointInfo,
            FlinkResourceContext<FlinkDeployment> ctxFlinkDeployment,
            String jobId) {
        var resource = ctx.getResource();
        var resourceName = resource.getMetadata().getName();

        if (checkpointInfo.isPending()) {
            LOG.debug(
                    "Checkpoint for {} with ID {} is pending",
                    resourceName,
                    resource.getStatus().getTriggerId());
            return;
        }

        if (checkpointInfo.getError() != null) {
            throw new ReconciliationException(checkpointInfo.getError());
        }

        LOG.debug("Checkpoint {} was successful, querying final checkpoint path...", resourceName);
        var checkpointStatsResult =
                ctxFlinkDeployment
                        .getFlinkService()
                        .fetchCheckpointStats(
                                jobId,
                                checkpointInfo.getCheckpointId(),
                                ctx.getReferencedJobObserveConfig());

        if (checkpointStatsResult.isPending()) {
            return;
        } else if (checkpointStatsResult.getError() != null) {
            throw new ReconciliationException(checkpointStatsResult.getError());
        }

        LOG.info("Checkpoint {} successful: {}", resourceName, checkpointStatsResult.getPath());
        FlinkStateSnapshotUtils.snapshotSuccessful(
                resource, checkpointStatsResult.getPath(), false);
    }
}
