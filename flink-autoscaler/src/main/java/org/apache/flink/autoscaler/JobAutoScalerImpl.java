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

package org.apache.flink.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.exceptions.NotReadyException;
import org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics;
import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_EVENT_INTERVAL;

/** The default implementation of {@link JobAutoScaler}. */
public class JobAutoScalerImpl<KEY, Context extends JobAutoScalerContext<KEY>>
        implements JobAutoScaler<KEY, Context> {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

    @VisibleForTesting protected static final String AUTOSCALER_ERROR = "AutoscalerError";

    private final ScalingMetricCollector<KEY, Context> metricsCollector;
    @VisibleForTesting final ScalingMetricEvaluator<KEY, Context> metricsEvaluator;
    private final ScalingExecutor<KEY, Context> scalingExecutor;
    private final AutoScalerEventHandler<KEY, Context> eventHandler;
    private final ScalingRealizer<KEY, Context> scalingRealizer;
    private final AutoScalerStateStore<KEY, Context> stateStore;

    private Clock clock = Clock.systemDefaultZone();

    @VisibleForTesting
    final Map<KEY, AutoscalerFlinkMetrics> flinkMetrics = new ConcurrentHashMap<>();

    public JobAutoScalerImpl(
            ScalingMetricCollector<KEY, Context> metricsCollector,
            ScalingMetricEvaluator<KEY, Context> metricsEvaluator,
            ScalingExecutor<KEY, Context> scalingExecutor,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            ScalingRealizer<KEY, Context> scalingRealizer,
            AutoScalerStateStore<KEY, Context> stateStore) {
        this.metricsCollector = metricsCollector;
        this.metricsEvaluator = metricsEvaluator;
        this.scalingExecutor = scalingExecutor;
        this.eventHandler = eventHandler;
        this.scalingRealizer = scalingRealizer;
        this.stateStore = stateStore;
    }

    @Override
    public void scale(Context ctx) throws Exception {
        var autoscalerMetrics = getOrInitAutoscalerFlinkMetrics(ctx);

        try {
            if (!ctx.getConfiguration().get(AUTOSCALER_ENABLED)) {
                LOG.debug("Autoscaler is disabled");
                stateStore.clearAll(ctx);
                stateStore.flush(ctx);
                return;
            }

            if (ctx.getJobStatus() != JobStatus.RUNNING) {
                LOG.debug("Autoscaler is waiting for stable, running state");
                metricsEvaluator.cleanup(ctx.getJobKey());
                return;
            }

            runScalingLogic(ctx, autoscalerMetrics);
            stateStore.flush(ctx);
        } catch (NotReadyException e) {
            LOG.debug("Not ready for scaling", e);
        } catch (Throwable e) {
            onError(ctx, autoscalerMetrics, e);
        } finally {
            try {
                applyParallelismOverrides(ctx);
                applyConfigOverrides(ctx);
            } catch (Exception e) {
                LOG.error("Error applying overrides.", e);
                onError(ctx, autoscalerMetrics, e);
            }
        }
    }

    @Override
    public void cleanup(Context ctx) {
        LOG.info("Cleaning up autoscaling meta data");
        metricsCollector.cleanup(ctx.getJobKey());
        metricsEvaluator.cleanup(ctx.getJobKey());
        flinkMetrics.remove(ctx.getJobKey());
        try {
            stateStore.clearAll(ctx);
            stateStore.flush(ctx);
        } catch (Exception e) {
            LOG.error("Error cleaning up autoscaling meta data for {}", ctx.getJobKey(), e);
        }
    }

    @VisibleForTesting
    protected Map<String, String> getParallelismOverrides(Context ctx) throws Exception {
        return stateStore.getParallelismOverrides(ctx);
    }

    /**
     * If there are any parallelism overrides by the {@link JobAutoScaler} apply them to the
     * scalingRealizer.
     *
     * @param ctx Job context
     */
    @VisibleForTesting
    protected void applyParallelismOverrides(Context ctx) throws Exception {
        var overrides = getParallelismOverrides(ctx);
        if (overrides.isEmpty()) {
            return;
        }
        LOG.debug("Applying parallelism overrides: {}", overrides);

        var conf = ctx.getConfiguration();
        var userOverrides = new HashMap<>(conf.get(PipelineOptions.PARALLELISM_OVERRIDES));
        var exclusions = conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS);

        overrides.forEach(
                (k, v) -> {
                    // Respect user override for excluded vertices
                    if (exclusions.contains(k)) {
                        userOverrides.putIfAbsent(k, v);
                    } else {
                        userOverrides.put(k, v);
                    }
                });
        scalingRealizer.realizeParallelismOverrides(ctx, userOverrides);
    }

    @VisibleForTesting
    void applyConfigOverrides(Context ctx) throws Exception {
        if (!ctx.getConfiguration().get(AutoScalerOptions.MEMORY_TUNING_ENABLED)) {
            return;
        }

        ConfigChanges configChanges = stateStore.getConfigChanges(ctx);
        LOG.debug("Applying config overrides: {}", configChanges);
        scalingRealizer.realizeConfigOverrides(ctx, configChanges);
    }

    private void runScalingLogic(Context ctx, AutoscalerFlinkMetrics autoscalerMetrics)
            throws Exception {
        var cycleState = ctx.getScalingCycleState();
        cycleState.setNow(clock.instant());
        cycleState.setAutoscalerMetrics(autoscalerMetrics);

        if (!metricsCollector.collect(ctx)) {
            return;
        }
        if (!metricsEvaluator.evaluate(ctx)) {
            return;
        }
        var parallelismChanged = scalingExecutor.execute(ctx);
        var sourceAssignmentRebalanceTriggered = false;
        if (!parallelismChanged) {
            var delayedScaleDown = stateStore.getDelayedScaleDown(ctx);
            var collectedMetrics = cycleState.getCollectedMetrics();
            var rebalanceRequest =
                    scalingExecutor.getSourceAssignmentRebalanceRequest(
                            ctx,
                            collectedMetrics.getJobTopology(),
                            collectedMetrics.getMetricHistory(),
                            delayedScaleDown,
                            cycleState.getNow());
            if (rebalanceRequest.isPresent()) {
                // Persist the stable nonce before mutating desired state. A later pass can retry
                // the same nonce if reconciliation fails after the realizer accepts it.
                if (delayedScaleDown.isUpdated()) {
                    stateStore.storeDelayedScaleDown(ctx, delayedScaleDown);
                    stateStore.flush(ctx);
                }

                var effectiveRequestId =
                        scalingRealizer.realizeSourceAssignmentRebalance(
                                ctx, rebalanceRequest.get().getRequestId());
                if (effectiveRequestId.isPresent()) {
                    if (effectiveRequestId.getAsLong() != rebalanceRequest.get().getRequestId()) {
                        delayedScaleDown.replaceSourceAssignmentRebalanceRequestId(
                                effectiveRequestId.getAsLong());
                    }
                    sourceAssignmentRebalanceTriggered =
                            !delayedScaleDown.isSourceAssignmentRebalanceTriggered();
                    delayedScaleDown.markSourceAssignmentRebalanceTriggered();
                    if (sourceAssignmentRebalanceTriggered) {
                        eventHandler.handleEvent(
                                ctx,
                                AutoScalerEventHandler.Type.Normal,
                                ScalingExecutor.SOURCE_ASSIGNMENT_REBALANCE_REASON,
                                String.format(
                                        "Covered dynamic source assignment holes in vertices %s "
                                                + "with controlled same-parallelism recovery.",
                                        rebalanceRequest.get().getVertices()),
                                ScalingExecutor.SOURCE_ASSIGNMENT_REBALANCE_REASON,
                                ctx.getConfiguration().get(SCALING_EVENT_INTERVAL));
                    }
                }
            }

            if (delayedScaleDown.isUpdated()) {
                stateStore.storeDelayedScaleDown(ctx, delayedScaleDown);
            }
        }

        if (parallelismChanged || sourceAssignmentRebalanceTriggered) {
            autoscalerMetrics.incrementScaling();
        } else {
            autoscalerMetrics.incrementBalanced();
        }
    }

    private void onError(Context ctx, AutoscalerFlinkMetrics autoscalerMetrics, Throwable e) {
        LOG.error("Error while scaling job", e);
        autoscalerMetrics.incrementError();
        eventHandler.handleException(ctx, AUTOSCALER_ERROR, e);
    }

    private AutoscalerFlinkMetrics getOrInitAutoscalerFlinkMetrics(Context ctx) {
        return this.flinkMetrics.computeIfAbsent(
                ctx.getJobKey(),
                id -> new AutoscalerFlinkMetrics(ctx.getMetricGroup().addGroup("AutoScaler")));
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
        this.metricsCollector.setClock(clock);
        this.scalingExecutor.setClock(clock);
    }
}
