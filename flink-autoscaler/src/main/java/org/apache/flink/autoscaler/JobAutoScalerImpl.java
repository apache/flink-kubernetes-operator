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
import org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics.initRecommendedParallelism;
import static org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics.resetRecommendedParallelism;

/** The default implementation of {@link JobAutoScaler}. */
public class JobAutoScalerImpl<KEY, Context extends JobAutoScalerContext<KEY>>
        implements JobAutoScaler<KEY, Context> {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

    @VisibleForTesting protected static final String AUTOSCALER_ERROR = "AutoscalerError";

    private final ScalingMetricCollector<KEY, Context> metricsCollector;
    private final ScalingMetricEvaluator evaluator;
    private final ScalingExecutor<KEY, Context> scalingExecutor;
    private final AutoScalerEventHandler<KEY, Context> eventHandler;
    private final ScalingRealizer<KEY, Context> scalingRealizer;
    private final AutoScalerStateStore<KEY, Context> stateStore;

    @VisibleForTesting
    final Map<KEY, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
            lastEvaluatedMetrics = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<KEY, AutoscalerFlinkMetrics> flinkMetrics = new ConcurrentHashMap<>();

    public JobAutoScalerImpl(
            ScalingMetricCollector<KEY, Context> metricsCollector,
            ScalingMetricEvaluator evaluator,
            ScalingExecutor<KEY, Context> scalingExecutor,
            AutoScalerEventHandler<KEY, Context> eventHandler,
            ScalingRealizer<KEY, Context> scalingRealizer,
            AutoScalerStateStore<KEY, Context> stateStore) {
        this.metricsCollector = metricsCollector;
        this.evaluator = evaluator;
        this.scalingExecutor = scalingExecutor;
        this.eventHandler = eventHandler;
        this.scalingRealizer = scalingRealizer;
        this.stateStore = stateStore;
    }

    @Override
    public void scale(Context ctx) throws Exception {
        var autoscalerMetrics = getOrInitAutoscalerFlinkMetrics(ctx);

        try {
            if (!ctx.getConfiguration().getBoolean(AUTOSCALER_ENABLED)) {
                LOG.debug("Autoscaler is disabled");
                clearParallelismOverrides(ctx);
                return;
            }

            if (ctx.getJobStatus() != JobStatus.RUNNING) {
                lastEvaluatedMetrics.remove(ctx.getJobKey());
                return;
            }

            runScalingLogic(ctx, autoscalerMetrics);
            stateStore.flush(ctx);
        } catch (Throwable e) {
            onError(ctx, autoscalerMetrics, e);
        } finally {
            applyParallelismOverrides(ctx);
        }
    }

    @Override
    public void cleanup(KEY jobKey) {
        LOG.info("Cleaning up autoscaling meta data");
        metricsCollector.cleanup(jobKey);
        lastEvaluatedMetrics.remove(jobKey);
        flinkMetrics.remove(jobKey);
        stateStore.removeInfoFromCache(jobKey);
    }

    private void clearParallelismOverrides(Context ctx) throws Exception {
        var parallelismOverrides = stateStore.getParallelismOverrides(ctx);
        if (parallelismOverrides.isPresent()) {
            stateStore.removeParallelismOverrides(ctx);
            stateStore.flush(ctx);
        }
    }

    @VisibleForTesting
    protected Optional<Map<String, String>> getParallelismOverrides(Context ctx) throws Exception {
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
        var overridesOpt = getParallelismOverrides(ctx);
        if (overridesOpt.isEmpty() || overridesOpt.get().isEmpty()) {
            return;
        }
        Map<String, String> overrides = overridesOpt.get();
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
        scalingRealizer.realize(ctx, userOverrides);
    }

    private void runScalingLogic(Context ctx, AutoscalerFlinkMetrics autoscalerMetrics)
            throws Exception {

        var collectedMetrics = metricsCollector.updateMetrics(ctx, stateStore);

        if (collectedMetrics.getMetricHistory().isEmpty()) {
            return;
        }
        LOG.debug("Collected metrics: {}", collectedMetrics);

        var evaluatedMetrics = evaluator.evaluate(ctx.getConfiguration(), collectedMetrics);
        LOG.debug("Evaluated metrics: {}", evaluatedMetrics);
        lastEvaluatedMetrics.put(ctx.getJobKey(), evaluatedMetrics);

        initRecommendedParallelism(evaluatedMetrics);
        autoscalerMetrics.registerScalingMetrics(
                collectedMetrics.getJobTopology().getVerticesInTopologicalOrder(),
                () -> lastEvaluatedMetrics.get(ctx.getJobKey()));

        if (!collectedMetrics.isFullyCollected()) {
            // We have done an upfront evaluation, but we are not ready for scaling.
            resetRecommendedParallelism(evaluatedMetrics);
            return;
        }

        var parallelismChanged = scalingExecutor.scaleResource(ctx, evaluatedMetrics);

        if (parallelismChanged) {
            autoscalerMetrics.incrementScaling();
        } else {
            autoscalerMetrics.incrementBalanced();
        }
    }

    private void onError(Context ctx, AutoscalerFlinkMetrics autoscalerMetrics, Throwable e) {
        LOG.error("Error while scaling job", e);
        autoscalerMetrics.incrementError();
        eventHandler.handleEvent(
                ctx,
                AutoScalerEventHandler.Type.Warning,
                AUTOSCALER_ERROR,
                e.getMessage(),
                null,
                null);
    }

    private AutoscalerFlinkMetrics getOrInitAutoscalerFlinkMetrics(Context ctx) {
        return this.flinkMetrics.computeIfAbsent(
                ctx.getJobKey(),
                id -> new AutoscalerFlinkMetrics(ctx.getMetricGroup().addGroup("AutoScaler")));
    }
}
