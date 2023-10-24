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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_EVENT_INTERVAL;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.addToScalingHistoryAndStore;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.getTrimmedScalingHistory;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.SCALE_DOWN_RATE_THRESHOLD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.SCALE_UP_RATE_THRESHOLD;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;

/** Class responsible for executing scaling decisions. */
public class ScalingExecutor<KEY, Context extends JobAutoScalerContext<KEY>> {
    private static final Logger LOG = LoggerFactory.getLogger(ScalingExecutor.class);

    private final JobVertexScaler<KEY, Context> jobVertexScaler;
    private final AutoScalerEventHandler<KEY, Context> autoScalerEventHandler;
    private final AutoScalerStateStore<KEY, Context> autoScalerStateStore;
    private Clock clock = Clock.system(ZoneId.systemDefault());

    public ScalingExecutor(
            AutoScalerEventHandler<KEY, Context> autoScalerEventHandler,
            AutoScalerStateStore<KEY, Context> autoScalerStateStore) {
        this(
                new JobVertexScaler<>(autoScalerEventHandler),
                autoScalerEventHandler,
                autoScalerStateStore);
    }

    public ScalingExecutor(
            JobVertexScaler<KEY, Context> jobVertexScaler,
            AutoScalerEventHandler<KEY, Context> autoScalerEventHandler,
            AutoScalerStateStore<KEY, Context> autoScalerStateStore) {
        this.jobVertexScaler = jobVertexScaler;
        this.autoScalerEventHandler = autoScalerEventHandler;
        this.autoScalerStateStore = autoScalerStateStore;
    }

    public boolean scaleResource(
            Context context,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics)
            throws Exception {

        var conf = context.getConfiguration();
        var scalingHistory = getTrimmedScalingHistory(autoScalerStateStore, context, Instant.now());
        var scalingSummaries = computeScalingSummary(context, evaluatedMetrics, scalingHistory);

        if (scalingSummaries.isEmpty()) {
            LOG.info("All job vertices are currently running at their target parallelism.");
            return false;
        }

        if (allVerticesWithinUtilizationTarget(evaluatedMetrics, scalingSummaries)) {
            return false;
        }

        updateRecommendedParallelism(evaluatedMetrics, scalingSummaries);

        var scaleEnabled = conf.get(SCALING_ENABLED);
        autoScalerEventHandler.handleScalingEvent(
                context, scalingSummaries, scaleEnabled, conf.get(SCALING_EVENT_INTERVAL));

        if (!scaleEnabled) {
            return false;
        }

        addToScalingHistoryAndStore(
                autoScalerStateStore, context, scalingHistory, clock.instant(), scalingSummaries);
        autoScalerStateStore.storeParallelismOverrides(
                context, getVertexParallelismOverrides(evaluatedMetrics, scalingSummaries));

        return true;
    }

    private void updateRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {
        scalingSummaries.forEach(
                (jobVertexID, scalingSummary) ->
                        evaluatedMetrics
                                .get(jobVertexID)
                                .put(
                                        ScalingMetric.RECOMMENDED_PARALLELISM,
                                        EvaluatedScalingMetric.of(
                                                scalingSummary.getNewParallelism())));
    }

    protected static boolean allVerticesWithinUtilizationTarget(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> scalingSummaries) {

        for (Map.Entry<JobVertexID, ScalingSummary> entry : scalingSummaries.entrySet()) {
            var vertex = entry.getKey();
            var metrics = evaluatedMetrics.get(vertex);

            double processingRate = metrics.get(TRUE_PROCESSING_RATE).getAverage();
            double scaleUpRateThreshold = metrics.get(SCALE_UP_RATE_THRESHOLD).getCurrent();
            double scaleDownRateThreshold = metrics.get(SCALE_DOWN_RATE_THRESHOLD).getCurrent();

            if (processingRate < scaleUpRateThreshold || processingRate > scaleDownRateThreshold) {
                LOG.debug(
                        "Vertex {} processing rate {} is outside ({}, {})",
                        vertex,
                        processingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
                return false;
            } else {
                LOG.debug(
                        "Vertex {} processing rate {} is within target ({}, {})",
                        vertex,
                        processingRate,
                        scaleUpRateThreshold,
                        scaleDownRateThreshold);
            }
        }
        LOG.info("All vertex processing rates are within target.");
        return true;
    }

    @VisibleForTesting
    Map<JobVertexID, ScalingSummary> computeScalingSummary(
            Context context,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {

        var out = new HashMap<JobVertexID, ScalingSummary>();
        var excludeVertexIdList =
                context.getConfiguration().get(AutoScalerOptions.VERTEX_EXCLUDE_IDS);
        evaluatedMetrics.forEach(
                (v, metrics) -> {
                    if (excludeVertexIdList.contains(v.toHexString())) {
                        LOG.debug(
                                "Vertex {} is part of `vertex.exclude.ids` config, Ignoring it for scaling",
                                v);
                    } else {
                        var currentParallelism =
                                (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent();
                        var newParallelism =
                                jobVertexScaler.computeScaleTargetParallelism(
                                        context,
                                        v,
                                        metrics,
                                        scalingHistory.getOrDefault(
                                                v, Collections.emptySortedMap()));
                        if (currentParallelism != newParallelism) {
                            out.put(
                                    v,
                                    new ScalingSummary(
                                            currentParallelism, newParallelism, metrics));
                        }
                    }
                });

        return out;
    }

    private static Map<String, String> getVertexParallelismOverrides(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics,
            Map<JobVertexID, ScalingSummary> summaries) {
        var overrides = new HashMap<String, String>();
        evaluatedMetrics.forEach(
                (id, metrics) -> {
                    if (summaries.containsKey(id)) {
                        overrides.put(
                                id.toString(),
                                String.valueOf(summaries.get(id).getNewParallelism()));
                    } else {
                        overrides.put(
                                id.toString(),
                                String.valueOf(
                                        (int) metrics.get(ScalingMetric.PARALLELISM).getCurrent()));
                    }
                });
        return overrides;
    }

    @VisibleForTesting
    protected void setClock(Clock clock) {
        this.clock = Preconditions.checkNotNull(clock);
        jobVertexScaler.setClock(clock);
    }
}
