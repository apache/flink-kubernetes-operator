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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.kubernetes.operator.autoscaler.utils.AutoScalerUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.commons.math3.stat.StatUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.BACKLOG_PROCESSING_LAG_THRESHOLD;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.CATCH_UP_DATA_RATE;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.CURRENT_PROCESSING_RATE;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.LAG;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.MAX_PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.OUTPUT_RATIO;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.SCALE_DOWN_RATE_THRESHOLD;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.SCALE_UP_RATE_THRESHOLD;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.SOURCE_DATA_RATE;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.TARGET_DATA_RATE;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.TRUE_OUTPUT_RATE;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;

/** Job scaling evaluator for autoscaler. */
public class ScalingMetricEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(ScalingMetricEvaluator.class);

    public Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluate(
            Configuration conf, CollectedMetrics collectedMetrics) {

        var scalingOutput = new HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>();
        var metricsHistory = collectedMetrics.getMetricHistory();
        var topology = collectedMetrics.getJobTopology();

        boolean processingBacklog = isProcessingBacklog(topology, metricsHistory, conf);

        for (var vertex : topology.getVerticesInTopologicalOrder()) {
            scalingOutput.put(
                    vertex,
                    computeVertexScalingSummary(
                            conf,
                            scalingOutput,
                            metricsHistory,
                            topology,
                            vertex,
                            processingBacklog));
        }

        return scalingOutput;
    }

    @VisibleForTesting
    protected static boolean isProcessingBacklog(
            JobTopology topology,
            SortedMap<Instant, Map<JobVertexID, Map<ScalingMetric, Double>>> metricsHistory,
            Configuration conf) {
        var lastMetrics = metricsHistory.get(metricsHistory.lastKey());
        return topology.getVerticesInTopologicalOrder().stream()
                .filter(topology::isSource)
                .anyMatch(
                        vertex -> {
                            double lag = lastMetrics.get(vertex).getOrDefault(LAG, 0.0);
                            double avgProcRate =
                                    getAverage(CURRENT_PROCESSING_RATE, vertex, metricsHistory);
                            if (Double.isNaN(avgProcRate)) {
                                return false;
                            }
                            double lagSeconds = lag / avgProcRate;
                            if (lagSeconds
                                    > conf.get(BACKLOG_PROCESSING_LAG_THRESHOLD).toSeconds()) {
                                LOG.info("Currently processing backlog at source {}", vertex);
                                return true;
                            } else {
                                return false;
                            }
                        });
    }

    @NotNull
    private Map<ScalingMetric, EvaluatedScalingMetric> computeVertexScalingSummary(
            Configuration conf,
            HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> scalingOutput,
            SortedMap<Instant, Map<JobVertexID, Map<ScalingMetric, Double>>> metricsHistory,
            JobTopology topology,
            JobVertexID vertex,
            boolean processingBacklog) {

        var latestVertexMetrics = metricsHistory.get(metricsHistory.lastKey()).get(vertex);

        var evaluatedMetrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        computeTargetDataRate(
                topology,
                vertex,
                conf,
                scalingOutput,
                metricsHistory,
                latestVertexMetrics,
                evaluatedMetrics);

        evaluatedMetrics.put(
                TRUE_PROCESSING_RATE,
                new EvaluatedScalingMetric(
                        latestVertexMetrics.get(TRUE_PROCESSING_RATE),
                        getAverage(TRUE_PROCESSING_RATE, vertex, metricsHistory)));

        evaluatedMetrics.put(
                PARALLELISM, EvaluatedScalingMetric.of(topology.getParallelisms().get(vertex)));
        evaluatedMetrics.put(
                MAX_PARALLELISM,
                EvaluatedScalingMetric.of(topology.getMaxParallelisms().get(vertex)));

        computeProcessingRateThresholds(evaluatedMetrics, conf, processingBacklog);

        var isSink = topology.getOutputs().get(vertex).isEmpty();
        if (!isSink) {
            evaluatedMetrics.put(
                    TRUE_OUTPUT_RATE,
                    new EvaluatedScalingMetric(
                            latestVertexMetrics.get(TRUE_OUTPUT_RATE),
                            getAverage(TRUE_OUTPUT_RATE, vertex, metricsHistory)));
            evaluatedMetrics.put(
                    OUTPUT_RATIO,
                    new EvaluatedScalingMetric(
                            latestVertexMetrics.get(OUTPUT_RATIO),
                            getAverage(OUTPUT_RATIO, vertex, metricsHistory)));
        }

        return evaluatedMetrics;
    }

    @VisibleForTesting
    protected static void computeProcessingRateThresholds(
            Map<ScalingMetric, EvaluatedScalingMetric> metrics,
            Configuration conf,
            boolean processingBacklog) {

        double utilizationBoundary = conf.getDouble(TARGET_UTILIZATION_BOUNDARY);
        double targetUtilization = conf.get(TARGET_UTILIZATION);

        double upperUtilization;
        double lowerUtilization;

        if (processingBacklog) {
            // When we are processing backlog we allow max utilization and we do not trigger scale
            // down on under utilization to avoid creating more lag.
            upperUtilization = 1.0;
            lowerUtilization = 0.0;
        } else {
            upperUtilization = targetUtilization + utilizationBoundary;
            lowerUtilization = targetUtilization - utilizationBoundary;
        }

        double scaleUpThreshold =
                AutoScalerUtils.getTargetProcessingCapacity(metrics, conf, upperUtilization, false);

        double scaleDownThreshold =
                AutoScalerUtils.getTargetProcessingCapacity(metrics, conf, lowerUtilization, true);

        metrics.put(SCALE_UP_RATE_THRESHOLD, EvaluatedScalingMetric.of(scaleUpThreshold));
        metrics.put(SCALE_DOWN_RATE_THRESHOLD, EvaluatedScalingMetric.of(scaleDownThreshold));
    }

    private void computeTargetDataRate(
            JobTopology topology,
            JobVertexID vertex,
            Configuration conf,
            HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> alreadyEvaluated,
            SortedMap<Instant, Map<JobVertexID, Map<ScalingMetric, Double>>> metricsHistory,
            Map<ScalingMetric, Double> latestVertexMetrics,
            Map<ScalingMetric, EvaluatedScalingMetric> out) {

        if (topology.isSource(vertex)) {
            double catchUpTargetSec = conf.get(AutoScalerOptions.CATCH_UP_DURATION).toSeconds();

            var sourceRateMetric =
                    latestVertexMetrics.containsKey(TARGET_DATA_RATE)
                            ? TARGET_DATA_RATE
                            : SOURCE_DATA_RATE;
            if (!latestVertexMetrics.containsKey(sourceRateMetric)) {
                throw new RuntimeException(
                        "Cannot evaluate metrics without source target rate information");
            }

            out.put(
                    TARGET_DATA_RATE,
                    new EvaluatedScalingMetric(
                            latestVertexMetrics.get(sourceRateMetric),
                            getAverage(sourceRateMetric, vertex, metricsHistory)));

            double lag = latestVertexMetrics.getOrDefault(LAG, 0.);
            double catchUpInputRate = catchUpTargetSec == 0 ? 0 : lag / catchUpTargetSec;
            if (catchUpInputRate > 0) {
                LOG.debug(
                        "Extra backlog processing input rate for {} is {}",
                        vertex,
                        catchUpInputRate);
            }
            out.put(CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(catchUpInputRate));
        } else {
            var inputs = topology.getInputs().get(vertex);
            double sumCurrentTargetRate = 0;
            double sumAvgTargetRate = 0;
            double sumCatchUpDataRate = 0;
            for (var inputVertex : inputs) {
                var inputEvaluatedMetrics = alreadyEvaluated.get(inputVertex);
                var inputTargetRate = inputEvaluatedMetrics.get(TARGET_DATA_RATE);
                var outputRateMultiplier = inputEvaluatedMetrics.get(OUTPUT_RATIO).getAverage();
                sumCurrentTargetRate += inputTargetRate.getCurrent() * outputRateMultiplier;
                sumAvgTargetRate += inputTargetRate.getAverage() * outputRateMultiplier;
                sumCatchUpDataRate +=
                        inputEvaluatedMetrics.get(CATCH_UP_DATA_RATE).getCurrent()
                                * outputRateMultiplier;
            }
            out.put(
                    TARGET_DATA_RATE,
                    new EvaluatedScalingMetric(sumCurrentTargetRate, sumAvgTargetRate));
            out.put(CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(sumCatchUpDataRate));
        }
    }

    private static double getAverage(
            ScalingMetric metric,
            JobVertexID jobVertexId,
            SortedMap<Instant, Map<JobVertexID, Map<ScalingMetric, Double>>> metricsHistory) {
        return StatUtils.mean(
                metricsHistory.values().stream()
                        .map(m -> m.get(jobVertexId))
                        .filter(m -> m.containsKey(metric))
                        .mapToDouble(m -> m.get(metric))
                        .filter(d -> !Double.isNaN(d))
                        .toArray());
    }
}
