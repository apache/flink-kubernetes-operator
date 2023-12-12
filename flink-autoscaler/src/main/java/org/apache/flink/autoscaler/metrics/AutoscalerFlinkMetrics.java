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

package org.apache.flink.autoscaler.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;

/** Autoscaler metrics for observability. */
public class AutoscalerFlinkMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerFlinkMetrics.class);
    @VisibleForTesting public static final String CURRENT = "Current";
    @VisibleForTesting public static final String AVERAGE = "Average";
    @VisibleForTesting public static final String JOB_VERTEX_ID = "jobVertexID";

    private final Counter numScalings;

    private final Counter numErrors;

    private final Counter numBalanced;

    private final MetricGroup metricGroup;

    private boolean scalingMetricsInitialized;

    public AutoscalerFlinkMetrics(MetricGroup metricGroup) {
        this.numScalings = metricGroup.counter("scalings");
        this.numErrors = metricGroup.counter("errors");
        this.numBalanced = metricGroup.counter("balanced");
        this.metricGroup = metricGroup;
    }

    public void incrementScaling() {
        numScalings.inc();
    }

    public void incrementError() {
        numErrors.inc();
    }

    public void incrementBalanced() {
        numBalanced.inc();
    }

    public void registerScalingMetrics(
            List<JobVertexID> jobVertices, Supplier<EvaluatedMetrics> metricsSupplier) {
        if (scalingMetricsInitialized) {
            // It is important that we only initialize these metrics once because the Flink API does
            // not support registering counters / gauges multiple times.
            // The metrics will be updated via the provided metricsSupplier.
            return;
        }
        scalingMetricsInitialized = true;

        LOG.info("Registering scaling metrics");
        for (JobVertexID jobVertexID : jobVertices) {

            MetricGroup jobVertexGroup =
                    metricGroup.addGroup(JOB_VERTEX_ID, jobVertexID.toHexString());

            for (ScalingMetric scalingMetric : ScalingMetric.REPORTED_VERTEX_METRICS) {
                MetricGroup scalingMetricGroup = jobVertexGroup.addGroup(scalingMetric.name());

                scalingMetricGroup.gauge(
                        CURRENT,
                        () ->
                                Optional.ofNullable(metricsSupplier.get())
                                        .map(EvaluatedMetrics::getVertexMetrics)
                                        .map(m -> m.get(jobVertexID))
                                        .map(metrics -> metrics.get(scalingMetric))
                                        .map(EvaluatedScalingMetric::getCurrent)
                                        .orElse(Double.NaN));

                if (scalingMetric.isCalculateAverage()) {
                    scalingMetricGroup.gauge(
                            AVERAGE,
                            () ->
                                    Optional.ofNullable(metricsSupplier.get())
                                            .map(EvaluatedMetrics::getVertexMetrics)
                                            .map(m -> m.get(jobVertexID))
                                            .map(metrics -> metrics.get(scalingMetric))
                                            .map(EvaluatedScalingMetric::getAverage)
                                            .orElse(Double.NaN));
                }
            }
        }
    }

    public static void initRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        evaluatedMetrics.forEach(
                (jobVertexID, evaluatedScalingMetricMap) ->
                        evaluatedScalingMetricMap.put(
                                RECOMMENDED_PARALLELISM,
                                evaluatedScalingMetricMap.get(PARALLELISM)));
    }

    @VisibleForTesting
    public static void resetRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        evaluatedMetrics.forEach(
                (jobVertexID, evaluatedScalingMetricMap) ->
                        evaluatedScalingMetricMap.put(RECOMMENDED_PARALLELISM, null));
    }

    @VisibleForTesting
    public long getNumScalingsCount() {
        return numScalings.getCount();
    }

    @VisibleForTesting
    public long getNumErrorsCount() {
        return numErrors.getCount();
    }

    @VisibleForTesting
    public long getNumBalancedCount() {
        return numBalanced.getCount();
    }

    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }
}
