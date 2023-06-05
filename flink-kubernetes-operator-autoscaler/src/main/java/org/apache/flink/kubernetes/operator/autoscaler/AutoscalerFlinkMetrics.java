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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;

/** Autoscaler metrics for observability. */
public class AutoscalerFlinkMetrics {

    final Counter numScalings;

    final Counter numErrors;

    final Counter numBalanced;

    private final MetricGroup metricGroup;

    private final Map<JobVertexID, MetricGroup> vertexMetricGroups = new ConcurrentHashMap<>();
    private final Map<Tuple2<JobVertexID, ScalingMetric>, MetricGroup> scalingMetricGroups =
            new ConcurrentHashMap<>();
    private final Map<Tuple2<JobVertexID, ScalingMetric>, Gauge<Double>> currentScalingMetrics =
            new ConcurrentHashMap<>();
    private final Map<Tuple2<JobVertexID, ScalingMetric>, Gauge<Double>> averageScalingMetrics =
            new ConcurrentHashMap<>();

    public AutoscalerFlinkMetrics(MetricGroup metricGroup) {
        this.numScalings = metricGroup.counter("scalings");
        this.numErrors = metricGroup.counter("errors");
        this.numBalanced = metricGroup.counter("balanced");
        this.metricGroup = metricGroup;
    }

    public void registerEvaluatedScalingMetrics(
            Supplier<Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
                    currentVertexMetrics) {
        currentVertexMetrics
                .get()
                .forEach(
                        (jobVertexID, evaluatedScalingMetrics) -> {
                            var jobVertexMg = getJobVertexMetricGroup(jobVertexID);
                            evaluatedScalingMetrics.forEach(
                                    (scalingMetric, esm) -> {
                                        var smg =
                                                getScalingMetricGroup(
                                                        jobVertexID, jobVertexMg, scalingMetric);
                                        registerScalingMetric(
                                                currentVertexMetrics,
                                                jobVertexID,
                                                scalingMetric,
                                                smg);
                                    });
                        });
    }

    public void registerRecommendedParallelismMetrics(
            Supplier<Map<JobVertexID, Integer>> recommendedParallelisms) {

        if (recommendedParallelisms == null || recommendedParallelisms.get() == null) {
            return;
        }
        recommendedParallelisms
                .get()
                .forEach(
                        (jobVertexID, parallelism) -> {
                            var jobVertexMg = getJobVertexMetricGroup(jobVertexID);
                            var smg =
                                    getScalingMetricGroup(
                                            jobVertexID, jobVertexMg, RECOMMENDED_PARALLELISM);

                            currentScalingMetrics.computeIfAbsent(
                                    Tuple2.of(jobVertexID, RECOMMENDED_PARALLELISM),
                                    key ->
                                            smg.gauge(
                                                    "Current",
                                                    () ->
                                                            getCurrentValue(
                                                                    recommendedParallelisms,
                                                                    jobVertexID)));
                        });
    }

    private MetricGroup getJobVertexMetricGroup(JobVertexID jobVertexID) {
        return vertexMetricGroups.computeIfAbsent(
                jobVertexID, key -> metricGroup.addGroup("jobVertexID", jobVertexID.toHexString()));
    }

    private MetricGroup getScalingMetricGroup(
            JobVertexID jobVertexID, MetricGroup jobVertexMg, ScalingMetric scalingMetric) {
        return scalingMetricGroups.computeIfAbsent(
                Tuple2.of(jobVertexID, scalingMetric),
                key -> jobVertexMg.addGroup(scalingMetric.name()));
    }

    private void registerScalingMetric(
            Supplier<Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
                    currentVertexMetrics,
            JobVertexID jobVertexID,
            ScalingMetric scalingMetric,
            MetricGroup smg) {

        currentScalingMetrics.computeIfAbsent(
                Tuple2.of(jobVertexID, scalingMetric),
                key ->
                        smg.gauge(
                                "Current",
                                () ->
                                        getCurrentValue(
                                                currentVertexMetrics, jobVertexID, scalingMetric)));
        if (scalingMetric.isCalculateAverage()) {
            averageScalingMetrics.computeIfAbsent(
                    Tuple2.of(jobVertexID, scalingMetric),
                    key ->
                            smg.gauge(
                                    "Average",
                                    () ->
                                            getAverageValue(
                                                    currentVertexMetrics,
                                                    jobVertexID,
                                                    scalingMetric)));
        }
    }

    private static Double getCurrentValue(
            Supplier<Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
                    currentVertexMetrics,
            JobVertexID jobVertexID,
            ScalingMetric scalingMetric) {
        return Optional.ofNullable(currentVertexMetrics.get())
                .map(m -> m.get(jobVertexID))
                .map(metrics -> metrics.get(scalingMetric).getCurrent())
                .orElse(null);
    }

    private static Double getAverageValue(
            Supplier<Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
                    currentVertexMetrics,
            JobVertexID jobVertexID,
            ScalingMetric scalingMetric) {
        return Optional.ofNullable(currentVertexMetrics.get())
                .map(m -> m.get(jobVertexID))
                .map(metrics -> metrics.get(scalingMetric).getAverage())
                .orElse(null);
    }

    private static Double getCurrentValue(
            Supplier<Map<JobVertexID, Integer>> recommendedParallelisms, JobVertexID jobVertexID) {
        return Optional.ofNullable(recommendedParallelisms.get())
                .map(m -> Double.valueOf(m.get(jobVertexID)))
                .orElse(null);
    }
}
