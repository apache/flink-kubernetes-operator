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

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics.AVERAGE;
import static org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics.CURRENT;
import static org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics.JOB_VERTEX_ID;
import static org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics.initRecommendedParallelism;
import static org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics.resetRecommendedParallelism;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link AutoscalerFlinkMetrics} tests. */
public class AutoScalerFlinkMetricsTest {

    public static final String DELIMITER = ".";
    private final JobVertexID jobVertexID = new JobVertexID();
    private JobID jobID;

    private GenericMetricGroup metricGroup;

    private AutoscalerFlinkMetrics metrics;
    private final Map<String, Metric> collectedMetrics = new HashMap<>();

    @BeforeEach
    public void init() {
        TestingMetricRegistry registry =
                TestingMetricRegistry.builder()
                        .setDelimiter(DELIMITER.charAt(0))
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    this.collectedMetrics.put(
                                            group.getMetricIdentifier(name), metric);
                                })
                        .build();
        metricGroup = new GenericMetricGroup(registry, null, "test");

        metrics = new AutoscalerFlinkMetrics(metricGroup);
        jobID = new JobID();
    }

    @Test
    public void testMetricsRegistration() {
        var evaluatedMetrics = new EvaluatedMetrics(Map.of(jobVertexID, testMetrics()), Map.of());
        var lastEvaluatedMetrics = new HashMap<JobID, EvaluatedMetrics>();
        initRecommendedParallelism(evaluatedMetrics.getVertexMetrics());
        lastEvaluatedMetrics.put(jobID, evaluatedMetrics);

        metrics.registerScalingMetrics(List.of(jobVertexID), () -> lastEvaluatedMetrics.get(jobID));
        metrics.registerScalingMetrics(List.of(jobVertexID), () -> lastEvaluatedMetrics.get(jobID));

        assertEquals(1.0, getCurrentMetricValue(PARALLELISM));
        assertEquals(1.0, getCurrentMetricValue(RECOMMENDED_PARALLELISM));
        assertEquals(1000., getCurrentMetricValue(TRUE_PROCESSING_RATE));
        assertEquals(2000., getAverageMetricValue(TRUE_PROCESSING_RATE));
    }

    @Test
    public void testAllVertexScalingMetricsAreRegistered() {
        int numMetricsAlreadyRegistered = collectedMetrics.size();
        metrics.registerScalingMetrics(List.of(jobVertexID), () -> null);
        int numScalingMetrics = 0;
        for (ScalingMetric scalingMetric : ScalingMetric.REPORTED_VERTEX_METRICS) {
            if (scalingMetric.isCalculateAverage()) {
                numScalingMetrics += 2;
            } else {
                numScalingMetrics += 1;
            }
        }
        assertEquals(numMetricsAlreadyRegistered + numScalingMetrics, collectedMetrics.size());
    }

    @Test
    public void testMetricsCleanup() {
        var evaluatedMetrics = new EvaluatedMetrics(Map.of(jobVertexID, testMetrics()), Map.of());
        var lastEvaluatedMetrics = new HashMap<JobID, EvaluatedMetrics>();
        initRecommendedParallelism(evaluatedMetrics.getVertexMetrics());
        lastEvaluatedMetrics.put(jobID, evaluatedMetrics);
        metrics.registerScalingMetrics(List.of(jobVertexID), () -> lastEvaluatedMetrics.get(jobID));

        assertEquals(1.0, getCurrentMetricValue(PARALLELISM));
        assertEquals(1.0, getCurrentMetricValue(RECOMMENDED_PARALLELISM));
        assertEquals(1000., getCurrentMetricValue(TRUE_PROCESSING_RATE));
        assertEquals(2000., getAverageMetricValue(TRUE_PROCESSING_RATE));

        lastEvaluatedMetrics.remove(jobID);
        assertEquals(Double.NaN, getCurrentMetricValue(PARALLELISM));
        assertEquals(Double.NaN, getCurrentMetricValue(RECOMMENDED_PARALLELISM));
        assertEquals(Double.NaN, getCurrentMetricValue(TRUE_PROCESSING_RATE));
        assertEquals(Double.NaN, getAverageMetricValue(TRUE_PROCESSING_RATE));
    }

    @Test
    public void testRecommendedParallelismWithinMetricWindow() {
        var evaluatedMetrics = new EvaluatedMetrics(Map.of(jobVertexID, testMetrics()), Map.of());
        var lastEvaluatedMetrics = new HashMap<JobID, EvaluatedMetrics>();
        initRecommendedParallelism(evaluatedMetrics.getVertexMetrics());
        resetRecommendedParallelism(evaluatedMetrics.getVertexMetrics());
        lastEvaluatedMetrics.put(jobID, evaluatedMetrics);

        metrics.registerScalingMetrics(List.of(jobVertexID), () -> lastEvaluatedMetrics.get(jobID));
        assertEquals(1.0, getCurrentMetricValue(PARALLELISM));
        assertEquals(Double.NaN, getCurrentMetricValue(RECOMMENDED_PARALLELISM));
        assertEquals(1000., getCurrentMetricValue(TRUE_PROCESSING_RATE));
        assertEquals(2000., getAverageMetricValue(TRUE_PROCESSING_RATE));
    }

    @Test
    public void testRecommendedParallelismPastMetricWindow() {
        var evaluatedMetrics = new EvaluatedMetrics(Map.of(jobVertexID, testMetrics()), Map.of());
        var lastEvaluatedMetrics = new HashMap<JobID, EvaluatedMetrics>();
        initRecommendedParallelism(evaluatedMetrics.getVertexMetrics());
        lastEvaluatedMetrics.put(jobID, evaluatedMetrics);

        metrics.registerScalingMetrics(List.of(jobVertexID), () -> lastEvaluatedMetrics.get(jobID));
        assertEquals(1.0, getCurrentMetricValue(PARALLELISM));
        assertEquals(1.0, getCurrentMetricValue(RECOMMENDED_PARALLELISM));
        assertEquals(1000., getCurrentMetricValue(TRUE_PROCESSING_RATE));
        assertEquals(2000., getAverageMetricValue(TRUE_PROCESSING_RATE));
    }

    private static Map<ScalingMetric, EvaluatedScalingMetric> testMetrics() {
        var metrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        metrics.put(PARALLELISM, EvaluatedScalingMetric.of(1));
        metrics.put(ScalingMetric.TRUE_PROCESSING_RATE, new EvaluatedScalingMetric(1000., 2000.));

        return metrics;
    }

    private Object getCurrentMetricValue(ScalingMetric metric) {
        return Optional.ofNullable((Gauge) this.collectedMetrics.get(getCurrentMetricId(metric)))
                .orElse(() -> Double.NaN)
                .getValue();
    }

    private Object getAverageMetricValue(ScalingMetric metric) {
        return Optional.ofNullable((Gauge) this.collectedMetrics.get(getAverageMetricId(metric)))
                .orElse(() -> Double.NaN)
                .getValue();
    }

    private String getCurrentMetricId(ScalingMetric metric) {
        return getMetricId(metric, CURRENT);
    }

    private String getAverageMetricId(ScalingMetric metric) {
        return getMetricId(metric, AVERAGE);
    }

    private String getMetricId(ScalingMetric metric, String classifier) {
        return metricGroup.getMetricIdentifier(
                String.join(
                        DELIMITER,
                        JOB_VERTEX_ID,
                        jobVertexID.toHexString(),
                        metric.name(),
                        classifier));
    }
}
