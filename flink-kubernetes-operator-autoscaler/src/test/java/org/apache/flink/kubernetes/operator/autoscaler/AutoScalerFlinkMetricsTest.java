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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.metrics.TestingMetricListener;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFlinkMetrics.AVERAGE;
import static org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFlinkMetrics.CURRENT;
import static org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFlinkMetrics.JOB_VERTEX_ID;
import static org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFlinkMetrics.initRecommendedParallelism;
import static org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFlinkMetrics.resetRecommendedParallelism;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.TRUE_PROCESSING_RATE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link AutoscalerFlinkMetrics} tests. */
public class AutoScalerFlinkMetricsTest {

    private final Configuration configuration = new Configuration();
    private final JobVertexID jobVertexID = new JobVertexID();
    private ResourceID resourceID;
    private TestingMetricListener listener;
    private AutoscalerFlinkMetrics metrics;

    @BeforeEach
    public void init() {
        listener = new TestingMetricListener(configuration);
        metrics = new AutoscalerFlinkMetrics(listener.getMetricGroup());
        resourceID = ResourceID.fromResource(TestUtils.buildApplicationCluster());
    }

    @Test
    public void testMetricsRegistration() {
        var evaluatedMetrics = Map.of(jobVertexID, testMetrics());
        var lastEvaluatedMetrics =
                new HashMap<
                        ResourceID, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>();
        initRecommendedParallelism(evaluatedMetrics);
        lastEvaluatedMetrics.put(resourceID, evaluatedMetrics);

        metrics.registerScalingMetrics(
                () -> List.of(jobVertexID), () -> lastEvaluatedMetrics.get(resourceID));
        metrics.registerScalingMetrics(
                () -> List.of(jobVertexID), () -> lastEvaluatedMetrics.get(resourceID));

        assertEquals(1.0, getCurrentMetricValue(PARALLELISM));
        assertEquals(1.0, getCurrentMetricValue(RECOMMENDED_PARALLELISM));
        assertEquals(1000., getCurrentMetricValue(TRUE_PROCESSING_RATE));
        assertEquals(2000., getAverageMetricValue(TRUE_PROCESSING_RATE));
    }

    @Test
    public void testAllScalingMetricsAreRegistered() {
        int numMetricsAlreadyRegistered = listener.size();
        metrics.registerScalingMetrics(() -> List.of(jobVertexID), () -> null);
        int numScalingMetrics = 0;
        for (ScalingMetric scalingMetric : ScalingMetric.values()) {
            if (scalingMetric.isCalculateAverage()) {
                numScalingMetrics += 2;
            } else {
                numScalingMetrics += 1;
            }
        }
        assertEquals(numMetricsAlreadyRegistered + numScalingMetrics, listener.size());
    }

    @Test
    public void testMetricsCleanup() {
        var evaluatedMetrics = Map.of(jobVertexID, testMetrics());
        var lastEvaluatedMetrics =
                new HashMap<
                        ResourceID, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>();
        initRecommendedParallelism(evaluatedMetrics);
        lastEvaluatedMetrics.put(resourceID, evaluatedMetrics);
        metrics.registerScalingMetrics(
                () -> List.of(jobVertexID), () -> lastEvaluatedMetrics.get(resourceID));

        assertEquals(1.0, getCurrentMetricValue(PARALLELISM));
        assertEquals(1.0, getCurrentMetricValue(RECOMMENDED_PARALLELISM));
        assertEquals(1000., getCurrentMetricValue(TRUE_PROCESSING_RATE));
        assertEquals(2000., getAverageMetricValue(TRUE_PROCESSING_RATE));

        lastEvaluatedMetrics.remove(resourceID);
        assertEquals(Double.NaN, getCurrentMetricValue(PARALLELISM));
        assertEquals(Double.NaN, getCurrentMetricValue(RECOMMENDED_PARALLELISM));
        assertEquals(Double.NaN, getCurrentMetricValue(TRUE_PROCESSING_RATE));
        assertEquals(Double.NaN, getAverageMetricValue(TRUE_PROCESSING_RATE));
    }

    @Test
    public void testRecommendedParallelismWithinMetricWindow() {
        var evaluatedMetrics = Map.of(jobVertexID, testMetrics());
        var lastEvaluatedMetrics =
                new HashMap<
                        ResourceID, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>();
        initRecommendedParallelism(evaluatedMetrics);
        resetRecommendedParallelism(evaluatedMetrics);
        lastEvaluatedMetrics.put(resourceID, evaluatedMetrics);

        metrics.registerScalingMetrics(
                () -> List.of(jobVertexID), () -> lastEvaluatedMetrics.get(resourceID));
        assertEquals(1.0, getCurrentMetricValue(PARALLELISM));
        assertEquals(Double.NaN, getCurrentMetricValue(RECOMMENDED_PARALLELISM));
        assertEquals(1000., getCurrentMetricValue(TRUE_PROCESSING_RATE));
        assertEquals(2000., getAverageMetricValue(TRUE_PROCESSING_RATE));
    }

    @Test
    public void testRecommendedParallelismPastMetricWindow() {
        var evaluatedMetrics = Map.of(jobVertexID, testMetrics());
        var lastEvaluatedMetrics =
                new HashMap<
                        ResourceID, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>();
        initRecommendedParallelism(evaluatedMetrics);
        lastEvaluatedMetrics.put(resourceID, evaluatedMetrics);

        metrics.registerScalingMetrics(
                () -> List.of(jobVertexID), () -> lastEvaluatedMetrics.get(resourceID));
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
        return listener.getGauge(getCurrentMetricId(metric)).orElse(() -> Double.NaN).getValue();
    }

    private Object getAverageMetricValue(ScalingMetric metric) {
        return listener.getGauge(getAverageMetricId(metric)).get().getValue();
    }

    private String getCurrentMetricId(ScalingMetric metric) {
        return getMetricId(metric, CURRENT);
    }

    private String getAverageMetricId(ScalingMetric metric) {
        return getMetricId(metric, AVERAGE);
    }

    private String getMetricId(ScalingMetric metric, String classifier) {
        return listener.getMetricId(
                JOB_VERTEX_ID, jobVertexID.toHexString(), metric.name(), classifier);
    }
}
