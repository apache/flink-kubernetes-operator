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

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.topology.IOMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for scaling metrics computation logic. */
public class ScalingMetricsTest {

    private static final double PREV_TPR = 123;
    private static final JobVertexID SOURCE = new JobVertexID();

    @Test
    public void testProcessingAndOutputMetrics() {
        var source = new JobVertexID();
        var op = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(
                                source, Collections.emptyMap(), 1, 1, new IOMetrics(1, 2, 3)),
                        new VertexInfo(
                                op, Map.of(source, REBALANCE), 1, 1, new IOMetrics(1, 2, 3)));

        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        ScalingMetrics.computeDataRateMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric(
                                "", Double.NaN, 900., Double.NaN, Double.NaN, Double.NaN),
                        FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC,
                        aggSum(1000.)),
                scalingMetrics,
                topology,
                new Configuration(),
                () -> PREV_TPR);

        assertEquals(
                Map.of(
                        ScalingMetric.NUM_RECORDS_IN,
                        1.,
                        ScalingMetric.NUM_RECORDS_OUT,
                        2.,
                        ScalingMetric.OBSERVED_TPR,
                        PREV_TPR),
                scalingMetrics);

        scalingMetrics.clear();
        ScalingMetrics.computeDataRateMetrics(
                op,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric(
                                "", Double.NaN, 100., Double.NaN, Double.NaN, Double.NaN),
                        FlinkMetric.SOURCE_TASK_NUM_RECORDS_IN_PER_SEC,
                        aggSum(1000.)),
                scalingMetrics,
                topology,
                new Configuration(),
                () -> 0.);

        assertEquals(
                Map.of(ScalingMetric.NUM_RECORDS_IN, 1., ScalingMetric.NUM_RECORDS_OUT, 2.),
                scalingMetrics);
    }

    @ParameterizedTest
    @EnumSource(MetricAggregator.class)
    public void testLegacySourceScaling(MetricAggregator busyTimeAggregator) {
        var source = new JobVertexID();
        var sink = new JobVertexID();

        var ioMetrics = new IOMetrics(0, 0, 0);

        Configuration conf = new Configuration();
        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, busyTimeAggregator);
        assertTrue(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS).isEmpty());
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, List.of(sink.toHexString()));

        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        ScalingMetrics.computeLoadMetrics(
                source,
                Map.of(
                        // Busy time is NaN for legacy sources
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        aggSum(Double.NaN),
                        FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC,
                        aggSum(2000.)),
                scalingMetrics,
                ioMetrics,
                conf);

        // Make sure vertex won't be scaled
        assertTrue(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS).contains(source.toHexString()));
        // Existing overrides should be preserved
        assertTrue(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS).contains(sink.toHexString()));
    }

    @Test
    public void testLoadMetrics() {
        var source = new JobVertexID();
        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        var conf = new Configuration();
        var ioMetrics = new IOMetrics(0, 0, 123);

        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.MAX);
        ScalingMetrics.computeLoadMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", 100., 200., 150., Double.NaN, Double.NaN)),
                scalingMetrics,
                ioMetrics,
                conf);
        assertEquals(
                Map.of(ScalingMetric.LOAD, .2, ScalingMetric.ACCUMULATED_BUSY_TIME, 123.),
                scalingMetrics);

        scalingMetrics.clear();
        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.MIN);
        ScalingMetrics.computeLoadMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", 100., 200., 150., Double.NaN, Double.NaN)),
                scalingMetrics,
                ioMetrics,
                conf);
        assertEquals(
                Map.of(ScalingMetric.LOAD, .1, ScalingMetric.ACCUMULATED_BUSY_TIME, 123.),
                scalingMetrics);

        scalingMetrics.clear();
        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.AVG);
        ScalingMetrics.computeLoadMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", 100., 200., 150., Double.NaN, Double.NaN)),
                scalingMetrics,
                ioMetrics,
                conf);
        assertEquals(
                Map.of(ScalingMetric.LOAD, .15, ScalingMetric.ACCUMULATED_BUSY_TIME, 123.),
                scalingMetrics);
    }

    @Test
    public void testComputeTprWithBackpressure() {
        assertEquals(Double.NaN, ScalingMetrics.computeObservedTprWithBackpressure(100, 1000));
        assertEquals(500, ScalingMetrics.computeObservedTprWithBackpressure(500., 0));
        assertEquals(1000, ScalingMetrics.computeObservedTprWithBackpressure(250, 750));
    }

    @Test
    public void computeObservedTpr() {
        // Without lag we cannot compute observed tpr, we compare against old
        assertEquals(PREV_TPR, computeObservedTpr(500, 1000, 500, 500));

        assertEquals(PREV_TPR, computeObservedTpr(0, 1000, 500, 500));

        // When there is enough lag, observed rate is computed. Switch to busyness because diff is
        // within limit
        assertEquals(900 / 0.9, computeObservedTpr(10000000, 900, 850, 100));

        // Should stay with busyness after switching as diff is still small
        assertEquals(900 / 0.91, computeObservedTpr(10000000, 900, 900, 90));

        // Use observed when diff is large and switch to observed
        assertEquals(1000 / 0.8, computeObservedTpr(10000000, 1000, 500, 200));
        assertEquals(1000 / 0.81, computeObservedTpr(10000000, 1000, 500, 190));

        // When no incoming records observed TPR should be infinity
        assertEquals(Double.POSITIVE_INFINITY, computeObservedTpr(500, 0, 100, 100));
    }

    public static double computeObservedTpr(
            double lag, double processingRate, double busyness, double backpressure) {
        return computeObservedTpr(lag, processingRate, busyness, backpressure, new Configuration());
    }

    public static double computeObservedTpr(
            double lag,
            double processingRate,
            double busyness,
            double backpressure,
            Configuration conf) {
        var sink = new JobVertexID();
        var topology =
                new JobTopology(
                        new VertexInfo(
                                SOURCE, Collections.emptyMap(), 1, 1, new IOMetrics(0, 0, 0)),
                        new VertexInfo(
                                sink, Map.of(SOURCE, REBALANCE), 1, 1, new IOMetrics(0, 0, 0)));

        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        scalingMetrics.put(ScalingMetric.LAG, lag);
        ScalingMetrics.computeDataRateMetrics(
                SOURCE,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric(
                                "", Double.NaN, busyness, Double.NaN, Double.NaN, Double.NaN),
                        FlinkMetric.BACKPRESSURE_TIME_PER_SEC,
                        new AggregatedMetric(
                                "", Double.NaN, Double.NaN, backpressure, Double.NaN, Double.NaN),
                        FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric(
                                "",
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                processingRate,
                                Double.NaN)),
                scalingMetrics,
                topology,
                conf,
                () -> PREV_TPR);
        return scalingMetrics.get(ScalingMetric.OBSERVED_TPR);
    }

    @Test
    public void testGlobalMetrics() {
        Configuration conf = new Configuration();
        assertEquals(Map.of(), ScalingMetrics.computeGlobalMetrics(Map.of(), Map.of(), conf));
        assertEquals(
                Map.of(),
                ScalingMetrics.computeGlobalMetrics(
                        Map.of(), Map.of(FlinkMetric.HEAP_MEMORY_USED, aggMax(100)), conf));

        assertEquals(
                Map.of(
                        ScalingMetric.HEAP_MAX_USAGE_RATIO,
                        0.5,
                        ScalingMetric.HEAP_MEMORY_USED,
                        100.,
                        ScalingMetric.MANAGED_MEMORY_USED,
                        133.,
                        ScalingMetric.METASPACE_MEMORY_USED,
                        22.),
                ScalingMetrics.computeGlobalMetrics(
                        Map.of(),
                        Map.of(
                                FlinkMetric.HEAP_MEMORY_USED,
                                aggAvgMax(75, 100),
                                FlinkMetric.MANAGED_MEMORY_USED,
                                aggAvgMax(128, 133),
                                FlinkMetric.METASPACE_MEMORY_USED,
                                aggAvgMax(11, 22),
                                FlinkMetric.HEAP_MEMORY_MAX,
                                aggMax(200.)),
                        conf));
    }

    private static AggregatedMetric aggSum(double sum) {
        return new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, sum, Double.NaN);
    }

    private static AggregatedMetric aggMax(double max) {
        return new AggregatedMetric("", Double.NaN, max, Double.NaN, Double.NaN, Double.NaN);
    }

    private static AggregatedMetric aggAvgMax(double avg, double max) {
        return new AggregatedMetric("", Double.NaN, max, avg, Double.NaN, Double.NaN);
    }
}
