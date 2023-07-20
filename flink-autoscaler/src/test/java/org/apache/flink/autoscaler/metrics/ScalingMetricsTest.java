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
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for scaling metrics computation logic. */
public class ScalingMetricsTest {

    @Test
    public void testProcessingAndOutputMetrics() {
        var source = new JobVertexID();
        var op = new JobVertexID();
        var sink = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source, Collections.emptySet(), 1, 1),
                        new VertexInfo(op, Set.of(source), 1, 1),
                        new VertexInfo(sink, Set.of(op), 1, 1));

        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        ScalingMetrics.computeDataRateMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", Double.NaN, 100., Double.NaN, Double.NaN),
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        aggSum(1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(2000.)),
                scalingMetrics,
                topology,
                15.,
                new Configuration());

        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        10000.,
                        ScalingMetric.SOURCE_DATA_RATE,
                        1015.,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        1000.),
                scalingMetrics);

        // test negative lag growth (catch up)
        scalingMetrics.clear();
        ScalingMetrics.computeDataRateMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", Double.NaN, 100., Double.NaN, Double.NaN),
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        aggSum(1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(2000.)),
                scalingMetrics,
                topology,
                -50.,
                new Configuration());

        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        10000.,
                        ScalingMetric.SOURCE_DATA_RATE,
                        950.,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        1000.),
                scalingMetrics);

        scalingMetrics.clear();
        ScalingMetrics.computeDataRateMetrics(
                op,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", Double.NaN, 100., Double.NaN, Double.NaN),
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        aggSum(1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(2000.)),
                scalingMetrics,
                topology,
                0.,
                new Configuration());

        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        10000.,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        1000.),
                scalingMetrics);

        // Test using avg busyTime aggregator
        scalingMetrics.clear();
        var conf = new Configuration();
        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.AVG);
        ScalingMetrics.computeDataRateMetrics(
                op,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, 100., Double.NaN),
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        aggSum(1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(2000.)),
                scalingMetrics,
                topology,
                0.,
                conf);

        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        10000.,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        1000.),
                scalingMetrics);
    }

    @Test
    public void testLegacySourceScaling() {
        var source = new JobVertexID();
        var sink = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source, Collections.emptySet(), 5, 1),
                        new VertexInfo(sink, Collections.singleton(source), 10, 100));

        Configuration conf = new Configuration();
        assertTrue(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS).isEmpty());
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, List.of(sink.toHexString()));

        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        ScalingMetrics.computeDataRateMetrics(
                source,
                Map.of(
                        // Busy time is NaN for legacy sources
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        aggSum(Double.NaN),
                        FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC,
                        aggSum(2000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(4000.)),
                scalingMetrics,
                topology,
                0.,
                conf);

        // Make sure vertex won't be scaled
        assertTrue(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS).contains(source.toHexString()));
        // Existing overrides should be preserved
        assertTrue(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS).contains(sink.toHexString()));
        // Legacy source rates are computed based on the current rate and a balanced utilization
        assertEquals(
                2000 / conf.get(AutoScalerOptions.TARGET_UTILIZATION),
                scalingMetrics.get(ScalingMetric.TRUE_PROCESSING_RATE));
        assertEquals(2000, scalingMetrics.get(ScalingMetric.SOURCE_DATA_RATE));
    }

    @Test
    public void testLoadMetrics() {
        var source = new JobVertexID();
        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        var conf = new Configuration();

        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.MAX);
        ScalingMetrics.computeLoadMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", 100., 200., 150., Double.NaN)),
                scalingMetrics,
                conf);
        assertEquals(.2, scalingMetrics.get(ScalingMetric.LOAD));

        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.MIN);
        ScalingMetrics.computeLoadMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", 100., 200., 150., Double.NaN)),
                scalingMetrics,
                conf);
        assertEquals(.1, scalingMetrics.get(ScalingMetric.LOAD));

        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.AVG);
        ScalingMetrics.computeLoadMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", 100., 200., 150., Double.NaN)),
                scalingMetrics,
                conf);
        assertEquals(.15, scalingMetrics.get(ScalingMetric.LOAD));
    }

    @Test
    public void testZeroValuesForBusyness() {
        double dataRate = 10;
        Map<ScalingMetric, Double> scalingMetrics =
                testZeroValuesForRatesOrBusyness(dataRate, dataRate, 0);
        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        // When not busy at all, we have infinite processing power
                        Double.POSITIVE_INFINITY,
                        ScalingMetric.SOURCE_DATA_RATE,
                        dataRate,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        10.),
                scalingMetrics);
    }

    @Test
    public void testZeroValuesForRates() {
        double busyMillisecondPerSec = 100;
        Map<ScalingMetric, Double> scalingMetrics =
                testZeroValuesForRatesOrBusyness(0, 0, busyMillisecondPerSec);
        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        // When no records are coming in, we assume infinite processing power
                        Double.POSITIVE_INFINITY,
                        ScalingMetric.SOURCE_DATA_RATE,
                        0.,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        0.),
                scalingMetrics);
    }

    @Test
    public void testZeroProcessingRateOnly() {
        Map<ScalingMetric, Double> scalingMetrics = testZeroValuesForRatesOrBusyness(0, 1, 100);
        assertEquals(
                Map.of(
                        // If there is zero input the out ratio must be zero
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        // When no records are coming in, we assume infinite processing power
                        Double.POSITIVE_INFINITY,
                        ScalingMetric.SOURCE_DATA_RATE,
                        0.,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        0.),
                scalingMetrics);
    }

    @Test
    public void testZeroValuesForRatesAndBusyness() {
        Map<ScalingMetric, Double> scalingMetrics = testZeroValuesForRatesOrBusyness(0, 0, 0);
        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        // Nothing is coming in, we must assume infinite processing power
                        Double.POSITIVE_INFINITY,
                        ScalingMetric.SOURCE_DATA_RATE,
                        0.,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        0.),
                scalingMetrics);
    }

    private static Map<ScalingMetric, Double> testZeroValuesForRatesOrBusyness(
            double processingRate, double outputRate, double busyness) {
        var source = new JobVertexID();
        var op = new JobVertexID();
        var sink = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source, Collections.emptySet(), 1, 1),
                        new VertexInfo(op, Set.of(source), 1, 1),
                        new VertexInfo(sink, Set.of(op), 1, 1));

        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        ScalingMetrics.computeDataRateMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", Double.NaN, busyness, Double.NaN, Double.NaN),
                        FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric(
                                "", Double.NaN, Double.NaN, Double.NaN, processingRate),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(outputRate)),
                scalingMetrics,
                topology,
                0.,
                new Configuration());

        return scalingMetrics;
    }

    @Test
    public void testComputableOutputRatios() {
        var source1 = new JobVertexID();
        var source2 = new JobVertexID();

        var op1 = new JobVertexID();
        var sink1 = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source1, Collections.emptySet(), 1, 1),
                        new VertexInfo(source2, Collections.emptySet(), 1, 1),
                        new VertexInfo(op1, Set.of(source1, source2), 1, 1),
                        new VertexInfo(sink1, Set.of(op1), 1, 1));

        var allMetrics = new HashMap<JobVertexID, Map<FlinkMetric, AggregatedMetric>>();
        allMetrics.put(
                source1,
                Map.of(
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        aggSum(100),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(200)));
        allMetrics.put(
                source2,
                Map.of(
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        aggSum(100),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(50)));

        allMetrics.put(op1, Map.of(FlinkMetric.NUM_RECORDS_IN_PER_SEC, aggSum(250)));
        allMetrics.put(sink1, Map.of(FlinkMetric.NUM_RECORDS_IN_PER_SEC, aggSum(50)));

        assertEquals(
                Map.of(
                        new Edge(source1, op1), 2.,
                        new Edge(source2, op1), 0.5,
                        new Edge(op1, sink1), 0.2),
                ScalingMetrics.computeOutputRatios(allMetrics, topology));
    }

    @Test
    public void testOutputRatioFallbackToOutPerSecond() {
        var source1 = new JobVertexID();
        var source2 = new JobVertexID();

        var op1 = new JobVertexID();
        var op2 = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source1, Collections.emptySet(), 1, 1),
                        new VertexInfo(source2, Collections.emptySet(), 1, 1),
                        new VertexInfo(op1, Set.of(source1, source2), 1, 1),
                        new VertexInfo(op2, Set.of(source1, source2), 1, 1));

        var allMetrics = new HashMap<JobVertexID, Map<FlinkMetric, AggregatedMetric>>();
        allMetrics.put(
                source1,
                Map.of(
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        aggSum(100),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(200)));
        allMetrics.put(
                source2,
                Map.of(
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        aggSum(100),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        aggSum(50)));

        assertEquals(
                Map.of(
                        new Edge(source1, op1), 2.,
                        new Edge(source2, op1), 0.5,
                        new Edge(source1, op2), 2.,
                        new Edge(source2, op2), 0.5),
                ScalingMetrics.computeOutputRatios(allMetrics, topology));
    }

    private static AggregatedMetric aggSum(double sum) {
        return new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, sum);
    }
}
