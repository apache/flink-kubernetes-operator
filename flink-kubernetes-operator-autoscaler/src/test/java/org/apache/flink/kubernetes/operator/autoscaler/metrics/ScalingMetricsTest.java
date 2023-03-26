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

package org.apache.flink.kubernetes.operator.autoscaler.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.kubernetes.operator.autoscaler.topology.VertexInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
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
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.)),
                scalingMetrics,
                topology,
                15.,
                new Configuration());

        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        10000.,
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        20000.,
                        ScalingMetric.OUTPUT_RATIO,
                        2.,
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
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.)),
                scalingMetrics,
                topology,
                -50.,
                new Configuration());

        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        10000.,
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        20000.,
                        ScalingMetric.OUTPUT_RATIO,
                        2.,
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
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.)),
                scalingMetrics,
                topology,
                0.,
                new Configuration());

        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        10000.,
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        20000.,
                        ScalingMetric.OUTPUT_RATIO,
                        2.,
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
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.)),
                scalingMetrics,
                topology,
                0.,
                conf);

        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        10000.,
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        20000.,
                        ScalingMetric.OUTPUT_RATIO,
                        2.,
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

        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        ScalingMetrics.computeDataRateMetrics(
                source,
                Map.of(
                        // Busy time is NaN for legacy sources
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, Double.NaN),
                        FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 4000.)),
                scalingMetrics,
                topology,
                0.,
                conf);

        // Make sure vertex won't be scaled
        assertTrue(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS).contains(source.toHexString()));
        // Legacy source rates are computed based on the current rate and a balanced utilization
        assertEquals(
                2000 / conf.get(AutoScalerOptions.TARGET_UTILIZATION),
                scalingMetrics.get(ScalingMetric.TRUE_PROCESSING_RATE));
        assertEquals(2000, scalingMetrics.get(ScalingMetric.SOURCE_DATA_RATE));
        assertEquals(
                scalingMetrics.get(ScalingMetric.TRUE_PROCESSING_RATE) * 2,
                scalingMetrics.get(ScalingMetric.TRUE_OUTPUT_RATE));
        assertEquals(2, scalingMetrics.get(ScalingMetric.OUTPUT_RATIO));
    }

    @Test
    public void testLoadMetrics() {
        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        ScalingMetrics.computeLoadMetrics(
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", Double.NaN, 200., 100., Double.NaN)),
                scalingMetrics);

        assertEquals(0.2, scalingMetrics.get(ScalingMetric.LOAD_MAX));
        assertEquals(0.1, scalingMetrics.get(ScalingMetric.LOAD_AVG));
    }

    @Test
    public void testZeroValuesForBusyness() {
        double dataRate = 10;
        Map<ScalingMetric, Double> scalingMetrics =
                testZeroValuesForRatesOrBusyness(dataRate, dataRate, 0);
        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        1.0E14,
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        1.0E14,
                        ScalingMetric.OUTPUT_RATIO,
                        1.,
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
                        1.0E-9,
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        1.0E-9,
                        ScalingMetric.OUTPUT_RATIO,
                        1.,
                        ScalingMetric.SOURCE_DATA_RATE,
                        ScalingMetrics.EFFECTIVELY_ZERO,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        ScalingMetrics.EFFECTIVELY_ZERO),
                scalingMetrics);
    }

    @Test
    public void testZeroProcessingRateOnly() {
        Map<ScalingMetric, Double> scalingMetrics = testZeroValuesForRatesOrBusyness(0, 1, 100);
        assertEquals(
                Map.of(
                        // Output ratio should be kept to 1
                        ScalingMetric.OUTPUT_RATIO,
                        1.,
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        ScalingMetrics.EFFECTIVELY_ZERO * 10,
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        ScalingMetrics.EFFECTIVELY_ZERO * 10,
                        ScalingMetric.SOURCE_DATA_RATE,
                        ScalingMetrics.EFFECTIVELY_ZERO,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        ScalingMetrics.EFFECTIVELY_ZERO),
                scalingMetrics);
    }

    @Test
    public void testZeroValuesForRatesAndBusyness() {
        Map<ScalingMetric, Double> scalingMetrics = testZeroValuesForRatesOrBusyness(0, 0, 0);
        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        1000.0,
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        1000.0,
                        ScalingMetric.OUTPUT_RATIO,
                        1.,
                        ScalingMetric.SOURCE_DATA_RATE,
                        ScalingMetrics.EFFECTIVELY_ZERO,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        ScalingMetrics.EFFECTIVELY_ZERO),
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
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, outputRate)),
                scalingMetrics,
                topology,
                0.,
                new Configuration());

        return scalingMetrics;
    }
}
