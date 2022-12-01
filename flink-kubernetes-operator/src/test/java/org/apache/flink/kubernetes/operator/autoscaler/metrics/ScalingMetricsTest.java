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
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
                        new AggregatedMetric("", Double.NaN, Double.NaN, 100., Double.NaN),
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.)),
                scalingMetrics,
                topology,
                Optional.of(15.),
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
                        1015.),
                scalingMetrics);

        // test negative lag growth (catch up)
        scalingMetrics.clear();
        ScalingMetrics.computeDataRateMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, 100., Double.NaN),
                        FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.)),
                scalingMetrics,
                topology,
                Optional.of(-50.),
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
                        950.),
                scalingMetrics);

        scalingMetrics.clear();
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
                Optional.empty(),
                new Configuration());

        assertEquals(
                Map.of(
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        10000.,
                        ScalingMetric.TRUE_OUTPUT_RATE,
                        20000.,
                        ScalingMetric.OUTPUT_RATIO,
                        2.),
                scalingMetrics);
    }

    @Test
    public void testSourceScalingDisabled() {
        var source = new JobVertexID();

        var topology = new JobTopology(new VertexInfo(source, Collections.emptySet(), 1, 1));

        Configuration conf = new Configuration();
        // Disable scaling sources
        conf.setBoolean(AutoScalerOptions.SOURCE_SCALING_ENABLED, false);

        Map<ScalingMetric, Double> scalingMetrics = new HashMap<>();
        ScalingMetrics.computeDataRateMetrics(
                source,
                Map.of(
                        FlinkMetric.BUSY_TIME_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, 500., Double.NaN),
                        FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.),
                        FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                        new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 4000.)),
                scalingMetrics,
                topology,
                Optional.empty(),
                conf);

        // Sources are not scaled, the rates are solely computed on the basis of the true output
        // rate
        assertEquals(Double.NaN, scalingMetrics.get(ScalingMetric.TRUE_PROCESSING_RATE));
        assertEquals(8000, scalingMetrics.get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(8000, scalingMetrics.get(ScalingMetric.TRUE_OUTPUT_RATE));
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
}
