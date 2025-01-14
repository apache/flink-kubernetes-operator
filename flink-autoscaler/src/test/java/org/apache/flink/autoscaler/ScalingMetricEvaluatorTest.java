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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.MetricAggregator;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.CATCH_UP_DURATION;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.PREFER_TRACKED_RESTART_TIME;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.RESTART_TIME;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.UTILIZATION_TARGET;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Scaling evaluator test. */
public class ScalingMetricEvaluatorTest {

    private ScalingMetricEvaluator evaluator = new ScalingMetricEvaluator();

    @Test
    public void testLagBasedSourceScaling() {
        var source = new JobVertexID();
        var sink = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source, Collections.emptyMap(), 1, 1, null),
                        new VertexInfo(sink, Map.of(source, REBALANCE), 1, 1, null));

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        950.,
                                        ScalingMetric.NUM_RECORDS_IN,
                                        0.,
                                        ScalingMetric.NUM_RECORDS_OUT,
                                        0.,
                                        ScalingMetric.LOAD,
                                        .8),
                                sink,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 0., ScalingMetric.LOAD, .4)),
                        Map.of()));

        // Lag 950 -> 1000
        // Input records 0 -> 100
        // -> Source Data rate = 150
        // Output ratio 2
        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        1000.,
                                        ScalingMetric.NUM_RECORDS_IN,
                                        100.,
                                        ScalingMetric.NUM_RECORDS_OUT,
                                        200.,
                                        ScalingMetric.LOAD,
                                        .6),
                                sink,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 200., ScalingMetric.LOAD, .3)),
                        Map.of()));

        var conf = new Configuration();

        conf.set(CATCH_UP_DURATION, Duration.ofSeconds(2));
        var evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();

        assertEquals(
                EvaluatedScalingMetric.avg(.7),
                evaluatedMetrics.get(source).get(ScalingMetric.LOAD));

        assertEquals(
                EvaluatedScalingMetric.avg(.35),
                evaluatedMetrics.get(sink).get(ScalingMetric.LOAD));

        assertEquals(
                EvaluatedScalingMetric.avg(150),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(500.),
                evaluatedMetrics.get(source).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(300),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(1000),
                evaluatedMetrics.get(sink).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(1000),
                evaluatedMetrics.get(source).get(ScalingMetric.LAG));
        assertFalse(evaluatedMetrics.get(sink).containsKey(ScalingMetric.LAG));

        conf.set(CATCH_UP_DURATION, Duration.ofSeconds(1));
        evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();
        assertEquals(
                EvaluatedScalingMetric.avg(150),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(1000),
                evaluatedMetrics.get(source).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(300),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(2000),
                evaluatedMetrics.get(sink).get(ScalingMetric.CATCH_UP_DATA_RATE));

        // Restart time should not affect evaluated metrics
        conf.set(RESTART_TIME, Duration.ofSeconds(2));

        evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();
        assertEquals(
                EvaluatedScalingMetric.avg(150),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(1000),
                evaluatedMetrics.get(source).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(300),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(2000),
                evaluatedMetrics.get(sink).get(ScalingMetric.CATCH_UP_DATA_RATE));

        // Turn off lag based scaling
        conf.set(CATCH_UP_DURATION, Duration.ZERO);
        evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();
        assertEquals(
                EvaluatedScalingMetric.avg(150),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(0),
                evaluatedMetrics.get(source).get(ScalingMetric.CATCH_UP_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(300),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.of(0),
                evaluatedMetrics.get(sink).get(ScalingMetric.CATCH_UP_DATA_RATE));

        // Test 0 lag
        metricHistory.clear();
        metricHistory.put(
                Instant.ofEpochMilli(3000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        0.,
                                        ScalingMetric.NUM_RECORDS_IN,
                                        100.,
                                        ScalingMetric.NUM_RECORDS_OUT,
                                        200.,
                                        ScalingMetric.LOAD,
                                        .6),
                                sink,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 200., ScalingMetric.LOAD, .3)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(4000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        0.,
                                        ScalingMetric.NUM_RECORDS_IN,
                                        200.,
                                        ScalingMetric.NUM_RECORDS_OUT,
                                        400.,
                                        ScalingMetric.LOAD,
                                        .6),
                                sink,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 400., ScalingMetric.LOAD, .3)),
                        Map.of()));

        conf.set(CATCH_UP_DURATION, Duration.ofMinutes(1));
        evaluatedMetrics =
                evaluator
                        .evaluate(
                                conf,
                                new CollectedMetricHistory(topology, metricHistory, Instant.now()),
                                Duration.ZERO)
                        .getVertexMetrics();
        assertEquals(
                EvaluatedScalingMetric.avg(100),
                evaluatedMetrics.get(source).get(ScalingMetric.TARGET_DATA_RATE));
        assertEquals(
                EvaluatedScalingMetric.avg(200),
                evaluatedMetrics.get(sink).get(ScalingMetric.TARGET_DATA_RATE));
    }

    @Test
    public void testUtilizationBoundaryComputation() {

        var conf = new Configuration();
        conf.set(UTILIZATION_TARGET, 0.8);
        conf.set(AutoScalerOptions.UTILIZATION_MAX, 0.9);
        conf.set(AutoScalerOptions.UTILIZATION_MIN, 0.7);
        conf.set(RESTART_TIME, Duration.ofSeconds(1));
        conf.set(CATCH_UP_DURATION, Duration.ZERO);

        // Default behaviour, restart time does not factor in
        assertEquals(Tuple2.of(778.0, 1000.0), getThresholds(700, 0, conf));

        conf.set(CATCH_UP_DURATION, Duration.ofSeconds(2));
        assertEquals(Tuple2.of(1128.0, 1700.0), getThresholds(700, 350, conf));
        assertEquals(Tuple2.of(778.0, 1350.0), getThresholds(700, 0, conf));

        // Test thresholds during catchup periods
        assertEquals(
                Tuple2.of(1050., Double.POSITIVE_INFINITY), getThresholds(700, 350, conf, true));
        assertEquals(Tuple2.of(700., Double.POSITIVE_INFINITY), getThresholds(700, 0, conf, true));
    }

    @Test
    public void testUtilizationBoundaryComputationWithRestartTimesTracking() {

        var conf = new Configuration();
        conf.set(UTILIZATION_TARGET, 0.8);
        conf.set(AutoScalerOptions.UTILIZATION_MAX, 0.9);
        conf.set(AutoScalerOptions.UTILIZATION_MIN, 0.7);
        conf.set(RESTART_TIME, Duration.ofMinutes(10));
        conf.set(CATCH_UP_DURATION, Duration.ZERO);
        conf.set(PREFER_TRACKED_RESTART_TIME, true);

        var scalingTracking = new ScalingTracking();
        scalingTracking.addScalingRecord(
                Instant.parse("2023-11-15T16:00:00.00Z"), new ScalingRecord(Duration.ofMinutes(3)));
        scalingTracking.addScalingRecord(
                Instant.parse("2023-11-15T16:20:00.00Z"), new ScalingRecord(Duration.ofMinutes(5)));

        var restartTimeSec = scalingTracking.getMaxRestartTimeOrDefault(conf);
        // Restart time does not factor in
        assertEquals(Tuple2.of(778.0, 1000.0), getThresholds(700, 0, restartTimeSec, conf));

        conf.set(CATCH_UP_DURATION, Duration.ofMinutes(1));
        assertEquals(Tuple2.of(1128.0, 4850.0), getThresholds(700, 350, restartTimeSec, conf));
        assertEquals(Tuple2.of(778.0, 4500.0), getThresholds(700, 0, restartTimeSec, conf));

        // Test thresholds during catchup periods
        assertEquals(
                Tuple2.of(1050., Double.POSITIVE_INFINITY),
                getThresholds(700, 350, restartTimeSec, conf, true));
        assertEquals(
                Tuple2.of(700., Double.POSITIVE_INFINITY),
                getThresholds(700, 0, restartTimeSec, conf, true));
    }

    @Test
    public void testBacklogProcessingEvaluation() {
        var source = new JobVertexID();
        var sink = new JobVertexID();
        var conf = new Configuration();

        var topology =
                new JobTopology(
                        new VertexInfo(source, Collections.emptyMap(), 1, 1),
                        new VertexInfo(sink, Map.of(source, REBALANCE), 1, 1));

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        // 0 lag
        metricHistory.put(
                Instant.ofEpochMilli(0),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(ScalingMetric.LAG, 0., ScalingMetric.NUM_RECORDS_IN, 0.),
                                sink,
                                Map.of()),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(ScalingMetric.LAG, 0., ScalingMetric.NUM_RECORDS_IN, 100.),
                                sink,
                                Map.of()),
                        Map.of()));
        assertFalse(ScalingMetricEvaluator.isProcessingBacklog(topology, metricHistory, conf));

        // Missing lag
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.NUM_RECORDS_IN, 100.), sink, Map.of()),
                        Map.of()));

        assertFalse(ScalingMetricEvaluator.isProcessingBacklog(topology, metricHistory, conf));

        // Catch up time is more than a minute at avg proc rate (200)
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        250.
                                                * conf.get(
                                                                AutoScalerOptions
                                                                        .BACKLOG_PROCESSING_LAG_THRESHOLD)
                                                        .toSeconds(),
                                        ScalingMetric.NUM_RECORDS_IN,
                                        200.),
                                sink,
                                Map.of()),
                        Map.of()));

        assertTrue(ScalingMetricEvaluator.isProcessingBacklog(topology, metricHistory, conf));

        // Catch up time is less than a minute at avg proc rate (200)
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source,
                                Map.of(
                                        ScalingMetric.LAG,
                                        180.
                                                * conf.get(
                                                                AutoScalerOptions
                                                                        .BACKLOG_PROCESSING_LAG_THRESHOLD)
                                                        .toSeconds(),
                                        ScalingMetric.NUM_RECORDS_IN,
                                        200.),
                                sink,
                                Map.of()),
                        Map.of()));
        assertFalse(ScalingMetricEvaluator.isProcessingBacklog(topology, metricHistory, conf));
    }

    @Test
    public void testObservedTprEvaluation() {
        var source = new JobVertexID();
        var conf = new Configuration();

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.OBSERVED_TPR, 200.)), Map.of()));
        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.OBSERVED_TPR, 400.)), Map.of()));

        // Observed TPR average : 300

        // Set diff threshold to 20% -> within threshold
        conf.set(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_SWITCH_THRESHOLD, 0.2);

        // Test that we used busy time based TPR
        assertEquals(
                350,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        100, 35, metricHistory, source, conf));

        // Set diff threshold to 10% -> outside threshold
        conf.set(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_SWITCH_THRESHOLD, 0.1);

        // Test that we used the observed TPR
        assertEquals(
                300,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        100, 35, metricHistory, source, conf));

        // Test that observed tpr min observations are respected. If less, use busy time
        conf.set(AutoScalerOptions.OBSERVED_TRUE_PROCESSING_RATE_MIN_OBSERVATIONS, 3);
        assertEquals(
                350,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        100, 35, metricHistory, source, conf));
    }

    @Test
    public void testMissingObservedTpr() {
        var source = new JobVertexID();
        var conf = new Configuration();
        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        // Test that we used busy time based TPR even if infinity
        assertEquals(
                350.,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        100, 35, metricHistory, source, conf));

        assertEquals(
                Double.POSITIVE_INFINITY,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        0, 100, metricHistory, source, conf));

        assertEquals(
                Double.POSITIVE_INFINITY,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        0, 0, metricHistory, source, conf));

        assertEquals(
                Double.NaN,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        Double.NaN, Double.NaN, metricHistory, source, conf));
    }

    @Test
    public void testMissingBusyTimeTpr() {
        var source = new JobVertexID();

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();
        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.OBSERVED_TPR, 200.)), Map.of()));
        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(source, Map.of(ScalingMetric.OBSERVED_TPR, 400.)), Map.of()));
        assertEquals(
                300.,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        Double.NaN, 1., metricHistory, source, new Configuration()));
    }

    @Test
    public void testGlobalMetricEvaluation() {
        var globalMetrics = new TreeMap<Instant, CollectedMetrics>();
        globalMetrics.put(Instant.now(), new CollectedMetrics(Map.of(), Map.of()));

        assertEquals(
                Map.of(
                        ScalingMetric.HEAP_MAX_USAGE_RATIO,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.GC_PRESSURE,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.HEAP_MEMORY_USED,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.MANAGED_MEMORY_USED,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.METASPACE_MEMORY_USED,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.NUM_TASK_SLOTS_USED,
                        EvaluatedScalingMetric.of(Double.NaN)),
                ScalingMetricEvaluator.evaluateGlobalMetrics(globalMetrics));

        globalMetrics.put(
                Instant.now(),
                new CollectedMetrics(
                        Map.of(),
                        Map.of(
                                ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                0.5,
                                ScalingMetric.GC_PRESSURE,
                                0.6,
                                ScalingMetric.HEAP_MEMORY_USED,
                                512.,
                                ScalingMetric.MANAGED_MEMORY_USED,
                                420.,
                                ScalingMetric.METASPACE_MEMORY_USED,
                                110.)));
        assertEquals(
                Map.of(
                        ScalingMetric.HEAP_MAX_USAGE_RATIO,
                        new EvaluatedScalingMetric(0.5, 0.5),
                        ScalingMetric.GC_PRESSURE,
                        EvaluatedScalingMetric.of(0.6),
                        ScalingMetric.HEAP_MEMORY_USED,
                        new EvaluatedScalingMetric(512, 512),
                        ScalingMetric.MANAGED_MEMORY_USED,
                        new EvaluatedScalingMetric(420, 420),
                        ScalingMetric.METASPACE_MEMORY_USED,
                        new EvaluatedScalingMetric(110, 110),
                        ScalingMetric.NUM_TASK_SLOTS_USED,
                        EvaluatedScalingMetric.of(Double.NaN)),
                ScalingMetricEvaluator.evaluateGlobalMetrics(globalMetrics));

        globalMetrics.put(
                Instant.now(),
                new CollectedMetrics(
                        Map.of(),
                        Map.of(
                                ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                0.7,
                                ScalingMetric.GC_PRESSURE,
                                0.8,
                                ScalingMetric.HEAP_MEMORY_USED,
                                1024.,
                                ScalingMetric.MANAGED_MEMORY_USED,
                                840.,
                                ScalingMetric.METASPACE_MEMORY_USED,
                                220.,
                                ScalingMetric.NUM_TASK_SLOTS_USED,
                                42.)));
        assertEquals(
                Map.of(
                        ScalingMetric.HEAP_MAX_USAGE_RATIO,
                        new EvaluatedScalingMetric(0.7, 0.6),
                        ScalingMetric.GC_PRESSURE,
                        EvaluatedScalingMetric.of(0.8),
                        ScalingMetric.HEAP_MEMORY_USED,
                        new EvaluatedScalingMetric(1024., 768.),
                        ScalingMetric.MANAGED_MEMORY_USED,
                        new EvaluatedScalingMetric(840., 630.),
                        ScalingMetric.METASPACE_MEMORY_USED,
                        new EvaluatedScalingMetric(220., 165.),
                        ScalingMetric.NUM_TASK_SLOTS_USED,
                        EvaluatedScalingMetric.of(42.)),
                ScalingMetricEvaluator.evaluateGlobalMetrics(globalMetrics));
    }

    @Test
    public void testZeroValuesForRatesOrBusyness() {
        assertInfiniteTpr(0, 0);
        assertInfiniteTpr(0, 1);
        assertInfiniteTpr(1, 0);
        assertInfiniteTpr(Double.NaN, 0);
    }

    private static void assertInfiniteTpr(double busyTime, long inputRate) {
        assertEquals(
                Double.POSITIVE_INFINITY,
                ScalingMetricEvaluator.computeTrueProcessingRate(
                        busyTime,
                        inputRate,
                        new TreeMap<>(),
                        new JobVertexID(),
                        new Configuration()));
    }

    @Test
    public void testBusyTimeEvaluation() {
        var v = new JobVertexID();
        var conf = new Configuration();
        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                v,
                                Map.of(
                                        ScalingMetric.LOAD, 0.2,
                                        ScalingMetric.ACCUMULATED_BUSY_TIME, 10000.)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(
                                v,
                                Map.of(
                                        ScalingMetric.LOAD, 0.3,
                                        ScalingMetric.ACCUMULATED_BUSY_TIME, 10200.)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(3000),
                new CollectedMetrics(
                        Map.of(
                                v,
                                Map.of(
                                        ScalingMetric.LOAD, 0.4,
                                        ScalingMetric.ACCUMULATED_BUSY_TIME, 10400.)),
                        Map.of()));

        // With MAX or MIN we should compute from LOAD only, parallelism should not matter
        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.MAX);
        assertEquals(300., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 2));
        assertEquals(300., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 0));

        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.MIN);
        assertEquals(300., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 2));
        assertEquals(300., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 0));

        conf.set(AutoScalerOptions.BUSY_TIME_AGGREGATOR, MetricAggregator.AVG);
        // With AVG we compute from accumulated busy time
        // Diff 400 over 2 seconds -> 200 / second (for the whole job -> we need to divide for
        // parallelism)
        assertEquals(100., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 2));
        assertEquals(200., ScalingMetricEvaluator.computeBusyTimeAvg(conf, metricHistory, v, 1));
    }

    @Test
    public void testComputableOutputRatios() {
        var source1 = new JobVertexID();
        var source2 = new JobVertexID();

        var op1 = new JobVertexID();
        var sink1 = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source1, Collections.emptyMap(), 1, 1),
                        new VertexInfo(source2, Collections.emptyMap(), 1, 1),
                        new VertexInfo(op1, Map.of(source1, REBALANCE, source2, REBALANCE), 1, 1),
                        new VertexInfo(sink1, Map.of(op1, REBALANCE), 1, 1));

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source1,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 100.,
                                        ScalingMetric.NUM_RECORDS_OUT, 100.),
                                source2,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 100.,
                                        ScalingMetric.NUM_RECORDS_OUT, 100.),
                                op1,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 100.),
                                sink1,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 100.)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(
                                source1,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 200.,
                                        ScalingMetric.NUM_RECORDS_OUT, 300.),
                                source2,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 200.,
                                        ScalingMetric.NUM_RECORDS_OUT, 150.),
                                op1,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 350.),
                                sink1,
                                Map.of(ScalingMetric.NUM_RECORDS_IN, 150.)),
                        Map.of()));

        assertEquals(
                2.,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source1, op1, topology, metricHistory));
        assertEquals(
                0.5,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source2, op1, topology, metricHistory));
        assertEquals(
                0.2,
                ScalingMetricEvaluator.computeEdgeOutputRatio(op1, sink1, topology, metricHistory));
    }

    @Test
    public void testOutputRatioFallbackToOutPerSecond() {
        var source1 = new JobVertexID();
        var source2 = new JobVertexID();

        var op1 = new JobVertexID();
        var op2 = new JobVertexID();

        var topology =
                new JobTopology(
                        new VertexInfo(source1, Collections.emptyMap(), 1, 1),
                        new VertexInfo(source2, Collections.emptyMap(), 1, 1),
                        new VertexInfo(op1, Map.of(source1, REBALANCE, source2, REBALANCE), 1, 1),
                        new VertexInfo(op2, Map.of(source1, REBALANCE, source2, REBALANCE), 1, 1));

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();

        metricHistory.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(
                        Map.of(
                                source1,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 0.,
                                        ScalingMetric.NUM_RECORDS_OUT, 0.),
                                source2,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 0.,
                                        ScalingMetric.NUM_RECORDS_OUT, 0.)),
                        Map.of()));

        metricHistory.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(
                        Map.of(
                                source1,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 100.,
                                        ScalingMetric.NUM_RECORDS_OUT, 200.),
                                source2,
                                Map.of(
                                        ScalingMetric.NUM_RECORDS_IN, 100.,
                                        ScalingMetric.NUM_RECORDS_OUT, 50.)),
                        Map.of()));

        assertEquals(
                2.,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source1, op1, topology, metricHistory));
        assertEquals(
                0.5,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source2, op1, topology, metricHistory));
        assertEquals(
                2.,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source1, op2, topology, metricHistory));
        assertEquals(
                0.5,
                ScalingMetricEvaluator.computeEdgeOutputRatio(
                        source2, op2, topology, metricHistory));
    }

    @Test
    public void getRateTest() {
        var m1 = ScalingMetric.NUM_RECORDS_IN;
        var m2 = ScalingMetric.NUM_RECORDS_OUT;

        var v1 = new JobVertexID();
        var v2 = new JobVertexID();

        var history = new TreeMap<Instant, CollectedMetrics>();
        history.put(
                Instant.ofEpochMilli(1000),
                new CollectedMetrics(Map.of(v1, Map.of(m1, 0.), v2, Map.of()), null));
        history.put(
                Instant.ofEpochMilli(2000),
                new CollectedMetrics(Map.of(v1, Map.of(m1, 0., m2, 10.), v2, Map.of()), null));
        history.put(
                Instant.ofEpochMilli(3000),
                new CollectedMetrics(
                        Map.of(v1, Map.of(m1, 4., m2, 20.), v2, Map.of(m1, 1.)), null));

        assertEquals(2, ScalingMetricEvaluator.getRate(m1, v1, history));
        assertEquals(10., ScalingMetricEvaluator.getRate(m2, v1, history));
        assertEquals(Double.NaN, ScalingMetricEvaluator.getRate(m1, v2, history));
        assertEquals(Double.NaN, ScalingMetricEvaluator.getRate(m2, v2, history));
    }

    private Tuple2<Double, Double> getThresholds(
            double inputTargetRate, double catchUpRate, Configuration conf) {
        return getThresholds(inputTargetRate, catchUpRate, conf, false);
    }

    private Tuple2<Double, Double> getThresholds(
            double inputTargetRate, double catchUpRate, Duration restartTime, Configuration conf) {
        return getThresholds(inputTargetRate, catchUpRate, restartTime, conf, false);
    }

    private Tuple2<Double, Double> getThresholds(
            double inputTargetRate, double catchUpRate, Configuration conf, boolean catchingUp) {
        var restartTime = conf.get(AutoScalerOptions.RESTART_TIME);
        return getThresholds(inputTargetRate, catchUpRate, restartTime, conf, catchingUp);
    }

    private Tuple2<Double, Double> getThresholds(
            double inputTargetRate,
            double catchUpRate,
            Duration restartTime,
            Configuration conf,
            boolean catchingUp) {
        var map = new HashMap<ScalingMetric, EvaluatedScalingMetric>();

        map.put(ScalingMetric.TARGET_DATA_RATE, EvaluatedScalingMetric.avg(inputTargetRate));
        map.put(ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(catchUpRate));

        ScalingMetricEvaluator.computeProcessingRateThresholds(map, conf, catchingUp, restartTime);
        return Tuple2.of(
                map.get(ScalingMetric.SCALE_UP_RATE_THRESHOLD).getCurrent(),
                map.get(ScalingMetric.SCALE_DOWN_RATE_THRESHOLD).getCurrent());
    }
}
