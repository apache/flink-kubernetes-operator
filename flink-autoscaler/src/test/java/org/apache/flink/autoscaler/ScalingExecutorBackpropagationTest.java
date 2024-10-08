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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALE_DOWN_INTERVAL;
import static org.apache.flink.autoscaler.topology.ShipStrategy.HASH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for backpropagation in {@link ScalingExecutor}. */
public class ScalingExecutorBackpropagationTest {

    private static final int MAX_PARALLELISM = 720;

    private JobAutoScalerContext<JobID> context;
    private TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector;
    private ScalingExecutor<JobID, JobAutoScalerContext<JobID>> scalingExecutor;

    private InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    private static final Map<ScalingMetric, EvaluatedScalingMetric> dummyGlobalMetrics =
            Map.of(
                    ScalingMetric.GC_PRESSURE, EvaluatedScalingMetric.of(Double.NaN),
                    ScalingMetric.HEAP_MAX_USAGE_RATIO, EvaluatedScalingMetric.of(Double.NaN));

    @BeforeEach
    public void setup() {
        eventCollector = new TestingEventCollector<>();
        context = createDefaultJobAutoScalerContext();
        stateStore = new InMemoryAutoScalerStateStore<>();

        scalingExecutor =
                new ScalingExecutor<>(eventCollector, stateStore) {
                    @Override
                    protected boolean scalingWouldExceedMaxResources(
                            Configuration tunedConfig,
                            JobTopology jobTopology,
                            EvaluatedMetrics evaluatedMetrics,
                            Map<JobVertexID, ScalingSummary> scalingSummaries,
                            JobAutoScalerContext<JobID> ctx) {
                        return super.scalingWouldExceedMaxResources(
                                tunedConfig, jobTopology, evaluatedMetrics, scalingSummaries, ctx);
                    }
                };
        Configuration conf = context.getConfiguration();
        conf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ZERO);
        conf.set(AutoScalerOptions.SCALING_ENABLED, true);
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 1.0);
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ZERO);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.0);
        conf.set(AutoScalerOptions.PROCESSING_RATE_BACKPROPAGATION_ENABLED, true);
    }

    @Test
    public void testMetricsNotUpdateOnNoPropagation() throws Exception {
        var sourceHexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source = JobVertexID.fromHexString(sourceHexString);
        var filterOperatorHexString = "869fb403873411306404e9f2e4438c0e";
        var filterOperator = JobVertexID.fromHexString(filterOperatorHexString);
        var sinkHexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink = JobVertexID.fromHexString(sinkHexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 10, 20, false, null),
                        new VertexInfo(filterOperator, Map.of(source, HASH), 10, 20, false, null),
                        new VertexInfo(sink, Map.of(filterOperator, HASH), 10, 20, false, null));

        jobTopology.get(filterOperator).getInputRatios().put(source, 1.0);
        jobTopology.get(sink).getInputRatios().put(filterOperator, 1.0);

        var conf = context.getConfiguration();
        conf.set(SCALE_DOWN_INTERVAL, Duration.ofMillis(0));
        var metrics =
                new EvaluatedMetrics(
                        Map.of(
                                source,
                                evaluated(10, 50, 100),
                                filterOperator,
                                evaluated(10, 50, 100),
                                sink,
                                evaluated(10, 50, 100)),
                        dummyGlobalMetrics);

        // there is no bottlenecks, so scaling should not change target data rate
        var now = Instant.now();
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology,
                        new DelayedScaleDown()));
        assertEquals(
                50.0,
                metrics.getVertexMetrics()
                        .get(source)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                50.0,
                metrics.getVertexMetrics()
                        .get(filterOperator)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                50.0,
                metrics.getVertexMetrics()
                        .get(sink)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());

        metrics =
                new EvaluatedMetrics(
                        Map.of(
                                source,
                                evaluated(10, 100, 100),
                                filterOperator,
                                evaluated(10, 200, 100),
                                sink,
                                evaluated(10, 400, 100)),
                        dummyGlobalMetrics);
        jobTopology.get(filterOperator).getInputRatios().put(source, 2.0);
        jobTopology.get(sink).getInputRatios().put(filterOperator, 2.0);
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology,
                        new DelayedScaleDown()));
        assertEquals(
                100,
                metrics.getVertexMetrics()
                        .get(source)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                200.0,
                metrics.getVertexMetrics()
                        .get(filterOperator)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                400.0,
                metrics.getVertexMetrics()
                        .get(sink)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
    }

    @Test
    public void testMetricsUpdateOnPropagation() throws Exception {
        var sourceHexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source = JobVertexID.fromHexString(sourceHexString);
        var filterOperatorHexString = "869fb403873411306404e9f2e4438c0e";
        var filterOperator = JobVertexID.fromHexString(filterOperatorHexString);
        var sinkHexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink = JobVertexID.fromHexString(sinkHexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 10, 20, false, null),
                        new VertexInfo(filterOperator, Map.of(source, HASH), 10, 20, false, null),
                        new VertexInfo(sink, Map.of(filterOperator, HASH), 10, 20, false, null));

        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.0);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 1.0);
        var metrics =
                new EvaluatedMetrics(
                        Map.of(
                                source,
                                evaluated(10, 100, 100),
                                filterOperator,
                                evaluated(10, 400, 100),
                                sink,
                                evaluated(10, 400, 100)),
                        dummyGlobalMetrics);
        jobTopology.get(filterOperator).getInputRatios().put(source, 4.0);
        jobTopology.get(sink).getInputRatios().put(filterOperator, 1.0);
        conf.set(AutoScalerOptions.PROCESSING_RATE_BACKPROPAGATION_IMPACT, 1.0);
        var now = Instant.now();
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology,
                        new DelayedScaleDown()));
        assertEquals(
                50.0,
                metrics.getVertexMetrics()
                        .get(source)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                200.0,
                metrics.getVertexMetrics()
                        .get(filterOperator)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                200.0,
                metrics.getVertexMetrics()
                        .get(sink)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
    }

    @Test
    public void testMetricsUpdateOnPropagationMaxParallelismLimit() throws Exception {
        var sourceHexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source = JobVertexID.fromHexString(sourceHexString);
        var filterOperatorHexString = "869fb403873411306404e9f2e4438c0e";
        var filterOperator = JobVertexID.fromHexString(filterOperatorHexString);
        var sinkHexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink = JobVertexID.fromHexString(sinkHexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 10, 20, false, null),
                        new VertexInfo(filterOperator, Map.of(source, HASH), 10, 20, false, null),
                        new VertexInfo(sink, Map.of(filterOperator, HASH), 10, 10, false, null));

        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.0);
        var metrics =
                new EvaluatedMetrics(
                        Map.of(
                                source,
                                evaluated(10, 100, 100),
                                filterOperator,
                                evaluated(10, 400, 100),
                                sink,
                                evaluated(10, 400, 100)),
                        dummyGlobalMetrics);
        jobTopology.get(filterOperator).getInputRatios().put(source, 4.0);
        jobTopology.get(sink).getInputRatios().put(filterOperator, 1.0);
        conf.set(AutoScalerOptions.PROCESSING_RATE_BACKPROPAGATION_IMPACT, 1.0);

        metrics.getVertexMetrics()
                .get(sink)
                .put(ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(10));

        var now = Instant.now();
        assertFalse(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology,
                        new DelayedScaleDown()));
        assertEquals(
                25.0,
                metrics.getVertexMetrics()
                        .get(source)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                100.0,
                metrics.getVertexMetrics()
                        .get(filterOperator)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                100.0,
                metrics.getVertexMetrics()
                        .get(sink)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
    }

    @Test
    public void testMetricsUpdateOnPropagationExcludedVertices() throws Exception {
        var sourceHexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source = JobVertexID.fromHexString(sourceHexString);
        var filterOperatorHexString = "869fb403873411306404e9f2e4438c0e";
        var filterOperator = JobVertexID.fromHexString(filterOperatorHexString);
        var sinkHexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink = JobVertexID.fromHexString(sinkHexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 10, 20, false, null),
                        new VertexInfo(filterOperator, Map.of(source, HASH), 10, 20, false, null),
                        new VertexInfo(sink, Map.of(filterOperator, HASH), 10, 20, false, null));

        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, List.of(sinkHexString));
        var metrics =
                new EvaluatedMetrics(
                        Map.of(
                                source,
                                evaluated(10, 100, 100),
                                filterOperator,
                                evaluated(10, 400, 100),
                                sink,
                                evaluated(10, 400, 100)),
                        dummyGlobalMetrics);
        jobTopology.get(filterOperator).getInputRatios().put(source, 4.0);
        jobTopology.get(sink).getInputRatios().put(filterOperator, 1.0);
        var now = Instant.now();
        conf.set(AutoScalerOptions.PROCESSING_RATE_BACKPROPAGATION_IMPACT, 1.0);
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology,
                        new DelayedScaleDown()));
        assertEquals(
                50.0,
                metrics.getVertexMetrics()
                        .get(source)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                200.0,
                metrics.getVertexMetrics()
                        .get(filterOperator)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                200.0,
                metrics.getVertexMetrics()
                        .get(sink)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
    }

    @Test
    public void testDisconnectedJobMetricsUpdate() throws Exception {
        var source1HexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source1 = JobVertexID.fromHexString(source1HexString);
        var sink1HexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink1 = JobVertexID.fromHexString(sink1HexString);
        var source2HexString = "74b8b4bd0762e2177f8d71481f79f07c";
        var source2 = JobVertexID.fromHexString(source2HexString);
        var sink2HexString = "c044d7d1304e32489deb16b4e0b080ef";
        var sink2 = JobVertexID.fromHexString(sink2HexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source1, Map.of(), 10, 20, false, null),
                        new VertexInfo(source2, Map.of(), 10, 20, false, null),
                        new VertexInfo(sink1, Map.of(source1, HASH), 10, 20, false, null),
                        new VertexInfo(sink2, Map.of(source2, HASH), 10, 20, false, null));

        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.0);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 1.0);
        conf.set(AutoScalerOptions.PROCESSING_RATE_BACKPROPAGATION_ENABLED, true);
        var metrics =
                new EvaluatedMetrics(
                        Map.of(
                                source1,
                                evaluated(10, 100, 100),
                                source2,
                                evaluated(10, 200, 100),
                                sink1,
                                evaluated(10, 800, 100),
                                sink2,
                                evaluated(10, 500, 100)),
                        dummyGlobalMetrics);
        jobTopology.get(sink1).getInputRatios().put(source1, 8.0);
        jobTopology.get(sink2).getInputRatios().put(source2, 2.5);
        var now = Instant.now();
        conf.set(AutoScalerOptions.PROCESSING_RATE_BACKPROPAGATION_IMPACT, 1.0);
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology,
                        new DelayedScaleDown()));
        assertEquals(
                25,
                metrics.getVertexMetrics()
                        .get(source1)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                80.0,
                metrics.getVertexMetrics()
                        .get(source2)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                200.0,
                metrics.getVertexMetrics()
                        .get(sink1)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                200.0,
                metrics.getVertexMetrics()
                        .get(sink2)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
    }

    @Test
    public void testComplexJobMetricsUpdate() throws Exception {
        var source1HexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source1 = JobVertexID.fromHexString(source1HexString);
        var op1HexString = "b155797987c1d39a2c13f083476bce0d";
        var op1 = JobVertexID.fromHexString(op1HexString);
        var sink1HexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink1 = JobVertexID.fromHexString(sink1HexString);

        var source2HexString = "74b8b4bd0762e2177f8d71481f79f07c";
        var source2 = JobVertexID.fromHexString(source2HexString);
        var op2HexString = "958f423134cae6985a59e2d16bde444e";
        var op2 = JobVertexID.fromHexString(op2HexString);
        var sink2HexString = "c044d7d1304e32489deb16b4e0b080ef";
        var sink2 = JobVertexID.fromHexString(sink2HexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source1, Map.of(), 10, 20, false, null),
                        new VertexInfo(source2, Map.of(), 10, 20, false, null),
                        new VertexInfo(
                                op1, Map.of(source1, HASH, source2, HASH), 10, 20, false, null),
                        new VertexInfo(
                                op2, Map.of(source1, HASH, source2, HASH), 10, 20, false, null),
                        new VertexInfo(sink1, Map.of(op1, HASH, op2, HASH), 10, 20, false, null),
                        new VertexInfo(sink2, Map.of(op1, HASH, op2, HASH), 10, 20, false, null));

        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.0);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 1.0);
        var metrics =
                new EvaluatedMetrics(
                        Map.of(
                                source1,
                                evaluated(10, 100, 100),
                                source2,
                                evaluated(10, 100, 100),
                                op1,
                                evaluated(10, 200, 100),
                                op2,
                                evaluated(10, 200, 100),
                                sink1,
                                evaluated(10, 600, 100),
                                sink2,
                                evaluated(10, 500, 100)),
                        dummyGlobalMetrics);
        jobTopology.get(op1).getInputRatios().put(source1, 1.0);
        jobTopology.get(op1).getInputRatios().put(source2, 1.0);
        jobTopology.get(op2).getInputRatios().put(source1, 1.0);
        jobTopology.get(op2).getInputRatios().put(source2, 1.0);

        jobTopology.get(sink1).getInputRatios().put(op1, 1.0);
        jobTopology.get(sink1).getInputRatios().put(op2, 2.0);
        jobTopology.get(sink2).getInputRatios().put(op1, 2.0);
        jobTopology.get(sink2).getInputRatios().put(op2, 0.5);

        var now = Instant.now();
        conf.set(AutoScalerOptions.PROCESSING_RATE_BACKPROPAGATION_IMPACT, 0.8);
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology,
                        new DelayedScaleDown()));
        assertEquals(
                46.667,
                metrics.getVertexMetrics()
                        .get(source1)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                46.667,
                metrics.getVertexMetrics()
                        .get(source2)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                93.333,
                metrics.getVertexMetrics()
                        .get(op1)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                93.333,
                metrics.getVertexMetrics()
                        .get(op2)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());

        // since aggressive scaling down is disabled, the vertex is still a bottleneck
        assertEquals(
                280.0,
                metrics.getVertexMetrics()
                        .get(sink1)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
        assertEquals(
                233.333,
                metrics.getVertexMetrics()
                        .get(sink2)
                        .get(ScalingMetric.TARGET_DATA_RATE)
                        .getAverage());
    }

    private Map<ScalingMetric, EvaluatedScalingMetric> evaluated(
            int parallelism, double target, double trueProcessingRate) {
        var metrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        metrics.put(ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(parallelism));
        metrics.put(ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(MAX_PARALLELISM));
        metrics.put(ScalingMetric.TARGET_DATA_RATE, new EvaluatedScalingMetric(target, target));
        metrics.put(ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(0.0));
        metrics.put(
                ScalingMetric.TRUE_PROCESSING_RATE,
                new EvaluatedScalingMetric(trueProcessingRate, trueProcessingRate));

        var restartTime = context.getConfiguration().get(AutoScalerOptions.RESTART_TIME);
        ScalingMetricEvaluator.computeProcessingRateThresholds(
                metrics, context.getConfiguration(), false, restartTime);
        return metrics;
    }
}
