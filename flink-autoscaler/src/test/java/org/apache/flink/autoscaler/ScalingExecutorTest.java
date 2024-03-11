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
import org.apache.flink.autoscaler.resources.ResourceCheck;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.IOMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_REPORT_REASON;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_ENTRY;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED;
import static org.apache.flink.autoscaler.event.AutoScalerEventHandler.SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED;
import static org.apache.flink.autoscaler.topology.ShipStrategy.HASH;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link ScalingExecutor}. */
public class ScalingExecutorTest {

    private static final int MAX_PARALLELISM = 720;

    private JobAutoScalerContext<JobID> context;
    private TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector;
    private ScalingExecutor<JobID, JobAutoScalerContext<JobID>> scalingExecutor;

    private InMemoryAutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    private Configuration conf;

    private ScalingTracking scalingTracking = new ScalingTracking();

    private static final Map<ScalingMetric, EvaluatedScalingMetric> dummyGlobalMetrics =
            Map.of(
                    ScalingMetric.GC_PRESSURE, EvaluatedScalingMetric.of(Double.NaN),
                    ScalingMetric.HEAP_MAX_USAGE_RATIO, EvaluatedScalingMetric.of(Double.NaN));

    @BeforeEach
    public void setup() {
        eventCollector = new TestingEventCollector<>();
        context = createDefaultJobAutoScalerContext();
        stateStore = new InMemoryAutoScalerStateStore<>();

        scalingExecutor = new ScalingExecutor<>(eventCollector, stateStore);
        conf = context.getConfiguration();
        conf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ZERO);
        conf.set(AutoScalerOptions.SCALING_ENABLED, true);
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, (double) Integer.MAX_VALUE);
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ZERO);
    }

    @Test
    public void testUtilizationBoundaries() throws Exception {
        // Restart time should not affect utilization boundary
        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.RESTART_TIME, Duration.ZERO);
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ZERO);

        var op1 = new JobVertexID();

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.6);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.);

        var evaluated = Map.of(op1, evaluated(1, 70, 100));
        var scalingSummary = Map.of(op1, new ScalingSummary(2, 1, evaluated.get(op1)));
        assertFalse(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.2);
        evaluated = Map.of(op1, evaluated(1, 70, 100));
        scalingSummary = Map.of(op1, new ScalingSummary(2, 1, evaluated.get(op1)));
        assertTrue(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));
        assertTrue(getScaledParallelism(stateStore, context).isEmpty());

        var op2 = new JobVertexID();
        evaluated =
                Map.of(
                        op1, evaluated(1, 70, 100),
                        op2, evaluated(1, 85, 100));
        scalingSummary =
                Map.of(
                        op1,
                        new ScalingSummary(1, 2, evaluated.get(op1)),
                        op2,
                        new ScalingSummary(1, 2, evaluated.get(op2)));

        assertFalse(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));

        evaluated =
                Map.of(
                        op1, evaluated(1, 70, 100),
                        op2, evaluated(1, 70, 100));
        scalingSummary =
                Map.of(
                        op1,
                        new ScalingSummary(1, 2, evaluated.get(op1)),
                        op2,
                        new ScalingSummary(1, 2, evaluated.get(op2)));
        assertTrue(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));

        // Test with backlog based scaling
        evaluated = Map.of(op1, evaluated(1, 70, 100, 15));
        scalingSummary = Map.of(op1, new ScalingSummary(1, 2, evaluated.get(op1)));
        assertFalse(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));
    }

    @Test
    public void testNoScaleDownOnZeroLowerUtilizationBoundary() throws Exception {
        var conf = context.getConfiguration();
        // Target utilization and boundary are identical
        // which will set the scale down boundary to infinity
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.6);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.6);

        var vertex = new JobVertexID();
        int parallelism = 100;
        int expectedParallelism = 1;
        int targetRate = 1000;
        // Intentionally also set the true processing rate to infinity
        // to test the boundaries of the scaling condition.
        double trueProcessingRate = Double.POSITIVE_INFINITY;

        var evaluated =
                new EvaluatedMetrics(
                        Map.of(vertex, evaluated(parallelism, targetRate, trueProcessingRate)),
                        dummyGlobalMetrics);

        // Verify precondition
        var scalingSummary =
                Map.of(
                        vertex,
                        new ScalingSummary(
                                parallelism,
                                expectedParallelism,
                                evaluated.getVertexMetrics().get(vertex)));
        assertTrue(
                ScalingExecutor.allVerticesWithinUtilizationTarget(
                        evaluated.getVertexMetrics(), scalingSummary));

        // Execute the full scaling path
        var now = Instant.now();
        var jobTopology =
                new JobTopology(
                        new VertexInfo(
                                vertex,
                                Map.of(),
                                parallelism,
                                Integer.MAX_VALUE,
                                new IOMetrics(10000, 10000, 100)));
        assertFalse(
                scalingExecutor.scaleResource(
                        context, evaluated, new HashMap<>(), scalingTracking, now, jobTopology));
    }

    @Test
    public void testVertexesExclusionForScaling() throws Exception {
        var sourceHexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source = JobVertexID.fromHexString(sourceHexString);
        var filterOperatorHexString = "869fb403873411306404e9f2e4438c0e";
        var filterOperator = JobVertexID.fromHexString(filterOperatorHexString);
        var sinkHexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink = JobVertexID.fromHexString(sinkHexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 10, 1000, false, null),
                        new VertexInfo(filterOperator, Map.of(source, HASH), 10, 1000, false, null),
                        new VertexInfo(sink, Map.of(filterOperator, HASH), 10, 1000, false, null));

        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        var metrics =
                new EvaluatedMetrics(
                        Map.of(
                                source,
                                evaluated(10, 80, 100),
                                filterOperator,
                                evaluated(10, 30, 100),
                                sink,
                                evaluated(10, 80, 100)),
                        dummyGlobalMetrics);
        // filter operator should not scale
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, List.of(filterOperatorHexString));
        var now = Instant.now();
        assertFalse(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));
        // filter operator should scale
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, List.of());
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));
    }

    @Test
    public void testExcludedPeriodsForScaling() throws Exception {
        var sourceHexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source = JobVertexID.fromHexString(sourceHexString);
        var sinkHexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink = JobVertexID.fromHexString(sinkHexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 10, 1000, false, null),
                        new VertexInfo(sink, Map.of(source, HASH), 10, 1000, false, null));

        var conf = context.getConfiguration();
        var now = Instant.now();
        var localTime = ZonedDateTime.ofInstant(now, ZoneId.systemDefault()).toLocalTime();
        // scaling execution in excluded periods
        var excludedPeriod =
                new StringBuilder(localTime.toString().split("\\.")[0])
                        .append("-")
                        .append(localTime.plusSeconds(300).toString().split("\\.")[0])
                        .toString();
        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of(excludedPeriod));
        var metrics =
                new EvaluatedMetrics(
                        Map.of(source, evaluated(10, 110, 100), sink, evaluated(10, 110, 100)),
                        dummyGlobalMetrics);
        assertFalse(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));
        // scaling execution outside excluded periods
        excludedPeriod =
                new StringBuilder(localTime.plusSeconds(100).toString().split("\\.")[0])
                        .append("-")
                        .append(localTime.plusSeconds(300).toString().split("\\.")[0])
                        .toString();
        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of(excludedPeriod));
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));
    }

    @Test
    public void testBlockScalingOnFailedResourceCheck() throws Exception {
        var sourceHexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source = JobVertexID.fromHexString(sourceHexString);
        var sinkHexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink = JobVertexID.fromHexString(sinkHexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 10, 1000, false, null),
                        new VertexInfo(sink, Map.of(source, HASH), 10, 1000, false, null));

        var now = Instant.now();
        var metrics =
                new EvaluatedMetrics(
                        Map.of(source, evaluated(10, 100, 50), sink, evaluated(10, 100, 50)),
                        Map.of(
                                ScalingMetric.NUM_TASK_SLOTS_USED,
                                EvaluatedScalingMetric.of(9),
                                ScalingMetric.GC_PRESSURE,
                                EvaluatedScalingMetric.of(Double.NaN),
                                ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                EvaluatedScalingMetric.of(Double.NaN)));

        // Would normally scale without resource usage check
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));

        scalingExecutor =
                new ScalingExecutor<>(
                        eventCollector,
                        stateStore,
                        new ResourceCheck() {
                            @Override
                            public boolean trySchedule(
                                    int currentInstances,
                                    int newInstances,
                                    double cpuPerInstance,
                                    MemorySize memoryPerInstance) {
                                return false;
                            }
                        });

        // Scaling blocked due to unavailable resources
        assertFalse(
                scalingExecutor.scaleResource(
                        TestingAutoscalerUtils.createResourceAwareContext(),
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));
    }

    @Test
    public void testMemoryTuning() throws Exception {
        context = TestingAutoscalerUtils.createResourceAwareContext();
        context.getConfiguration().set(AutoScalerOptions.MEMORY_TUNING_ENABLED, true);
        context.getConfiguration().set(TaskManagerOptions.NUM_TASK_SLOTS, 5);
        context.getConfiguration()
                .set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("30 gb"));

        var source = new JobVertexID();
        var sink = new JobVertexID();
        var now = Instant.now();

        var globalMetrics =
                Map.of(
                        ScalingMetric.NUM_TASK_SLOTS_USED,
                        EvaluatedScalingMetric.of(9),
                        ScalingMetric.HEAP_MEMORY_USED,
                        EvaluatedScalingMetric.avg(MemorySize.parse("5 Gb").getBytes()),
                        ScalingMetric.MANAGED_MEMORY_USED,
                        EvaluatedScalingMetric.avg(MemorySize.parse("2 Gb").getBytes()),
                        ScalingMetric.METASPACE_MEMORY_USED,
                        EvaluatedScalingMetric.avg(MemorySize.parse("300 mb").getBytes()),
                        ScalingMetric.HEAP_MAX_USAGE_RATIO,
                        EvaluatedScalingMetric.of(Double.NaN),
                        ScalingMetric.GC_PRESSURE,
                        EvaluatedScalingMetric.of(Double.NaN));
        var vertexMetrics =
                Map.of(source, evaluated(10, 100, 50, 0), sink, evaluated(10, 100, 50, 0));
        var metrics = new EvaluatedMetrics(vertexMetrics, globalMetrics);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 10, 1000, false, null),
                        new VertexInfo(sink, Map.of(source, REBALANCE), 10, 1000, false, null));

        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));
        assertThat(stateStore.getConfigChanges(context).getOverrides())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
                                "0.652",
                                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                                "24320 kb",
                                TaskManagerOptions.NETWORK_MEMORY_MAX.key(),
                                "24320 kb",
                                TaskManagerOptions.JVM_METASPACE.key(),
                                "360 mb",
                                TaskManagerOptions.JVM_OVERHEAD_FRACTION.key(),
                                "0.053",
                                TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(),
                                "0 bytes",
                                TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                                "20400832696 bytes"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testScalingEventsWith0IntervalConfig(boolean scalingEnabled) throws Exception {
        testScalingEvents(scalingEnabled, Duration.ofSeconds(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testScalingEventsWithIntervalConfig(boolean scalingEnabled) throws Exception {
        testScalingEvents(scalingEnabled, Duration.ofSeconds(1800));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testScalingEventsWithDefaultIntervalConfig(boolean scalingEnabled)
            throws Exception {
        testScalingEvents(scalingEnabled, null);
    }

    private void testScalingEvents(boolean scalingEnabled, Duration interval) throws Exception {
        var jobVertexID = new JobVertexID();

        JobTopology jobTopology =
                new JobTopology(new VertexInfo(jobVertexID, Map.of(), 10, 1000, false, null));

        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.SCALING_ENABLED, scalingEnabled);
        if (interval != null) {
            conf.set(AutoScalerOptions.SCALING_EVENT_INTERVAL, interval);
        }

        var now = Instant.now();
        var metrics =
                new EvaluatedMetrics(
                        Map.of(jobVertexID, evaluated(1, 110, 100)), dummyGlobalMetrics);
        assertEquals(
                scalingEnabled,
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));
        assertEquals(
                scalingEnabled,
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));
        int expectedSize = (interval == null || interval.toMillis() > 0) && !scalingEnabled ? 1 : 2;
        assertEquals(expectedSize, eventCollector.events.size());

        TestingEventCollector.Event<JobID, JobAutoScalerContext<JobID>> event;
        do {
            event = eventCollector.events.poll();
        } while (!eventCollector.events.isEmpty());

        assertTrue(
                event.getMessage()
                        .contains(
                                String.format(
                                        SCALING_SUMMARY_ENTRY,
                                        jobVertexID,
                                        1,
                                        2,
                                        100.0,
                                        157.0,
                                        110.0)));
        assertTrue(
                event.getMessage()
                        .contains(
                                scalingEnabled
                                        ? SCALING_SUMMARY_HEADER_SCALING_EXECUTION_ENABLED
                                        : SCALING_SUMMARY_HEADER_SCALING_EXECUTION_DISABLED));
        assertEquals(SCALING_REPORT_REASON, event.getReason());

        metrics =
                new EvaluatedMetrics(
                        Map.of(jobVertexID, evaluated(1, 110, 101)), dummyGlobalMetrics);
        assertEquals(
                scalingEnabled,
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        now,
                        jobTopology));
        var event2 = eventCollector.events.poll();
        assertThat(event2).isNotNull();
        assertThat(event2.getContext()).isSameAs(event.getContext());
        assertEquals(expectedSize + 1, event2.getCount());
        assertEquals(!scalingEnabled, stateStore.getParallelismOverrides(context).isEmpty());
    }

    @Test
    public void testScalingUnderGcPressure() throws Exception {
        var jobVertexID = new JobVertexID();
        conf.set(AutoScalerOptions.SCALING_ENABLED, true);
        conf.set(AutoScalerOptions.GC_PRESSURE_THRESHOLD, 0.5);
        conf.set(AutoScalerOptions.HEAP_USAGE_THRESHOLD, 0.8);

        var vertexMetrics = Map.of(jobVertexID, evaluated(1, 110, 100));
        JobTopology jobTopology =
                new JobTopology(new VertexInfo(jobVertexID, Map.of(), 10, 1000, false, null));
        var metrics =
                new EvaluatedMetrics(
                        vertexMetrics,
                        Map.of(
                                ScalingMetric.GC_PRESSURE,
                                EvaluatedScalingMetric.of(Double.NaN),
                                ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                EvaluatedScalingMetric.of(Double.NaN)));

        // Baseline, no GC/Heap metrics
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        Instant.now(),
                        jobTopology));

        // Just below the thresholds
        metrics =
                new EvaluatedMetrics(
                        vertexMetrics,
                        Map.of(
                                ScalingMetric.GC_PRESSURE,
                                EvaluatedScalingMetric.of(0.49),
                                ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                new EvaluatedScalingMetric(0.9, 0.79)));
        assertTrue(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        Instant.now(),
                        jobTopology));

        eventCollector.events.clear();

        // GC Pressure above limit
        metrics =
                new EvaluatedMetrics(
                        vertexMetrics,
                        Map.of(
                                ScalingMetric.GC_PRESSURE,
                                EvaluatedScalingMetric.of(0.51),
                                ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                new EvaluatedScalingMetric(0.9, 0.79)));
        assertFalse(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        Instant.now(),
                        jobTopology));
        assertEquals("MemoryPressure", eventCollector.events.poll().getReason());
        assertTrue(eventCollector.events.isEmpty());

        // Heap usage above limit
        metrics =
                new EvaluatedMetrics(
                        vertexMetrics,
                        Map.of(
                                ScalingMetric.GC_PRESSURE,
                                EvaluatedScalingMetric.of(0.49),
                                ScalingMetric.HEAP_MAX_USAGE_RATIO,
                                new EvaluatedScalingMetric(0.6, 0.81)));
        assertFalse(
                scalingExecutor.scaleResource(
                        context,
                        metrics,
                        new HashMap<>(),
                        new ScalingTracking(),
                        Instant.now(),
                        jobTopology));
        assertEquals("MemoryPressure", eventCollector.events.poll().getReason());
        assertTrue(eventCollector.events.isEmpty());
    }

    @Test
    public void testAdjustByMaxParallelism() throws Exception {
        var sourceHexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source = JobVertexID.fromHexString(sourceHexString);
        var filterOperatorHexString = "869fb403873411306404e9f2e4438c0e";
        var filterOperator = JobVertexID.fromHexString(filterOperatorHexString);
        var sinkHexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink = JobVertexID.fromHexString(sinkHexString);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(source, Map.of(), 2, MAX_PARALLELISM, false, null),
                        new VertexInfo(
                                filterOperator,
                                Map.of(source, REBALANCE),
                                2,
                                MAX_PARALLELISM,
                                false,
                                null),
                        new VertexInfo(
                                sink,
                                Map.of(filterOperator, HASH),
                                2,
                                MAX_PARALLELISM,
                                false,
                                null));

        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.d);

        // The expected new parallelism is 7 without adjustment by max parallelism.
        var metrics =
                new EvaluatedMetrics(
                        Map.of(
                                source,
                                evaluated(2, 70, 20),
                                filterOperator,
                                evaluated(2, 70, 20),
                                sink,
                                evaluated(2, 70, 20)),
                        dummyGlobalMetrics);
        var now = Instant.now();
        assertThat(
                        scalingExecutor.scaleResource(
                                context,
                                metrics,
                                new HashMap<>(),
                                new ScalingTracking(),
                                now,
                                jobTopology))
                .isTrue();

        Map<String, String> parallelismOverrides = stateStore.getParallelismOverrides(context);
        // The source and keyed Operator should enable the parallelism adjustment, so the
        // parallelism of source and sink are adjusted, but filter is not.
        assertThat(parallelismOverrides)
                .containsAllEntriesOf(
                        Map.of(
                                "0bfd135746ac8efb3cce668b12e16d3a",
                                "8",
                                "869fb403873411306404e9f2e4438c0e",
                                "7",
                                "a6b7102b8d3e3a9564998c1ffeb5e2b7",
                                "8"));
    }

    private Map<ScalingMetric, EvaluatedScalingMetric> evaluated(
            int parallelism, double target, double trueProcessingRate, double catchupRate) {
        var metrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        metrics.put(ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(parallelism));
        metrics.put(ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(MAX_PARALLELISM));
        metrics.put(ScalingMetric.TARGET_DATA_RATE, new EvaluatedScalingMetric(target, target));
        metrics.put(ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(catchupRate));
        metrics.put(
                ScalingMetric.TRUE_PROCESSING_RATE,
                new EvaluatedScalingMetric(trueProcessingRate, trueProcessingRate));

        var restartTime = context.getConfiguration().get(AutoScalerOptions.RESTART_TIME);
        ScalingMetricEvaluator.computeProcessingRateThresholds(
                metrics, context.getConfiguration(), false, restartTime);
        return metrics;
    }

    private Map<ScalingMetric, EvaluatedScalingMetric> evaluated(
            int parallelism, double target, double trueProcessingRate) {
        return evaluated(parallelism, target, trueProcessingRate, 0.);
    }

    protected static <KEY, Context extends JobAutoScalerContext<KEY>>
            Map<JobVertexID, Integer> getScaledParallelism(
                    AutoScalerStateStore<KEY, Context> stateStore, Context context)
                    throws Exception {
        return stateStore.getParallelismOverrides(context).entrySet().stream()
                .collect(
                        Collectors.toMap(
                                e -> JobVertexID.fromHexString(e.getKey()),
                                e -> Integer.valueOf(e.getValue())));
    }
}
