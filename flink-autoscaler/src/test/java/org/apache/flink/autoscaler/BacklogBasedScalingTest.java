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
import org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.state.TestingAutoscalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.JobAutoScalerImpl.AUTOSCALER_ERROR;
import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for scaling metrics collection logic. */
public class BacklogBasedScalingTest {

    private JobAutoScalerContext<JobID> context;
    private TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector;
    private TestingAutoscalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    private TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>> metricsCollector;
    private ScalingExecutor<JobID, JobAutoScalerContext<JobID>> scalingExecutor;

    private JobVertexID source1, sink;

    private JobAutoScalerImpl<JobID, JobAutoScalerContext<JobID>> autoscaler;

    @BeforeEach
    public void setup() {
        context = createDefaultJobAutoScalerContext();

        eventCollector = new TestingEventCollector<>();
        stateStore = new TestingAutoscalerStateStore<>();

        scalingExecutor = new ScalingExecutor<>(eventCollector, stateStore);

        source1 = new JobVertexID();
        sink = new JobVertexID();

        metricsCollector =
                new TestingMetricsCollector<>(
                        new JobTopology(
                                new VertexInfo(source1, Set.of(), 1, 720),
                                new VertexInfo(sink, Set.of(source1), 1, 720)));

        var defaultConf = context.getConfiguration();
        defaultConf.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        defaultConf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ZERO);
        defaultConf.set(AutoScalerOptions.RESTART_TIME, Duration.ofSeconds(1));
        defaultConf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ofSeconds(2));
        defaultConf.set(AutoScalerOptions.SCALING_ENABLED, true);
        defaultConf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        defaultConf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, (double) Integer.MAX_VALUE);
        defaultConf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.8);
        defaultConf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.1);
        defaultConf.set(AutoScalerOptions.SCALE_UP_GRACE_PERIOD, Duration.ZERO);

        autoscaler =
                new JobAutoScalerImpl<>(
                        metricsCollector,
                        new ScalingMetricEvaluator(),
                        scalingExecutor,
                        eventCollector,
                        new TestingScalingRealizer<>(),
                        stateStore);

        // Reset custom window size to default
        metricsCollector.setTestMetricWindowSize(null);
    }

    @Test
    public void test() throws Exception {
        /* Test scaling up. */
        var now = Instant.ofEpochMilli(0);
        setClocksTo(now);
        metricsCollector.setJobUpdateTs(now);
        // Adjust metric window size, so we can fill the metric window with two metrics
        metricsCollector.setTestMetricWindowSize(Duration.ofSeconds(1));
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 850., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 2000.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 850., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 500.))));

        autoscaler.scale(context);
        assertEvaluatedMetricsSize(1);
        assertFlinkMetricsCount(0, 0);

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);
        assertEvaluatedMetricsSize(2);

        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));
        assertFlinkMetricsCount(1, 0);

        /* Test stability while processing pending records. */

        // Update topology to reflect updated parallelisms
        metricsCollector.setJobTopology(
                new JobTopology(
                        new VertexInfo(source1, Set.of(), 4, 24),
                        new VertexInfo(sink, Set.of(source1), 4, 720)));
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1800.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1800.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 2500.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 1800.))));
        now = now.plusSeconds(1);
        setClocksTo(now);
        // Redeploying which erases metric history
        metricsCollector.setJobUpdateTs(now);
        // Adjust metric window size, so we can fill the metric window with three metrics
        metricsCollector.setTestMetricWindowSize(Duration.ofSeconds(2));

        autoscaler.scale(context);
        assertFlinkMetricsCount(1, 0);
        assertEvaluatedMetricsSize(1);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));

        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1800.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1800.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 1200.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 1800.))));

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);
        assertFlinkMetricsCount(1, 0);
        assertEvaluatedMetricsSize(2);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));

        /* Test scaling down. */

        // We have finally caught up to our original lag, time to scale down
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 600., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 800.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 800.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 600., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 800.))));
        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);
        assertFlinkMetricsCount(2, 0);
        assertEvaluatedMetricsSize(3);

        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));

        metricsCollector.setJobUpdateTs(now);
        metricsCollector.setJobTopology(
                new JobTopology(
                        new VertexInfo(source1, Set.of(), 2, 24),
                        new VertexInfo(sink, Set.of(source1), 2, 720)));

        /* Test stability while processing backlog. */

        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 900.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 900.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 900.))));
        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);
        assertFlinkMetricsCount(2, 0);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));

        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 900.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 900.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 100.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 900.))));
        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);
        assertFlinkMetricsCount(2, 0);

        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));

        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 500., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 500., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 500.))));
        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);
        assertFlinkMetricsCount(2, 1);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));
    }

    @Test
    public void testMetricsPersistedAfterRedeploy() throws Exception {
        var now = Instant.ofEpochMilli(0);
        setClocksTo(now);
        metricsCollector.setJobUpdateTs(now);
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 850., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 850., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 500.))));

        autoscaler.scale(context);
        assertFalse(stateStore.getEvaluatedMetrics(context).get().isEmpty());
    }

    @Test
    public void testEventOnError() throws Exception {
        // Invalid config
        context.getConfiguration().setString(AutoScalerOptions.AUTOSCALER_ENABLED.key(), "3");
        autoscaler.scale(context);

        var event = eventCollector.events.poll();
        assertTrue(eventCollector.events.isEmpty());
        Assertions.assertEquals(AUTOSCALER_ERROR, event.getReason());
        assertTrue(event.getMessage().startsWith("Could not parse"));
    }

    @Test
    public void testNoEvaluationDuringStabilization() throws Exception {
        context.getConfiguration()
                .set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ofMinutes(1));

        var now = Instant.ofEpochMilli(0);
        setClocksTo(now);
        metricsCollector.setJobUpdateTs(now);
        autoscaler.scale(context);
        assertTrue(stateStore.getEvaluatedMetrics(context).isEmpty());
        assertTrue(eventCollector.events.isEmpty());
    }

    private void assertEvaluatedMetricsSize(int expectedSize) {
        Optional<SortedMap<Instant, CollectedMetrics>> evaluatedMetricsOpt =
                stateStore.getEvaluatedMetrics(context);
        assertThat(evaluatedMetricsOpt).isPresent();
        assertThat(evaluatedMetricsOpt.get()).hasSize(expectedSize);
    }

    private void setClocksTo(Instant time) {
        var clock = Clock.fixed(time, ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        scalingExecutor.setClock(clock);
    }

    private void assertFlinkMetricsCount(int scalingCount, int balancedCount) {
        AutoscalerFlinkMetrics autoscalerFlinkMetrics =
                autoscaler.flinkMetrics.get(context.getJobKey());
        assertEquals(scalingCount, autoscalerFlinkMetrics.getNumScalingsCount());
        assertEquals(balancedCount, autoscalerFlinkMetrics.getNumBalancedCount());
    }
}
