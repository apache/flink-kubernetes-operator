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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.TestMetrics;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.IOMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.JobAutoScalerImpl.AUTOSCALER_ERROR;
import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for scaling metrics collection logic. */
public class BacklogBasedScalingTest {

    private JobAutoScalerContext<JobID> context;
    private TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector;
    private AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    private TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>> metricsCollector;
    private ScalingExecutor<JobID, JobAutoScalerContext<JobID>> scalingExecutor;

    private JobVertexID source1, sink;

    private JobAutoScalerImpl<JobID, JobAutoScalerContext<JobID>> autoscaler;

    @BeforeEach
    public void setup() {
        context = createDefaultJobAutoScalerContext();

        eventCollector = new TestingEventCollector<>();
        stateStore = new InMemoryAutoScalerStateStore<>();

        scalingExecutor = new ScalingExecutor<>(eventCollector, stateStore);

        source1 = new JobVertexID();
        sink = new JobVertexID();

        metricsCollector =
                new TestingMetricsCollector<>(
                        new JobTopology(
                                new VertexInfo(source1, Map.of(), 1, 720, new IOMetrics(0, 0, 0)),
                                new VertexInfo(
                                        sink,
                                        Map.of(source1, REBALANCE),
                                        1,
                                        720,
                                        new IOMetrics(0, 0, 0))));

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
        defaultConf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ZERO);
        defaultConf.set(AutoScalerOptions.BACKLOG_PROCESSING_LAG_THRESHOLD, Duration.ofSeconds(1));

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
        metricsCollector.updateMetrics(
                source1,
                TestMetrics.builder()
                        .numRecordsIn(0)
                        .numRecordsOut(0)
                        .numRecordsInPerSec(500.)
                        .maxBusyTimePerSec(850)
                        .pendingRecords(2000L)
                        .build());
        metricsCollector.updateMetrics(
                sink, TestMetrics.builder().numRecordsIn(0).maxBusyTimePerSec(850).build());

        autoscaler.scale(context);
        assertCollectedMetricsSize(1);
        assertFlinkMetricsCount(0, 0);

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);

        metricsCollector.updateMetrics(
                source1, m -> m.setNumRecordsIn(500), m -> m.setNumRecordsOut(500));
        metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(500));

        autoscaler.scale(context);
        assertCollectedMetricsSize(2);

        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));
        assertFlinkMetricsCount(1, 0);

        /* Test stability while processing pending records. */

        // Update topology to reflect updated parallelisms
        metricsCollector.setJobTopology(
                new JobTopology(
                        new VertexInfo(source1, Map.of(), 4, 24),
                        new VertexInfo(sink, Map.of(source1, REBALANCE), 4, 720)));

        metricsCollector.updateMetrics(
                source1,
                TestMetrics.builder()
                        .numRecordsIn(0)
                        .numRecordsOut(0)
                        .numRecordsInPerSec(1800.)
                        .maxBusyTimePerSec(1000)
                        .pendingRecords(6000L)
                        .build());
        metricsCollector.updateMetrics(
                sink, TestMetrics.builder().numRecordsIn(0).maxBusyTimePerSec(1000).build());

        now = now.plusSeconds(1);
        setClocksTo(now);
        // Redeploying which erases metric history
        metricsCollector.setJobUpdateTs(now);
        // Adjust metric window size, so we can fill the metric window with three metrics
        metricsCollector.setTestMetricWindowSize(Duration.ofSeconds(2));

        autoscaler.scale(context);

        assertFlinkMetricsCount(1, 0);
        assertCollectedMetricsSize(1);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));

        metricsCollector.updateMetrics(
                source1,
                m -> m.setNumRecordsIn(1800),
                m -> m.setNumRecordsOut(1800),
                m -> m.setPendingRecords(3600L));
        metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(1800));

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);

        metricsCollector.updateMetrics(
                source1,
                m -> m.setNumRecordsIn(3600),
                m -> m.setNumRecordsOut(3600),
                m -> m.setPendingRecords(2000L));
        metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(3600));

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);

        assertFlinkMetricsCount(1, 1);
        assertCollectedMetricsSize(3);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));

        /* Test scaling down. */

        // We have finally caught up, time to scale down

        metricsCollector.updateMetrics(
                source1,
                m -> m.setNumRecordsIn(5400),
                m -> m.setNumRecordsIn(5400),
                m -> m.setPendingRecords(400L));
        metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(5400));

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);
        assertFlinkMetricsCount(2, 1);
        assertCollectedMetricsSize(3);

        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));

        metricsCollector.setJobUpdateTs(now);
        metricsCollector.setJobTopology(
                new JobTopology(
                        new VertexInfo(source1, Map.of(), 2, 24),
                        new VertexInfo(sink, Map.of(source1, REBALANCE), 2, 720)));

        /* Test stability while processing backlog. */

        metricsCollector.updateMetrics(
                source1,
                TestMetrics.builder()
                        .numRecordsIn(0)
                        .numRecordsOut(0)
                        .numRecordsInPerSec(900.)
                        .maxBusyTimePerSec(1000)
                        .pendingRecords(2000L)
                        .build());
        metricsCollector.updateMetrics(
                sink, TestMetrics.builder().numRecordsIn(0).maxBusyTimePerSec(1000).build());

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);

        metricsCollector.updateMetrics(
                source1,
                m -> m.setNumRecordsIn(900),
                m -> m.setNumRecordsIn(900),
                m -> m.setPendingRecords(1400L));
        metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(900));

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);

        assertFlinkMetricsCount(2, 1);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));

        metricsCollector.updateMetrics(
                source1,
                m -> m.setNumRecordsIn(1800),
                m -> m.setNumRecordsIn(1800),
                m -> m.setPendingRecords(800L));
        metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(1800));

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);
        assertFlinkMetricsCount(2, 2);

        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));

        metricsCollector.updateMetrics(
                source1,
                m -> m.setNumRecordsIn(2700),
                m -> m.setNumRecordsIn(2700),
                m -> m.setPendingRecords(300L));
        metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(2700));

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);
        assertFlinkMetricsCount(2, 3);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));
    }

    @Test
    public void shouldTrackRestartDurationCorrectly() throws Exception {
        var now = Instant.ofEpochMilli(0);
        setClocksTo(now);
        metricsCollector.setJobUpdateTs(now);
        metricsCollector.setTestMetricWindowSize(Duration.ofSeconds(1));
        // Trigger scaling up
        metricsCollector.updateMetrics(
                source1,
                TestMetrics.builder()
                        .numRecordsIn(0)
                        .numRecordsOut(0)
                        .numRecordsInPerSec(500.)
                        .maxBusyTimePerSec(850)
                        .pendingRecords(2000L)
                        .build());
        metricsCollector.updateMetrics(
                sink, TestMetrics.builder().numRecordsIn(0).maxBusyTimePerSec(850).build());

        autoscaler.scale(context);
        assertCollectedMetricsSize(1);
        assertFlinkMetricsCount(0, 0);

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);

        metricsCollector.updateMetrics(
                source1, m -> m.setNumRecordsIn(500), m -> m.setNumRecordsOut(500));
        metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(100));

        autoscaler.scale(context);
        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(context);

        // Scaling was applied
        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));

        // New tracking with null end time was added
        assertLastTrackingEndTimeIs(null);

        context = context.toBuilder().jobStatus(JobStatus.INITIALIZING).build();
        autoscaler.scale(context);

        // Tracking end time not set until RUNNING
        assertLastTrackingEndTimeIs(null);

        // Job transitioned to RUNNING and the target parallelism was reached
        context = context.toBuilder().jobStatus(JobStatus.RUNNING).build();
        metricsCollector.setJobTopology(
                new JobTopology(
                        new VertexInfo(source1, Map.of(), 4, 720),
                        new VertexInfo(sink, Map.of(source1, REBALANCE), 4, 720)));

        var expectedEndTime = Instant.ofEpochMilli(10);
        metricsCollector.setJobUpdateTs(expectedEndTime);
        autoscaler.scale(context);

        assertLastTrackingEndTimeIs(expectedEndTime);
    }

    private void assertLastTrackingEndTimeIs(Instant expectedEndTime) throws Exception {
        var scalingTracking = stateStore.getScalingTracking(context);
        var latestScalingRecordEntry = scalingTracking.getLatestScalingRecordEntry().get();
        var startTime = latestScalingRecordEntry.getKey();
        var restartDuration = latestScalingRecordEntry.getValue().getRestartDuration();
        if (expectedEndTime == null) {
            assertThat(restartDuration).isNull();
        } else {
            assertThat(restartDuration).isEqualTo(Duration.between(startTime, expectedEndTime));
        }
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
        assertTrue(autoscaler.lastEvaluatedMetrics.isEmpty());
        assertTrue(eventCollector.events.isEmpty());
    }

    private void assertCollectedMetricsSize(int expectedSize) throws Exception {
        SortedMap<Instant, CollectedMetrics> evaluatedMetrics =
                stateStore.getCollectedMetrics(context);
        assertThat(evaluatedMetrics).hasSize(expectedSize);
    }

    private void setClocksTo(Instant time) {
        var clock = Clock.fixed(time, ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        autoscaler.setClock(clock);
    }

    private void assertFlinkMetricsCount(int scalingCount, int balancedCount) {
        AutoscalerFlinkMetrics autoscalerFlinkMetrics =
                autoscaler.flinkMetrics.get(context.getJobKey());
        assertEquals(scalingCount, autoscalerFlinkMetrics.getNumScalingsCount());
        assertEquals(balancedCount, autoscalerFlinkMetrics.getNumBalancedCount());
    }
}
