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
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.TestingAutoscalerUtils.getRestClusterClientSupplier;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for recommended parallelism. */
public class RecommendedParallelismTest {

    private JobAutoScalerContext<JobID> context;
    private AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    private TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>> metricsCollector;
    private ScalingExecutor<JobID, JobAutoScalerContext<JobID>> scalingExecutor;

    private JobVertexID source, sink;

    private JobAutoScalerImpl<JobID, JobAutoScalerContext<JobID>> autoscaler;

    @BeforeEach
    public void setup() {
        context = createDefaultJobAutoScalerContext();

        TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector =
                new TestingEventCollector<>();
        stateStore = new InMemoryAutoScalerStateStore<>();

        scalingExecutor = new ScalingExecutor<>(eventCollector, stateStore);

        source = new JobVertexID();
        sink = new JobVertexID();

        metricsCollector =
                new TestingMetricsCollector<>(
                        new JobTopology(
                                new VertexInfo(source, Set.of(), 1, 720),
                                new VertexInfo(sink, Set.of(source), 1, 720)));

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
    public void endToEnd() throws Exception {

        // we start the autoscaler in advisor mode
        context.getConfiguration().setString(AutoScalerOptions.SCALING_ENABLED.key(), "false");

        // initially the last evaluated metrics are empty
        assertNull(autoscaler.lastEvaluatedMetrics.get(context.getJobKey()));

        var now = Instant.ofEpochMilli(0);
        setClocksTo(now);
        running(now);

        metricsCollector.setTestMetricWindowSize(Duration.ofSeconds(2));
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source,
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

        // the recommended parallelism values are empty initially
        autoscaler.scale(context);
        assertEvaluatedMetricsSize(1);
        assertNull(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM));
        assertNull(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM));
        assertEquals(1., getCurrentMetricValue(source, PARALLELISM));
        assertEquals(1., getCurrentMetricValue(sink, PARALLELISM));
        // the auto scaler is running in recommendation mode
        assertTrue(ScalingExecutorTest.getScaledParallelism(stateStore, context).isEmpty());

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);

        // it stays empty until the metric window is full
        autoscaler.scale(context);
        assertEvaluatedMetricsSize(2);
        assertNull(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM));
        assertNull(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM));
        assertEquals(1., getCurrentMetricValue(source, PARALLELISM));
        assertEquals(1., getCurrentMetricValue(sink, PARALLELISM));
        assertTrue(ScalingExecutorTest.getScaledParallelism(stateStore, context).isEmpty());

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);

        // then the recommended parallelism can change according to the evaluated metrics
        autoscaler.scale(context);
        assertEvaluatedMetricsSize(3);
        assertEquals(1., getCurrentMetricValue(source, PARALLELISM));
        assertEquals(1., getCurrentMetricValue(sink, PARALLELISM));
        assertEquals(4., getCurrentMetricValue(source, RECOMMENDED_PARALLELISM));
        assertEquals(4., getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM));
        assertTrue(ScalingExecutorTest.getScaledParallelism(stateStore, context).isEmpty());

        // once scaling is enabled
        context.getConfiguration().setString(AutoScalerOptions.SCALING_ENABLED.key(), "true");

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);

        // the scaled parallelism will pick up the recommended parallelism values
        autoscaler.scale(context);
        assertEvaluatedMetricsSize(3);
        assertEquals(4., getCurrentMetricValue(source, RECOMMENDED_PARALLELISM));
        assertEquals(4., getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM));
        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source));
        assertEquals(4, scaledParallelism.get(sink));

        // updating the topology to reflect the scale
        metricsCollector.setJobTopology(
                new JobTopology(
                        new VertexInfo(source, Set.of(), 4, 24),
                        new VertexInfo(sink, Set.of(source), 4, 720)));

        now = now.plus(Duration.ofSeconds(10));
        setClocksTo(now);
        restart(now);

        // after restart while the job is not running the evaluated metrics are gone
        autoscaler.scale(context);
        assertEquals(3, stateStore.getCollectedMetrics(context).size());
        assertNull(autoscaler.lastEvaluatedMetrics.get(context.getJobKey()));
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source));
        assertEquals(4, scaledParallelism.get(sink));

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        running(now);

        // once the job is running we got back the evaluated metric except the recommended
        // parallelisms (until the metric window is full again)
        autoscaler.scale(context);
        assertEvaluatedMetricsSize(1);
        assertEquals(4., getCurrentMetricValue(source, PARALLELISM));
        assertEquals(4., getCurrentMetricValue(sink, PARALLELISM));
        assertNull(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM));
        assertNull(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM));
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(4, scaledParallelism.get(source));
        assertEquals(4, scaledParallelism.get(sink));
    }

    private void assertEvaluatedMetricsSize(int expectedSize) throws Exception {
        SortedMap<Instant, CollectedMetrics> evaluatedMetrics =
                stateStore.getCollectedMetrics(context);
        assertThat(evaluatedMetrics).hasSize(expectedSize);
    }

    private Double getCurrentMetricValue(JobVertexID jobVertexID, ScalingMetric scalingMetric) {
        var metric =
                autoscaler
                        .lastEvaluatedMetrics
                        .get(context.getJobKey())
                        .get(jobVertexID)
                        .get(scalingMetric);
        return metric == null ? null : metric.getCurrent();
    }

    private void restart(Instant now) {
        metricsCollector.setJobUpdateTs(now);
        context =
                new JobAutoScalerContext<>(
                        context.getJobKey(),
                        context.getJobID(),
                        JobStatus.CREATED,
                        context.getConfiguration(),
                        context.getMetricGroup(),
                        getRestClusterClientSupplier());
    }

    private void running(Instant now) {
        metricsCollector.setJobUpdateTs(now);
        context =
                new JobAutoScalerContext<>(
                        context.getJobKey(),
                        context.getJobID(),
                        JobStatus.RUNNING,
                        context.getConfiguration(),
                        context.getMetricGroup(),
                        getRestClusterClientSupplier());
    }

    private void setClocksTo(Instant time) {
        var clock = Clock.fixed(time, ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        scalingExecutor.setClock(clock);
    }
}
