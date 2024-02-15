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
import org.apache.flink.autoscaler.exceptions.NotReadyException;
import org.apache.flink.autoscaler.metrics.AutoscalerFlinkMetrics;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Map.entry;
import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.VERTEX_SCALING_HISTORY_AGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for JobAutoScalerImpl. */
public class JobAutoScalerImplTest {

    private JobAutoScalerContext<JobID> context;
    private TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer;
    private TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector;
    private AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    @BeforeEach
    public void setup() {
        context = createDefaultJobAutoScalerContext();
        context.getConfiguration().set(AUTOSCALER_ENABLED, true);

        scalingRealizer = new TestingScalingRealizer<>();
        eventCollector = new TestingEventCollector<>();
        stateStore = new InMemoryAutoScalerStateStore<>();
    }

    @Test
    void testMetricReporting() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        JobTopology jobTopology = new JobTopology(new VertexInfo(jobVertexID, Set.of(), 1, 10));

        TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>> metricsCollector =
                new TestingMetricsCollector<>(jobTopology);
        metricsCollector.setCurrentMetrics(
                Map.of(
                        jobVertexID,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("load", 0., 420., 0., 0.))));
        metricsCollector.setJobUpdateTs(Instant.ofEpochMilli(0));

        ScalingMetricEvaluator evaluator = new ScalingMetricEvaluator();
        ScalingExecutor<JobID, JobAutoScalerContext<JobID>> scalingExecutor =
                new ScalingExecutor<>(eventCollector, stateStore);

        var autoscaler =
                new JobAutoScalerImpl<>(
                        metricsCollector,
                        evaluator,
                        scalingExecutor,
                        eventCollector,
                        scalingRealizer,
                        stateStore);

        autoscaler.scale(context);

        MetricGroup metricGroup = autoscaler.flinkMetrics.get(context.getJobKey()).getMetricGroup();
        assertEquals(
                0.42,
                getGaugeValue(
                        metricGroup,
                        AutoscalerFlinkMetrics.CURRENT,
                        AutoscalerFlinkMetrics.JOB_VERTEX_ID,
                        jobVertexID.toHexString(),
                        ScalingMetric.LOAD.name()),
                "Expected scaling metric LOAD was not reported. Reporting is broken");
    }

    @SuppressWarnings("unchecked")
    private static double getGaugeValue(
            MetricGroup metricGroup, String gaugeName, String... nestedMetricGroupNames) {
        for (String nestedMetricGroupName : nestedMetricGroupNames) {
            metricGroup =
                    ((Map<String, GenericMetricGroup>)
                                    Whitebox.getInternalState(metricGroup, "groups"))
                            .get(nestedMetricGroupName);
        }
        var metrics = (Map<String, Metric>) Whitebox.getInternalState(metricGroup, "metrics");
        return ((Gauge<Double>) metrics.get(gaugeName)).getValue();
    }

    @Test
    void testErrorReporting() throws Exception {
        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore);

        autoscaler.scale(context);
        Assertions.assertEquals(
                1, autoscaler.flinkMetrics.get(context.getJobKey()).getNumErrorsCount());

        autoscaler.scale(context);
        Assertions.assertEquals(
                2, autoscaler.flinkMetrics.get(context.getJobKey()).getNumErrorsCount());

        assertEquals(0, autoscaler.flinkMetrics.get(context.getJobKey()).getNumScalingsCount());
    }

    @Test
    public void testTolerateRecoverableExceptions() throws Exception {
        TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>>
                collectorWhichThrowsRecoverableException =
                        new TestingMetricsCollector<>(new JobTopology(Collections.emptySet())) {
                            @Override
                            protected Collection<String> queryAggregatedMetricNames(
                                    RestClusterClient<?> restClient,
                                    JobID jobID,
                                    JobVertexID jobVertexID) {
                                throw new NotReadyException(new Exception());
                            }
                        };
        collectorWhichThrowsRecoverableException.setJobUpdateTs(Instant.now());

        var autoscaler =
                new JobAutoScalerImpl<>(
                        collectorWhichThrowsRecoverableException,
                        null,
                        null,
                        eventCollector,
                        scalingRealizer,
                        stateStore);

        // Should not produce an error
        autoscaler.scale(context);
        Assertions.assertEquals(
                0, autoscaler.flinkMetrics.get(context.getJobKey()).getNumErrorsCount());
    }

    @Test
    void testParallelismOverrides() throws Exception {
        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore);

        // Initially we should return empty overrides, do not crate any state
        assertThat(autoscaler.getParallelismOverrides(context)).isEmpty();
        assertThat(stateStore.getParallelismOverrides(context)).isEmpty();

        var v1 = new JobVertexID().toString();
        var v2 = new JobVertexID().toString();
        stateStore.storeParallelismOverrides(context, Map.of(v1, "1", v2, "2"));
        stateStore.flush(context);

        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(Map.of(v1, "1", v2, "2"));

        assertThat(stateStore.getParallelismOverrides(context)).isEqualTo(Map.of(v1, "1", v2, "2"));

        // Disabling autoscaler should clear overrides
        context.getConfiguration().setString(AUTOSCALER_ENABLED.key(), "false");

        autoscaler.scale(context);
        assertThat(autoscaler.getParallelismOverrides(context)).isEmpty();
        assertParallelismOverrides(null);

        // Make sure we don't update in kubernetes once removed
        autoscaler.scale(context);

        context.getConfiguration().setString(AUTOSCALER_ENABLED.key(), "true");
        autoscaler.applyParallelismOverrides(context);

        assertThat(stateStore.getParallelismOverrides(context)).isEmpty();
        assertParallelismOverrides(null);

        stateStore.storeParallelismOverrides(context, Map.of(v1, "1", v2, "2"));
        stateStore.flush(context);
        autoscaler.applyParallelismOverrides(context);

        assertThat(autoscaler.getParallelismOverrides(context)).isEqualTo(Map.of(v1, "1", v2, "2"));
        assertParallelismOverrides(Map.of(v1, "1", v2, "2"));

        context.getConfiguration().setString(SCALING_ENABLED.key(), "false");

        autoscaler.applyParallelismOverrides(context);
        assertThat(autoscaler.getParallelismOverrides(context)).isEqualTo(Map.of(v1, "1", v2, "2"));
        assertParallelismOverrides(Map.of(v1, "1", v2, "2"));

        // Test error handling
        // Invalid config
        context.getConfiguration().setString(AUTOSCALER_ENABLED.key(), "asd");
        autoscaler.scale(context);
        assertParallelismOverrides(Map.of(v1, "1", v2, "2"));
    }

    @Test
    public void testApplyAutoscalerParallelism() throws Exception {
        var overrides = new HashMap<String, String>();
        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore) {
                    public Map<String, String> getParallelismOverrides(
                            JobAutoScalerContext<JobID> ctx) {
                        return new HashMap<>(overrides);
                    }
                };

        // Verify no scalingRealizer if overrides are empty
        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(null);

        // Make sure overrides are applied to the scalingRealizer
        var v1 = new JobVertexID();
        overrides.put(v1.toHexString(), "2");

        // Verify no upgrades if overrides are empty
        autoscaler.applyParallelismOverrides(context);

        assertParallelismOverrides(Map.of(v1.toHexString(), "2"));

        // We set a user override for v1, it should be ignored and the autoscaler override should
        // take precedence
        context.getConfiguration()
                .setString(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":1");
        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(Map.of(v1.toHexString(), "2"));

        // Define partly overlapping overrides, user overrides for new vertices should be applied
        var v2 = new JobVertexID();

        context.getConfiguration()
                .setString(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":1," + v2 + ":4");
        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(Map.of(v1.toString(), "2", v2.toString(), "4"));

        // Make sure user overrides apply to excluded vertices
        context.getConfiguration()
                .setString(AutoScalerOptions.VERTEX_EXCLUDE_IDS.key(), v1.toString());
        context.getConfiguration()
                .setString(PipelineOptions.PARALLELISM_OVERRIDES.key(), v1 + ":1," + v2 + ":4");

        autoscaler.applyParallelismOverrides(context);
        assertParallelismOverrides(Map.of(v1.toString(), "1", v2.toString(), "4"));
    }

    @Test
    void testApplyConfigOverrides() throws Exception {
        context.getConfiguration().set(AutoScalerOptions.MEMORY_TUNING_ENABLED, true);
        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore);

        // Initially we should return empty overrides, do not crate any state
        assertThat(stateStore.getConfigOverrides(context).toMap()).isEmpty();

        var config = new Configuration();
        config.set(TaskManagerOptions.TASK_HEAP_MEMORY, new MemorySize(42));
        stateStore.storeConfigOverrides(context, config);
        stateStore.flush(context);

        autoscaler.applyConfigOverrides(context);
        assertThat(getEvent().getConfigOverrides().toMap())
                .containsExactly(entry(TaskManagerOptions.TASK_HEAP_MEMORY.key(), "42 bytes"));
        assertThat(stateStore.getConfigOverrides(context)).isEqualTo(config);

        // Disabling autoscaler should clear overrides
        context.getConfiguration().setString(AUTOSCALER_ENABLED.key(), "false");
        autoscaler.scale(context);
        autoscaler.applyConfigOverrides(context);
        assertThat(getEvent().getConfigOverrides().toMap()).isEmpty();
    }

    @Test
    void testAutoscalerDisabled() throws Exception {
        context.getConfiguration().setBoolean(AUTOSCALER_ENABLED, false);
        context.getConfiguration().set(VERTEX_SCALING_HISTORY_AGE, Duration.ofMillis(200));

        var scalingHistory = new TreeMap<Instant, ScalingSummary>();
        scalingHistory.put(Instant.ofEpochMilli(100), new ScalingSummary());
        scalingHistory.put(Instant.ofEpochMilli(200), new ScalingSummary());

        stateStore.storeScalingHistory(context, Map.of(new JobVertexID(), scalingHistory));
        assertFalse(stateStore.getScalingHistory(context).isEmpty());

        stateStore.storeParallelismOverrides(context, Map.of("vertex", "4"));
        assertFalse(stateStore.getParallelismOverrides(context).isEmpty());

        TreeMap<Instant, CollectedMetrics> metrics = new TreeMap<>();
        metrics.put(Instant.now(), new CollectedMetrics());
        stateStore.storeCollectedMetrics(context, metrics);
        assertFalse(stateStore.getCollectedMetrics(context).isEmpty());

        var autoscaler =
                new JobAutoScalerImpl<>(
                        null, null, null, eventCollector, scalingRealizer, stateStore);
        autoscaler.scale(context);

        assertTrue(stateStore.getScalingHistory(context).isEmpty());
        assertTrue(stateStore.getScalingHistory(context).isEmpty());
        assertTrue(stateStore.getParallelismOverrides(context).isEmpty());
    }

    private void assertParallelismOverrides(Map<String, String> expectedOverrides) {
        TestingScalingRealizer.Event<JobID, JobAutoScalerContext<JobID>> scalingEvent;
        do {
            scalingEvent = getEvent();
        } while (scalingEvent != null && scalingEvent.getParallelismOverrides() == null);

        if (scalingEvent == null) {
            return;
        }
        assertThat(scalingEvent).isNotNull();
        assertEquals(expectedOverrides, scalingEvent.getParallelismOverrides());
    }

    @Nullable
    private TestingScalingRealizer.Event<JobID, JobAutoScalerContext<JobID>> getEvent(
            Map<String, String> expectedOverrides) {
        var scalingEvent = getEvent();
        if (expectedOverrides == null) {
            assertThat(scalingEvent).isNull();
            return null;
        }
        return scalingEvent;
    }

    @Nullable
    private TestingScalingRealizer.Event<JobID, JobAutoScalerContext<JobID>> getEvent() {
        return scalingRealizer.events.poll();
    }
}
