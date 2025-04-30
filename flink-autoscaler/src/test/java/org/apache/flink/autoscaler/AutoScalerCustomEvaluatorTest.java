/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.autoscaler.metrics.CustomEvaluator;
import org.apache.flink.autoscaler.metrics.TestCustomEvaluator;
import org.apache.flink.autoscaler.metrics.TestMetrics;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.IOMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for testing the integration of custom evaluators with the Flink job autoscaler. */
public class AutoScalerCustomEvaluatorTest {
    private JobAutoScalerContext<JobID> context;
    private AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    private TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>> metricsCollector;

    private JobVertexID source1, sink;

    private JobAutoScalerImpl<JobID, JobAutoScalerContext<JobID>> autoscaler;

    @BeforeEach
    public void setup() {
        context = createDefaultJobAutoScalerContext();

        TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector =
                new TestingEventCollector<>();
        stateStore = new InMemoryAutoScalerStateStore<>();

        ScalingExecutor<JobID, JobAutoScalerContext<JobID>> scalingExecutor =
                new ScalingExecutor<>(eventCollector, stateStore);
        String testCustomEvaluatorName = "test-custom-evaluator";

        var customEvaluators = createTestCustomEvaluator();

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
        defaultConf.set(AutoScalerOptions.UTILIZATION_TARGET, 0.8);
        defaultConf.set(AutoScalerOptions.UTILIZATION_MAX, 0.9);
        defaultConf.set(AutoScalerOptions.UTILIZATION_MIN, 0.7);
        defaultConf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ZERO);
        defaultConf.set(AutoScalerOptions.BACKLOG_PROCESSING_LAG_THRESHOLD, Duration.ofSeconds(1));

        defaultConf.set(AutoScalerOptions.CUSTOM_EVALUATOR_NAME, testCustomEvaluatorName);

        autoscaler =
                new JobAutoScalerImpl<>(
                        metricsCollector,
                        new ScalingMetricEvaluator(),
                        scalingExecutor,
                        eventCollector,
                        new TestingScalingRealizer<>(),
                        stateStore,
                        customEvaluators);

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
                        .maxBusyTimePerSec(8)
                        .pendingRecords(0L)
                        .build());
        metricsCollector.updateMetrics(
                sink, TestMetrics.builder().numRecordsIn(0).maxBusyTimePerSec(850).build());

        autoscaler.scale(context);

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);

        metricsCollector.updateMetrics(
                source1, m -> m.setNumRecordsIn(500), m -> m.setNumRecordsOut(500));
        metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(500));

        autoscaler.scale(context);

        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(stateStore, context);
        assertEquals(3, scaledParallelism.get(source1));
        assertEquals(200, scaledParallelism.get(sink));
        assertFlinkMetricsCount(1, 0);
    }

    private void setClocksTo(Instant time) {
        var clock = Clock.fixed(time, ZoneId.systemDefault());
        autoscaler.setClock(clock);
    }

    private void assertFlinkMetricsCount(int scalingCount, int balancedCount) {
        AutoscalerFlinkMetrics autoscalerFlinkMetrics =
                autoscaler.flinkMetrics.get(context.getJobKey());
        assertEquals(scalingCount, autoscalerFlinkMetrics.getNumScalingsCount());
        assertEquals(balancedCount, autoscalerFlinkMetrics.getNumBalancedCount());
    }

    private Map<String, CustomEvaluator> createTestCustomEvaluator() {
        var testCustomEvaluator = new TestCustomEvaluator();
        testCustomEvaluator.configure(new Configuration());
        return Map.of(testCustomEvaluator.getName(), testCustomEvaluator);
    }
}
