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

package org.apache.flink.autoscaler.tuning;

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.TestingAutoscalerUtils;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.autoscaler.topology.ShipStrategy.FORWARD;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.apache.flink.autoscaler.topology.ShipStrategy.RESCALE;
import static org.apache.flink.autoscaler.topology.ShipStrategy.UNKNOWN;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** Tests for {@link MemoryTuning}. */
public class MemoryTuningTest {

    TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventHandler =
            new TestingEventCollector<>();

    @Test
    void testMemoryTuning() {
        var context = TestingAutoscalerUtils.createResourceAwareContext();
        var config = context.getConfiguration();
        config.set(AutoScalerOptions.MEMORY_TUNING_ENABLED, true);
        config.set(AutoScalerOptions.MEMORY_SCALING_ENABLED, false);
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, 5);
        config.set(AutoScalerOptions.SCALING_EVENT_INTERVAL, Duration.ZERO);
        MemorySize totalMemory = MemorySize.parse("30 gb");
        config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, totalMemory);

        var jobVertex1 = new JobVertexID();
        var jobVertex2 = new JobVertexID();
        var vertexMetrics =
                Map.of(
                        jobVertex1,
                        Map.of(
                                ScalingMetric.EXPECTED_PROCESSING_RATE,
                                        EvaluatedScalingMetric.of(50),
                                ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(50)),
                        jobVertex2,
                        Map.of(
                                ScalingMetric.EXPECTED_PROCESSING_RATE,
                                        EvaluatedScalingMetric.of(50),
                                ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(50)));

        var globalMetrics =
                Map.of(
                        ScalingMetric.HEAP_MEMORY_USED,
                        EvaluatedScalingMetric.avg(MemorySize.ofMebiBytes(5096).getBytes()),
                        ScalingMetric.MANAGED_MEMORY_USED,
                        EvaluatedScalingMetric.avg(MemorySize.ofMebiBytes(10000).getBytes()),
                        ScalingMetric.METASPACE_MEMORY_USED,
                        EvaluatedScalingMetric.avg(MemorySize.ofMebiBytes(100).getBytes()),
                        ScalingMetric.NUM_TASK_SLOTS_USED,
                        EvaluatedScalingMetric.of(50));

        var metrics = new EvaluatedMetrics(vertexMetrics, globalMetrics);

        JobTopology jobTopology =
                new JobTopology(
                        new VertexInfo(jobVertex1, Map.of(), 50, 1000, false, null),
                        new VertexInfo(
                                jobVertex2, Map.of(jobVertex1, REBALANCE), 50, 1000, false, null));

        Map<JobVertexID, ScalingSummary> scalingSummaries =
                Map.of(
                        jobVertex1, new ScalingSummary(50, 25, Map.of()),
                        jobVertex2, new ScalingSummary(50, 10, Map.of()));

        ConfigChanges configChanges =
                MemoryTuning.tuneTaskManagerMemory(
                        context, metrics, jobTopology, scalingSummaries, eventHandler);
        // Test reducing overall memory
        assertThat(configChanges.getOverrides())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
                                "0.654",
                                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                                "13760 kb",
                                TaskManagerOptions.NETWORK_MEMORY_MAX.key(),
                                "13760 kb",
                                TaskManagerOptions.JVM_METASPACE.key(),
                                "120 mb",
                                TaskManagerOptions.JVM_OVERHEAD_FRACTION.key(),
                                "0.054",
                                TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(),
                                "0 bytes",
                                TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                                "20108162027 bytes"));

        assertThat(configChanges.getRemovals())
                .containsExactlyInAnyOrder(
                        TaskManagerOptions.TOTAL_FLINK_MEMORY.key(),
                        TaskManagerOptions.TASK_HEAP_MEMORY.key(),
                        TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
                        TaskManagerOptions.MANAGED_MEMORY_SIZE
                                .fallbackKeys()
                                .iterator()
                                .next()
                                .getKey());

        assertThat(eventHandler.events.poll().getMessage())
                .startsWith(
                        "Memory tuning recommends the following configuration (automatic tuning is enabled):");

        // Test maximize managed memory
        config.set(AutoScalerOptions.MEMORY_TUNING_MAXIMIZE_MANAGED_MEMORY, true);
        configChanges =
                MemoryTuning.tuneTaskManagerMemory(
                        context, metrics, jobTopology, scalingSummaries, eventHandler);
        assertThat(configChanges.getOverrides())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
                                "0.789",
                                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                                "13760 kb",
                                TaskManagerOptions.NETWORK_MEMORY_MAX.key(),
                                "13760 kb",
                                TaskManagerOptions.JVM_METASPACE.key(),
                                "120 mb",
                                TaskManagerOptions.JVM_OVERHEAD_FRACTION.key(),
                                "0.034",
                                TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(),
                                "0 bytes",
                                TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                                totalMemory.toString()));

        // Test managed memory is zero
        metrics = new EvaluatedMetrics(vertexMetrics, new HashMap<>(globalMetrics));
        metrics.getGlobalMetrics()
                .put(ScalingMetric.MANAGED_MEMORY_USED, EvaluatedScalingMetric.avg(0));
        configChanges =
                MemoryTuning.tuneTaskManagerMemory(
                        context, metrics, jobTopology, scalingSummaries, eventHandler);
        assertThat(configChanges.getOverrides())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
                                "0.0",
                                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                                "13760 kb",
                                TaskManagerOptions.NETWORK_MEMORY_MAX.key(),
                                "13760 kb",
                                TaskManagerOptions.JVM_METASPACE.key(),
                                "120 mb",
                                TaskManagerOptions.JVM_OVERHEAD_FRACTION.key(),
                                "0.139",
                                TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(),
                                "0 bytes",
                                TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                                "7760130867 bytes"));

        // Test integration with MemoryScaling
        config.set(AutoScalerOptions.MEMORY_SCALING_ENABLED, true);
        configChanges =
                MemoryTuning.tuneTaskManagerMemory(
                        context, metrics, jobTopology, scalingSummaries, eventHandler);
        assertThat(configChanges.getOverrides())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
                                "0.0",
                                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                                "13760 kb",
                                TaskManagerOptions.NETWORK_MEMORY_MAX.key(),
                                "13760 kb",
                                TaskManagerOptions.JVM_METASPACE.key(),
                                "120 mb",
                                TaskManagerOptions.JVM_OVERHEAD_FRACTION.key(),
                                "0.076",
                                TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(),
                                "0 bytes",
                                TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                                "14172382822 bytes"));

        // Test METASPACE when memory usage is so high that it could take all the memory
        metrics = new EvaluatedMetrics(vertexMetrics, new HashMap<>(globalMetrics));
        metrics.getGlobalMetrics()
                .put(ScalingMetric.HEAP_MEMORY_USED, EvaluatedScalingMetric.avg(30812254720d));
        // Set usage to max metaspace usage to ensure the calculation take in account max size
        metrics.getGlobalMetrics()
                .put(ScalingMetric.METASPACE_MEMORY_USED, EvaluatedScalingMetric.avg(268435456d));
        configChanges =
                MemoryTuning.tuneTaskManagerMemory(
                        context, metrics, jobTopology, scalingSummaries, eventHandler);
        assertThat(configChanges.getOverrides())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
                                "0.0",
                                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                                "13760 kb",
                                TaskManagerOptions.NETWORK_MEMORY_MAX.key(),
                                "13760 kb",
                                TaskManagerOptions.JVM_METASPACE.key(),
                                "322122547 bytes",
                                TaskManagerOptions.JVM_OVERHEAD_FRACTION.key(),
                                "0.034",
                                TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(),
                                "0 bytes",
                                TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                                "30 gb"));

        // Test tuning disabled
        config.set(AutoScalerOptions.MEMORY_TUNING_ENABLED, false);
        assertThat(
                        MemoryTuning.tuneTaskManagerMemory(
                                        context,
                                        metrics,
                                        jobTopology,
                                        scalingSummaries,
                                        eventHandler)
                                .getOverrides())
                .isEmpty();

        assertThat(eventHandler.events.poll().getMessage())
                .startsWith(
                        "Memory tuning recommends the following configuration (automatic tuning is disabled):");
    }

    @Test
    void testCalculateNetworkSegmentNumber() {
        // Test FORWARD
        assertThat(MemoryTuning.calculateNetworkSegmentNumber(10, 10, FORWARD, 2, 8)).isEqualTo(10);

        // Test FORWARD is changed to RESCALE.
        assertThat(MemoryTuning.calculateNetworkSegmentNumber(10, 15, FORWARD, 2, 8)).isEqualTo(12);

        // Test RESCALE.
        assertThat(MemoryTuning.calculateNetworkSegmentNumber(10, 15, RESCALE, 2, 8)).isEqualTo(12);

        // Test REBALANCE.
        assertThat(MemoryTuning.calculateNetworkSegmentNumber(10, 15, REBALANCE, 2, 8))
                .isEqualTo(38);

        // Test UNKNOWN.
        assertThat(MemoryTuning.calculateNetworkSegmentNumber(10, 15, UNKNOWN, 2, 8)).isEqualTo(38);
    }
}
