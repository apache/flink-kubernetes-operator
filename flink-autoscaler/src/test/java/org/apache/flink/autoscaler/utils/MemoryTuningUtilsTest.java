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

package org.apache.flink.autoscaler.utils;

import org.apache.flink.autoscaler.TestingAutoscalerUtils;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** Tests for {@link MemoryTuningUtils}. */
public class MemoryTuningUtilsTest {

    @Test
    void testMemoryTuning() {
        var context = TestingAutoscalerUtils.createResourceAwareContext();
        var config = context.getConfiguration();
        config.set(AutoScalerOptions.MEMORY_TUNING_ENABLED, true);
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, 5);
        config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("30 gb"));

        var jobVertex1 = new JobVertexID();
        var jobVertex2 = new JobVertexID();
        var vertexMetrics =
                Map.of(
                        jobVertex1,
                        Map.of(
                                ScalingMetric.CURRENT_PROCESSING_RATE,
                                        EvaluatedScalingMetric.avg(100),
                                ScalingMetric.EXPECTED_PROCESSING_RATE,
                                        EvaluatedScalingMetric.of(50),
                                ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(50)),
                        jobVertex2,
                        Map.of(
                                ScalingMetric.CURRENT_PROCESSING_RATE,
                                        EvaluatedScalingMetric.avg(100),
                                ScalingMetric.EXPECTED_PROCESSING_RATE,
                                        EvaluatedScalingMetric.of(50),
                                ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(50)));

        var globalMetrics =
                Map.of(
                        ScalingMetric.HEAP_AVERAGE_SIZE,
                        EvaluatedScalingMetric.avg(MemorySize.ofMebiBytes(5096).getBytes()));

        var metrics = new EvaluatedMetrics(vertexMetrics, globalMetrics);

        Map<String, String> overrides =
                MemoryTuningUtils.tuneTaskManagerHeapMemory(context, metrics).toMap();
        Assertions.assertThat(overrides)
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                TaskManagerOptions.TASK_HEAP_MEMORY.key(),
                                "5096 mb",
                                TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
                                "12348031160 bytes",
                                TaskManagerOptions.JVM_OVERHEAD_FRACTION.key(),
                                "0.475",
                                TaskManagerOptions.NETWORK_MEMORY_FRACTION.key(),
                                "0.096",
                                MemoryTuningUtils.TOTAL_MEMORY_CONFIG_NAME,
                                "22389194982"));
    }
}
