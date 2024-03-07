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
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MemoryScalingTest {

    JobAutoScalerContext<JobID> context = TestingAutoscalerUtils.createResourceAwareContext();

    @BeforeEach
    void setup() {
        context.getConfiguration().set(AutoScalerOptions.MEMORY_TUNING_ENABLED, true);
    }

    @Test
    void testMemoryScalingDownscaling() {
        int currentParallelism = 20;
        int rescaleParallelism = 10;
        MemorySize currentMemory = MemorySize.parse("10 gb");
        MemoryBudget memoryBudget = new MemoryBudget(MemorySize.parse("30 gb").getBytes());

        assertThat(
                        runMemoryScaling(
                                currentParallelism,
                                rescaleParallelism,
                                context,
                                currentMemory,
                                memoryBudget))
                .isEqualTo(MemorySize.parse("20 gb"));
    }

    @Test
    void testMemoryScalingUpscaling() {
        int currentParallelism = 10;
        int rescaleParallelism = 20;
        MemorySize currentMemory = MemorySize.parse("10 gb");
        MemoryBudget memoryBudget = new MemoryBudget(MemorySize.parse("30 gb").getBytes());

        assertThat(
                        runMemoryScaling(
                                currentParallelism,
                                rescaleParallelism,
                                context,
                                currentMemory,
                                memoryBudget))
                .isEqualTo(MemorySize.parse("10 gb"));
    }

    @Test
    void testMemoryScalingDisabled() {
        context.getConfiguration().set(AutoScalerOptions.MEMORY_SCALING_ENABLED, false);
        MemorySize currentMemory = MemorySize.parse("10 gb");
        MemoryBudget memoryBudget = new MemoryBudget(MemorySize.parse("30 gb").getBytes());

        assertThat(
                        MemoryScaling.applyMemoryScaling(
                                currentMemory, memoryBudget, context, Map.of(), null))
                .isEqualTo(currentMemory);
    }

    private static MemorySize runMemoryScaling(
            int currentParallelism,
            int rescaleParallelism,
            JobAutoScalerContext<JobID> context,
            MemorySize currentMemory,
            MemoryBudget memoryBudget) {
        var globalMetrics =
                Map.of(
                        ScalingMetric.NUM_TASK_SLOTS_USED,
                        EvaluatedScalingMetric.of(currentParallelism));
        var jobVertex1 = new JobVertexID();
        var jobVertex2 = new JobVertexID();
        var vertexMetrics =
                Map.of(
                        jobVertex1,
                                Map.of(
                                        ScalingMetric.PARALLELISM,
                                        EvaluatedScalingMetric.of(currentParallelism)),
                        jobVertex2,
                                Map.of(
                                        ScalingMetric.PARALLELISM,
                                        EvaluatedScalingMetric.of(currentParallelism)));
        var metrics = new EvaluatedMetrics(vertexMetrics, globalMetrics);

        Map<JobVertexID, ScalingSummary> scalingSummaries =
                Map.of(
                        jobVertex1,
                                new ScalingSummary(
                                        currentParallelism, rescaleParallelism, Map.of()),
                        jobVertex2,
                                new ScalingSummary(
                                        currentParallelism, rescaleParallelism, Map.of()));

        return MemoryScaling.applyMemoryScaling(
                currentMemory, memoryBudget, context, scalingSummaries, metrics);
    }
}
