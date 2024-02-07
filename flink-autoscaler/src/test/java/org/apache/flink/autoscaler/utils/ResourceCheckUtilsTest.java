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

import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link ResourceCheckUtils}. */
class ResourceCheckUtilsTest {

    @Test
    void testEstimateNumTaskSlotsAfterRescale() {
        var source = new JobVertexID();
        var sink = new JobVertexID();
        int sourceParallelism = 2;
        int sinkParallelism = 3;
        var metrics =
                Map.of(
                        source,
                        Map.of(
                                ScalingMetric.PARALLELISM,
                                EvaluatedScalingMetric.of(sourceParallelism)),
                        sink,
                        Map.of(
                                ScalingMetric.PARALLELISM,
                                EvaluatedScalingMetric.of(sinkParallelism)));

        Map<JobVertexID, ScalingSummary> scalingSummaries =
                Map.of(source, new ScalingSummary(sourceParallelism, 7, Map.of()));

        // With slot sharing, the max parallelism determines the number of task slots required
        int numTaskSlotsUsed = Math.max(sourceParallelism, sinkParallelism);
        assertThat(
                        ResourceCheckUtils.estimateNumTaskSlotsAfterRescale(
                                metrics, scalingSummaries, numTaskSlotsUsed))
                .isEqualTo(7);

        // Slot sharing disabled, the number of task slots equals the sum of all parallelisms
        numTaskSlotsUsed = sourceParallelism + sinkParallelism;
        assertThat(
                        ResourceCheckUtils.estimateNumTaskSlotsAfterRescale(
                                metrics, scalingSummaries, numTaskSlotsUsed))
                .isEqualTo(10);

        // Slot sharing partially disabled, for lack of a better metric, assume slot sharing is
        // disabled
        numTaskSlotsUsed = numTaskSlotsUsed - 1;
        assertThat(
                        ResourceCheckUtils.estimateNumTaskSlotsAfterRescale(
                                metrics, scalingSummaries, numTaskSlotsUsed))
                .isEqualTo(10);
    }
}
