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

import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DelayedScaleDown}. */
public class DelayedScaleDownTest {

    private final JobVertexID vertex = new JobVertexID();

    @Test
    void testTriggerUpdateAndClean() {
        var instant = Instant.now();
        var delayedScaleDown = new DelayedScaleDown();
        assertThat(delayedScaleDown.isUpdated()).isFalse();

        // First trigger time as the trigger time, and it won't be updated.
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant, 5), instant, 5);
        assertThat(delayedScaleDown.isUpdated()).isTrue();

        // The lower parallelism doesn't update the result
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(5), 3), instant, 5);

        // The higher parallelism will update the result
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(10), 8), instant, 8);

        // The scale down could be re-triggered again after clean
        delayedScaleDown.clearVertex(vertex);
        assertThat(delayedScaleDown.getDelayedVertices()).isEmpty();
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(15), 4),
                instant.plusSeconds(15),
                4);

        // The scale down could be re-triggered again after cleanAll
        delayedScaleDown.clearAll();
        assertThat(delayedScaleDown.getDelayedVertices()).isEmpty();
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(15), 2),
                instant.plusSeconds(15),
                2);
    }

    void assertVertexDelayedScaleDownInfo(
            DelayedScaleDown.VertexDelayedScaleDownInfo vertexDelayedScaleDownInfo,
            Instant expectedTriggerTime,
            int expectedMaxRecommendedParallelism) {
        assertThat(vertexDelayedScaleDownInfo.getFirstTriggerTime()).isEqualTo(expectedTriggerTime);
        assertThat(vertexDelayedScaleDownInfo.getMaxRecommendedParallelism())
                .isEqualTo(expectedMaxRecommendedParallelism);
    }
}
