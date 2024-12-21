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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DelayedScaleDown}. */
public class DelayedScaleDownTest {

    private final JobVertexID vertex = new JobVertexID();

    @Test
    void testWrongWindowStartTime() {
        var instant = Instant.now();
        var delayedScaleDown = new DelayedScaleDown();

        // First trigger time as the trigger time, and it won't be updated.
        var vertexDelayedScaleDownInfo = delayedScaleDown.triggerScaleDown(vertex, instant, 5);
        assertVertexDelayedScaleDownInfo(vertexDelayedScaleDownInfo, instant, 5, instant);

        // Get the max recommended parallelism from a wrong window, and no any recommended
        // parallelism since the start window.
        assertThatThrownBy(
                        () ->
                                vertexDelayedScaleDownInfo.getMaxRecommendedParallelism(
                                        instant.plusSeconds(1)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testMaxRecommendedParallelismForSlidingWindow() {
        var instant = Instant.now();
        var delayedScaleDown = new DelayedScaleDown();
        assertThat(delayedScaleDown.isUpdated()).isFalse();

        // [5] -> 5
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant, 5), instant, 5, instant);
        // [5, 8] -> 8
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(1), 8),
                instant,
                8,
                instant);
        // [5, 8, 6] -> 8
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(2), 6),
                instant,
                8,
                instant);
        // [5, 8, 6, 4] -> 8
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(3), 4),
                instant,
                8,
                instant);
        // 5, [8, 6, 4, 3] -> 8
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(4), 3),
                instant,
                8,
                instant.plusSeconds(1));
        // 5, 8, [6, 4, 3, 3] -> 6
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(5), 3),
                instant,
                6,
                instant.plusSeconds(2));
        // 5, 8, 6, [4, 3, 3, 3] -> 4
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(6), 3),
                instant,
                4,
                instant.plusSeconds(3));
        // 5, 8, 6, 4, [3, 3, 3, 3] -> 3
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(7), 3),
                instant,
                3,
                instant.plusSeconds(4));
        // Check the timestamp of latest parallelism is maintained correctly.
        // 5, 8, 6, 4, 3, 3, 3, 3, [3] -> 3
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(8), 3),
                instant,
                3,
                instant.plusSeconds(8));
        // 5, 8, 6, 4, 3, 3, 3, 3, [3, 9] -> 9
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(9), 9),
                instant,
                9,
                instant.plusSeconds(8));
    }

    @Test
    void testTriggerUpdateAndClean() {
        var instant = Instant.now();
        var delayedScaleDown = new DelayedScaleDown();
        assertThat(delayedScaleDown.isUpdated()).isFalse();

        // First trigger time as the trigger time, and it won't be updated.
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant, 5), instant, 5, instant);
        assertThat(delayedScaleDown.isUpdated()).isTrue();

        // The lower parallelism doesn't update the result
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(5), 3),
                instant,
                5,
                instant);

        // The higher parallelism will update the result
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(10), 8),
                instant,
                8,
                instant);

        // The scale down could be re-triggered again after clean
        delayedScaleDown.clearVertex(vertex);
        assertThat(delayedScaleDown.getDelayedVertices()).isEmpty();
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(15), 4),
                instant.plusSeconds(15),
                4,
                instant);

        // The scale down could be re-triggered again after cleanAll
        delayedScaleDown.clearAll();
        assertThat(delayedScaleDown.getDelayedVertices()).isEmpty();
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(15), 2),
                instant.plusSeconds(15),
                2,
                instant);
    }

    void assertVertexDelayedScaleDownInfo(
            DelayedScaleDown.VertexDelayedScaleDownInfo vertexDelayedScaleDownInfo,
            Instant expectedTriggerTime,
            int expectedMaxRecommendedParallelism,
            Instant windowStartTime) {
        assertThat(vertexDelayedScaleDownInfo.getFirstTriggerTime()).isEqualTo(expectedTriggerTime);
        assertThat(vertexDelayedScaleDownInfo.getMaxRecommendedParallelism(windowStartTime))
                .isEqualTo(expectedMaxRecommendedParallelism);
    }
}
