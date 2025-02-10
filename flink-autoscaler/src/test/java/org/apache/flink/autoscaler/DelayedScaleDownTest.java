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
        var vertexDelayedScaleDownInfo =
                delayedScaleDown.triggerScaleDown(vertex, instant, 5, true);
        assertVertexDelayedScaleDownInfo(
                vertexDelayedScaleDownInfo,
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant, 5, true),
                instant);

        // Get the max recommended parallelism from a wrong window, and no any recommended
        // parallelism since the start window.
        assertThatThrownBy(
                        () ->
                                vertexDelayedScaleDownInfo.getMaxRecommendedParallelism(
                                        instant.plusSeconds(1)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testMaxRecommendedParallelismForInitialWindow() {
        var instant = Instant.now();
        var delayedScaleDown = new DelayedScaleDown();
        assertThat(delayedScaleDown.isUpdated()).isFalse();

        delayedScaleDown.triggerScaleDown(vertex, instant, 5, true);
        delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(1), 9, false);
        delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(2), 4, true);
        delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(3), 5, true);
        var vertexDelayedScaleDownInfo =
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(4), 6, true);

        // [5, 9, 4, 5, 6] -> 9
        assertVertexDelayedScaleDownInfo(
                vertexDelayedScaleDownInfo,
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(1), 9, false),
                instant);
        // 5, [9, 4, 5, 6] -> 9
        assertVertexDelayedScaleDownInfo(
                vertexDelayedScaleDownInfo,
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(1), 9, false),
                instant.plusSeconds(1));
        // 5, 9, [4, 5, 6] -> 6
        assertVertexDelayedScaleDownInfo(
                vertexDelayedScaleDownInfo,
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(4), 6, true),
                instant.plusSeconds(2));
        // 5, 9, 4, [5, 6] -> 6
        assertVertexDelayedScaleDownInfo(
                vertexDelayedScaleDownInfo,
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(4), 6, true),
                instant.plusSeconds(3));
        // 5, 9, 4, 5, [6] -> 6
        assertVertexDelayedScaleDownInfo(
                vertexDelayedScaleDownInfo,
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(4), 6, true),
                instant.plusSeconds(4));
    }

    @Test
    void testMaxRecommendedParallelismForSlidingWindow() {
        var instant = Instant.now();
        var delayedScaleDown = new DelayedScaleDown();
        assertThat(delayedScaleDown.isUpdated()).isFalse();

        // [5] -> 5
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant, 5, true),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant, 5, true),
                instant);
        // [5, 8] -> 8
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(1), 8, false),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(1), 8, false),
                instant);
        // [5, 8, 6] -> 8
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(2), 6, true),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(1), 8, false),
                instant);
        // [5, 8, 6, 4] -> 8
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(3), 4, true),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(1), 8, false),
                instant);
        // 5, [8, 6, 4, 3] -> 8
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(4), 3, true),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(1), 8, false),
                instant.plusSeconds(1));
        // 5, 8, [6, 4, 3, 3] -> 6
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(5), 3, true),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(2), 6, true),
                instant.plusSeconds(2));
        // 5, 8, 6, [4, 3, 3, 3] -> 4
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(6), 3, true),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(3), 4, true),
                instant.plusSeconds(3));

        // Check the timestamp of latest parallelism is maintained correctly when recommended
        // parallelism is same.
        // 5, 8, 6, 4, [3, 3, 3, 3] -> 3
        // 5, 8, 6, 4, 3, [3, 3, 3] -> 3
        // 5, 8, 6, 4, 3, 3, [3, 3] -> 3
        // 5, 8, 6, 4, 3, 3, 3, [3] -> 3
        var vertexDelayedScaleDownInfo =
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(7), 3, true);
        for (int offset = 4; offset <= 7; offset++) {
            assertVertexDelayedScaleDownInfo(
                    vertexDelayedScaleDownInfo,
                    instant,
                    new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(7), 3, true),
                    instant.plusSeconds(offset));
        }
        // 5, 8, 6, 4, 3, 3, 3, [3, 9] -> 9
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(9), 9, false),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(9), 9, false),
                instant.plusSeconds(7));
    }

    @Test
    void testTriggerUpdateAndClean() {
        var instant = Instant.now();
        var delayedScaleDown = new DelayedScaleDown();
        assertThat(delayedScaleDown.isUpdated()).isFalse();

        // First trigger time as the trigger time, and it won't be updated.
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant, 5, true),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant, 5, true),
                instant);
        assertThat(delayedScaleDown.isUpdated()).isTrue();

        // The lower parallelism doesn't update the result
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(5), 3, true),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant, 5, true),
                instant);

        // The higher parallelism will update the result
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(10), 8, true),
                instant,
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(10), 8, true),
                instant);

        // The scale down could be re-triggered again after clean
        delayedScaleDown.clearVertex(vertex);
        assertThat(delayedScaleDown.getDelayedVertices()).isEmpty();
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(15), 4, true),
                instant.plusSeconds(15),
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(15), 4, true),
                instant);

        // The scale down could be re-triggered again after cleanAll
        delayedScaleDown.clearAll();
        assertThat(delayedScaleDown.getDelayedVertices()).isEmpty();
        assertVertexDelayedScaleDownInfo(
                delayedScaleDown.triggerScaleDown(vertex, instant.plusSeconds(16), 2, true),
                instant.plusSeconds(16),
                new DelayedScaleDown.RecommendedParallelism(instant.plusSeconds(16), 2, true),
                instant);
    }

    void assertVertexDelayedScaleDownInfo(
            DelayedScaleDown.VertexDelayedScaleDownInfo vertexDelayedScaleDownInfo,
            Instant expectedTriggerTime,
            DelayedScaleDown.RecommendedParallelism expectedMaxRecommendedParallelism,
            Instant windowStartTime) {
        assertThat(vertexDelayedScaleDownInfo.getFirstTriggerTime()).isEqualTo(expectedTriggerTime);
        assertThat(vertexDelayedScaleDownInfo.getMaxRecommendedParallelism(windowStartTime))
                .isEqualTo(expectedMaxRecommendedParallelism);
    }
}
