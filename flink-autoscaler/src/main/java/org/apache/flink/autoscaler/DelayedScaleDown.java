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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.Getter;

import javax.annotation.Nonnull;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** All delayed scale down requests. */
public class DelayedScaleDown {

    @Data
    private static class RecommendedParallelism {
        @Nonnull private final Instant triggerTime;
        private final int parallelism;

        @JsonCreator
        public RecommendedParallelism(
                @Nonnull @JsonProperty("triggerTime") Instant triggerTime,
                @JsonProperty("parallelism") int parallelism) {
            this.triggerTime = triggerTime;
            this.parallelism = parallelism;
        }
    }

    /** The delayed scale down info for vertex. */
    @Data
    public static class VertexDelayedScaleDownInfo {
        private final Instant firstTriggerTime;

        /**
         * It maintains all recommended parallelisms at each time within the past
         * `scale-down.interval` window period. So all recommended parallelisms before the window
         * start time will be evicted.
         *
         * <p>Also, if latest parallelism is greater than the past parallelism, all smaller
         * parallelism in the past never be the max recommended parallelism, so we could evict all
         * smaller parallelism in the past. It's a general optimization for calculating max value
         * for sliding window. So We only need to maintain a list with monotonically decreasing
         * parallelism within the past window, and the first parallelism will be the max recommended
         * parallelism within the past scale-down.interval window period.
         */
        private final LinkedList<RecommendedParallelism> recommendedParallelisms;

        public VertexDelayedScaleDownInfo(Instant firstTriggerTime) {
            this.firstTriggerTime = firstTriggerTime;
            this.recommendedParallelisms = new LinkedList<>();
        }

        @JsonCreator
        public VertexDelayedScaleDownInfo(
                @JsonProperty("firstTriggerTime") Instant firstTriggerTime,
                @JsonProperty("recommendedParallelisms")
                        LinkedList<RecommendedParallelism> recommendedParallelisms) {
            this.firstTriggerTime = firstTriggerTime;
            this.recommendedParallelisms = recommendedParallelisms;
        }

        /** Record current recommended parallelism. */
        public void recordRecommendedParallelism(Instant triggerTime, int parallelism) {
            // Evict all recommended parallelisms that are lower than the latest parallelism.
            while (!recommendedParallelisms.isEmpty()
                    && recommendedParallelisms.peekLast().getParallelism() <= parallelism) {
                recommendedParallelisms.pollLast();
            }

            recommendedParallelisms.addLast(new RecommendedParallelism(triggerTime, parallelism));
        }

        @JsonIgnore
        public int getMaxRecommendedParallelism(Instant windowStartTime) {
            // Evict all recommended parallelisms before the window start time.
            while (!recommendedParallelisms.isEmpty()
                    && recommendedParallelisms
                            .peekFirst()
                            .getTriggerTime()
                            .isBefore(windowStartTime)) {
                recommendedParallelisms.pollFirst();
            }

            var maxRecommendedParallelism = recommendedParallelisms.peekFirst();
            checkState(
                    maxRecommendedParallelism != null,
                    "The getMaxRecommendedParallelism should be called after triggering a scale down, it may be a bug.");
            return maxRecommendedParallelism.getParallelism();
        }
    }

    @Getter private final Map<JobVertexID, VertexDelayedScaleDownInfo> delayedVertices;

    // Have any scale down request been updated? It doesn't need to be stored, it is only used to
    // determine whether DelayedScaleDown needs to be stored.
    @JsonIgnore @Getter private boolean updated = false;

    public DelayedScaleDown() {
        this.delayedVertices = new HashMap<>();
    }

    /** Trigger a scale down, and return the corresponding {@link VertexDelayedScaleDownInfo}. */
    @Nonnull
    public VertexDelayedScaleDownInfo triggerScaleDown(
            JobVertexID vertex, Instant triggerTime, int parallelism) {
        // The vertexDelayedScaleDownInfo is updated once scale down is triggered due to we need
        // update the triggerTime each time.
        updated = true;

        var vertexDelayedScaleDownInfo = delayedVertices.get(vertex);
        if (vertexDelayedScaleDownInfo == null) {
            vertexDelayedScaleDownInfo = new VertexDelayedScaleDownInfo(triggerTime);
            delayedVertices.put(vertex, vertexDelayedScaleDownInfo);
        }
        vertexDelayedScaleDownInfo.recordRecommendedParallelism(triggerTime, parallelism);

        return vertexDelayedScaleDownInfo;
    }

    // Clear the delayed scale down for corresponding vertex when the recommended parallelism is
    // greater than or equal to the currentParallelism.
    void clearVertex(JobVertexID vertex) {
        VertexDelayedScaleDownInfo removed = delayedVertices.remove(vertex);
        if (removed != null) {
            updated = true;
        }
    }

    // Clear all delayed scale down when rescale happens.
    void clearAll() {
        if (delayedVertices.isEmpty()) {
            return;
        }
        delayedVertices.clear();
        updated = true;
    }
}
