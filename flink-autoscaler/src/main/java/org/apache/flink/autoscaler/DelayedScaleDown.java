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
import javax.annotation.Nullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/** All delayed scale down requests. */
public class DelayedScaleDown {

    /** Details of the recommended parallelism. */
    @Data
    public static class RecommendedParallelism {
        @Nonnull private final Instant triggerTime;
        private final int parallelism;
        private final boolean outsideUtilizationBound;

        @JsonCreator
        public RecommendedParallelism(
                @Nonnull @JsonProperty("triggerTime") Instant triggerTime,
                @JsonProperty("parallelism") int parallelism,
                @JsonProperty("outsideUtilizationBound") boolean outsideUtilizationBound) {
            this.triggerTime = triggerTime;
            this.parallelism = parallelism;
            this.outsideUtilizationBound = outsideUtilizationBound;
        }
    }

    /** The delayed scale down info for vertex. */
    @Data
    public static class VertexDelayedScaleDownInfo {
        private final Instant firstTriggerTime;

        /**
         * In theory, it maintains all recommended parallelisms at each time within the past
         * `scale-down.interval` window period, so all recommended parallelisms before the window
         * start time will be evicted.
         *
         * <p>Also, if latest parallelism is greater than the past parallelism, all smaller
         * parallelism in the past never be the max recommended parallelism, so we could evict all
         * smaller parallelism in the past. It's a general optimization for calculating max value
         * for sliding window. So we only need to maintain a list with monotonically decreasing
         * parallelism within the past window, and the first parallelism will be the max recommended
         * parallelism within the past `scale-down.interval` window period.
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
        public void recordRecommendedParallelism(
                Instant triggerTime, int parallelism, boolean outsideUtilizationBound) {
            // Evict all recommended parallelisms that are lower than or equal to the latest
            // parallelism. When the past parallelism is equal to the latest parallelism,
            // triggerTime needs to be updated, so it also needs to be evicted.
            while (!recommendedParallelisms.isEmpty()
                    && recommendedParallelisms.peekLast().getParallelism() <= parallelism) {
                recommendedParallelisms.pollLast();
            }

            recommendedParallelisms.addLast(
                    new RecommendedParallelism(triggerTime, parallelism, outsideUtilizationBound));
        }

        @JsonIgnore
        public RecommendedParallelism getMaxRecommendedParallelism(Instant windowStartTime) {
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
            return maxRecommendedParallelism;
        }
    }

    @Getter private final Map<JobVertexID, VertexDelayedScaleDownInfo> delayedVertices;

    /**
     * Dynamic-source vertices covered by one same-parallelism recovery request.
     *
     * <p>The request stays latched until those vertices report a persistent non-hole topology.
     * Keeping this tiny bit of durable state prevents a persistent assignment hole from restarting
     * the job on every autoscaler pass.
     */
    @Getter private final Set<String> sourceAssignmentRebalanceVertices;

    /** Stable restart nonce for the currently latched source-assignment recovery. */
    @Nullable @Getter private Long sourceAssignmentRebalanceRequestId;

    /** Whether the current recovery request has been accepted by the realizer. */
    @Getter private boolean sourceAssignmentRebalanceTriggered;

    // Have any scale down request been updated? It doesn't need to be stored, it is only used to
    // determine whether DelayedScaleDown needs to be stored.
    @JsonIgnore @Getter private boolean updated = false;

    public DelayedScaleDown() {
        this.delayedVertices = new HashMap<>();
        this.sourceAssignmentRebalanceVertices = new HashSet<>();
    }

    /** Trigger a scale down, and return the corresponding {@link VertexDelayedScaleDownInfo}. */
    @Nonnull
    public VertexDelayedScaleDownInfo triggerScaleDown(
            JobVertexID vertex,
            Instant triggerTime,
            int parallelism,
            boolean outsideUtilizationBound) {
        // The vertexDelayedScaleDownInfo is updated once scale down is triggered due to we need
        // update the triggerTime each time.
        updated = true;

        var vertexDelayedScaleDownInfo =
                delayedVertices.computeIfAbsent(
                        vertex, k -> new VertexDelayedScaleDownInfo(triggerTime));
        vertexDelayedScaleDownInfo.recordRecommendedParallelism(
                triggerTime, parallelism, outsideUtilizationBound);

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

    /**
     * Create or reuse one stable recovery request for the current assignment hole.
     *
     * <p>A pending job-level restart can cover new holes before the realizer accepts it. Once
     * accepted, a newly observed uncovered hole needs a fresh nonce so the realizer performs one
     * more recovery.
     */
    long getOrCreateSourceAssignmentRebalanceRequestId(
            Set<JobVertexID> vertices, long candidateRequestId) {
        if (sourceAssignmentRebalanceRequestId == null) {
            sourceAssignmentRebalanceRequestId = Math.max(1L, candidateRequestId);
            sourceAssignmentRebalanceVertices.clear();
            vertices.forEach(vertex -> sourceAssignmentRebalanceVertices.add(vertex.toHexString()));
            sourceAssignmentRebalanceTriggered = false;
            updated = true;
        } else if (!sourceAssignmentRebalanceTriggered) {
            boolean changed = false;
            for (JobVertexID vertex : vertices) {
                changed |= sourceAssignmentRebalanceVertices.add(vertex.toHexString());
            }
            if (changed) {
                // One pending job-level restart covers all holes observed before it is accepted.
                updated = true;
            }
        } else if (vertices.stream()
                .map(JobVertexID::toHexString)
                .anyMatch(vertex -> !sourceAssignmentRebalanceVertices.contains(vertex))) {
            sourceAssignmentRebalanceRequestId =
                    Math.max(sourceAssignmentRebalanceRequestId + 1, candidateRequestId);
            sourceAssignmentRebalanceVertices.clear();
            vertices.forEach(vertex -> sourceAssignmentRebalanceVertices.add(vertex.toHexString()));
            sourceAssignmentRebalanceTriggered = false;
            updated = true;
        }
        return sourceAssignmentRebalanceRequestId;
    }

    /** Adopt a pending restart nonce already present on the resource. */
    void replaceSourceAssignmentRebalanceRequestId(long requestId) {
        if (!Objects.equals(sourceAssignmentRebalanceRequestId, requestId)) {
            sourceAssignmentRebalanceRequestId = requestId;
            updated = true;
        }
    }

    /** Mark the current recovery request as accepted so it is only reported once. */
    void markSourceAssignmentRebalanceTriggered() {
        if (!sourceAssignmentRebalanceTriggered) {
            sourceAssignmentRebalanceTriggered = true;
            updated = true;
        }
    }

    /** Clear the recovery latch after a persistent non-hole topology is observed. */
    void clearSourceAssignmentRebalance() {
        if (sourceAssignmentRebalanceRequestId == null
                && sourceAssignmentRebalanceVertices.isEmpty()
                && !sourceAssignmentRebalanceTriggered) {
            return;
        }
        sourceAssignmentRebalanceVertices.clear();
        sourceAssignmentRebalanceRequestId = null;
        sourceAssignmentRebalanceTriggered = false;
        updated = true;
    }

    // Clear all delayed scale down when rescale happens.
    void clearAll() {
        if (delayedVertices.isEmpty()
                && sourceAssignmentRebalanceRequestId == null
                && sourceAssignmentRebalanceVertices.isEmpty()
                && !sourceAssignmentRebalanceTriggered) {
            return;
        }
        delayedVertices.clear();
        sourceAssignmentRebalanceVertices.clear();
        sourceAssignmentRebalanceRequestId = null;
        sourceAssignmentRebalanceTriggered = false;
        updated = true;
    }
}
