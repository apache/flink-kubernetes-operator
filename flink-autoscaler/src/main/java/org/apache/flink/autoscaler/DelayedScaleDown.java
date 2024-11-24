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
import java.util.Map;

/** All delayed scale down requests. */
public class DelayedScaleDown {

    /** The delayed scale down info for vertex. */
    @Data
    public static class VertexDelayedScaleDownInfo {
        private final Instant firstTriggerTime;
        private int maxRecommendedParallelism;

        @JsonCreator
        public VertexDelayedScaleDownInfo(
                @JsonProperty("firstTriggerTime") Instant firstTriggerTime,
                @JsonProperty("maxRecommendedParallelism") int maxRecommendedParallelism) {
            this.firstTriggerTime = firstTriggerTime;
            this.maxRecommendedParallelism = maxRecommendedParallelism;
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
        var vertexDelayedScaleDownInfo = delayedVertices.get(vertex);
        if (vertexDelayedScaleDownInfo == null) {
            // It's the first trigger
            vertexDelayedScaleDownInfo = new VertexDelayedScaleDownInfo(triggerTime, parallelism);
            delayedVertices.put(vertex, vertexDelayedScaleDownInfo);
            updated = true;
        } else if (parallelism > vertexDelayedScaleDownInfo.getMaxRecommendedParallelism()) {
            // Not the first trigger, but the maxRecommendedParallelism needs to be updated.
            vertexDelayedScaleDownInfo.setMaxRecommendedParallelism(parallelism);
            updated = true;
        }

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
