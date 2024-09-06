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

import lombok.Getter;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** All delayed scale down requests. */
public class DelayedScaleDown {

    @Getter private final Map<JobVertexID, Instant> firstTriggerTime;

    // Have any scale down request been updated? It doesn't need to be stored, it is only used to
    // determine whether DelayedScaleDown needs to be stored.
    @Getter private boolean isUpdated = false;

    public DelayedScaleDown() {
        this.firstTriggerTime = new HashMap<>();
    }

    public DelayedScaleDown(Map<JobVertexID, Instant> firstTriggerTime) {
        this.firstTriggerTime = firstTriggerTime;
    }

    Optional<Instant> getFirstTriggerTimeForVertex(JobVertexID vertex) {
        return Optional.ofNullable(firstTriggerTime.get(vertex));
    }

    void updateTriggerTime(JobVertexID vertex, Instant instant) {
        firstTriggerTime.put(vertex, instant);
        isUpdated = true;
    }

    void clearVertex(JobVertexID vertex) {
        Instant removed = firstTriggerTime.remove(vertex);
        if (removed != null) {
            isUpdated = true;
        }
    }

    void clearAll() {
        if (firstTriggerTime.isEmpty()) {
            return;
        }
        firstTriggerTime.clear();
        isUpdated = true;
    }
}
