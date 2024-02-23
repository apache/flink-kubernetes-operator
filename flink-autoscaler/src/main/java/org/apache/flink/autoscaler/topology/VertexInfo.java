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

package org.apache.flink.autoscaler.topology;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.Data;

import java.util.Map;

/** Job vertex information. */
@Data
public class VertexInfo {

    private final JobVertexID id;

    // All input vertices and the ship_strategy
    private final Map<JobVertexID, String> inputs;

    // All output vertices and the ship_strategy
    private Map<JobVertexID, String> outputs;

    private final int parallelism;

    private int maxParallelism;

    private final int originalMaxParallelism;

    private final boolean finished;

    private IOMetrics ioMetrics;

    public VertexInfo(
            JobVertexID id,
            Map<JobVertexID, String> inputs,
            int parallelism,
            int maxParallelism,
            boolean finished,
            IOMetrics ioMetrics) {
        this.id = id;
        this.inputs = inputs;
        this.parallelism = parallelism;
        this.maxParallelism = maxParallelism;
        this.originalMaxParallelism = maxParallelism;
        this.finished = finished;
        this.ioMetrics = ioMetrics;
    }

    @VisibleForTesting
    public VertexInfo(
            JobVertexID id,
            Map<JobVertexID, String> inputs,
            int parallelism,
            int maxParallelism,
            IOMetrics ioMetrics) {
        this(id, inputs, parallelism, maxParallelism, false, ioMetrics);
    }

    @VisibleForTesting
    public VertexInfo(
            JobVertexID id, Map<JobVertexID, String> inputs, int parallelism, int maxParallelism) {
        this(id, inputs, parallelism, maxParallelism, null);
    }

    public void updateMaxParallelism(int maxParallelism) {
        setMaxParallelism(Math.min(originalMaxParallelism, maxParallelism));
    }
}
