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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Class for storing intermediate scaling results. */
public class IntermediateScalingResult {

    private final Map<JobVertexID, ScalingSummary> scalingSummaries;
    private final List<JobVertexID> bottlenecks;

    private double backpropagationScaleFactor = 1.0;

    public IntermediateScalingResult() {
        scalingSummaries = new HashMap<>();
        bottlenecks = new ArrayList<>();
    }

    void addScalingSummary(JobVertexID vertex, ScalingSummary scalingSummary) {
        scalingSummaries.put(vertex, scalingSummary);
    }

    void addBottleneckVertex(JobVertexID bottleneck, double factor) {
        bottlenecks.add(bottleneck);
        backpropagationScaleFactor = Math.min(backpropagationScaleFactor, factor);
    }

    public List<JobVertexID> getBottlenecks() {
        return bottlenecks;
    }

    public double getBackpropagationScaleFactor() {
        return backpropagationScaleFactor;
    }

    public Map<JobVertexID, ScalingSummary> getScalingSummaries() {
        return scalingSummaries;
    }
}
