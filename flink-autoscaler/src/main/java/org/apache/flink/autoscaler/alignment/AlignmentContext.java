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

package org.apache.flink.autoscaler.alignment;

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collection;
import java.util.Map;

/**
 * Immutable inputs to a single per-vertex parallelism alignment, handed to {@link
 * AlignmentMode#align(AlignmentContext)}. Besides the parallelism inputs it exposes the full
 * autoscaler context, the per-vertex evaluated metrics, the job topology, and a prefix-stripped
 * configuration for the selected mode, so custom modes have what they need.
 */
public class AlignmentContext {

    /**
     * Emits a {@code SCALING_LIMITED} event. Wired by {@code JobVertexScaler} (which owns the typed
     * event handler) and used only by the deprecated blocking modes.
     */
    @FunctionalInterface
    public interface ScalingLimitedEmitter {
        void emit(int expectedParallelism, int actualParallelism);
    }

    private final JobVertexID vertex;

    private final int currentParallelism;

    /** The clamped computed target parallelism, before alignment. */
    private final int newParallelism;

    private final int numSourcePartitions;

    private final int maxParallelism;

    private final int parallelismLowerLimit;

    private final int parallelismUpperLimit;

    private final Collection<ShipStrategy> inputShipStrategies;

    /** The full autoscaler context (cluster client, job configuration, etc.). */
    private final JobAutoScalerContext<?> jobContext;

    /** The per-vertex evaluated metrics. */
    private final Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics;

    private final JobTopology jobTopology;

    /** The prefix-stripped configuration for the selected (custom) mode. */
    private final Configuration modeConfiguration;

    private final ScalingLimitedEmitter scalingLimitedEmitter;

    public AlignmentContext(
            JobVertexID vertex,
            int currentParallelism,
            int newParallelism,
            int numSourcePartitions,
            int maxParallelism,
            int parallelismLowerLimit,
            int parallelismUpperLimit,
            Collection<ShipStrategy> inputShipStrategies,
            JobAutoScalerContext<?> jobContext,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            JobTopology jobTopology,
            Configuration modeConfiguration,
            ScalingLimitedEmitter scalingLimitedEmitter) {
        this.vertex = vertex;
        this.currentParallelism = currentParallelism;
        this.newParallelism = newParallelism;
        this.numSourcePartitions = numSourcePartitions;
        this.maxParallelism = maxParallelism;
        this.parallelismLowerLimit = parallelismLowerLimit;
        this.parallelismUpperLimit = parallelismUpperLimit;
        this.inputShipStrategies = inputShipStrategies;
        this.jobContext = jobContext;
        this.evaluatedMetrics = evaluatedMetrics;
        this.jobTopology = jobTopology;
        this.modeConfiguration = modeConfiguration;
        this.scalingLimitedEmitter = scalingLimitedEmitter;
    }

    public JobVertexID getVertex() {
        return vertex;
    }

    public int getCurrentParallelism() {
        return currentParallelism;
    }

    public int getNewParallelism() {
        return newParallelism;
    }

    public int getNumSourcePartitions() {
        return numSourcePartitions;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public int getParallelismLowerLimit() {
        return parallelismLowerLimit;
    }

    public int getParallelismUpperLimit() {
        return parallelismUpperLimit;
    }

    public Collection<ShipStrategy> getInputShipStrategies() {
        return inputShipStrategies;
    }

    public JobAutoScalerContext<?> getJobContext() {
        return jobContext;
    }

    public Map<ScalingMetric, EvaluatedScalingMetric> getEvaluatedMetrics() {
        return evaluatedMetrics;
    }

    public JobTopology getJobTopology() {
        return jobTopology;
    }

    public Configuration getModeConfiguration() {
        return modeConfiguration;
    }

    public boolean isScaleUp() {
        return newParallelism > currentParallelism;
    }

    /** {@code N}: the number of key groups or source partitions to align to. */
    public int numKeyGroupsOrPartitions() {
        return numSourcePartitions <= 0 ? maxParallelism : numSourcePartitions;
    }

    /** The alignment cap: {@code min(N, min(maxParallelism, parallelismUpperLimit))}. */
    public int upperBoundForAlignment() {
        return Math.min(
                numKeyGroupsOrPartitions(), Math.min(maxParallelism, parallelismUpperLimit));
    }

    /** Whether the built-in source / keyBy alignment applies to this vertex. */
    public boolean isSourceOrHashVertex() {
        return numSourcePartitions > 0 || inputShipStrategies.contains(ShipStrategy.HASH);
    }

    public void emitScalingLimited(int expectedParallelism, int actualParallelism) {
        if (scalingLimitedEmitter != null) {
            scalingLimitedEmitter.emit(expectedParallelism, actualParallelism);
        }
    }
}
