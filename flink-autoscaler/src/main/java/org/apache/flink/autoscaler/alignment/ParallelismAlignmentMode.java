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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.Getter;

import java.util.Collection;
import java.util.Map;

/**
 * Determines the parallelism to apply for a vertex, given the autoscaler's computed target and the
 * surrounding {@link Context}.
 *
 * <p>An alignment mode reasons about regions relative to the current parallelism and the computed
 * target:
 *
 * <pre>{@code
 * scale-up:
 *      current │── within-range ──│ target │── above-target ──│ upperAlignLimit
 * scale-down:
 *      lowerAlignLimit │── below-target ──│ target │── within-range ──│ current
 * }</pre>
 *
 * <p>This is the extension seam for tuning how the computed parallelism is adjusted. The built-in
 * behaviors are provided by {@link BuiltInAlignmentMode}. Custom implementations are discovered as
 * plugins (via {@code ServiceLoader} in the standalone autoscaler, or Flink's {@code PluginManager}
 * in the operator) and selected by name through {@code scaling.parallelism-alignment.mode} plus
 * {@code scaling.parallelism-alignment.mode.<name>.class}.
 *
 * <p>An implementation may keep the computed target unchanged or adjust it, and decides for itself
 * whether it applies to a given vertex (see {@link #isApplicable(Context)}).
 */
@Experimental
public interface ParallelismAlignmentMode {

    /**
     * Whether this mode applies to the vertex described by {@code ctx}. When it returns {@code
     * false} the autoscaler keeps the computed target parallelism and {@link
     * #alignParallelism(Context)} is not called. Defaults to keyBy (hash) vertices and to
     * partitioned sources that report a partition count (Kafka and Pulsar do by default). A custom
     * mode can widen this, for example to align custom partitioned vertices.
     */
    default boolean isApplicable(Context<?> ctx) {
        return ctx.getNumSourcePartitions() > 0
                || ctx.getInputShipStrategies().contains(ShipStrategy.HASH);
    }

    /**
     * Returns the parallelism to apply for the vertex described by {@code ctx}. Called only when
     * {@link #isApplicable(Context)} returned {@code true}.
     */
    int alignParallelism(Context<?> ctx);

    /**
     * The parallelism alignment context. It {@code extends} {@link JobAutoScalerContext}, sharing
     * its {@link org.apache.flink.autoscaler.JobAutoScalerContext.ScalingCycleState} and inherited
     * cycle accessors (such as {@link #getJobTopology()} and {@link #getEvaluatedMetrics()}). Its
     * {@code getConfiguration()} returns the effective per-mode configuration: the job
     * configuration with this mode's prefix-stripped {@code
     * scaling.parallelism-alignment.mode.<name>.} overrides merged on top. It adds only the
     * per-vertex alignment inputs that are not already available on the canonical context.
     *
     * @param <KEY> The job key.
     */
    @Getter
    class Context<KEY> extends JobAutoScalerContext<KEY> {

        /** The vertex being aligned. */
        private final JobVertexID vertex;

        /** The current parallelism of the vertex. */
        private final int currentParallelism;

        /** The clamped computed target parallelism, before alignment. */
        private final int newParallelism;

        /** The number of source partitions, or a non-positive value when not a source. */
        private final int numSourcePartitions;

        /** The vertex max parallelism (number of key groups). */
        private final int maxParallelism;

        /** The configured parallelism lower limit. */
        private final int parallelismLowerLimit;

        /** The configured parallelism upper limit. */
        private final int parallelismUpperLimit;

        /** The ship strategies of the vertex inputs. */
        private final Collection<ShipStrategy> inputShipStrategies;

        /** The evaluated metrics of the vertex being aligned. */
        private final Map<ScalingMetric, EvaluatedScalingMetric> evaluatedVertexMetrics;

        public Context(
                JobAutoScalerContext<KEY> autoScalerContext,
                Configuration overrides,
                JobVertexID vertex,
                int currentParallelism,
                int newParallelism,
                int numSourcePartitions,
                int maxParallelism,
                int parallelismLowerLimit,
                int parallelismUpperLimit,
                Collection<ShipStrategy> inputShipStrategies,
                Map<ScalingMetric, EvaluatedScalingMetric> evaluatedVertexMetrics) {
            super(autoScalerContext, overrides);
            this.vertex = vertex;
            this.currentParallelism = currentParallelism;
            this.newParallelism = newParallelism;
            this.numSourcePartitions = numSourcePartitions;
            this.maxParallelism = maxParallelism;
            this.parallelismLowerLimit = parallelismLowerLimit;
            this.parallelismUpperLimit = parallelismUpperLimit;
            this.inputShipStrategies = inputShipStrategies;
            this.evaluatedVertexMetrics = evaluatedVertexMetrics;
        }
    }
}
