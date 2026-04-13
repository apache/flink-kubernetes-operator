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

import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Map;
import java.util.Optional;

/**
 * A pluggable plugin that allows users to intercept, modify, or reject computed scaling decisions
 * before they are applied. Implementations are invoked in the {@link ScalingExecutor} after scaling
 * summaries have been computed and the blocked check has passed, but before the actual application
 * of parallelism overrides.
 *
 * <p>Multiple plugin implementations can be chained. Each plugin receives the (possibly modified)
 * output of the previous plugin. If any plugin returns an empty {@link Optional}, the scaling
 * operation is vetoed entirely.
 *
 * <p>This is complementary to FLIP-514 (Custom Evaluator plugin for Flink Autoscaler) and provides
 * extensibility at the scaling decision execution layer.
 *
 * <p>Implementations are discovered via Java's {@link java.util.ServiceLoader} mechanism. To
 * register a custom plugin, add the fully qualified class name of the implementation to {@code
 * META-INF/services/org.apache.flink.autoscaler.ScalingExecutorPlugin}.
 *
 * @param <KEY> The job key.
 * @param <Context> Instance of {@link JobAutoScalerContext}.
 */
public interface ScalingExecutorPlugin<KEY, Context extends JobAutoScalerContext<KEY>> {

    /**
     * Returns the priority of this plugin in the chain. Plugins with lower priority values are
     * executed first. The default priority is 0. Plugins with equal priority have no guaranteed
     * relative ordering.
     *
     * <p>Example usage: a resource-gating plugin (priority -100) should run before a
     * parallelism-alignment plugin (priority 0), which should run before a cost-cap plugin
     * (priority 100).
     *
     * @return the priority value; lower values execute first.
     */
    default int priority() {
        return 0;
    }

    /**
     * Filters the computed scaling summaries before they are applied.
     *
     * <p>Implementations can:
     *
     * <ul>
     *   <li>Return the summaries unmodified to approve the scaling decision as-is.
     *   <li>Return a modified map (e.g., with adjusted parallelism values) to alter the decision.
     *   <li>Return an empty {@link Optional} to veto/reject the scaling operation entirely.
     * </ul>
     *
     * @param context The autoscaler context for the current job.
     * @param conf The current configuration.
     * @param evaluatedMetrics The evaluated scaling metrics for all vertices and global metrics.
     * @param jobTopology The job topology.
     * @param scalingSummaries The computed scaling summaries keyed by vertex ID. This map contains
     *     only vertices that require a parallelism change.
     * @return An {@link Optional} containing the (possibly modified) scaling summaries to apply, or
     *     an empty {@link Optional} to veto the scaling.
     */
    Optional<Map<JobVertexID, ScalingSummary>> filterScalingDecisions(
            Context context,
            Configuration conf,
            EvaluatedMetrics evaluatedMetrics,
            JobTopology jobTopology,
            Map<JobVertexID, ScalingSummary> scalingSummaries);
}
