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

/**
 * Determines the parallelism to apply for a vertex, given the autoscaler's computed target and the
 * surrounding {@link AlignmentContext}.
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
 * in the operator) and selected by name through {@code scaling.alignment.mode} plus {@code
 * scaling.alignment.mode.<name>.class}.
 *
 * <p>An implementation may keep the computed target unchanged or adjust it, and decides for itself
 * whether it applies to a given vertex (see {@link #isApplicable(AlignmentContext)}).
 */
@Experimental
public interface AlignmentMode {

    /**
     * Whether this mode applies to the vertex described by {@code ctx}. When it returns {@code
     * false} the autoscaler keeps the computed target parallelism and {@link
     * #align(AlignmentContext)} is not called. Defaults to source and keyBy (hash) vertices. A
     * custom mode can widen this, for example to align custom partitioned vertices.
     */
    default boolean isApplicable(AlignmentContext ctx) {
        return ctx.isSourceOrHashVertex();
    }

    /**
     * Returns the parallelism to apply for the vertex described by {@code ctx}. Called only when
     * {@link #isApplicable(AlignmentContext)} returned {@code true}.
     *
     * @param ctx the alignment inputs (parallelism, partitions, ship strategies, metrics, topology,
     *     and the per-mode configuration)
     * @return the parallelism to apply
     */
    int align(AlignmentContext ctx);
}
