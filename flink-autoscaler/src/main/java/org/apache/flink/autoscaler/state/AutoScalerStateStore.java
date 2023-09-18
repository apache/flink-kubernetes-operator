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

package org.apache.flink.autoscaler.state;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

/**
 * The state store is responsible for storing all state during scaling.
 *
 * @param <KEY> The job key.
 * @param <Context> Instance of JobAutoScalerContext.
 */
@Experimental
public interface AutoScalerStateStore<KEY, Context extends JobAutoScalerContext<KEY>> {

    void storeScalingHistory(
            Context jobContext, Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory)
            throws Exception;

    Optional<Map<JobVertexID, SortedMap<Instant, ScalingSummary>>> getScalingHistory(
            Context jobContext) throws Exception;

    void removeScalingHistory(Context jobContext) throws Exception;

    void storeEvaluatedMetrics(
            Context jobContext, SortedMap<Instant, CollectedMetrics> evaluatedMetrics)
            throws Exception;

    Optional<SortedMap<Instant, CollectedMetrics>> getEvaluatedMetrics(Context jobContext)
            throws Exception;

    void removeEvaluatedMetrics(Context jobContext) throws Exception;

    void storeParallelismOverrides(Context jobContext, Map<String, String> parallelismOverrides)
            throws Exception;

    Optional<Map<String, String>> getParallelismOverrides(Context jobContext) throws Exception;

    void removeParallelismOverrides(Context jobContext) throws Exception;

    /**
     * Flushing is needed because we just save data in cache for all store methods. For less write
     * operations, we flush the cached data to the physical storage only after all operations have
     * been performed.
     */
    void flush(Context jobContext) throws Exception;

    /** Clean up all information related to the current job. */
    void removeInfoFromCache(KEY jobKey);
}
