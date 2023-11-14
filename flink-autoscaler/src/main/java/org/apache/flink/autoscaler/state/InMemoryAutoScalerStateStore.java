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

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * State store based on the Java Heap, the state will be discarded after process restarts.
 *
 * @param <KEY> The job key.
 * @param <Context> The job autoscaler context.
 */
public class InMemoryAutoScalerStateStore<KEY, Context extends JobAutoScalerContext<KEY>>
        implements AutoScalerStateStore<KEY, Context> {

    private final Map<KEY, Map<JobVertexID, SortedMap<Instant, ScalingSummary>>>
            scalingHistoryStore;

    private final Map<KEY, SortedMap<Instant, CollectedMetrics>> collectedMetricsStore;

    private final Map<KEY, Map<String, String>> parallelismOverridesStore;

    public InMemoryAutoScalerStateStore() {
        scalingHistoryStore = new ConcurrentHashMap<>();
        collectedMetricsStore = new ConcurrentHashMap<>();
        parallelismOverridesStore = new ConcurrentHashMap<>();
    }

    @Override
    public void storeScalingHistory(
            Context jobContext,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {
        scalingHistoryStore.put(jobContext.getJobKey(), scalingHistory);
    }

    @Override
    public Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory(
            Context jobContext) {
        return Optional.ofNullable(scalingHistoryStore.get(jobContext.getJobKey()))
                .orElse(new HashMap<>());
    }

    @Override
    public void removeScalingHistory(Context jobContext) {
        scalingHistoryStore.remove(jobContext.getJobKey());
    }

    @Override
    public void storeCollectedMetrics(
            Context jobContext, SortedMap<Instant, CollectedMetrics> metrics) {
        collectedMetricsStore.put(jobContext.getJobKey(), metrics);
    }

    @Override
    public SortedMap<Instant, CollectedMetrics> getCollectedMetrics(Context jobContext) {
        return Optional.ofNullable(collectedMetricsStore.get(jobContext.getJobKey()))
                .orElse(new TreeMap<>());
    }

    @Override
    public void removeCollectedMetrics(Context jobContext) {
        collectedMetricsStore.remove(jobContext.getJobKey());
    }

    @Override
    public void storeParallelismOverrides(
            Context jobContext, Map<String, String> parallelismOverrides) {
        parallelismOverridesStore.put(jobContext.getJobKey(), parallelismOverrides);
    }

    @Override
    public Map<String, String> getParallelismOverrides(Context jobContext) {
        return Optional.ofNullable(parallelismOverridesStore.get(jobContext.getJobKey()))
                .orElse(new HashMap<>());
    }

    @Override
    public void removeParallelismOverrides(Context jobContext) {
        parallelismOverridesStore.remove(jobContext.getJobKey());
    }

    @Override
    public void clearAll(Context jobContext) {
        scalingHistoryStore.remove(jobContext.getJobKey());
        parallelismOverridesStore.remove(jobContext.getJobKey());
        collectedMetricsStore.remove(jobContext.getJobKey());
    }

    @Override
    public void flush(Context jobContext) {
        // The InMemory state store doesn't persist data.
    }

    @Override
    public void removeInfoFromCache(KEY jobKey) {
        scalingHistoryStore.remove(jobKey);
        collectedMetricsStore.remove(jobKey);
        parallelismOverridesStore.remove(jobKey);
    }
}
