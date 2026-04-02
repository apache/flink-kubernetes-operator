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

import org.apache.flink.autoscaler.DelayedScaleDown;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.TestJobAutoScalerContext;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class TestAutoScalerStateStore
        implements AutoScalerStateStore<ResourceID, TestJobAutoScalerContext> {
    @Override
    public void storeScalingHistory(
            TestJobAutoScalerContext jobContext,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {}

    @NotNull
    @Override
    public Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory(
            TestJobAutoScalerContext jobContext) {
        return Map.of();
    }

    @Override
    public void storeScalingTracking(
            TestJobAutoScalerContext jobContext, ScalingTracking scalingTrack) {}

    @Override
    public ScalingTracking getScalingTracking(TestJobAutoScalerContext jobContext) {
        return null;
    }

    @Override
    public void removeScalingHistory(TestJobAutoScalerContext jobContext) {}

    @Override
    public void storeCollectedMetrics(
            TestJobAutoScalerContext jobContext, SortedMap<Instant, CollectedMetrics> metrics) {}

    @NotNull
    @Override
    public SortedMap<Instant, CollectedMetrics> getCollectedMetrics(
            TestJobAutoScalerContext jobContext) {
        return new TreeMap<>();
    }

    @Override
    public void removeCollectedMetrics(TestJobAutoScalerContext jobContext) {}

    @Override
    public void storeParallelismOverrides(
            TestJobAutoScalerContext jobContext, Map<String, String> parallelismOverrides) {}

    @NotNull
    @Override
    public Map<String, String> getParallelismOverrides(TestJobAutoScalerContext jobContext) {
        return Map.of();
    }

    @Override
    public void removeParallelismOverrides(TestJobAutoScalerContext jobContext) {}

    @Override
    public void storeConfigChanges(
            TestJobAutoScalerContext jobContext, ConfigChanges configChanges) {}

    @NotNull
    @Override
    public ConfigChanges getConfigChanges(TestJobAutoScalerContext jobContext) {
        return new ConfigChanges();
    }

    @Override
    public void removeConfigChanges(TestJobAutoScalerContext jobContext) {}

    @Override
    public void storeDelayedScaleDown(
            TestJobAutoScalerContext jobContext, DelayedScaleDown delayedScaleDown) {}

    @NotNull
    @Override
    public DelayedScaleDown getDelayedScaleDown(TestJobAutoScalerContext jobContext) {
        return new DelayedScaleDown();
    }

    @Override
    public void clearAll(TestJobAutoScalerContext jobContext) {}

    @Override
    public void flush(TestJobAutoScalerContext jobContext) {}

    @Override
    public void close() {}
}
