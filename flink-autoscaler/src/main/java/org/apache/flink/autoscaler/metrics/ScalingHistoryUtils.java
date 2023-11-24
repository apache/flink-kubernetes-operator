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

package org.apache.flink.autoscaler.metrics;

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nonnull;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/** The utils for scaling history. */
public class ScalingHistoryUtils {

    public static <KEY, Context extends JobAutoScalerContext<KEY>> void addToScalingHistoryAndStore(
            AutoScalerStateStore<KEY, Context> stateStore,
            Context context,
            Instant now,
            Map<JobVertexID, ScalingSummary> summaries)
            throws Exception {
        addToScalingHistoryAndStore(
                stateStore,
                context,
                getTrimmedScalingHistory(stateStore, context, now),
                now,
                summaries);
    }

    public static <KEY, Context extends JobAutoScalerContext<KEY>> void addToScalingHistoryAndStore(
            AutoScalerStateStore<KEY, Context> stateStore,
            Context context,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory,
            Instant now,
            Map<JobVertexID, ScalingSummary> summaries)
            throws Exception {

        summaries.forEach(
                (id, summary) ->
                        scalingHistory.computeIfAbsent(id, j -> new TreeMap<>()).put(now, summary));
        stateStore.storeScalingHistory(context, scalingHistory);
    }

    public static <KEY, Context extends JobAutoScalerContext<KEY>> void updateVertexList(
            AutoScalerStateStore<KEY, Context> stateStore,
            Context ctx,
            Instant now,
            Set<JobVertexID> vertexSet)
            throws Exception {
        Map<JobVertexID, SortedMap<Instant, ScalingSummary>> trimmedScalingHistory =
                getTrimmedScalingHistory(stateStore, ctx, now);

        if (trimmedScalingHistory.keySet().removeIf(v -> !vertexSet.contains(v))) {
            stateStore.storeScalingHistory(ctx, trimmedScalingHistory);
        }
    }

    @Nonnull
    public static <KEY, Context extends JobAutoScalerContext<KEY>>
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getTrimmedScalingHistory(
                    AutoScalerStateStore<KEY, Context> autoScalerStateStore,
                    Context context,
                    Instant now)
                    throws Exception {
        var conf = context.getConfiguration();
        return trimScalingHistory(now, conf, autoScalerStateStore.getScalingHistory(context));
    }

    public static Map<JobVertexID, SortedMap<Instant, ScalingSummary>> trimScalingHistory(
            Instant now,
            Configuration conf,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {
        Instant expectedStartTime =
                now.minus(conf.get(AutoScalerOptions.VERTEX_SCALING_HISTORY_AGE));
        var result = new TreeMap<>(scalingHistory);
        var entryIt = result.entrySet().iterator();
        while (entryIt.hasNext()) {
            var entry = entryIt.next();
            // Limit how long past scaling decisions are remembered
            entry.setValue(new TreeMap<>(entry.getValue().tailMap(expectedStartTime)));
            var vertexHistory = entry.getValue();
            while (vertexHistory.size()
                    > conf.get(AutoScalerOptions.VERTEX_SCALING_HISTORY_COUNT)) {
                vertexHistory.remove(vertexHistory.firstKey());
            }
            if (vertexHistory.isEmpty()) {
                entryIt.remove();
            }
        }
        return result;
    }

    @Nonnull
    public static <KEY, Context extends JobAutoScalerContext<KEY>>
            ScalingTracking getTrimmedScalingTracking(
                    AutoScalerStateStore<KEY, Context> autoScalerStateStore,
                    Context context,
                    Instant now)
                    throws Exception {
        var conf = context.getConfiguration();
        var scalingTracking = autoScalerStateStore.getScalingTracking(context);
        // Reusing settings used for scaling history
        scalingTracking.removeOldRecords(
                now,
                conf.get(AutoScalerOptions.VERTEX_SCALING_HISTORY_AGE),
                conf.get(AutoScalerOptions.VERTEX_SCALING_HISTORY_COUNT));
        return scalingTracking;
    }
}
