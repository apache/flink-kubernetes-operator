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
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingRecord;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.addToScalingHistoryAndStore;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.getTrimmedScalingHistory;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.updateVertexList;
import static org.assertj.core.api.Assertions.assertThat;

/** Abstract ITCase for {@link AutoScalerStateStore}. */
public abstract class AbstractAutoScalerStateStoreTest<
        KEY, Context extends JobAutoScalerContext<KEY>> {

    protected AutoScalerStateStore<KEY, Context> stateStore;

    protected Context ctx;

    /** Create autoscaler state store based on the physical storage. */
    protected abstract AutoScalerStateStore<KEY, Context> createPhysicalAutoScalerStateStore()
            throws Exception;

    /**
     * Create autoscaler state store based on the cache if the state store has cache. It creates
     * physical state store by default.
     */
    protected AutoScalerStateStore<KEY, Context> createCachedAutoScalerStateStore()
            throws Exception {
        return createPhysicalAutoScalerStateStore();
    }

    protected abstract Context createJobContext();

    /** Some preliminary setup before abstract class setup. */
    protected void preSetup() throws Exception {}

    @BeforeEach
    void setup() throws Exception {
        preSetup();
        stateStore = createCachedAutoScalerStateStore();
        ctx = createJobContext();
    }

    @Test
    void testTopologyUpdate() throws Exception {
        var v1 = new JobVertexID();
        var v2 = new JobVertexID();
        var v3 = new JobVertexID();

        var summaries = new HashMap<JobVertexID, ScalingSummary>();
        summaries.put(v1, new ScalingSummary(1, 2, null));
        summaries.put(v2, new ScalingSummary(1, 2, null));

        var now = Instant.now();

        addToScalingHistoryAndStore(stateStore, ctx, now, summaries);
        stateStore.flush(ctx);

        Assertions.assertEquals(
                summaries.keySet(), getTrimmedScalingHistory(stateStore, ctx, now).keySet());
        Assertions.assertEquals(
                summaries.keySet(),
                getTrimmedScalingHistory(createCachedAutoScalerStateStore(), ctx, now).keySet());
        Assertions.assertEquals(
                summaries.keySet(),
                getTrimmedScalingHistory(createPhysicalAutoScalerStateStore(), ctx, now).keySet());

        updateVertexList(stateStore, ctx, now, Set.of(v2, v3));
        stateStore.flush(ctx);

        // Expect v1 to be removed
        Assertions.assertEquals(
                Set.of(v2), getTrimmedScalingHistory(stateStore, ctx, now).keySet());
        Assertions.assertEquals(
                Set.of(v2),
                getTrimmedScalingHistory(createCachedAutoScalerStateStore(), ctx, now).keySet());
        Assertions.assertEquals(
                Set.of(v2),
                getTrimmedScalingHistory(createPhysicalAutoScalerStateStore(), ctx, now).keySet());
    }

    @Test
    void testHistorySizeConfigs() throws Exception {
        var v1 = new JobVertexID();

        var history = new HashMap<JobVertexID, ScalingSummary>();
        history.put(v1, new ScalingSummary(1, 2, null));

        var conf = ctx.getConfiguration();
        conf.set(AutoScalerOptions.VERTEX_SCALING_HISTORY_COUNT, 2);
        conf.set(AutoScalerOptions.VERTEX_SCALING_HISTORY_AGE, Duration.ofSeconds(10));

        var now = Instant.now();

        // Verify count based expiration
        addToScalingHistoryAndStore(stateStore, ctx, now, history);
        Assertions.assertEquals(1, getTrimmedScalingHistory(stateStore, ctx, now).get(v1).size());

        addToScalingHistoryAndStore(stateStore, ctx, now.plus(Duration.ofSeconds(1)), history);
        addToScalingHistoryAndStore(stateStore, ctx, now.plus(Duration.ofSeconds(2)), history);

        Assertions.assertEquals(
                2,
                getTrimmedScalingHistory(stateStore, ctx, now.plus(Duration.ofSeconds(2)))
                        .get(v1)
                        .size());
        Assertions.assertEquals(
                Set.of(now.plus(Duration.ofSeconds(1)), now.plus(Duration.ofSeconds(2))),
                getTrimmedScalingHistory(stateStore, ctx, now.plus(Duration.ofSeconds(2)))
                        .get(v1)
                        .keySet());

        // Verify time based expiration
        addToScalingHistoryAndStore(stateStore, ctx, now.plus(Duration.ofSeconds(15)), history);
        stateStore.flush(ctx);

        Assertions.assertEquals(
                1,
                getTrimmedScalingHistory(stateStore, ctx, now.plus(Duration.ofSeconds(15)))
                        .get(v1)
                        .size());
        Assertions.assertEquals(
                Set.of(now.plus(Duration.ofSeconds(15))),
                getTrimmedScalingHistory(stateStore, ctx, now.plus(Duration.ofSeconds(15)))
                        .get(v1)
                        .keySet());
        Assertions.assertEquals(
                Set.of(now.plus(Duration.ofSeconds(15))),
                getTrimmedScalingHistory(
                                createCachedAutoScalerStateStore(),
                                ctx,
                                now.plus(Duration.ofSeconds(15)))
                        .get(v1)
                        .keySet());
        Assertions.assertEquals(
                Set.of(now.plus(Duration.ofSeconds(15))),
                getTrimmedScalingHistory(
                                createPhysicalAutoScalerStateStore(),
                                ctx,
                                now.plus(Duration.ofSeconds(15)))
                        .get(v1)
                        .keySet());
    }

    @Test
    protected void testDiscardAllState() throws Exception {
        assertThat(stateStore.getCollectedMetrics(ctx)).isEmpty();
        assertThat(stateStore.getScalingHistory(ctx)).isEmpty();
        assertThat(stateStore.getParallelismOverrides(ctx)).isEmpty();
        assertThat(stateStore.getConfigChanges(ctx).getOverrides()).isEmpty();
        assertThat(stateStore.getScalingTracking(ctx).getScalingRecords()).isEmpty();
        assertThat(stateStore.getDelayedScaleDown(ctx).getDelayedVertices()).isEmpty();

        stateStore.storeCollectedMetrics(
                ctx, new TreeMap<>(Map.of(Instant.now(), new CollectedMetrics())));
        stateStore.storeScalingHistory(
                ctx,
                Map.of(
                        new JobVertexID(),
                        new TreeMap<>(Map.of(Instant.now(), new ScalingSummary()))));
        stateStore.storeParallelismOverrides(ctx, Map.of(new JobVertexID().toHexString(), "23"));
        stateStore.storeConfigChanges(
                ctx, new ConfigChanges().addOverride("config.value", "value"));

        var scalingTracking = new ScalingTracking();
        scalingTracking.addScalingRecord(
                Instant.now().minus(Duration.ofHours(3)), new ScalingRecord());
        scalingTracking.addScalingRecord(
                Instant.now().minus(Duration.ofHours(2)), new ScalingRecord());
        scalingTracking.addScalingRecord(
                Instant.now().minus(Duration.ofHours(1)), new ScalingRecord());
        stateStore.storeScalingTracking(ctx, scalingTracking);

        var delayedScaleDown = new DelayedScaleDown();
        delayedScaleDown.triggerScaleDown(new JobVertexID(), Instant.now(), 10);
        delayedScaleDown.triggerScaleDown(new JobVertexID(), Instant.now().plusSeconds(10), 12);

        stateStore.storeDelayedScaleDown(ctx, delayedScaleDown);

        assertThat(stateStore.getCollectedMetrics(ctx)).isNotEmpty();
        assertThat(stateStore.getScalingHistory(ctx)).isNotEmpty();
        assertThat(stateStore.getParallelismOverrides(ctx)).isNotEmpty();
        assertThat(stateStore.getConfigChanges(ctx).getOverrides()).isNotEmpty();
        assertThat(stateStore.getScalingTracking(ctx)).isEqualTo(scalingTracking);
        assertThat(stateStore.getDelayedScaleDown(ctx).getDelayedVertices())
                .isEqualTo(delayedScaleDown.getDelayedVertices());

        stateStore.flush(ctx);

        assertThat(stateStore.getCollectedMetrics(ctx)).isNotEmpty();
        assertThat(stateStore.getScalingHistory(ctx)).isNotEmpty();
        assertThat(stateStore.getParallelismOverrides(ctx)).isNotEmpty();
        assertThat(stateStore.getConfigChanges(ctx).getOverrides()).isNotEmpty();
        assertThat(stateStore.getScalingTracking(ctx)).isEqualTo(scalingTracking);
        assertThat(stateStore.getDelayedScaleDown(ctx).getDelayedVertices())
                .isEqualTo(delayedScaleDown.getDelayedVertices());

        stateStore.clearAll(ctx);

        assertThat(stateStore.getCollectedMetrics(ctx)).isEmpty();
        assertThat(stateStore.getScalingHistory(ctx)).isEmpty();
        assertThat(stateStore.getParallelismOverrides(ctx)).isEmpty();
        assertThat(stateStore.getConfigChanges(ctx).getOverrides()).isEmpty();
        assertThat(stateStore.getScalingTracking(ctx).getScalingRecords()).isEmpty();
        assertThat(stateStore.getDelayedScaleDown(ctx).getDelayedVertices()).isEmpty();
    }
}
