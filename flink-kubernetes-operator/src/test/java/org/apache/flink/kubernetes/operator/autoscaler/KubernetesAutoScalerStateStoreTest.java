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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.addToScalingHistoryAndStore;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.getTrimmedScalingHistory;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.updateVertexList;
import static org.apache.flink.kubernetes.operator.autoscaler.KubernetesAutoScalerStateStore.serializeEvaluatedMetrics;
import static org.apache.flink.kubernetes.operator.autoscaler.KubernetesAutoScalerStateStore.serializeScalingHistory;
import static org.apache.flink.kubernetes.operator.autoscaler.TestingKubernetesAutoscalerUtils.createContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link KubernetesAutoScalerStateStore}. */
@EnableKubernetesMockClient(crud = true)
public class KubernetesAutoScalerStateStoreTest {

    KubernetesClient kubernetesClient;

    ConfigMapStore configMapStore;

    KubernetesAutoScalerStateStore stateStore;

    KubernetesJobAutoScalerContext ctx;

    @BeforeEach
    void setup() {
        configMapStore = new ConfigMapStore(kubernetesClient);
        stateStore = new KubernetesAutoScalerStateStore(configMapStore);
        ctx = createContext("cr1", kubernetesClient);
    }

    @Test
    public void testTopologyUpdate() throws Exception {
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
                getTrimmedScalingHistory(
                                new KubernetesAutoScalerStateStore(configMapStore), ctx, now)
                        .keySet());
        Assertions.assertEquals(
                summaries.keySet(),
                getTrimmedScalingHistory(
                                new KubernetesAutoScalerStateStore(
                                        new ConfigMapStore(kubernetesClient)),
                                ctx,
                                now)
                        .keySet());

        updateVertexList(stateStore, ctx, now, Set.of(v2, v3));
        stateStore.flush(ctx);

        // Expect v1 to be removed
        Assertions.assertEquals(
                Set.of(v2), getTrimmedScalingHistory(stateStore, ctx, now).keySet());
        Assertions.assertEquals(
                Set.of(v2),
                getTrimmedScalingHistory(
                                new KubernetesAutoScalerStateStore(configMapStore), ctx, now)
                        .keySet());
        Assertions.assertEquals(
                Set.of(v2),
                getTrimmedScalingHistory(
                                new KubernetesAutoScalerStateStore(
                                        new ConfigMapStore(kubernetesClient)),
                                ctx,
                                now)
                        .keySet());
    }

    @Test
    public void testHistorySizeConfigs() throws Exception {
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
                                new KubernetesAutoScalerStateStore(configMapStore),
                                ctx,
                                now.plus(Duration.ofSeconds(15)))
                        .get(v1)
                        .keySet());
        Assertions.assertEquals(
                Set.of(now.plus(Duration.ofSeconds(15))),
                getTrimmedScalingHistory(
                                new KubernetesAutoScalerStateStore(
                                        new ConfigMapStore(kubernetesClient)),
                                ctx,
                                now.plus(Duration.ofSeconds(15)))
                        .get(v1)
                        .keySet());
    }

    @Test
    public void testCompressionMigration() throws Exception {
        var jobUpdateTs = Instant.now();
        var v1 = new JobVertexID();

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();
        metricHistory.put(
                jobUpdateTs,
                new CollectedMetrics(
                        Map.of(v1, Map.of(ScalingMetric.TRUE_PROCESSING_RATE, 1.)), Map.of()));

        var scalingHistory = new HashMap<JobVertexID, SortedMap<Instant, ScalingSummary>>();
        scalingHistory.put(v1, new TreeMap<>());
        scalingHistory
                .get(v1)
                .put(
                        jobUpdateTs,
                        new ScalingSummary(
                                1, 2, Map.of(ScalingMetric.LAG, EvaluatedScalingMetric.of(2.))));

        // Store uncompressed data in map to simulate migration
        configMapStore.putSerializedState(
                ctx,
                KubernetesAutoScalerStateStore.COLLECTED_METRICS_KEY,
                serializeEvaluatedMetrics(metricHistory));
        configMapStore.putSerializedState(
                ctx,
                KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY,
                serializeScalingHistory(scalingHistory));

        var now = Instant.now();

        Assertions.assertEquals(scalingHistory, getTrimmedScalingHistory(stateStore, ctx, now));
        assertThat(stateStore.getEvaluatedMetrics(ctx)).hasValue(metricHistory);

        // Override with compressed data
        var newTs = Instant.now();

        addToScalingHistoryAndStore(stateStore, ctx, newTs, Map.of());
        stateStore.storeEvaluatedMetrics(ctx, metricHistory);

        // Make sure we can still access everything
        Assertions.assertEquals(scalingHistory, getTrimmedScalingHistory(stateStore, ctx, newTs));
        assertThat(stateStore.getEvaluatedMetrics(ctx)).hasValue(metricHistory);
    }

    @Test
    public void testMetricsTrimming() throws Exception {
        var v1 = new JobVertexID();
        Random rnd = new Random();

        var metricHistory = new TreeMap<Instant, CollectedMetrics>();
        for (int i = 0; i < 50; i++) {
            var m = new HashMap<JobVertexID, Map<ScalingMetric, Double>>();
            for (int j = 0; j < 500; j++) {
                m.put(
                        new JobVertexID(),
                        Map.of(ScalingMetric.TRUE_PROCESSING_RATE, rnd.nextDouble()));
            }
            metricHistory.put(Instant.now(), new CollectedMetrics(m, Collections.emptyMap()));
        }

        var scalingHistory = new HashMap<JobVertexID, SortedMap<Instant, ScalingSummary>>();
        scalingHistory.put(v1, new TreeMap<>());
        scalingHistory
                .get(v1)
                .put(
                        Instant.now(),
                        new ScalingSummary(
                                1, 2, Map.of(ScalingMetric.LAG, EvaluatedScalingMetric.of(2.))));

        addToScalingHistoryAndStore(
                stateStore,
                ctx,
                Instant.now(),
                Map.of(
                        v1,
                        new ScalingSummary(
                                1, 2, Map.of(ScalingMetric.LAG, EvaluatedScalingMetric.of(2.)))));

        stateStore.storeEvaluatedMetrics(ctx, metricHistory);

        assertFalse(
                configMapStore
                                        .getSerializedState(
                                                ctx,
                                                KubernetesAutoScalerStateStore
                                                        .COLLECTED_METRICS_KEY)
                                        .get()
                                        .length()
                                + configMapStore
                                        .getSerializedState(
                                                ctx,
                                                KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY)
                                        .get()
                                        .length()
                        < KubernetesAutoScalerStateStore.MAX_CM_BYTES);

        stateStore.trimHistoryToMaxCmSize(ctx);
        assertTrue(
                configMapStore
                                        .getSerializedState(
                                                ctx,
                                                KubernetesAutoScalerStateStore
                                                        .COLLECTED_METRICS_KEY)
                                        .get()
                                        .length()
                                + configMapStore
                                        .getSerializedState(
                                                ctx,
                                                KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY)
                                        .get()
                                        .length()
                        < KubernetesAutoScalerStateStore.MAX_CM_BYTES);
    }

    @Test
    public void testDiscardInvalidHistory() throws Exception {
        configMapStore.putSerializedState(
                ctx, KubernetesAutoScalerStateStore.COLLECTED_METRICS_KEY, "invalid");
        configMapStore.putSerializedState(
                ctx, KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY, "invalid2");

        var now = Instant.now();

        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.COLLECTED_METRICS_KEY))
                .isPresent();
        assertThat(stateStore.getEvaluatedMetrics(ctx)).isEmpty();
        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.COLLECTED_METRICS_KEY))
                .isEmpty();

        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY))
                .isPresent();
        Assertions.assertEquals(new TreeMap<>(), getTrimmedScalingHistory(stateStore, ctx, now));
        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY))
                .isEmpty();
    }
}
