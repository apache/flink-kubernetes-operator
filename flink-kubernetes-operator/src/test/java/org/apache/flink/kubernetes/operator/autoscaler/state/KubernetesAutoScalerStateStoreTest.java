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

package org.apache.flink.kubernetes.operator.autoscaler.state;

import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.state.AbstractAutoScalerStateStoreTest;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.addToScalingHistoryAndStore;
import static org.apache.flink.autoscaler.metrics.ScalingHistoryUtils.getTrimmedScalingHistory;
import static org.apache.flink.kubernetes.operator.autoscaler.TestingKubernetesAutoscalerUtils.createContext;
import static org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore.serializeEvaluatedMetrics;
import static org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore.serializeScalingHistory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link KubernetesAutoScalerStateStore}. */
@EnableKubernetesMockClient(crud = true)
public class KubernetesAutoScalerStateStoreTest
        extends AbstractAutoScalerStateStoreTest<ResourceID, KubernetesJobAutoScalerContext> {

    KubernetesClient kubernetesClient;

    ConfigMapStore configMapStore;

    @Override
    protected AutoScalerStateStore<ResourceID, KubernetesJobAutoScalerContext>
            createPhysicalAutoScalerStateStore() {
        return new KubernetesAutoScalerStateStore(new ConfigMapStore(kubernetesClient));
    }

    @Override
    protected AutoScalerStateStore<ResourceID, KubernetesJobAutoScalerContext>
            createCachedAutoScalerStateStore() {
        return new KubernetesAutoScalerStateStore(configMapStore);
    }

    @Override
    protected KubernetesJobAutoScalerContext createJobContext() {
        return createContext("cr1", kubernetesClient);
    }

    @Override
    protected void preSetup() {
        configMapStore = new ConfigMapStore(kubernetesClient);
    }

    @Test
    void testCompressionMigration() throws Exception {
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
        assertThat(stateStore.getCollectedMetrics(ctx)).isEqualTo(metricHistory);

        // Override with compressed data
        var newTs = Instant.now();

        addToScalingHistoryAndStore(stateStore, ctx, newTs, Map.of());
        stateStore.storeCollectedMetrics(ctx, metricHistory);

        // Make sure we can still access everything
        Assertions.assertEquals(scalingHistory, getTrimmedScalingHistory(stateStore, ctx, newTs));
        assertThat(stateStore.getCollectedMetrics(ctx)).isEqualTo(metricHistory);
    }

    @Test
    void testMetricsTrimming() throws Exception {
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

        stateStore.storeCollectedMetrics(ctx, metricHistory);

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

        ((KubernetesAutoScalerStateStore) stateStore).trimHistoryToMaxCmSize(ctx);
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
    void testDiscardInvalidHistory() throws Exception {
        configMapStore.putSerializedState(
                ctx, KubernetesAutoScalerStateStore.COLLECTED_METRICS_KEY, "invalid");
        configMapStore.putSerializedState(
                ctx, KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY, "invalid2");

        var now = Instant.now();

        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.COLLECTED_METRICS_KEY))
                .isPresent();
        assertThat(stateStore.getCollectedMetrics(ctx)).isEmpty();
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

    @Test
    protected void testDiscardAllState() throws Exception {
        super.testDiscardAllState();

        // We haven't flushed the clear operation, ConfigMap in Kubernetes should not be empty
        assertThat(configMapStore.getConfigMapFromKubernetes(ctx).getDataReadOnly()).isNotEmpty();

        stateStore.flush(ctx);

        // Contents should be removed from Kubernetes
        assertThat(configMapStore.getConfigMapFromKubernetes(ctx).getDataReadOnly()).isEmpty();
    }
}
