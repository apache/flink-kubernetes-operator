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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.status.CheckpointType;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.flink.kubernetes.operator.metrics.FlinkStateSnapshotMetrics.CHECKPOINT_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkStateSnapshotMetrics.COUNTER_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkStateSnapshotMetrics.SAVEPOINT_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkStateSnapshotMetricsUtils.assertSnapshotMetrics;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkStateSnapshotMetrics}. */
@Slf4j
public class FlinkStateSnapshotMetricsTest {
    private final Configuration configuration = new Configuration();
    private TestingMetricListener listener;
    private MetricManager<FlinkStateSnapshot> metricManager;

    @BeforeEach
    public void init() {
        listener = new TestingMetricListener(configuration);
        metricManager =
                MetricManager.createFlinkStateSnapshotMetricManager(
                        configuration, listener.getMetricGroup());
    }

    @Test
    public void testMetricsSameNamespace() {
        var namespace = TestUtils.TEST_NAMESPACE;
        var savepoint1 = createEmptySavepoint("savepoint1", State.COMPLETED);
        var savepoint2 = createEmptySavepoint("savepoint2", State.IN_PROGRESS);

        var counterIdSavepoint =
                listener.getNamespaceMetricId(
                        FlinkStateSnapshot.class,
                        namespace,
                        SAVEPOINT_GROUP_NAME,
                        COUNTER_GROUP_NAME);
        var counterIdCheckpoint =
                listener.getNamespaceMetricId(
                        FlinkStateSnapshot.class,
                        namespace,
                        CHECKPOINT_GROUP_NAME,
                        COUNTER_GROUP_NAME);
        assertThat(listener.getGauge(counterIdSavepoint)).isEmpty();
        assertThat(listener.getGauge(counterIdCheckpoint)).isEmpty();

        metricManager.onUpdate(savepoint1);
        metricManager.onUpdate(savepoint2);
        assertSnapshotMetrics(
                listener, namespace, Map.of(State.COMPLETED, 1, State.IN_PROGRESS, 1), Map.of());

        savepoint2.getStatus().setState(State.COMPLETED);
        metricManager.onUpdate(savepoint2);
        assertSnapshotMetrics(listener, namespace, Map.of(State.COMPLETED, 2), Map.of());

        var checkpoint1 = createEmptyCheckpoint("checkpoint1", State.FAILED);
        var checkpoint2 = createEmptyCheckpoint("checkpoint2", State.COMPLETED);
        var checkpoint3 = createEmptyCheckpoint("checkpoint3", State.COMPLETED);

        metricManager.onUpdate(checkpoint1);
        metricManager.onUpdate(checkpoint2);
        metricManager.onUpdate(checkpoint3);
        assertSnapshotMetrics(
                listener,
                namespace,
                Map.of(State.COMPLETED, 2),
                Map.of(State.FAILED, 1, State.COMPLETED, 2));

        metricManager.onRemove(savepoint1);
        metricManager.onRemove(checkpoint1);
        metricManager.onRemove(checkpoint2);
        assertSnapshotMetrics(
                listener, namespace, Map.of(State.COMPLETED, 1), Map.of(State.COMPLETED, 1));
    }

    @Test
    public void testMetricsMultipleNamespace() {
        var namespace1 = "namespace1";
        var namespace2 = "namespace2";
        var namespace3 = "namespace3";
        var savepoint1 = createEmptySavepoint(namespace1, "savepoint", State.IN_PROGRESS);
        var savepoint2 = createEmptySavepoint(namespace2, "savepoint", State.IN_PROGRESS);

        metricManager.onUpdate(savepoint1);
        metricManager.onUpdate(savepoint2);
        assertSnapshotMetrics(listener, namespace1, Map.of(State.IN_PROGRESS, 1), Map.of());
        assertSnapshotMetrics(listener, namespace2, Map.of(State.IN_PROGRESS, 1), Map.of());

        savepoint1.getStatus().setState(State.COMPLETED);
        metricManager.onUpdate(savepoint1);
        metricManager.onUpdate(savepoint2);
        assertSnapshotMetrics(listener, namespace1, Map.of(State.COMPLETED, 1), Map.of());
        assertSnapshotMetrics(listener, namespace2, Map.of(State.IN_PROGRESS, 1), Map.of());

        var checkpoint1 = createEmptyCheckpoint(namespace1, "checkpoint1", State.FAILED);
        var checkpoint2 = createEmptyCheckpoint(namespace2, "checkpoint2", State.COMPLETED);
        var checkpoint3 = createEmptyCheckpoint(namespace3, "checkpoint3", State.IN_PROGRESS);

        metricManager.onUpdate(checkpoint1);
        metricManager.onUpdate(checkpoint2);
        metricManager.onUpdate(checkpoint3);
        assertSnapshotMetrics(
                listener, namespace1, Map.of(State.COMPLETED, 1), Map.of(State.FAILED, 1));
        assertSnapshotMetrics(
                listener, namespace2, Map.of(State.IN_PROGRESS, 1), Map.of(State.COMPLETED, 1));
        assertSnapshotMetrics(listener, namespace3, Map.of(), Map.of(State.IN_PROGRESS, 1));

        metricManager.onRemove(savepoint1);
        metricManager.onRemove(checkpoint1);
        metricManager.onRemove(checkpoint2);
        assertSnapshotMetrics(listener, namespace1, Map.of(), Map.of());
        assertSnapshotMetrics(listener, namespace2, Map.of(State.IN_PROGRESS, 1), Map.of());
        assertSnapshotMetrics(listener, namespace3, Map.of(), Map.of(State.IN_PROGRESS, 1));
    }

    @Test
    public void testMetricsDisabled() {
        var conf = new Configuration();
        conf.set(OPERATOR_RESOURCE_METRICS_ENABLED, false);
        var listener = new TestingMetricListener(conf);
        var metricManager =
                MetricManager.createFlinkStateSnapshotMetricManager(
                        conf, listener.getMetricGroup());

        var namespace = TestUtils.TEST_NAMESPACE;
        var savepoint = createEmptySavepoint(namespace, "savepoint", State.IN_PROGRESS);
        metricManager.onUpdate(savepoint);
        var counterIdSavepoint =
                listener.getNamespaceMetricId(
                        FlinkStateSnapshot.class,
                        namespace,
                        SAVEPOINT_GROUP_NAME,
                        COUNTER_GROUP_NAME);
        var counterIdCheckpoint =
                listener.getNamespaceMetricId(
                        FlinkStateSnapshot.class,
                        namespace,
                        CHECKPOINT_GROUP_NAME,
                        COUNTER_GROUP_NAME);
        assertThat(listener.getGauge(counterIdSavepoint)).isEmpty();
        assertThat(listener.getGauge(counterIdCheckpoint)).isEmpty();
    }

    private FlinkStateSnapshot createEmptySavepoint(String name, State state) {
        return createEmptySavepoint(TestUtils.TEST_NAMESPACE, name, state);
    }

    private FlinkStateSnapshot createEmptySavepoint(String namespace, String name, State state) {
        var savepoint =
                TestUtils.buildFlinkStateSnapshotSavepoint(name, namespace, "", false, null);
        savepoint.setStatus(FlinkStateSnapshotStatus.builder().state(state).build());
        return savepoint;
    }

    private FlinkStateSnapshot createEmptyCheckpoint(String name, State state) {
        return createEmptyCheckpoint(TestUtils.TEST_NAMESPACE, name, state);
    }

    private FlinkStateSnapshot createEmptyCheckpoint(String namespace, String name, State state) {
        var checkpoint =
                TestUtils.buildFlinkStateSnapshotCheckpoint(
                        name, namespace, CheckpointType.FULL, null);
        checkpoint.setStatus(FlinkStateSnapshotStatus.builder().state(state).build());
        return checkpoint;
    }
}
