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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.autoscaler.utils.DateTimeUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.CheckpointSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.spec.SavepointSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.configuration.CheckpointingOptions.MAX_RETAINED_CHECKPOINTS;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.ABANDONED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.COMPLETED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.FAILED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.IN_PROGRESS;
import static org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType.MANUAL;
import static org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType.PERIODIC;
import static org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType.UPGRADE;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE_THRESHOLD;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT_THRESHOLD;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SnapshotObserver}. */
@EnableKubernetesMockClient(crud = true)
public class SnapshotObserverTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;
    private SnapshotObserver<FlinkDeployment, FlinkDeploymentStatus> observer;

    @Override
    public void setup() {
        observer = new SnapshotObserver<>(eventRecorder);
    }

    @Test
    public void testSnapshotTypeCleanup() {
        var conf = new Configuration().set(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT, 1);
        var operatorConfig = FlinkOperatorConfiguration.fromConfiguration(conf);

        var testData =
                List.of(
                        createSnapshot(CHECKPOINT, 0, PERIODIC, COMPLETED),
                        createSnapshot(CHECKPOINT, 1, PERIODIC, COMPLETED),
                        createSnapshot(CHECKPOINT, 2, PERIODIC, COMPLETED),
                        createSnapshot(SAVEPOINT, 3, PERIODIC, COMPLETED),
                        createSnapshot(SAVEPOINT, 4, PERIODIC, COMPLETED),
                        createSnapshot(SAVEPOINT, 5, PERIODIC, COMPLETED));

        var savepointResult =
                observer.getFlinkStateSnapshotsToCleanUp(testData, conf, operatorConfig, SAVEPOINT);
        assertThat(savepointResult).containsExactlyInAnyOrder(testData.get(3), testData.get(4));

        var checkpointResult =
                observer.getFlinkStateSnapshotsToCleanUp(
                        testData, conf, operatorConfig, CHECKPOINT);
        assertThat(checkpointResult).containsExactlyInAnyOrder(testData.get(0), testData.get(1));
    }

    @Test
    public void testSnapshotKeepOneCompleted() {
        var conf = new Configuration().set(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT, 1);
        var operatorConfig = FlinkOperatorConfiguration.fromConfiguration(conf);

        var testData =
                List.of(
                        createSnapshot(SAVEPOINT, 0, PERIODIC, COMPLETED),
                        createSnapshot(SAVEPOINT, 1, PERIODIC, FAILED),
                        createSnapshot(SAVEPOINT, 2, PERIODIC, FAILED),
                        createSnapshot(SAVEPOINT, 3, PERIODIC, FAILED));

        var savepointResult =
                observer.getFlinkStateSnapshotsToCleanUp(testData, conf, operatorConfig, SAVEPOINT);
        assertThat(savepointResult)
                .containsExactlyInAnyOrder(testData.get(1), testData.get(2), testData.get(3));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAgeBasedCleanupSavepoint(boolean setThreshold) {
        var conf = new Configuration().set(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT, 10000);

        if (setThreshold) {
            conf.set(OPERATOR_SAVEPOINT_HISTORY_MAX_AGE_THRESHOLD, Duration.ofMillis(5));
            conf.set(
                    OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                    Duration.ofMillis(System.currentTimeMillis() * 2));
        } else {
            conf.set(OPERATOR_SAVEPOINT_HISTORY_MAX_AGE, Duration.ofMillis(5));
        }

        var operatorConfig = FlinkOperatorConfiguration.fromConfiguration(conf);

        var testDataSavepoints =
                List.of(
                        createSnapshot(SAVEPOINT, 0, MANUAL, COMPLETED),
                        createSnapshot(SAVEPOINT, 1, PERIODIC, COMPLETED),
                        createSnapshot(SAVEPOINT, 2, PERIODIC, ABANDONED),
                        createSnapshot(SAVEPOINT, 3, PERIODIC, COMPLETED),
                        createSnapshot(SAVEPOINT, 4, UPGRADE, COMPLETED),
                        createSnapshot(SAVEPOINT, Long.MAX_VALUE, PERIODIC, COMPLETED));

        var removedSavepoints =
                observer.getFlinkStateSnapshotsToCleanUp(
                        testDataSavepoints, conf, operatorConfig, SAVEPOINT);
        assertThat(removedSavepoints)
                .containsExactlyInAnyOrder(
                        testDataSavepoints.get(1),
                        testDataSavepoints.get(2),
                        testDataSavepoints.get(3),
                        testDataSavepoints.get(4));
    }

    @Test
    public void testAgeBasedCleanupSavepointKeepOne() {
        var conf =
                new Configuration()
                        .set(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT, 10000)
                        .set(OPERATOR_SAVEPOINT_HISTORY_MAX_AGE, Duration.ofMillis(5));

        var operatorConfig = FlinkOperatorConfiguration.fromConfiguration(conf);

        var testDataSavepoints =
                List.of(
                        createSnapshot(SAVEPOINT, 0, PERIODIC, IN_PROGRESS),
                        createSnapshot(SAVEPOINT, 1, PERIODIC, IN_PROGRESS),
                        createSnapshot(SAVEPOINT, 2, PERIODIC, IN_PROGRESS),
                        createSnapshot(SAVEPOINT, 3, PERIODIC, IN_PROGRESS));

        var removedSavepoints =
                observer.getFlinkStateSnapshotsToCleanUp(
                        testDataSavepoints, conf, operatorConfig, SAVEPOINT);
        assertThat(removedSavepoints)
                .containsExactlyInAnyOrder(
                        testDataSavepoints.get(0),
                        testDataSavepoints.get(1),
                        testDataSavepoints.get(2));
    }

    @Test
    public void testAgeBasedCleanupCheckpoint() {
        var conf = new Configuration().set(MAX_RETAINED_CHECKPOINTS, 10000);
        var operatorConfig = FlinkOperatorConfiguration.fromConfiguration(conf);

        var testDataCheckpoints =
                List.of(
                        createSnapshot(CHECKPOINT, 0, MANUAL, COMPLETED),
                        createSnapshot(CHECKPOINT, 1, PERIODIC, COMPLETED),
                        createSnapshot(CHECKPOINT, 2, PERIODIC, ABANDONED),
                        createSnapshot(CHECKPOINT, 3, PERIODIC, COMPLETED),
                        createSnapshot(CHECKPOINT, 4, UPGRADE, COMPLETED),
                        createSnapshot(CHECKPOINT, Long.MAX_VALUE, PERIODIC, COMPLETED));

        var removedCheckpoints =
                observer.getFlinkStateSnapshotsToCleanUp(
                        testDataCheckpoints, conf, operatorConfig, CHECKPOINT);
        assertThat(removedCheckpoints).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCountBasedCleanupSavepoint(boolean setThreshold) {
        var conf = new Configuration();

        if (setThreshold) {
            conf.set(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT_THRESHOLD, 2);
            conf.set(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT, 10000);
        } else {
            conf.set(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT, 2);
        }
        var operatorConfig = FlinkOperatorConfiguration.fromConfiguration(conf);

        var testDataSavepoints =
                List.of(
                        createSnapshot(SAVEPOINT, Long.MAX_VALUE - 1, MANUAL, COMPLETED),
                        createSnapshot(SAVEPOINT, Long.MAX_VALUE - 2, PERIODIC, COMPLETED),
                        createSnapshot(SAVEPOINT, Long.MAX_VALUE - 3, PERIODIC, ABANDONED),
                        createSnapshot(SAVEPOINT, Long.MAX_VALUE - 4, UPGRADE, COMPLETED),
                        createSnapshot(SAVEPOINT, Long.MAX_VALUE - 5, PERIODIC, COMPLETED),
                        createSnapshot(SAVEPOINT, Long.MAX_VALUE - 6, PERIODIC, COMPLETED),
                        createSnapshot(SAVEPOINT, Long.MAX_VALUE - 7, PERIODIC, COMPLETED));

        var removedSavepoints =
                observer.getFlinkStateSnapshotsToCleanUp(
                        testDataSavepoints, conf, operatorConfig, SAVEPOINT);
        assertThat(removedSavepoints)
                .containsExactlyInAnyOrder(
                        testDataSavepoints.get(6),
                        testDataSavepoints.get(5),
                        testDataSavepoints.get(4),
                        testDataSavepoints.get(3));
    }

    @Test
    public void testCountBasedCleanupCheckpoint() {
        var conf = new Configuration().set(MAX_RETAINED_CHECKPOINTS, 2);
        var operatorConfig = FlinkOperatorConfiguration.fromConfiguration(conf);

        var testDataCheckpoints =
                List.of(
                        createSnapshot(CHECKPOINT, Long.MAX_VALUE - 1, MANUAL, COMPLETED),
                        createSnapshot(CHECKPOINT, Long.MAX_VALUE - 2, PERIODIC, COMPLETED),
                        createSnapshot(CHECKPOINT, Long.MAX_VALUE - 3, PERIODIC, ABANDONED),
                        createSnapshot(CHECKPOINT, Long.MAX_VALUE - 4, UPGRADE, COMPLETED),
                        createSnapshot(CHECKPOINT, Long.MAX_VALUE - 5, PERIODIC, COMPLETED),
                        createSnapshot(CHECKPOINT, Long.MAX_VALUE - 6, PERIODIC, COMPLETED),
                        createSnapshot(CHECKPOINT, Long.MAX_VALUE - 7, PERIODIC, COMPLETED));

        var removedCheckpoints =
                observer.getFlinkStateSnapshotsToCleanUp(
                        testDataCheckpoints, conf, operatorConfig, CHECKPOINT);
        assertThat(removedCheckpoints)
                .containsExactlyInAnyOrder(
                        testDataCheckpoints.get(6),
                        testDataCheckpoints.get(5),
                        testDataCheckpoints.get(4),
                        testDataCheckpoints.get(3));
    }

    private static FlinkStateSnapshot createSnapshot(
            SnapshotType snapshotType,
            long timestamp,
            SnapshotTriggerType triggerType,
            FlinkStateSnapshotStatus.State snapshotState) {
        var metadata =
                new ObjectMetaBuilder()
                        .withName(UUID.randomUUID().toString())
                        .withLabels(
                                Map.of(
                                        CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE,
                                        triggerType.name()))
                        .withCreationTimestamp(
                                DateTimeUtils.kubernetes(Instant.ofEpochMilli(timestamp)))
                        .build();

        var spec = new FlinkStateSnapshotSpec();
        if (snapshotType == SAVEPOINT) {
            spec.setSavepoint(new SavepointSpec());
        } else if (snapshotType == CHECKPOINT) {
            spec.setCheckpoint(new CheckpointSpec());
        }

        var status = FlinkStateSnapshotStatus.builder().state(snapshotState).build();

        var snapshot = new FlinkStateSnapshot();
        snapshot.setMetadata(metadata);
        snapshot.setSpec(spec);
        snapshot.setStatus(status);

        return snapshot;
    }
}
