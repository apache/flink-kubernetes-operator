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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.autoscaler.utils.DateTimeUtils;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.CheckpointSpec;
import org.apache.flink.kubernetes.operator.api.spec.SavepointSpec;
import org.apache.flink.kubernetes.operator.api.status.Checkpoint;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.COMPLETED;
import static org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType.PERIODIC;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SnapshotTriggerTimestampStoreTest {

    private final FlinkDeployment resource = TestUtils.buildApplicationCluster();

    @Test
    public void testTimestampStore() {
        for (var snapshotType : SnapshotType.values()) {
            testCreationTimestamp(snapshotType);
            testLegacySavepoint(snapshotType);
            testSingleSnapshot(snapshotType);
            testMultipleSnapshots(snapshotType);
            testUpdateTimestamp(snapshotType);
        }
    }

    private void testCreationTimestamp(SnapshotType snapshotType) {
        var store = new SnapshotTriggerTimestampStore();

        var instantCreation = Instant.ofEpochMilli(1);
        resource.getMetadata().setCreationTimestamp(DateTimeUtils.kubernetes(instantCreation));

        assertEquals(
                instantCreation,
                store.getLastPeriodicTriggerInstant(resource, snapshotType, Set::of));
    }

    private void testLegacySavepoint(SnapshotType snapshotType) {
        var store = new SnapshotTriggerTimestampStore();
        var instantLegacy = Instant.ofEpochMilli(2);

        if (snapshotType == SAVEPOINT) {
            resource.getStatus()
                    .getJobStatus()
                    .getSavepointInfo()
                    .updateLastSavepoint(new Savepoint(2L, "", PERIODIC, null, null));
        } else {
            resource.getStatus()
                    .getJobStatus()
                    .getCheckpointInfo()
                    .updateLastCheckpoint(new Checkpoint(2L, PERIODIC, null, null));
        }

        assertEquals(
                instantLegacy,
                store.getLastPeriodicTriggerInstant(resource, snapshotType, Set::of));
    }

    private void testSingleSnapshot(SnapshotType snapshotType) {
        var store = new SnapshotTriggerTimestampStore();
        var snapshots = Set.of(createSnapshot(snapshotType, SnapshotTriggerType.PERIODIC, 3L));
        assertEquals(
                Instant.ofEpochMilli(3),
                store.getLastPeriodicTriggerInstant(resource, snapshotType, () -> snapshots));
    }

    private void testMultipleSnapshots(SnapshotType snapshotType) {
        var store = new SnapshotTriggerTimestampStore();
        var snapshots =
                Set.of(
                        createSnapshot(snapshotType, SnapshotTriggerType.PERIODIC, 200L),
                        createSnapshot(snapshotType, SnapshotTriggerType.PERIODIC, 300L),
                        createSnapshot(snapshotType, SnapshotTriggerType.MANUAL, 10000L),
                        createSnapshot(snapshotType, SnapshotTriggerType.PERIODIC, 0L));
        assertEquals(
                Instant.ofEpochMilli(300),
                store.getLastPeriodicTriggerInstant(resource, snapshotType, () -> snapshots));
    }

    private void testUpdateTimestamp(SnapshotType snapshotType) {
        var store = new SnapshotTriggerTimestampStore();

        var instantInMemory = Instant.ofEpochMilli(111L);
        store.updateLastPeriodicTriggerTimestamp(resource, snapshotType, instantInMemory);
        assertEquals(
                instantInMemory,
                store.getLastPeriodicTriggerInstant(resource, snapshotType, Set::of));

        instantInMemory = Instant.ofEpochMilli(11L);
        store.updateLastPeriodicTriggerTimestamp(resource, snapshotType, instantInMemory);
        assertEquals(
                instantInMemory,
                store.getLastPeriodicTriggerInstant(resource, snapshotType, Set::of));
    }

    private FlinkStateSnapshot createSnapshot(
            SnapshotType snapshotType, SnapshotTriggerType triggerType, Long timestamp) {
        var snapshot = new FlinkStateSnapshot();
        snapshot.getMetadata()
                .setCreationTimestamp(DateTimeUtils.kubernetes(Instant.ofEpochMilli(timestamp)));
        if (snapshotType == SAVEPOINT) {
            snapshot.getSpec().setSavepoint(new SavepointSpec());
        } else {
            snapshot.getSpec().setCheckpoint(new CheckpointSpec());
        }
        snapshot.getMetadata()
                .getLabels()
                .put(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE, triggerType.name());
        snapshot.setStatus(new FlinkStateSnapshotStatus());
        snapshot.getStatus().setState(COMPLETED);
        return snapshot;
    }
}
