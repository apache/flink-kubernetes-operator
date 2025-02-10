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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.utils.SnapshotStatus;
import org.apache.flink.kubernetes.operator.utils.SnapshotUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link SnapshotObserver}. */
@EnableKubernetesMockClient(crud = true)
public class SnapshotObserverLegacyTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;
    private SnapshotObserver<FlinkDeployment, FlinkDeploymentStatus> observer;

    @Override
    public void setup() {
        observer = new SnapshotObserver<>(eventRecorder);
    }

    @Test
    public void testBasicObserve() {
        Configuration conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED, false);
        configManager.updateDefaultConfig(conf);

        var deployment = TestUtils.buildApplicationCluster();
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        var spInfo = deployment.getStatus().getJobStatus().getSavepointInfo();
        Assertions.assertTrue(spInfo.getSavepointHistory().isEmpty());

        Savepoint sp =
                new Savepoint(
                        1, "sp1", SnapshotTriggerType.MANUAL, SavepointFormatType.CANONICAL, 123L);
        spInfo.updateLastSavepoint(sp);
        observer.cleanupSavepointHistoryLegacy(getResourceContext(deployment), Set.of());

        Assertions.assertNotNull(spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp), spInfo.getSavepointHistory());
    }

    @Test
    public void testCountBasedDisposeWithSnapshotResources() {
        var cr = TestUtils.buildSessionCluster();
        cr.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(cr.getSpec(), cr);
        Configuration conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT, 2);
        configManager.updateDefaultConfig(conf);

        var spInfo = cr.getStatus().getJobStatus().getSavepointInfo();
        for (var i = 0; i < 5; i++) {
            var sp =
                    new Savepoint(
                            System.currentTimeMillis() * 2,
                            String.format("sp%d", i),
                            SnapshotTriggerType.MANUAL,
                            SavepointFormatType.CANONICAL,
                            (long) i);
            spInfo.updateLastSavepoint(sp);
        }

        assertThat(spInfo.getSavepointHistory()).hasSize(5);
        observer.cleanupSavepointHistoryLegacy(getResourceContext(cr), Set.of());
        assertThat(spInfo.getSavepointHistory()).hasSize(2);

        var snapshot1 =
                BaseTestUtils.buildFlinkStateSnapshotSavepoint(
                        "cr_sp1",
                        cr.getMetadata().getNamespace(),
                        "cr_sp1",
                        false,
                        JobReference.fromFlinkResource(cr));
        var snapshot2 =
                BaseTestUtils.buildFlinkStateSnapshotSavepoint(
                        "cr_sp2",
                        cr.getMetadata().getNamespace(),
                        "cr_sp2",
                        false,
                        JobReference.fromFlinkResource(cr));
        snapshot1.setStatus(new FlinkStateSnapshotStatus());
        snapshot1.getStatus().setState(FlinkStateSnapshotStatus.State.IN_PROGRESS);

        observer.cleanupSavepointHistoryLegacy(
                getResourceContext(cr), Set.of(snapshot1, snapshot2));
        assertThat(spInfo.getSavepointHistory()).hasSize(2);

        snapshot1.getStatus().setState(FlinkStateSnapshotStatus.State.COMPLETED);

        observer.cleanupSavepointHistoryLegacy(
                getResourceContext(cr), Set.of(snapshot1, snapshot2));
        assertThat(spInfo.getSavepointHistory()).hasSize(1);

        snapshot2.setStatus(new FlinkStateSnapshotStatus());
        snapshot2.getStatus().setState(FlinkStateSnapshotStatus.State.COMPLETED);

        observer.cleanupSavepointHistoryLegacy(
                getResourceContext(cr), Set.of(snapshot1, snapshot2));
        assertThat(spInfo.getSavepointHistory()).hasSize(0);
    }

    @Test
    public void testAgeBasedDisposeWithSnapshotResources() {
        var cr = TestUtils.buildSessionCluster();
        cr.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(cr.getSpec(), cr);
        Configuration conf = new Configuration();
        conf.set(
                KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                Duration.ofMillis(5));
        configManager.updateDefaultConfig(conf);

        var spInfo = cr.getStatus().getJobStatus().getSavepointInfo();
        for (var i = 0; i < 5; i++) {
            var sp =
                    new Savepoint(
                            i,
                            String.format("sp%d", i),
                            SnapshotTriggerType.MANUAL,
                            SavepointFormatType.CANONICAL,
                            (long) i);
            spInfo.updateLastSavepoint(sp);
        }

        assertThat(spInfo.getSavepointHistory()).hasSize(5);
        observer.cleanupSavepointHistoryLegacy(getResourceContext(cr), Set.of());
        assertThat(spInfo.getSavepointHistory()).hasSize(1);

        var snapshot1 =
                BaseTestUtils.buildFlinkStateSnapshotSavepoint(
                        "cr_sp1",
                        cr.getMetadata().getNamespace(),
                        "cr_sp1",
                        false,
                        JobReference.fromFlinkResource(cr));
        snapshot1.setStatus(new FlinkStateSnapshotStatus());
        snapshot1.getStatus().setState(FlinkStateSnapshotStatus.State.IN_PROGRESS);

        observer.cleanupSavepointHistoryLegacy(getResourceContext(cr), Set.of(snapshot1));
        assertThat(spInfo.getSavepointHistory()).hasSize(1);

        snapshot1.getStatus().setState(FlinkStateSnapshotStatus.State.COMPLETED);

        observer.cleanupSavepointHistoryLegacy(getResourceContext(cr), Set.of(snapshot1));
        assertThat(spInfo.getSavepointHistory()).hasSize(0);
    }

    @Test
    public void testAgeBasedDispose() {
        var cr = TestUtils.buildSessionCluster();
        cr.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(cr.getSpec(), cr);
        Configuration conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED, false);
        conf.set(
                KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                Duration.ofMillis(5));
        configManager.updateDefaultConfig(conf);

        var spInfo = cr.getStatus().getJobStatus().getSavepointInfo();

        var sp1 =
                new Savepoint(
                        1, "sp1", SnapshotTriggerType.MANUAL, SavepointFormatType.CANONICAL, 123L);
        spInfo.updateLastSavepoint(sp1);
        observer.cleanupSavepointHistoryLegacy(getResourceContext(cr), Set.of());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp1), spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.emptyList(), flinkService.getDisposedSavepoints());

        Savepoint sp2 =
                new Savepoint(
                        2, "sp2", SnapshotTriggerType.MANUAL, SavepointFormatType.CANONICAL, 123L);
        spInfo.updateLastSavepoint(sp2);
        observer.cleanupSavepointHistoryLegacy(getResourceContext(cr), Set.of());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp2), spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp1.getLocation()), flinkService.getDisposedSavepoints());
    }

    @Test
    public void testAgeBasedDisposeWithAgeThreshold() {
        var deployment = TestUtils.buildApplicationCluster();
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        Configuration conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED, false);
        conf.set(
                KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                Duration.ofMillis(System.currentTimeMillis() * 2));
        conf.set(
                KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE_THRESHOLD,
                Duration.ofMillis(5));
        configManager.updateDefaultConfig(conf);
        var spInfo = deployment.getStatus().getJobStatus().getSavepointInfo();

        var sp1 =
                new Savepoint(
                        1, "sp1", SnapshotTriggerType.MANUAL, SavepointFormatType.CANONICAL, 123L);
        spInfo.updateLastSavepoint(sp1);
        observer.cleanupSavepointHistoryLegacy(getResourceContext(deployment), Set.of());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp1), spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.emptyList(), flinkService.getDisposedSavepoints());

        var sp2 =
                new Savepoint(
                        2, "sp2", SnapshotTriggerType.MANUAL, SavepointFormatType.CANONICAL, 123L);
        spInfo.updateLastSavepoint(sp2);
        observer.cleanupSavepointHistoryLegacy(getResourceContext(deployment), Set.of());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp2), spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp1.getLocation()), flinkService.getDisposedSavepoints());

        configManager.updateDefaultConfig(new Configuration());
    }

    @Test
    public void testDisabledDispose() {
        var deployment = TestUtils.buildApplicationCluster();
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        Configuration conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED, false);
        conf.set(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_CLEANUP_ENABLED, false);
        conf.set(KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT, 1000);
        conf.set(
                KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                Duration.ofDays(100L));

        configManager.updateDefaultConfig(conf);

        var spInfo = deployment.getStatus().getJobStatus().getSavepointInfo();

        Savepoint sp1 =
                new Savepoint(
                        9999999999999998L,
                        "sp1",
                        SnapshotTriggerType.MANUAL,
                        SavepointFormatType.CANONICAL,
                        123L);
        spInfo.updateLastSavepoint(sp1);
        observer.cleanupSavepointHistoryLegacy(getResourceContext(deployment), Set.of());

        Savepoint sp2 =
                new Savepoint(
                        9999999999999999L,
                        "sp2",
                        SnapshotTriggerType.MANUAL,
                        SavepointFormatType.CANONICAL,
                        123L);
        spInfo.updateLastSavepoint(sp2);
        observer.cleanupSavepointHistoryLegacy(getResourceContext(deployment), Set.of());
        Assertions.assertIterableEquals(List.of(sp1, sp2), spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.emptyList(), flinkService.getDisposedSavepoints());
    }

    @Test
    public void testPeriodicSavepoint() throws Exception {
        var conf = new Configuration();
        conf.set(KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED, false);
        configManager.updateDefaultConfig(conf);

        var deployment = TestUtils.buildApplicationCluster();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        status.getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        jobStatus.setState(JobStatus.RUNNING);

        var savepointInfo = jobStatus.getSavepointInfo();
        flinkService.triggerSavepointLegacy(null, SnapshotTriggerType.PERIODIC, deployment, conf);

        var triggerTs = savepointInfo.getTriggerTimestamp();
        assertEquals(0L, savepointInfo.getLastPeriodicSavepointTimestamp());
        assertEquals(SnapshotTriggerType.PERIODIC, savepointInfo.getTriggerType());
        assertTrue(SnapshotUtils.savepointInProgress(jobStatus));
        assertEquals(
                SnapshotStatus.PENDING, SnapshotUtils.getLastSnapshotStatus(deployment, SAVEPOINT));
        assertTrue(triggerTs > 0);

        // Pending
        observer.observeSavepointStatus(getResourceContext(deployment));
        // Completed
        observer.observeSavepointStatus(getResourceContext(deployment));
        assertEquals(triggerTs, savepointInfo.getLastPeriodicSavepointTimestamp());
        assertFalse(SnapshotUtils.savepointInProgress(jobStatus));
        assertEquals(
                SnapshotUtils.getLastSnapshotStatus(deployment, SAVEPOINT),
                SnapshotStatus.SUCCEEDED);
        assertEquals(savepointInfo.getLastSavepoint(), savepointInfo.getSavepointHistory().get(0));
        assertEquals(
                SnapshotTriggerType.PERIODIC, savepointInfo.getLastSavepoint().getTriggerType());
        assertNull(savepointInfo.getLastSavepoint().getTriggerNonce());
    }

    @Test
    public void testPeriodicCheckpoint() {
        var conf = new Configuration();
        var deployment = TestUtils.buildApplicationCluster();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        status.getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        jobStatus.setState(JobStatus.RUNNING);

        var checkpointInfo = jobStatus.getCheckpointInfo();
        var triggerId = flinkService.triggerCheckpoint(null, CheckpointType.FULL, conf);
        checkpointInfo.setTrigger(
                triggerId,
                SnapshotTriggerType.PERIODIC,
                org.apache.flink.kubernetes.operator.api.status.CheckpointType.FULL);

        var triggerTs = checkpointInfo.getTriggerTimestamp();
        assertEquals(0L, checkpointInfo.getLastPeriodicTriggerTimestamp());
        assertTrue(SnapshotUtils.checkpointInProgress(jobStatus));
        assertEquals(
                SnapshotStatus.PENDING,
                SnapshotUtils.getLastSnapshotStatus(deployment, CHECKPOINT));
        assertTrue(triggerTs > 0);

        // Pending
        observer.observeCheckpointStatus(getResourceContext(deployment));
        // Completed
        observer.observeCheckpointStatus(getResourceContext(deployment));
        assertEquals(triggerTs, checkpointInfo.getLastPeriodicTriggerTimestamp());
        assertFalse(SnapshotUtils.checkpointInProgress(jobStatus));
        assertEquals(
                SnapshotUtils.getLastSnapshotStatus(deployment, CHECKPOINT),
                SnapshotStatus.SUCCEEDED);
        assertEquals(
                SnapshotTriggerType.PERIODIC, checkpointInfo.getLastCheckpoint().getTriggerType());
        assertNull(checkpointInfo.getLastCheckpoint().getTriggerNonce());
    }
}
