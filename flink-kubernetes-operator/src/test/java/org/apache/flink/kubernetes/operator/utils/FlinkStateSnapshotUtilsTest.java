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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobKind;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.status.CheckpointType;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.JobStatus.FAILED;
import static org.apache.flink.kubernetes.operator.TestUtils.reconcileSpec;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.ABANDONED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.COMPLETED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.IN_PROGRESS;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_DEPLOYMENT_NAME;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_SESSION_JOB_NAME;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_JOB_SAVEPOINT_DISPOSE_ON_DELETE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkStateSnapshotUtils}. */
@EnableKubernetesMockClient(crud = true)
public class FlinkStateSnapshotUtilsTest {

    private KubernetesClient client;
    private static final String NAMESPACE = "test";
    private static final String SAVEPOINT_NAME = "savepoint-01";
    private static final String SAVEPOINT_PATH = "/tmp/savepoint-01";

    @Test
    public void testGetSnapshotTriggerType() {
        var snapshot = new FlinkStateSnapshot();

        assertThat(FlinkStateSnapshotUtils.getSnapshotTriggerType(snapshot))
                .isEqualTo(SnapshotTriggerType.UNKNOWN);

        snapshot.getMetadata().getLabels().put(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE, "");
        assertThat(FlinkStateSnapshotUtils.getSnapshotTriggerType(snapshot))
                .isEqualTo(SnapshotTriggerType.UNKNOWN);

        snapshot.getMetadata()
                .getLabels()
                .put(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE, SnapshotTriggerType.MANUAL.name());
        assertThat(FlinkStateSnapshotUtils.getSnapshotTriggerType(snapshot))
                .isEqualTo(SnapshotTriggerType.MANUAL);

        snapshot.getMetadata()
                .getLabels()
                .put(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE, SnapshotTriggerType.UPGRADE.name());
        assertThat(FlinkStateSnapshotUtils.getSnapshotTriggerType(snapshot))
                .isEqualTo(SnapshotTriggerType.UPGRADE);

        snapshot.getMetadata()
                .getLabels()
                .put(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE, SnapshotTriggerType.PERIODIC.name());
        assertThat(FlinkStateSnapshotUtils.getSnapshotTriggerType(snapshot))
                .isEqualTo(SnapshotTriggerType.PERIODIC);
    }

    @Test
    public void testGetFlinkStateSnapshotsForResource() {
        var deployment = initDeployment();
        var snapshotCount = 100;

        List<String> snapshotNames = new ArrayList<>();
        for (int i = 0; i < snapshotCount; i++) {
            var snapshot = initSavepoint(COMPLETED, JobReference.fromFlinkResource(deployment));
            var name = String.format("snapshot-%d", i);
            snapshot.getMetadata().setName(name);
            client.resource(snapshot).create();
            snapshotNames.add(name);
        }

        var result = TestUtils.getFlinkStateSnapshotsForResource(client, deployment);
        assertEquals(
                snapshotNames,
                result.stream().map(s -> s.getMetadata().getName()).collect(Collectors.toList()));
    }

    @Test
    public void testCreatePeriodicCheckpointResource() {
        var deployment = initDeployment();

        var snapshot =
                FlinkStateSnapshotUtils.createCheckpointResource(
                        client, deployment, SnapshotTriggerType.PERIODIC);

        assertTrue(snapshot.getSpec().isCheckpoint());
        assertEquals(
                deployment.getMetadata().getName(), snapshot.getSpec().getJobReference().getName());
        assertEquals(JobKind.FLINK_DEPLOYMENT, snapshot.getSpec().getJobReference().getKind());
    }

    @Test
    public void testCreateUpgradeSavepointResource() {
        var deployment = initDeployment();
        var formatType = SavepointFormatType.CANONICAL;
        var disposeOnDelete = true;

        var snapshot =
                FlinkStateSnapshotUtils.createSavepointResource(
                        client,
                        deployment,
                        SAVEPOINT_PATH,
                        SnapshotTriggerType.UPGRADE,
                        formatType,
                        disposeOnDelete);
        assertSavepointResource(
                snapshot,
                deployment,
                SnapshotTriggerType.UPGRADE,
                disposeOnDelete,
                formatType,
                true);
    }

    @Test
    public void testCreatePeriodicSavepointResource() {
        var deployment = initDeployment();
        var formatType = SavepointFormatType.CANONICAL;
        var disposeOnDelete = true;

        var snapshot =
                FlinkStateSnapshotUtils.createSavepointResource(
                        client,
                        deployment,
                        SAVEPOINT_PATH,
                        SnapshotTriggerType.PERIODIC,
                        formatType,
                        disposeOnDelete);
        assertSavepointResource(
                snapshot,
                deployment,
                SnapshotTriggerType.PERIODIC,
                disposeOnDelete,
                formatType,
                false);
    }

    @Test
    public void testCreateSnapshotInSameNamespace() {
        var namespace = "different-namespace";
        var deployment = initDeployment();
        deployment.getMetadata().setNamespace(namespace);

        var savepoint =
                FlinkStateSnapshotUtils.createSavepointResource(
                        client,
                        deployment,
                        SAVEPOINT_PATH,
                        SnapshotTriggerType.PERIODIC,
                        SavepointFormatType.CANONICAL,
                        true);
        assertThat(savepoint.getMetadata().getNamespace()).isEqualTo(namespace);

        var checkpoint =
                FlinkStateSnapshotUtils.createCheckpointResource(
                        client, deployment, SnapshotTriggerType.MANUAL);
        assertThat(checkpoint.getMetadata().getNamespace()).isEqualTo(namespace);
    }

    @Test
    public void testCreateCheckpointResource() {
        var deployment = initDeployment();

        var snapshot =
                FlinkStateSnapshotUtils.createCheckpointResource(
                        client, deployment, SnapshotTriggerType.MANUAL);
        assertCheckpointResource(snapshot, deployment, SnapshotTriggerType.MANUAL);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCreateReferenceForUpgradeSavepointWithResource(boolean disposeOnDelete) {
        var deployment = initDeployment();
        var conf = new Configuration();
        conf.set(OPERATOR_JOB_SAVEPOINT_DISPOSE_ON_DELETE, disposeOnDelete);
        var operatorConf = FlinkOperatorConfiguration.fromConfiguration(conf);
        FlinkStateSnapshotUtils.createUpgradeSnapshotResource(
                conf,
                operatorConf,
                client,
                deployment,
                SavepointFormatType.CANONICAL,
                SAVEPOINT_PATH);
        var snapshots = TestUtils.getFlinkStateSnapshotsForResource(client, deployment);
        assertThat(snapshots)
                .hasSize(1)
                .allSatisfy(
                        snapshot -> {
                            assertEquals(
                                    disposeOnDelete,
                                    snapshot.getSpec().getSavepoint().getDisposeOnDelete());
                            assertEquals(
                                    SAVEPOINT_PATH, snapshot.getSpec().getSavepoint().getPath());
                            assertTrue(snapshot.getSpec().getSavepoint().getAlreadyExists());
                        });
    }

    @Test
    public void testAbandonSnapshotIfJobNotRunningNoAbandon() {
        var deployment = initDeployment();
        var snapshot = initSavepoint(IN_PROGRESS, null);
        var eventCollector = new FlinkStateSnapshotEventCollector();
        var eventRecorder = new EventRecorder((x, y) -> {}, eventCollector);

        var result =
                FlinkStateSnapshotUtils.abandonSnapshotIfJobNotRunning(
                        client, snapshot, deployment, eventRecorder);
        assertFalse(result);
        assertThat(eventCollector.events).isEmpty();

        deployment.getStatus().getJobStatus().setState(FAILED);
        result =
                FlinkStateSnapshotUtils.abandonSnapshotIfJobNotRunning(
                        client, snapshot, deployment, eventRecorder);
        assertTrue(result);
        assertThat(eventCollector.events)
                .hasSize(1)
                .allSatisfy(
                        event -> {
                            assertEquals(event.getType(), EventRecorder.Type.Warning.name());
                            assertEquals(
                                    event.getReason(),
                                    EventRecorder.Reason.SnapshotAbandoned.name());
                        });
    }

    @Test
    public void testAbandonSnapshotIfJobNotRunningJobFailed() {
        var deployment = initDeployment();
        deployment.getStatus().getJobStatus().setState(FAILED);
        var snapshot = initSavepoint(IN_PROGRESS, null);
        var eventCollector = new FlinkStateSnapshotEventCollector();
        var eventRecorder = new EventRecorder((x, y) -> {}, eventCollector);

        var result =
                FlinkStateSnapshotUtils.abandonSnapshotIfJobNotRunning(
                        client, snapshot, deployment, eventRecorder);
        assertTrue(result);
        assertThat(eventCollector.events)
                .hasSize(1)
                .allSatisfy(
                        event -> {
                            assertEquals(event.getType(), EventRecorder.Type.Warning.name());
                            assertEquals(
                                    event.getReason(),
                                    EventRecorder.Reason.SnapshotAbandoned.name());
                        });
    }

    @Test
    public void testAbandonSnapshotIfJobNotRunningJobDeleted() {
        var snapshot = initSavepoint(IN_PROGRESS, null);
        var eventCollector = new FlinkStateSnapshotEventCollector();
        var eventRecorder = new EventRecorder((x, y) -> {}, eventCollector);

        var result =
                FlinkStateSnapshotUtils.abandonSnapshotIfJobNotRunning(
                        client, snapshot, null, eventRecorder);
        assertTrue(result);
        assertThat(eventCollector.events)
                .hasSize(1)
                .allSatisfy(
                        event -> {
                            assertEquals(event.getType(), EventRecorder.Type.Warning.name());
                            assertEquals(
                                    event.getReason(),
                                    EventRecorder.Reason.SnapshotAbandoned.name());
                        });
    }

    @Test
    public void testGetSnapshotLabels() {
        var snapshot = initSavepoint(IN_PROGRESS, null);
        assertThat(FlinkStateSnapshotUtils.getSnapshotLabels(snapshot, Optional.empty()))
                .containsExactlyInAnyOrderEntriesOf(
                        Map.ofEntries(
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_TYPE,
                                        SnapshotType.SAVEPOINT.name()),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE,
                                        SnapshotTriggerType.MANUAL.name()),
                                Map.entry(CrdConstants.LABEL_SNAPSHOT_STATE, IN_PROGRESS.name())));

        var deployment = initDeployment();
        snapshot = initCheckpoint(COMPLETED, JobReference.fromFlinkResource(deployment));
        assertThat(FlinkStateSnapshotUtils.getSnapshotLabels(snapshot, Optional.of(deployment)))
                .containsExactlyInAnyOrderEntriesOf(
                        Map.ofEntries(
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_TYPE,
                                        SnapshotType.CHECKPOINT.name()),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE,
                                        SnapshotTriggerType.MANUAL.name()),
                                Map.entry(CrdConstants.LABEL_SNAPSHOT_STATE, COMPLETED.name()),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_KIND,
                                        "FlinkDeployment"),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_NAME,
                                        TEST_DEPLOYMENT_NAME)));

        // Null status should be handled correctly
        snapshot.setStatus(null);
        assertThat(FlinkStateSnapshotUtils.getSnapshotLabels(snapshot, Optional.of(deployment)))
                .containsExactlyInAnyOrderEntriesOf(
                        Map.ofEntries(
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_TYPE,
                                        SnapshotType.CHECKPOINT.name()),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE,
                                        SnapshotTriggerType.MANUAL.name()),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_KIND,
                                        "FlinkDeployment"),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_NAME,
                                        TEST_DEPLOYMENT_NAME)));

        var sessionJob = initFlinkSessionJob();
        snapshot = initCheckpoint(ABANDONED, JobReference.fromFlinkResource(sessionJob));
        assertThat(FlinkStateSnapshotUtils.getSnapshotLabels(snapshot, Optional.of(sessionJob)))
                .containsExactlyInAnyOrderEntriesOf(
                        Map.ofEntries(
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_TYPE,
                                        SnapshotType.CHECKPOINT.name()),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE,
                                        SnapshotTriggerType.MANUAL.name()),
                                Map.entry(CrdConstants.LABEL_SNAPSHOT_STATE, ABANDONED.name()),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_KIND,
                                        "FlinkSessionJob"),
                                Map.entry(
                                        CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_NAME,
                                        TEST_SESSION_JOB_NAME)));

        // Trigger type should not be overridden
        snapshot.getMetadata()
                .getLabels()
                .put(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE, SnapshotTriggerType.UPGRADE.name());
        assertThat(FlinkStateSnapshotUtils.getSnapshotLabels(snapshot, Optional.of(deployment)))
                .containsEntry(
                        CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE,
                        SnapshotTriggerType.UPGRADE.name());
    }

    private void assertSavepointResource(
            FlinkStateSnapshot snapshot,
            FlinkDeployment deployment,
            SnapshotTriggerType triggerType,
            boolean expectedDisposeOnDelete,
            SavepointFormatType expectedFormatType,
            boolean expectedAlreadyExists) {
        assertEquals(
                triggerType.name(),
                snapshot.getMetadata().getLabels().get(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE));
        assertTrue(snapshot.getSpec().isSavepoint());
        assertEquals(SAVEPOINT_PATH, snapshot.getSpec().getSavepoint().getPath());
        assertEquals(expectedFormatType, snapshot.getSpec().getSavepoint().getFormatType());
        assertEquals(
                expectedDisposeOnDelete, snapshot.getSpec().getSavepoint().getDisposeOnDelete());
        assertEquals(expectedAlreadyExists, snapshot.getSpec().getSavepoint().getAlreadyExists());

        assertEquals(
                deployment.getMetadata().getName(), snapshot.getSpec().getJobReference().getName());
        assertEquals(JobKind.FLINK_DEPLOYMENT, snapshot.getSpec().getJobReference().getKind());
    }

    private void assertCheckpointResource(
            FlinkStateSnapshot snapshot,
            FlinkDeployment deployment,
            SnapshotTriggerType triggerType) {
        assertEquals(
                triggerType.name(),
                snapshot.getMetadata().getLabels().get(CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE));
        assertTrue(snapshot.getSpec().isCheckpoint());

        assertEquals(
                deployment.getMetadata().getName(), snapshot.getSpec().getJobReference().getName());
        assertEquals(JobKind.FLINK_DEPLOYMENT, snapshot.getSpec().getJobReference().getKind());
    }

    private static FlinkDeployment initDeployment() {
        var deployment = TestUtils.buildApplicationCluster(FlinkVersion.v1_19);
        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconcileSpec(deployment);
        return deployment;
    }

    private static FlinkSessionJob initFlinkSessionJob() {
        var sessionJob = TestUtils.buildSessionJob();
        sessionJob.getStatus().getJobStatus().setState(JobStatus.RUNNING);
        return sessionJob;
    }

    private static FlinkStateSnapshot initSavepoint(
            FlinkStateSnapshotStatus.State snapshotState, JobReference jobReference) {
        var snapshot =
                TestUtils.buildFlinkStateSnapshotSavepoint(
                        SAVEPOINT_NAME, NAMESPACE, SAVEPOINT_PATH, false, jobReference);
        snapshot.setStatus(FlinkStateSnapshotStatus.builder().state(snapshotState).build());

        if (COMPLETED.equals(snapshotState)) {
            snapshot.getStatus().setPath(SAVEPOINT_PATH);
        }

        return snapshot;
    }

    private static FlinkStateSnapshot initCheckpoint(
            FlinkStateSnapshotStatus.State snapshotState, JobReference jobReference) {
        var snapshot =
                TestUtils.buildFlinkStateSnapshotCheckpoint(
                        SAVEPOINT_NAME, NAMESPACE, CheckpointType.FULL, jobReference);
        snapshot.setStatus(FlinkStateSnapshotStatus.builder().state(snapshotState).build());

        return snapshot;
    }
}
