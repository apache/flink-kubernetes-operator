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
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotReference;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobKind;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.status.CheckpointType;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotState;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.TestCustomResourceDefinitionWatcher;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.TestUtils.reconcileSpec;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkStateSnapshotUtils}. */
@EnableKubernetesMockClient(crud = true)
public class FlinkStateSnapshotUtilsTest {

    private KubernetesClient client;

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private static final String NAMESPACE = "test";
    private static final String SAVEPOINT_NAME = "savepoint-01";
    private static final String SAVEPOINT_PATH = "/tmp/savepoint-01";

    @Test
    public void testGetAndValidateFlinkStateSnapshotPathPathGiven() {
        var snapshotRef = FlinkStateSnapshotReference.builder().path(SAVEPOINT_PATH).build();
        var snapshotResult =
                FlinkStateSnapshotUtils.getAndValidateFlinkStateSnapshotPath(client, snapshotRef);
        assertEquals(SAVEPOINT_PATH, snapshotResult);
    }

    @Test
    public void testGetAndValidateFlinkStateSnapshotPathFoundResource() {
        var snapshot = initSnapshot(FlinkStateSnapshotState.COMPLETED, null);
        client.resource(snapshot).create();

        var snapshotRef =
                FlinkStateSnapshotReference.builder()
                        .namespace(NAMESPACE)
                        .name(SAVEPOINT_NAME)
                        .build();
        var snapshotResult =
                FlinkStateSnapshotUtils.getAndValidateFlinkStateSnapshotPath(client, snapshotRef);
        assertEquals(SAVEPOINT_PATH, snapshotResult);
    }

    @Test
    public void testGetAndValidateFlinkStateSnapshotPathInvalidName() {
        var snapshotRef =
                FlinkStateSnapshotReference.builder().namespace(NAMESPACE).name("  ").build();
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        FlinkStateSnapshotUtils.getAndValidateFlinkStateSnapshotPath(
                                client, snapshotRef));
    }

    @Test
    public void testGetAndValidateFlinkStateSnapshotPathNotFound() {
        var snapshotRef =
                FlinkStateSnapshotReference.builder()
                        .namespace("not-exists")
                        .name("not-exists")
                        .build();
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        FlinkStateSnapshotUtils.getAndValidateFlinkStateSnapshotPath(
                                client, snapshotRef));
    }

    @Test
    public void testGetAndValidateFlinkStateSnapshotPathNotCompleted() {
        var snapshot = initSnapshot(FlinkStateSnapshotState.IN_PROGRESS, null);
        client.resource(snapshot).create();

        var snapshotRef =
                FlinkStateSnapshotReference.builder()
                        .namespace(NAMESPACE)
                        .name(SAVEPOINT_NAME)
                        .build();
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        FlinkStateSnapshotUtils.getAndValidateFlinkStateSnapshotPath(
                                client, snapshotRef));
    }

    @Test
    public void testGetFlinkStateSnapshotsForResource() {
        var deployment = initDeployment();
        var snapshotCount = 100;

        List<String> snapshotNames = new ArrayList<>();
        for (int i = 0; i < snapshotCount; i++) {
            var snapshot =
                    initSnapshot(
                            FlinkStateSnapshotState.COMPLETED,
                            JobReference.fromFlinkResource(deployment));
            var name = String.format("snapshot-%d", i);
            snapshot.getMetadata().setName(name);
            client.resource(snapshot).create();
            snapshotNames.add(name);
        }

        var result = FlinkStateSnapshotUtils.getFlinkStateSnapshotsForResource(client, deployment);
        assertEquals(
                snapshotNames,
                result.stream().map(s -> s.getMetadata().getName()).collect(Collectors.toList()));
    }

    @Test
    public void testShouldCreateSnapshotResource() {
        var crdWatcher = new TestCustomResourceDefinitionWatcher(List.of(FlinkStateSnapshot.class));
        var conf = new Configuration().set(SNAPSHOT_RESOURCE_ENABLED, true);
        assertTrue(FlinkStateSnapshotUtils.shouldCreateSnapshotResource(crdWatcher, conf));

        crdWatcher = new TestCustomResourceDefinitionWatcher(List.of(FlinkStateSnapshot.class));
        conf = new Configuration().set(SNAPSHOT_RESOURCE_ENABLED, false);
        assertFalse(FlinkStateSnapshotUtils.shouldCreateSnapshotResource(crdWatcher, conf));

        crdWatcher = new TestCustomResourceDefinitionWatcher(List.of());
        conf = new Configuration().set(SNAPSHOT_RESOURCE_ENABLED, true);
        assertFalse(FlinkStateSnapshotUtils.shouldCreateSnapshotResource(crdWatcher, conf));

        crdWatcher = new TestCustomResourceDefinitionWatcher(List.of());
        conf = new Configuration().set(SNAPSHOT_RESOURCE_ENABLED, false);
        assertFalse(FlinkStateSnapshotUtils.shouldCreateSnapshotResource(crdWatcher, conf));
    }

    @Test
    public void testCreatePeriodicCheckpointResource() {
        var deployment = initDeployment();
        var checkpointType = CheckpointType.FULL;

        var snapshot =
                FlinkStateSnapshotUtils.createPeriodicCheckpointResource(
                        client, deployment, checkpointType);

        assertTrue(snapshot.getSpec().isCheckpoint());
        assertEquals(checkpointType, snapshot.getSpec().getCheckpoint().getCheckpointType());
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
                FlinkStateSnapshotUtils.createUpgradeSavepointResource(
                        client, deployment, SAVEPOINT_PATH, formatType, disposeOnDelete);
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
                FlinkStateSnapshotUtils.createPeriodicSavepointResource(
                        client, deployment, SAVEPOINT_PATH, formatType, disposeOnDelete);
        assertSavepointResource(
                snapshot,
                deployment,
                SnapshotTriggerType.PERIODIC,
                disposeOnDelete,
                formatType,
                false);
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
                snapshot.getMetadata().getLabels().get(CrdConstants.LABEL_SNAPSHOT_TYPE));
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

    private static FlinkDeployment initDeployment() {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster(FlinkVersion.v1_19);
        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconcileSpec(deployment);
        return deployment;
    }

    private static FlinkStateSnapshot initSnapshot(
            FlinkStateSnapshotState snapshotState, JobReference jobReference) {
        var snapshot =
                TestUtils.buildFlinkStateSnapshotSavepoint(
                        SAVEPOINT_NAME, NAMESPACE, SAVEPOINT_PATH, false, jobReference);
        snapshot.getStatus().setState(snapshotState);

        if (FlinkStateSnapshotState.COMPLETED.equals(snapshotState)) {
            snapshot.getStatus().setPath(SAVEPOINT_PATH);
        }

        return snapshot;
    }
}
