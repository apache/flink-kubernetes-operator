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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.status.CheckpointType;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.metrics.TestingMetricListener;
import org.apache.flink.kubernetes.operator.observer.snapshot.StateSnapshotObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;
import org.apache.flink.kubernetes.operator.reconciler.snapshot.StateSnapshotReconciler;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.flink.api.common.JobStatus.CANCELED;
import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.kubernetes.operator.api.CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_KIND;
import static org.apache.flink.kubernetes.operator.api.CrdConstants.LABEL_SNAPSHOT_JOB_REFERENCE_NAME;
import static org.apache.flink.kubernetes.operator.api.CrdConstants.LABEL_SNAPSHOT_STATE;
import static org.apache.flink.kubernetes.operator.api.CrdConstants.LABEL_SNAPSHOT_TRIGGER_TYPE;
import static org.apache.flink.kubernetes.operator.api.CrdConstants.LABEL_SNAPSHOT_TYPE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.ABANDONED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.COMPLETED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.FAILED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.IN_PROGRESS;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.TRIGGER_PENDING;
import static org.apache.flink.kubernetes.operator.metrics.FlinkStateSnapshotMetricsUtils.assertSnapshotMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test class for {@link FlinkStateSnapshotController}. */
@EnableKubernetesMockClient(crud = true)
public class FlinkStateSnapshotControllerTest {
    private static final String SAVEPOINT_NAME = "savepoint-test";
    private static final String CHECKPOINT_NAME = "checkpoint-test";
    private static final String SAVEPOINT_PATH = "/tmp/asd";
    private static final String JOB_ID = "fd72014d4c864993a2e5a9287b4a9c5d";

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private final StatusUpdateCounter statusUpdateCounter = new StatusUpdateCounter();
    private final TestingFlinkService flinkService = new TestingFlinkService();
    private KubernetesClient client;
    private FlinkStateSnapshotEventCollector flinkStateSnapshotEventCollector;
    private EventRecorder eventRecorder;
    private TestingFlinkResourceContextFactory ctxFactory;
    private TestingMetricListener listener;
    private MetricManager<FlinkStateSnapshot> metricManager;
    private StatusRecorder<FlinkStateSnapshot, FlinkStateSnapshotStatus> statusRecorder;
    private FlinkStateSnapshotController controller;
    private Context<FlinkStateSnapshot> context;

    @BeforeEach
    public void beforeEach() {
        flinkStateSnapshotEventCollector = new FlinkStateSnapshotEventCollector();
        eventRecorder =
                new EventRecorder(
                        new FlinkResourceEventCollector(), flinkStateSnapshotEventCollector);
        ctxFactory =
                new TestingFlinkResourceContextFactory(
                        configManager,
                        TestUtils.createTestMetricGroup(new Configuration()),
                        flinkService,
                        eventRecorder);

        listener = new TestingMetricListener(new Configuration());
        metricManager =
                MetricManager.createFlinkStateSnapshotMetricManager(
                        new Configuration(), listener.getMetricGroup());
        statusRecorder = new StatusRecorder<>(metricManager, statusUpdateCounter);
        controller =
                new FlinkStateSnapshotController(
                        ValidatorUtils.discoverValidators(configManager),
                        ctxFactory,
                        new StateSnapshotReconciler(ctxFactory, eventRecorder),
                        new StateSnapshotObserver(ctxFactory, eventRecorder),
                        eventRecorder,
                        metricManager,
                        statusRecorder);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3, 7})
    public void testReconcileBackoff(int backoffLimit) {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, deployment);
        var snapshot = createSavepoint(deployment, false, backoffLimit);
        snapshot.setStatus(new FlinkStateSnapshotStatus());

        flinkService.setTriggerSavepointFailure(true);

        for (int i = 0; i < backoffLimit; i++) {
            controller.updateErrorStatus(snapshot, context, new Exception());
            assertThat(snapshot.getStatus().getState()).isEqualTo(TRIGGER_PENDING);
        }

        controller.updateErrorStatus(snapshot, context, new Exception());
        assertThat(snapshot.getStatus().getState()).isEqualTo(FAILED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReconcileSavepointAlreadyExists(boolean jobReferenced) {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, jobReferenced ? deployment : null);
        var snapshot = createSavepoint(jobReferenced ? deployment : null, true);

        controller.reconcile(snapshot, context);

        var status = snapshot.getStatus();
        var createdAt = Instant.parse(snapshot.getMetadata().getCreationTimestamp());
        var resultAt = Instant.parse(status.getResultTimestamp());
        assertThat(resultAt).isAfter(createdAt);
        assertThat(status.getPath()).isEqualTo(SAVEPOINT_PATH);
        assertThat(status.getTriggerId()).isNull();
        assertThat(status.getError()).isNull();

        assertThat(statusUpdateCounter.getCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(SnapshotType.class)
    public void testReconcileLabels(SnapshotType snapshotType) {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, null);
        FlinkStateSnapshot snapshot;
        if (snapshotType == SnapshotType.SAVEPOINT) {
            snapshot = createSavepoint(deployment);
        } else {
            snapshot = createCheckpoint(deployment, CheckpointType.FULL, 0);
        }

        // First we have empty secondary resource, update labels but not status
        assertThat(snapshot.getMetadata().getLabels()).isEmpty();
        assertUpdateControl(controller.reconcile(snapshot, context), true, false);
        assertLabels(snapshot, null, snapshotType, SnapshotTriggerType.MANUAL, TRIGGER_PENDING);

        // Correct secondary resource, update status to IN_PROGRESS, update labels too
        context = TestUtils.createSnapshotContext(client, deployment);
        assertUpdateControl(controller.reconcile(snapshot, context), true, true);
        assertLabels(snapshot, deployment, snapshotType, SnapshotTriggerType.MANUAL, IN_PROGRESS);

        // No update to status or labels
        assertUpdateControl(controller.reconcile(snapshot, context), false, false);
        assertLabels(snapshot, deployment, snapshotType, SnapshotTriggerType.MANUAL, IN_PROGRESS);

        // Update to both status and labels
        assertUpdateControl(controller.reconcile(snapshot, context), true, true);
        assertLabels(snapshot, deployment, snapshotType, SnapshotTriggerType.MANUAL, COMPLETED);

        // Try to manually modify label
        snapshot.getMetadata().getLabels().put(LABEL_SNAPSHOT_TYPE, "custom-value");
        assertUpdateControl(controller.reconcile(snapshot, context), true, false);
        assertLabels(snapshot, deployment, snapshotType, SnapshotTriggerType.MANUAL, COMPLETED);
    }

    private void assertLabels(
            FlinkStateSnapshot snapshot,
            @Nullable AbstractFlinkResource<?, ?> secondaryResource,
            SnapshotType snapshotType,
            SnapshotTriggerType snapshotTriggerType,
            FlinkStateSnapshotStatus.State state) {
        assertThat(snapshot.getMetadata().getLabels().get(LABEL_SNAPSHOT_TYPE))
                .isEqualTo(snapshotType.name());
        assertThat(snapshot.getMetadata().getLabels().get(LABEL_SNAPSHOT_TRIGGER_TYPE))
                .isEqualTo(snapshotTriggerType.name());
        assertThat(snapshot.getMetadata().getLabels().get(LABEL_SNAPSHOT_STATE))
                .isEqualTo(state.name());
        if (secondaryResource == null) {
            assertThat(snapshot.getMetadata().getLabels())
                    .doesNotContainKey(LABEL_SNAPSHOT_JOB_REFERENCE_KIND);
            assertThat(snapshot.getMetadata().getLabels())
                    .doesNotContainKey(LABEL_SNAPSHOT_JOB_REFERENCE_NAME);
        } else {
            assertThat(snapshot.getMetadata().getLabels().get(LABEL_SNAPSHOT_JOB_REFERENCE_KIND))
                    .isEqualTo(secondaryResource.getKind());
            assertThat(snapshot.getMetadata().getLabels().get(LABEL_SNAPSHOT_JOB_REFERENCE_NAME))
                    .isEqualTo(secondaryResource.getMetadata().getName());
        }
    }

    private void assertUpdateControl(
            UpdateControl<FlinkStateSnapshot> actual, boolean updateResource, boolean patchStatus) {
        assertThat(actual.isUpdateResource()).isEqualTo(updateResource);
        assertThat(actual.isPatchStatus()).isEqualTo(patchStatus);
    }

    @Test
    public void testReconcileSnapshotDeploymentDoesNotExist() {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, null);
        var snapshot = createSavepoint(deployment);
        controller.reconcile(snapshot, context);

        assertThat(snapshot.getStatus().getState()).isEqualTo(TRIGGER_PENDING);

        assertThat(flinkStateSnapshotEventCollector.events)
                .hasSize(1)
                .allSatisfy(
                        e -> {
                            assertThat(e.getReason())
                                    .isEqualTo(EventRecorder.Reason.ValidationError.name());
                            assertThat(e.getType()).isEqualTo(EventRecorder.Type.Warning.name());
                            assertThat(e.getMessage()).contains("was not found");
                        });
    }

    @Test
    public void testReconcileSnapshotAbandoned() {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, deployment);
        var snapshot = createSavepoint(deployment);

        controller.reconcile(snapshot, context);
        assertThat(snapshot.getStatus().getState()).isEqualTo(IN_PROGRESS);

        deployment.getStatus().getJobStatus().setState(CANCELED);
        controller.reconcile(snapshot, context);
        var status = snapshot.getStatus();
        var createdAt = Instant.parse(snapshot.getMetadata().getCreationTimestamp());
        var triggerAt = Instant.parse(status.getTriggerTimestamp());
        assertThat(triggerAt).isAfter(createdAt);
        assertThat(status.getPath()).isNull();
        assertThat(status.getTriggerId()).isEqualTo("savepoint_trigger_0");
        assertThat(status.getState()).isEqualTo(ABANDONED);
        assertThat(statusUpdateCounter.getCount()).isEqualTo(2);
    }

    @Test
    public void testReconcileNewSavepoint() {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, deployment);
        var snapshot = createSavepoint(deployment);

        controller.reconcile(snapshot, context);

        var status = snapshot.getStatus();
        var createdAt = Instant.parse(snapshot.getMetadata().getCreationTimestamp());
        var triggerAt = Instant.parse(status.getTriggerTimestamp());
        assertThat(triggerAt).isAfter(createdAt);
        assertThat(status.getPath()).isNull();
        assertThat(status.getError()).isNull();
        assertThat(status.getTriggerId()).isEqualTo("savepoint_trigger_0");
        assertThat(status.getState()).isEqualTo(IN_PROGRESS);
        assertThat(snapshot.getMetadata().getLabels().get(LABEL_SNAPSHOT_TRIGGER_TYPE))
                .isEqualTo(SnapshotTriggerType.MANUAL.name());
        assertThat(statusUpdateCounter.getCount()).isEqualTo(1);

        // First time check will still result in pending due to TestingFlinkService impl
        controller.reconcile(snapshot, context);
        assertThat(status.getState()).isEqualTo(IN_PROGRESS);

        // Second time check complete
        controller.reconcile(snapshot, context);
        status = snapshot.getStatus();
        assertThat(status.getState()).isEqualTo(COMPLETED);
        assertThat(status.getPath()).isEqualTo("savepoint_0");
        assertThat(status.getError()).isNull();
        assertThat(statusUpdateCounter.getCount()).isEqualTo(2);
    }

    @Test
    public void testReconcileSavepointCleanup() {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, deployment);
        var snapshot = createSavepoint(deployment);
        snapshot.setStatus(new FlinkStateSnapshotStatus());

        snapshot.getSpec().getSavepoint().setDisposeOnDelete(true);
        snapshot.getStatus().setState(TRIGGER_PENDING);
        assertDeleteControl(controller.cleanup(snapshot, context), true, null);
        assertThat(flinkService.getDisposedSavepoints()).isEmpty();

        snapshot.getSpec().getSavepoint().setDisposeOnDelete(true);
        snapshot.getStatus().setState(FAILED);
        assertDeleteControl(controller.cleanup(snapshot, context), true, null);
        assertThat(flinkService.getDisposedSavepoints()).isEmpty();

        snapshot.getSpec().getSavepoint().setDisposeOnDelete(true);
        snapshot.getStatus().setState(ABANDONED);
        assertDeleteControl(controller.cleanup(snapshot, context), true, null);
        assertThat(flinkService.getDisposedSavepoints()).isEmpty();

        snapshot.getSpec().getSavepoint().setDisposeOnDelete(true);
        snapshot.getStatus().setState(IN_PROGRESS);
        assertDeleteControl(
                controller.cleanup(snapshot, context),
                false,
                configManager.getOperatorConfiguration().getReconcileInterval().toMillis());
        assertThat(flinkService.getDisposedSavepoints()).isEmpty();

        // No disposal requested
        snapshot.getSpec().getSavepoint().setDisposeOnDelete(false);
        snapshot.getStatus().setState(COMPLETED);
        assertDeleteControl(controller.cleanup(snapshot, context), true, null);
        assertThat(flinkService.getDisposedSavepoints()).isEmpty();

        snapshot.getStatus().setPath(SAVEPOINT_PATH);
        snapshot.getStatus().setState(COMPLETED);

        // Failed dispose, job not found
        snapshot.getSpec().getSavepoint().setDisposeOnDelete(true);
        assertDeleteControl(
                controller.cleanup(snapshot, TestUtils.createSnapshotContext(client, null)),
                false,
                configManager.getOperatorConfiguration().getReconcileInterval().toMillis());
        assertThat(flinkService.getDisposedSavepoints()).isEmpty();

        // Failed dispose, job not running
        deployment.getStatus().getJobStatus().setState(CANCELED);
        snapshot.getSpec().getSavepoint().setDisposeOnDelete(true);
        assertDeleteControl(
                controller.cleanup(snapshot, context),
                false,
                configManager.getOperatorConfiguration().getReconcileInterval().toMillis());
        assertThat(flinkService.getDisposedSavepoints()).isEmpty();
        deployment.getStatus().getJobStatus().setState(RUNNING);

        // Failed dispose, REST error
        snapshot.getSpec().getSavepoint().setDisposeOnDelete(true);
        flinkService.setDisposeSavepointFailure(true);
        assertDeleteControl(
                controller.cleanup(snapshot, TestUtils.createSnapshotContext(client, null)),
                false,
                configManager.getOperatorConfiguration().getReconcileInterval().toMillis());
        assertThat(flinkService.getDisposedSavepoints()).isEmpty();
        flinkService.setDisposeSavepointFailure(false);

        // Successful disposal
        snapshot.getSpec().getSavepoint().setDisposeOnDelete(true);
        assertDeleteControl(controller.cleanup(snapshot, context), true, null);
        assertThat(flinkService.getDisposedSavepoints())
                .hasSize(1)
                .allSatisfy(s -> assertThat(s).isEqualTo(SAVEPOINT_PATH));
    }

    private void assertDeleteControl(
            DeleteControl deleteControl, boolean removeFinalizer, @Nullable Long scheduleDelay) {
        assertThat(deleteControl)
                .satisfies(
                        c -> {
                            assertThat(c.isRemoveFinalizer()).isEqualTo(removeFinalizer);
                            assertThat(c.getScheduleDelay())
                                    .isEqualTo(Optional.ofNullable(scheduleDelay));
                        });
    }

    @Test
    public void testReconcileNewSavepointNoPath() {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, deployment);
        var snapshot = createSavepoint(deployment);
        snapshot.getSpec().getSavepoint().setPath(null);

        var ex =
                assertThrows(
                        ReconciliationException.class,
                        () -> controller.reconcile(snapshot, context));
        controller.updateErrorStatus(snapshot, context, ex);
        assertThat(snapshot.getStatus().getState()).isEqualTo(TRIGGER_PENDING);
        assertThat(snapshot.getStatus().getPath()).isNull();
        assertThat(snapshot.getStatus().getError()).contains("savepoint path");

        // Add path to spec, it should work then
        snapshot.getSpec().getSavepoint().setPath(SAVEPOINT_PATH);
        controller.reconcile(snapshot, context);
        assertThat(snapshot.getStatus().getState()).isEqualTo(IN_PROGRESS);

        assertThat(flinkStateSnapshotEventCollector.events)
                .hasSize(1)
                .allSatisfy(
                        event -> {
                            assertThat(event.getReason())
                                    .isEqualTo(EventRecorder.Reason.SavepointError.name());
                            assertThat(event.getType())
                                    .isEqualTo(EventRecorder.Type.Warning.name());
                            assertThat(event.getMessage())
                                    .isEqualTo(snapshot.getStatus().getError());
                        });
    }

    @Test
    public void testReconcileNewCheckpoint() {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, deployment);
        var checkpointType = CheckpointType.FULL;
        var snapshot = createCheckpoint(deployment, checkpointType, 1);

        controller.reconcile(snapshot, context);
        var status = snapshot.getStatus();
        var createdAt = Instant.parse(snapshot.getMetadata().getCreationTimestamp());
        var triggerAt = Instant.parse(status.getTriggerTimestamp());
        assertThat(triggerAt).isAfter(createdAt);
        assertThat(status.getPath()).isNull();
        assertThat(status.getError()).isNull();
        assertThat(status.getTriggerId()).isEqualTo("checkpoint_trigger_0");
        assertThat(status.getState()).isEqualTo(IN_PROGRESS);
        assertThat(statusUpdateCounter.getCount()).isEqualTo(1);

        // First time check will still result in pending due to TestingFlinkService impl
        controller.reconcile(snapshot, context);
        assertThat(status.getState()).isEqualTo(IN_PROGRESS);

        // Second time check complete
        controller.reconcile(snapshot, context);
        status = snapshot.getStatus();
        assertThat(status.getState()).isEqualTo(COMPLETED);
        assertThat(status.getPath()).isEqualTo("checkpoint_1");
        assertThat(statusUpdateCounter.getCount()).isEqualTo(2);
    }

    @Test
    public void testReconcileNewCheckpointUnsupportedFlinkVersion() {
        var deployment = createDeployment(FlinkVersion.v1_16);
        context = TestUtils.createSnapshotContext(client, deployment);
        var checkpointType = CheckpointType.FULL;
        var snapshot = createCheckpoint(deployment, checkpointType, 0);

        var ex =
                assertThrows(
                        ReconciliationException.class,
                        () -> controller.reconcile(snapshot, context));
        controller.updateErrorStatus(snapshot, context, ex);

        var status = snapshot.getStatus();
        assertThat(status.getState()).isEqualTo(FAILED);
        assertThat(status.getPath()).isNull();
        assertThat(status.getFailures()).isEqualTo(1);
        assertThat(status.getError()).contains("requires Flink 1.17+");
    }

    @Test
    public void testReconcileSavepointError() {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, deployment);
        var snapshot = createSavepoint(deployment, false, 1);

        controller.reconcile(snapshot, context);

        // Remove savepoint triggers so that fetching the savepoint will result in an error
        flinkService.getSavepointTriggers().clear();
        var ex =
                assertThrows(
                        ReconciliationException.class,
                        () -> controller.reconcile(snapshot, context));
        controller.updateErrorStatus(snapshot, context, ex);

        // Backoff limit not reached, we retry in the next reconcile loop
        var status = snapshot.getStatus();
        var createdAt = Instant.parse(snapshot.getMetadata().getCreationTimestamp());
        var triggerAt = Instant.parse(status.getTriggerTimestamp());
        assertThat(triggerAt).isAfter(createdAt);
        assertThat(status.getState()).isEqualTo(TRIGGER_PENDING);
        assertThat(status.getError()).contains(TestingFlinkService.SNAPSHOT_ERROR_MESSAGE);

        controller.reconcile(snapshot, context);
        assertThat(snapshot.getStatus().getState()).isEqualTo(IN_PROGRESS);
        flinkService.getSavepointTriggers().clear();

        // Backoff limit reached, we have FAILED state
        ex =
                assertThrows(
                        ReconciliationException.class,
                        () -> controller.reconcile(snapshot, context));
        controller.updateErrorStatus(snapshot, context, ex);
        status = snapshot.getStatus();
        createdAt = Instant.parse(snapshot.getMetadata().getCreationTimestamp());
        triggerAt = Instant.parse(status.getTriggerTimestamp());

        assertThat(triggerAt).isAfter(createdAt);
        assertThat(status.getState()).isEqualTo(FAILED);
        assertThat(status.getPath()).isNull();
        assertThat(status.getFailures()).isEqualTo(2);
        assertThat(status.getError()).contains(TestingFlinkService.SNAPSHOT_ERROR_MESSAGE);

        assertThat(statusUpdateCounter.getCount()).isEqualTo(4);
        assertThat(flinkStateSnapshotEventCollector.events)
                .hasSize(2)
                .allSatisfy(
                        event -> {
                            assertThat(event.getReason())
                                    .isEqualTo(EventRecorder.Reason.SavepointError.name());
                            assertThat(event.getType())
                                    .isEqualTo(EventRecorder.Type.Warning.name());
                            assertThat(event.getMessage())
                                    .contains(TestingFlinkService.SNAPSHOT_ERROR_MESSAGE);
                        });
    }

    @Test
    public void testReconcileCheckpointError() {
        var deployment = createDeployment();
        context = TestUtils.createSnapshotContext(client, deployment);
        var snapshot = createCheckpoint(deployment, CheckpointType.FULL, 1);

        controller.reconcile(snapshot, context);

        // Remove savepoint triggers so that fetching the savepoint will result in an error
        flinkService.getCheckpointTriggers().clear();
        var ex =
                assertThrows(
                        ReconciliationException.class,
                        () -> controller.reconcile(snapshot, context));
        controller.updateErrorStatus(snapshot, context, ex);

        // Backoff limit not reached, we retry in the next reconcile loop
        var status = snapshot.getStatus();
        var createdAt = Instant.parse(snapshot.getMetadata().getCreationTimestamp());
        var triggerAt = Instant.parse(status.getTriggerTimestamp());
        assertThat(triggerAt).isAfter(createdAt);
        assertThat(status.getState()).isEqualTo(TRIGGER_PENDING);
        assertThat(status.getError()).contains(TestingFlinkService.SNAPSHOT_ERROR_MESSAGE);

        controller.reconcile(snapshot, context);
        assertThat(snapshot.getStatus().getState()).isEqualTo(IN_PROGRESS);
        flinkService.getCheckpointTriggers().clear();

        // Backoff limit reached, we have FAILED state
        ex =
                assertThrows(
                        ReconciliationException.class,
                        () -> controller.reconcile(snapshot, context));
        controller.updateErrorStatus(snapshot, context, ex);

        status = snapshot.getStatus();
        createdAt = Instant.parse(snapshot.getMetadata().getCreationTimestamp());
        triggerAt = Instant.parse(status.getTriggerTimestamp());

        assertThat(triggerAt).isAfter(createdAt);
        assertThat(status.getState()).isEqualTo(FAILED);
        assertThat(status.getPath()).isNull();
        assertThat(status.getFailures()).isEqualTo(2);
        assertThat(status.getError()).contains(TestingFlinkService.SNAPSHOT_ERROR_MESSAGE);

        assertThat(statusUpdateCounter.getCount()).isEqualTo(4);
        assertThat(flinkStateSnapshotEventCollector.events)
                .hasSize(2)
                .allSatisfy(
                        event -> {
                            assertThat(event.getReason())
                                    .isEqualTo(EventRecorder.Reason.CheckpointError.name());
                            assertThat(event.getType())
                                    .isEqualTo(EventRecorder.Type.Warning.name());
                            assertThat(event.getMessage())
                                    .contains(TestingFlinkService.SNAPSHOT_ERROR_MESSAGE);
                        });
    }

    @Test
    public void testReconcileJobNotFound() {
        var deployment = createDeployment();
        var snapshot = createSavepoint(deployment);
        var errorMessage =
                String.format(
                        "Secondary resource %s (%s) for savepoint %s was not found",
                        deployment.getMetadata().getName(),
                        CrdConstants.KIND_FLINK_DEPLOYMENT,
                        SAVEPOINT_NAME);

        // First reconcile will trigger the snapshot.
        controller.reconcile(snapshot, TestUtils.createSnapshotContext(client, deployment));

        var status = snapshot.getStatus();
        assertThat(status.getState()).isEqualTo(IN_PROGRESS);
        assertThat(status.getPath()).isNull();
        assertThat(status.getError()).isNull();

        // Second reconcile will abandon the snapshot, as secondary resource won't be found in
        // observe phase.
        controller.reconcile(snapshot, TestUtils.createSnapshotContext(client, null));

        status = snapshot.getStatus();
        assertThat(status.getState()).isEqualTo(ABANDONED);
        assertThat(status.getPath()).isNull();
        assertThat(status.getError()).isEqualTo(errorMessage);

        // observe phase triggers event for snapshot abandoned, then validation will also trigger an
        // event.
        assertThat(flinkStateSnapshotEventCollector.events).hasSize(1);
        assertThat(flinkStateSnapshotEventCollector.events.get(0))
                .satisfies(
                        event -> {
                            assertThat(event.getReason())
                                    .isEqualTo(EventRecorder.Reason.SnapshotAbandoned.name());
                            assertThat(event.getType())
                                    .isEqualTo(EventRecorder.Type.Warning.name());
                            assertThat(event.getMessage()).isEqualTo(errorMessage);
                        });
    }

    @Test
    public void testReconcileJobNotRunning() {
        var deployment = createDeployment();
        deployment.getStatus().getJobStatus().setState(CANCELED);
        context = TestUtils.createSnapshotContext(client, deployment);
        var snapshot = createSavepoint(deployment);
        var errorMessage =
                String.format(
                        "Secondary resource %s (%s) for savepoint %s is not running",
                        deployment.getMetadata().getName(),
                        CrdConstants.KIND_FLINK_DEPLOYMENT,
                        SAVEPOINT_NAME);

        controller.reconcile(snapshot, context);

        var status = snapshot.getStatus();
        assertThat(status.getState()).isEqualTo(ABANDONED);
        assertThat(status.getPath()).isNull();
        assertThat(status.getError()).isEqualTo(errorMessage);
        assertThat(status.getTriggerId()).isNull();

        assertThat(flinkStateSnapshotEventCollector.events)
                .hasSize(1)
                .allSatisfy(
                        event -> {
                            assertThat(event.getReason())
                                    .isEqualTo(EventRecorder.Reason.SnapshotAbandoned.name());
                            assertThat(event.getType())
                                    .isEqualTo(EventRecorder.Type.Warning.name());
                            assertThat(event.getMessage()).isEqualTo(errorMessage);
                        });
    }

    @Test
    public void testMetrics() {
        var deployment = createDeployment();
        var savepoint = createSavepoint(deployment);
        savepoint.getSpec().getSavepoint().setDisposeOnDelete(false);
        var checkpoint = createCheckpoint(deployment, CheckpointType.FULL, 1);

        context = TestUtils.createSnapshotContext(client, deployment);

        controller.reconcile(savepoint, context);
        controller.reconcile(savepoint, context);
        controller.reconcile(savepoint, context);
        assertThat(savepoint.getStatus().getState()).isEqualTo(COMPLETED);

        controller.reconcile(checkpoint, context);
        controller.reconcile(checkpoint, context);
        controller.reconcile(checkpoint, context);
        assertThat(checkpoint.getStatus().getState()).isEqualTo(COMPLETED);

        assertSnapshotMetrics(
                listener, TestUtils.TEST_NAMESPACE, Map.of(COMPLETED, 1), Map.of(COMPLETED, 1));

        // Remove savepoint
        assertDeleteControl(controller.cleanup(savepoint, context), true, null);
        assertSnapshotMetrics(listener, TestUtils.TEST_NAMESPACE, Map.of(), Map.of(COMPLETED, 1));

        // Remove checkpoint
        assertDeleteControl(controller.cleanup(checkpoint, context), true, null);
        assertSnapshotMetrics(listener, TestUtils.TEST_NAMESPACE, Map.of(), Map.of());
    }

    private FlinkStateSnapshot createSavepoint(FlinkDeployment deployment) {
        return createSavepoint(deployment, false, 7);
    }

    private FlinkStateSnapshot createSavepoint(FlinkDeployment deployment, boolean alreadyExists) {
        return createSavepoint(deployment, alreadyExists, 7);
    }

    private FlinkStateSnapshot createSavepoint(
            FlinkDeployment deployment, boolean alreadyExists, int backoffLimit) {
        var snapshot =
                TestUtils.buildFlinkStateSnapshotSavepoint(
                        SAVEPOINT_NAME,
                        TestUtils.TEST_NAMESPACE,
                        SAVEPOINT_PATH,
                        alreadyExists,
                        deployment == null ? null : JobReference.fromFlinkResource(deployment));
        snapshot.getSpec().setBackoffLimit(backoffLimit);
        snapshot.getSpec().getSavepoint().setFormatType(SavepointFormatType.CANONICAL);
        client.resource(snapshot).create();
        return snapshot;
    }

    private FlinkStateSnapshot createCheckpoint(
            FlinkDeployment deployment, CheckpointType checkpointType, int backoffLimit) {
        var snapshot =
                TestUtils.buildFlinkStateSnapshotCheckpoint(
                        CHECKPOINT_NAME,
                        TestUtils.TEST_NAMESPACE,
                        checkpointType,
                        JobReference.fromFlinkResource(deployment));
        snapshot.getSpec().setBackoffLimit(backoffLimit);
        client.resource(snapshot).create();
        return snapshot;
    }

    private FlinkDeployment createDeployment() {
        return createDeployment(FlinkVersion.v1_20);
    }

    private FlinkDeployment createDeployment(FlinkVersion flinkVersion) {
        var deployment = TestUtils.buildApplicationCluster();
        deployment
                .getStatus()
                .setJobStatus(JobStatus.builder().state(RUNNING).jobId(JOB_ID).build());
        deployment.getSpec().setFlinkVersion(flinkVersion);
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .remove(CheckpointingOptions.SAVEPOINT_DIRECTORY.key());
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        client.resource(deployment).create();
        return deployment;
    }

    private static class StatusUpdateCounter
            implements BiConsumer<FlinkStateSnapshot, FlinkStateSnapshotStatus> {

        private int counter;

        @Override
        public void accept(FlinkStateSnapshot resource, FlinkStateSnapshotStatus prevStatus) {
            counter++;
        }

        public int getCount() {
            return counter;
        }
    }
}
