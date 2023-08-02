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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link SnapshotUtils}. */
public class SnapshotUtilsTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @Test
    public void testSavepointTriggering() {
        SnapshotType snapshotType = SnapshotType.SAVEPOINT;
        FlinkDeployment deployment = initDeployment(FlinkVersion.v1_15);
        reconcileSpec(deployment);

        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));

        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL.key(), "10m");
        reconcileSpec(deployment);

        assertEquals(
                Optional.of(SnapshotTriggerType.PERIODIC),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
        deployment.getStatus().getJobStatus().getSavepointInfo().resetTrigger();

        deployment.getSpec().getJob().setSavepointTriggerNonce(123L);
        assertEquals(
                Optional.of(SnapshotTriggerType.MANUAL),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
    }

    @Test
    public void testCheckpointTriggeringPre1_17() {
        SnapshotType snapshotType = SnapshotType.CHECKPOINT;
        FlinkDeployment deployment = initDeployment(FlinkVersion.v1_16);
        reconcileSpec(deployment);

        // -- Triggering in versions below 1.17 is not supported
        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));

        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.PERIODIC_CHECKPOINT_INTERVAL.key(), "10m");
        reconcileSpec(deployment);

        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
        deployment.getStatus().getJobStatus().getCheckpointInfo().resetTrigger();

        deployment.getSpec().getJob().setCheckpointTriggerNonce(123L);
        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
    }

    @Test
    public void testCheckpointTriggeringPost1_17() {
        SnapshotType snapshotType = SnapshotType.CHECKPOINT;
        FlinkDeployment deployment = initDeployment(FlinkVersion.v1_17);
        reconcileSpec(deployment);

        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));

        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.PERIODIC_CHECKPOINT_INTERVAL.key(), "10m");
        reconcileSpec(deployment);

        assertEquals(
                Optional.of(SnapshotTriggerType.PERIODIC),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
        deployment.getStatus().getJobStatus().getCheckpointInfo().resetTrigger();

        deployment.getSpec().getJob().setCheckpointTriggerNonce(123L);
        assertEquals(
                Optional.of(SnapshotTriggerType.MANUAL),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
    }

    private static FlinkDeployment initDeployment(FlinkVersion flinkVersion) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster(flinkVersion);
        deployment
                .getMetadata()
                .setCreationTimestamp(Instant.now().minus(Duration.ofMinutes(15)).toString());

        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        return deployment;
    }

    private static void reconcileSpec(FlinkDeployment deployment) {
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
    }
}
