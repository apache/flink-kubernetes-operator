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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link SnapshotUtils}. */
public class SavepointUtilsTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @ParameterizedTest
    @EnumSource(SnapshotType.class)
    public void testTriggering(SnapshotType snapshotType) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster(FlinkVersion.v1_15);
        deployment
                .getMetadata()
                .setCreationTimestamp(Instant.now().minus(Duration.ofMinutes(15)).toString());

        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);

        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);

        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));

        deployment.getSpec().getFlinkConfiguration().put(getPeriodicConfigKey(snapshotType), "10m");
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);

        assertEquals(
                Optional.of(SnapshotTriggerType.PERIODIC),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
        deployment.getStatus().getJobStatus().getSavepointInfo().resetTrigger();

        setManualTriggerNonce(deployment, snapshotType);
        assertEquals(
                Optional.of(SnapshotTriggerType.MANUAL),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
    }

    private static void setManualTriggerNonce(
            FlinkDeployment deployment, SnapshotType snapshotType) {
        switch (snapshotType) {
            case SAVEPOINT:
                deployment.getSpec().getJob().setSavepointTriggerNonce(123L);
                break;
            case CHECKPOINT:
                deployment.getSpec().getJob().setCheckpointTriggerNonce(123L);
                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }
    }

    private static String getPeriodicConfigKey(SnapshotType snapshotType) {
        switch (snapshotType) {
            case SAVEPOINT:
                return KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL.key();
            case CHECKPOINT:
                return KubernetesOperatorConfigOptions.PERIODIC_CHECKPOINT_INTERVAL.key();
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }
    }
}
