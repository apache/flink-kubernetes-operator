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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.TestUtils.reconcileSpec;
import static org.apache.flink.kubernetes.operator.TestUtils.setupCronTrigger;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.PERIODIC_CHECKPOINT_INTERVAL;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.CHECKPOINT;
import static org.apache.flink.kubernetes.operator.reconciler.SnapshotType.SAVEPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link SnapshotUtils}. */
public class SnapshotUtilsTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @Test
    public void testSavepointTriggering() {
        FlinkDeployment deployment = initDeployment(FlinkVersion.v1_15);
        testSnapshotTriggering(deployment, SAVEPOINT, PERIODIC_SAVEPOINT_INTERVAL);
    }

    @Test
    public void testCheckpointTriggeringPost1_17() {
        FlinkDeployment deployment = initDeployment(FlinkVersion.v1_17);
        testSnapshotTriggering(deployment, CHECKPOINT, PERIODIC_CHECKPOINT_INTERVAL);
    }

    @Test
    public void testCheckpointTriggeringPre1_17() {
        SnapshotType snapshotType = CHECKPOINT;
        FlinkDeployment deployment = initDeployment(FlinkVersion.v1_16);
        reconcileSpec(deployment);

        // -- Triggering in versions below 1.17 is not supported
        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));

        deployment.getSpec().getFlinkConfiguration().put(PERIODIC_CHECKPOINT_INTERVAL.key(), "10m");
        reconcileSpec(deployment);

        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
        resetTrigger(deployment, snapshotType);

        setTriggerNonce(deployment, snapshotType, 123L);
        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
        resetTrigger(deployment, snapshotType);

        setupCronTrigger(snapshotType, deployment);
        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
        resetTrigger(deployment, snapshotType);
    }

    private void testSnapshotTriggering(
            FlinkDeployment deployment,
            SnapshotType snapshotType,
            ConfigOption<String> periodicSnapshotIntervalOption) {
        reconcileSpec(deployment);
        assertEquals(
                Optional.empty(),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));

        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(periodicSnapshotIntervalOption.key(), "10m");
        reconcileSpec(deployment);

        assertEquals(
                Optional.of(SnapshotTriggerType.PERIODIC),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
        resetTrigger(deployment, snapshotType);
        deployment.getSpec().getFlinkConfiguration().put(periodicSnapshotIntervalOption.key(), "0");
        reconcileSpec(deployment);

        setTriggerNonce(deployment, snapshotType, 123L);
        assertEquals(
                Optional.of(SnapshotTriggerType.MANUAL),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
        resetTrigger(deployment, snapshotType);
        reconcileSpec(deployment);

        setupCronTrigger(snapshotType, deployment);
        assertEquals(
                Optional.of(SnapshotTriggerType.PERIODIC),
                SnapshotUtils.shouldTriggerSnapshot(
                        deployment, configManager.getObserveConfig(deployment), snapshotType));
    }

    @Test
    public void testShouldTriggerCronBasedSnapshot_NextValidTimeBeforeCurrent() {
        String cronExpression = "0 */10 * * * ?"; // Every 10th minute
        Calendar calendar = Calendar.getInstance();
        calendar.set(2022, Calendar.JUNE, 5, 11, 5); // 11:05

        Instant now = calendar.getTime().toInstant();
        Instant lastTrigger =
                now.minus(Duration.ofMinutes(10)); // 10:05, should have fired at 11:00

        boolean result =
                SnapshotUtils.shouldTriggerCronBasedSnapshot(
                        CHECKPOINT, cronExpression, lastTrigger, now);

        assertTrue(result);
    }

    @Test
    public void testShouldTriggerCronBasedSnapshot_NextValidTimeAfterCurrent() {
        String cronExpression = "0 */10 * * * ?"; // Every 10th minute
        Calendar calendar = Calendar.getInstance();
        calendar.set(2022, Calendar.JUNE, 5, 11, 5);

        Instant now = calendar.getTime().toInstant(); // 11:05
        Instant lastTrigger = now.minus(Duration.ofMinutes(4)); // 11:01, next trigger at 11:10

        boolean result =
                SnapshotUtils.shouldTriggerCronBasedSnapshot(
                        CHECKPOINT, cronExpression, lastTrigger, now);

        assertFalse(result);
    }

    @Test
    public void testShouldTriggerCronBasedSnapshot_NoNextValidTime() {
        String cronExpression =
                "0 0 0 29 2 ? 1999"; // An impossible time (Feb 29, 1999 was not a leap year)

        Instant now = Instant.now();
        Instant lastTrigger = now.minus(Duration.ofDays(365));

        boolean result =
                SnapshotUtils.shouldTriggerCronBasedSnapshot(
                        CHECKPOINT, cronExpression, lastTrigger, now);

        assertFalse(result);
    }

    @Test
    public void testShouldTriggerCronBasedSnapshot_InvalidCron() {
        String cronExpression = "invalidCron";

        Instant now = Instant.now();
        Instant lastTrigger = now.minus(Duration.ofDays(365));

        boolean result =
                SnapshotUtils.shouldTriggerCronBasedSnapshot(
                        CHECKPOINT, cronExpression, lastTrigger, now);

        assertFalse(result);
    }

    private static void resetTrigger(FlinkDeployment deployment, SnapshotType snapshotType) {
        switch (snapshotType) {
            case SAVEPOINT:
                deployment.getStatus().getJobStatus().getSavepointInfo().resetTrigger();
                break;
            case CHECKPOINT:
                deployment.getStatus().getJobStatus().getCheckpointInfo().resetTrigger();
                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }
    }

    private static void setTriggerNonce(
            FlinkDeployment deployment, SnapshotType snapshotType, long nonce) {
        switch (snapshotType) {
            case SAVEPOINT:
                deployment.getSpec().getJob().setSavepointTriggerNonce(nonce);
                break;
            case CHECKPOINT:
                deployment.getSpec().getJob().setCheckpointTriggerNonce(nonce);
                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }
    }

    private static FlinkDeployment initDeployment(FlinkVersion flinkVersion) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster(flinkVersion);
        deployment
                .getMetadata()
                .setCreationTimestamp(Instant.now().minus(Duration.ofMinutes(15)).toString());

        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconcileSpec(deployment);
        return deployment;
    }
}
