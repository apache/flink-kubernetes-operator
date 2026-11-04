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

package org.apache.flink.kubernetes.operator.api.status;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkMiniClusterStatus}. */
class FlinkMiniClusterStatusTest {

    @Test
    void testNewStatusIsMissing() {
        var status = new FlinkMiniClusterStatus();
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.MISSING);
        assertThat(status.getKubernetesDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.MISSING);
        assertThat(status.getLifecycleState()).isEqualTo(ResourceLifecycleState.CREATED);
    }

    @ParameterizedTest
    @EnumSource(JobManagerDeploymentStatus.class)
    void testDeploymentStatusFollowsPodDeploymentStatus(JobManagerDeploymentStatus podStatus) {
        var status = new FlinkMiniClusterStatus();
        status.setPodDeploymentStatus(podStatus);

        assertThat(status.getKubernetesDeploymentStatus()).isEqualTo(podStatus);
        assertThat(status.isJmAccessible()).isEqualTo(podStatus.isRestApiAvailable());
    }

    @Test
    void testJobCancellableOnlyWhenRestApiAvailable() {
        var status = new FlinkMiniClusterStatus();
        status.getJobStatus().setState(JobStatus.RUNNING);

        status.setPodDeploymentStatus(JobManagerDeploymentStatus.READY);
        assertThat(status.isJobCancellable()).isTrue();

        status.setPodDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        assertThat(status.isJobCancellable()).isFalse();
    }

    @Test
    void testStableWhenDeployed() {
        var status = deployedStatus();
        status.getReconciliationStatus().markReconciledSpecAsStable();
        status.setPodDeploymentStatus(JobManagerDeploymentStatus.READY);

        assertThat(status.getLifecycleState()).isEqualTo(ResourceLifecycleState.STABLE);
    }

    @Test
    void testFailedWhenPodMissingWithUnrecoverableError() {
        var status = deployedStatus();
        status.setPodDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        status.setError(
                CommonStatus.MSG_HA_METADATA_NOT_AVAILABLE
                        + " "
                        + CommonStatus.MSG_JOB_FINISHED_OR_CONFIGMAPS_DELETED);

        assertThat(status.getLifecycleState()).isEqualTo(ResourceLifecycleState.FAILED);
    }

    @Test
    void testFailedWhenPodInErrorWithUnrecoverableError() {
        var status = deployedStatus();
        status.setPodDeploymentStatus(JobManagerDeploymentStatus.ERROR);
        status.setError(CommonStatus.MSG_MANUAL_RESTORE_REQUIRED);

        assertThat(status.getLifecycleState()).isEqualTo(ResourceLifecycleState.FAILED);
    }

    @Test
    void testNotFailedWhenPodReadyWithUnrecoverableError() {
        var status = deployedStatus();
        status.setPodDeploymentStatus(JobManagerDeploymentStatus.READY);
        status.setError(CommonStatus.MSG_MANUAL_RESTORE_REQUIRED);

        assertThat(status.getLifecycleState()).isEqualTo(ResourceLifecycleState.DEPLOYED);
    }

    private static FlinkMiniClusterStatus deployedStatus() {
        var miniCluster = BaseTestUtils.buildMiniCluster();
        var status = miniCluster.getStatus();
        var reconciliationStatus = status.getReconciliationStatus();
        reconciliationStatus.setLastReconciledSpec(
                SpecUtils.writeSpecWithMeta(miniCluster.getSpec(), miniCluster));
        reconciliationStatus.setState(ReconciliationState.DEPLOYED);
        return status;
    }
}
