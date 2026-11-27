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

package org.apache.flink.kubernetes.operator.controller.bluegreen;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.isCandidateReady;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.isFlinkDeploymentReady;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for blue/green child readiness checks. */
class BlueGreenKubernetesServiceTest {

    @Test
    void acceptsObservedRunningStableRevision() {
        var deployment = readyDeployment();
        assertThat(isFlinkDeploymentReady(deployment)).isTrue();
        assertThat(isCandidateReady(deployment)).isTrue();
    }

    @Test
    void rejectsUnobservedRevisionDespiteStableRunningStatus() {
        var deployment = readyDeployment();
        deployment.getMetadata().setGeneration(2L);
        assertThat(isCandidateReady(deployment)).isFalse();
    }

    @Test
    void sourceReadinessDoesNotRequireObservedGeneration() {
        // The source deployment is about to be replaced; a pending no-op revision on it must not
        // block starting a transition (only the candidate is held to the stricter check).
        var deployment = readyDeployment();
        deployment.getMetadata().setGeneration(2L);
        assertThat(isFlinkDeploymentReady(deployment)).isTrue();
    }

    @Test
    void acceptsNoOpRevisionOnceAcknowledgedWithoutRedeployment() {
        var deployment = readyDeployment();
        deployment.getSpec().getFlinkConfiguration().put("kubernetes.operator.test", "updated");
        deployment.getMetadata().setGeneration(2L);
        assertThat(isCandidateReady(deployment)).isFalse();

        // The child no-op path acknowledges the new generation without executing a redeployment.
        ReconciliationUtils.updateReconciliationMetadata(deployment);
        assertThat(isCandidateReady(deployment)).isTrue();
    }

    @Test
    void preservesAutoscaledCandidateReadiness() {
        // The autoscaler changes the effective spec without bumping the resource generation.
        var deployment = readyDeployment();
        var effectiveSpec = SpecUtils.clone(deployment.getSpec());
        effectiveSpec.getJob().setParallelism(24);
        var reconciliationStatus = deployment.getStatus().getReconciliationStatus();
        reconciliationStatus.serializeAndSetLastReconciledSpec(effectiveSpec, deployment);
        reconciliationStatus.markReconciledSpecAsStable();
        assertThat(isCandidateReady(deployment)).isTrue();
    }

    @Test
    void rejectsMissingOrInconsistentObservation() {
        var deployment = readyDeployment();
        deployment.getStatus().setObservedGeneration(null);
        assertThat(isCandidateReady(deployment)).isFalse();
        deployment.getStatus().setObservedGeneration(2L);
        assertThat(isCandidateReady(deployment)).isFalse();
        deployment = readyDeployment();
        deployment.getMetadata().setGeneration(null);
        assertThat(isCandidateReady(deployment)).isFalse();
    }

    @Test
    void rejectsTerminatingCandidate() {
        var deployment = readyDeployment();
        deployment.getMetadata().setDeletionTimestamp(Instant.now().toString());
        assertThat(isCandidateReady(deployment)).isFalse();
    }

    @Test
    void rejectsUnstableOrNonRunningCandidate() {
        var deployment = readyDeployment();
        deployment.getStatus().getJobStatus().setState(JobStatus.RECONCILING);
        assertThat(isCandidateReady(deployment)).isFalse();
        deployment = readyDeployment();
        deployment.getStatus().getReconciliationStatus().setLastStableSpec(null);
        assertThat(isCandidateReady(deployment)).isFalse();
        deployment = readyDeployment();
        deployment.getStatus().getReconciliationStatus().setState(ReconciliationState.ROLLED_BACK);
        assertThat(isCandidateReady(deployment)).isFalse();
    }

    private FlinkDeployment readyDeployment() {
        var deployment = TestUtils.buildApplicationCluster();
        deployment.getMetadata().setGeneration(1L);
        var status = deployment.getStatus();
        status.getJobStatus().setState(JobStatus.RUNNING);
        status.getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
        status.getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        status.getReconciliationStatus().markReconciledSpecAsStable();
        return deployment;
    }
}
