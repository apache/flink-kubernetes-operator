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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.reconciler.TestReconcilerAdapter;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link org.apache.flink.kubernetes.operator.reconciler.deployment.SessionReconciler}.
 */
@EnableKubernetesMockClient(crud = true)
public class SessionReconcilerTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;
    private TestReconcilerAdapter<FlinkDeployment, FlinkDeploymentSpec, FlinkDeploymentStatus>
            reconciler;

    @Override
    public void setup() {
        reconciler =
                new TestReconcilerAdapter<>(
                        this, new SessionReconciler(eventRecorder, statusRecorder));
    }

    @Test
    public void testStartSession() throws Exception {
        var count = new AtomicInteger(0);
        flinkService =
                new TestingFlinkService() {
                    @Override
                    public void submitSessionCluster(Configuration conf) throws Exception {
                        super.submitSessionCluster(conf);
                        count.addAndGet(1);
                    }
                };

        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        reconciler.reconcile(deployment, flinkService.getContext());
        assertEquals(1, count.get());
    }

    @Test
    public void testFailedUpgrade() throws Exception {
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        reconciler.reconcile(deployment, flinkService.getContext());

        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());

        deployment.getSpec().setRestartNonce(1234L);

        flinkService.setDeployFailure(true);
        try {
            reconciler.reconcile(deployment, flinkService.getContext());
            fail();
        } catch (Exception expected) {
        }

        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());
        flinkService.setDeployFailure(false);
        flinkService.clear();
        assertTrue(flinkService.getSessions().isEmpty());
        reconciler.reconcile(deployment, flinkService.getContext());

        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(
                1234L,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getRestartNonce());
        assertEquals(Set.of(deployment.getMetadata().getName()), flinkService.getSessions());
    }

    @Test
    public void testSetOwnerReference() throws Exception {
        FlinkDeployment flinkApp = TestUtils.buildApplicationCluster();
        ObjectMeta deployMeta = flinkApp.getMetadata();
        FlinkDeploymentStatus status = flinkApp.getStatus();
        FlinkDeploymentSpec spec = flinkApp.getSpec();
        Configuration deployConfig = configManager.getDeployConfig(deployMeta, spec);

        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED);
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconciler
                .getReconciler()
                .deploy(getResourceContext(flinkApp), spec, deployConfig, Optional.empty(), false);

        final List<Map<String, String>> expectedOwnerReferences =
                List.of(TestUtils.generateTestOwnerReferenceMap(flinkApp));
        List<Map<String, String>> or =
                deployConfig.get(KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE);
        Assertions.assertEquals(expectedOwnerReferences, or);
    }
}
