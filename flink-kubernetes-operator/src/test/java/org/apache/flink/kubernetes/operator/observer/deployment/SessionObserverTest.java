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

package org.apache.flink.kubernetes.operator.observer.deployment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.observer.TestObserverAdapter;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** {@link SessionObserver} unit tests. */
@EnableKubernetesMockClient(crud = true)
public class SessionObserverTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;

    private final Context<FlinkDeployment> readyContext =
            TestUtils.createContextWithReadyJobManagerDeployment(kubernetesClient);
    private TestObserverAdapter<FlinkDeployment> observer;

    @Override
    public void setup() {
        observer = new TestObserverAdapter<>(this, new SessionObserver(eventRecorder));
    }

    @Test
    public void observeSessionCluster() {
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        observer.observe(deployment, readyContext);
        assertNull(deployment.getStatus().getReconciliationStatus().getLastStableSpec());

        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        observer.observe(deployment, readyContext);

        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        assertEquals(
                deployment.getStatus().getReconciliationStatus().getLastReconciledSpec(),
                deployment.getStatus().getReconciliationStatus().getLastStableSpec());

        // Observe again, the JM should be READY
        observer.observe(deployment, readyContext);

        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        // Test job manager is unavailable suddenly
        flinkService.setPortReady(false);
        observer.observe(deployment, readyContext);

        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());
        // Job manager recovers
        flinkService.setPortReady(true);
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        observer.observe(deployment, readyContext);
        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
    }

    @Test
    public void observeAlreadyUpgraded() {
        var kubernetesDeployment = TestUtils.createDeployment(true);
        kubernetesDeployment.getMetadata().setAnnotations(new HashMap<>());

        var context = TestUtils.createContextWithDeployment(kubernetesDeployment, kubernetesClient);

        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        deployment.getMetadata().setGeneration(123L);

        var status = deployment.getStatus();
        var reconStatus = status.getReconciliationStatus();

        // New deployment
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        // Test regular upgrades
        deployment.getSpec().getFlinkConfiguration().put("k", "1");
        deployment.getMetadata().setGeneration(321L);
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(
                deployment,
                new FlinkConfigManager(new Configuration())
                        .getDeployConfig(deployment.getMetadata(), deployment.getSpec()));
        status = deployment.getStatus();
        reconStatus = status.getReconciliationStatus();

        assertEquals(status.getReconciliationStatus().getState(), ReconciliationState.UPGRADING);
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);

        observer.observe(deployment, TestUtils.createEmptyContext());
        assertEquals(JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());

        observer.observe(deployment, context);
        assertEquals(JobManagerDeploymentStatus.MISSING, status.getJobManagerDeploymentStatus());

        kubernetesDeployment
                .getMetadata()
                .getAnnotations()
                .put(FlinkUtils.CR_GENERATION_LABEL, "321");

        deployment.getMetadata().setGeneration(322L);
        deployment.getSpec().getFlinkConfiguration().put("k", "2");

        observer.observe(deployment, context);

        assertEquals(ReconciliationState.DEPLOYED, reconStatus.getState());
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                status.getJobManagerDeploymentStatus());

        var specWithMeta = status.getReconciliationStatus().deserializeLastReconciledSpecWithMeta();
        assertEquals(321L, status.getObservedGeneration());
        assertEquals("1", specWithMeta.getSpec().getFlinkConfiguration().get("k"));
    }
}
