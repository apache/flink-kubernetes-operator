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
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingStatusRecorder;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link org.apache.flink.kubernetes.operator.reconciler.deployment.SessionReconciler}.
 */
@EnableKubernetesMockClient(crud = true)
public class SessionReconcilerTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private KubernetesClient kubernetesClient;
    private EventRecorder eventRecorder;

    @BeforeEach
    public void before() {
        eventRecorder = new EventRecorder(kubernetesClient, (r, e) -> {});
    }

    @Test
    public void testStartSession() throws Exception {
        var count = new AtomicInteger(0);
        TestingFlinkService flinkService =
                new TestingFlinkService() {
                    @Override
                    public void submitSessionCluster(Configuration conf) throws Exception {
                        super.submitSessionCluster(conf);
                        count.addAndGet(1);
                    }
                };

        SessionReconciler reconciler =
                new SessionReconciler(
                        kubernetesClient,
                        flinkService,
                        configManager,
                        eventRecorder,
                        new TestingStatusRecorder<>());
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        kubernetesClient.resource(deployment).createOrReplace();
        reconciler.reconcile(deployment, flinkService.getContext());
        assertEquals(1, count.get());
    }

    @Test
    public void testFailedUpgrade() throws Exception {
        var flinkService = new TestingFlinkService();
        var reconciler =
                new SessionReconciler(
                        kubernetesClient,
                        flinkService,
                        configManager,
                        eventRecorder,
                        new TestingStatusRecorder<>());

        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        kubernetesClient.resource(deployment).createOrReplace();
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
}
