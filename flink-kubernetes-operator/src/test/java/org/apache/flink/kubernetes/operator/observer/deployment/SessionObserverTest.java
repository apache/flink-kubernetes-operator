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
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** {@link SessionObserver} unit tests. */
public class SessionObserverTest {
    private final Context readyContext = TestUtils.createContextWithReadyJobManagerDeployment();
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @Test
    public void observeSessionCluster() {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        SessionObserver observer = new SessionObserver(null, flinkService, configManager);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec());

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
    public void testWatchMultipleNamespaces() {
        FlinkService flinkService = new TestingFlinkService();
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec());

        Deployment k8sDeployment = new Deployment();
        k8sDeployment.setSpec(new DeploymentSpec());
        k8sDeployment.setStatus(new DeploymentStatus());

        AtomicInteger secondaryResourceAccessed = new AtomicInteger(0);
        Observer observer = new SessionObserver(null, flinkService, configManager);
        observer.observe(
                deployment,
                new Context() {
                    @Override
                    public Optional<RetryInfo> getRetryInfo() {
                        return Optional.empty();
                    }

                    @Override
                    public <T> Optional<T> getSecondaryResource(Class<T> aClass, String s) {
                        assertNull(s);
                        secondaryResourceAccessed.addAndGet(1);
                        return Optional.of((T) k8sDeployment);
                    }
                });

        assertEquals(1, secondaryResourceAccessed.get());

        configManager.setWatchedNamespaces(Set.of(deployment.getMetadata().getNamespace()));
        observer.observe(
                deployment,
                new Context() {
                    @Override
                    public Optional<RetryInfo> getRetryInfo() {
                        return Optional.empty();
                    }

                    @Override
                    public <T> Optional<T> getSecondaryResource(Class<T> aClass, String s) {
                        assertNull(s);
                        secondaryResourceAccessed.addAndGet(1);
                        return Optional.of((T) k8sDeployment);
                    }
                });

        assertEquals(2, secondaryResourceAccessed.get());

        configManager.setWatchedNamespaces(Set.of(deployment.getMetadata().getNamespace(), "ns"));
        observer.observe(
                deployment,
                new Context() {
                    @Override
                    public Optional<RetryInfo> getRetryInfo() {
                        return Optional.empty();
                    }

                    @Override
                    public <T> Optional<T> getSecondaryResource(Class<T> aClass, String s) {
                        assertEquals(deployment.getMetadata().getNamespace(), s);
                        secondaryResourceAccessed.addAndGet(1);
                        return Optional.of((T) k8sDeployment);
                    }
                });

        configManager.setWatchedNamespaces(null);
        assertEquals(3, secondaryResourceAccessed.get());
    }
}
