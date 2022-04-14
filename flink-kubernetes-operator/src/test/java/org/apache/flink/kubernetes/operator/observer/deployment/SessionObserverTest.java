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
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
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

    @Test
    public void observeSessionCluster() {
        TestingFlinkService flinkService = new TestingFlinkService();
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        Configuration flinkConf = new Configuration();
        SessionObserver observer =
                new SessionObserver(
                        flinkService,
                        FlinkOperatorConfiguration.fromConfiguration(new Configuration()),
                        flinkConf);
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
        Configuration flinkConf = new Configuration();
        FlinkService flinkService = new TestingFlinkService();
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec());

        FlinkOperatorConfiguration allNsConfig =
                FlinkOperatorConfiguration.fromConfiguration(new Configuration());
        FlinkOperatorConfiguration specificNsConfig =
                FlinkOperatorConfiguration.fromConfiguration(
                        new Configuration(), Set.of(deployment.getMetadata().getNamespace()));
        FlinkOperatorConfiguration multipleNsConfig =
                FlinkOperatorConfiguration.fromConfiguration(
                        new Configuration(), Set.of(deployment.getMetadata().getNamespace(), "ns"));

        Deployment k8sDeployment = new Deployment();
        k8sDeployment.setSpec(new DeploymentSpec());
        k8sDeployment.setStatus(new DeploymentStatus());

        AtomicInteger secondaryResourceAccessed = new AtomicInteger(0);
        Observer allNsObserver = new SessionObserver(flinkService, allNsConfig, flinkConf);
        allNsObserver.observe(
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

        Observer specificNsObserver =
                new SessionObserver(flinkService, specificNsConfig, flinkConf);
        specificNsObserver.observe(
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

        Observer multipleNsObserver =
                new SessionObserver(flinkService, multipleNsConfig, flinkConf);
        multipleNsObserver.observe(
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

        assertEquals(3, secondaryResourceAccessed.get());
    }
}
