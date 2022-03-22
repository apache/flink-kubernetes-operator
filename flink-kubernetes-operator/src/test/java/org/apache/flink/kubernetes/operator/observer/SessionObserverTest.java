/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
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
        SessionObserver observer =
                new SessionObserver(
                        flinkService,
                        FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        Configuration conf = FlinkUtils.getEffectiveConfig(deployment, new Configuration());
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(deployment.getSpec());

        observer.observe(deployment, readyContext, conf);

        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        observer.observe(deployment, readyContext, conf);

        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        // Observe again, the JM should be READY
        observer.observe(deployment, readyContext, conf);

        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        // Test job manager is unavailable suddenly
        flinkService.setPortReady(false);
        observer.observe(deployment, readyContext, conf);

        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                deployment.getStatus().getJobManagerDeploymentStatus());
        // Job manager recovers
        flinkService.setPortReady(true);
        observer.observe(deployment, readyContext, conf);
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
        observer.observe(deployment, readyContext, conf);
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
                .setLastReconciledSpec(deployment.getSpec());

        FlinkOperatorConfiguration allNsConfig =
                new FlinkOperatorConfiguration(
                        Duration.ofSeconds(1),
                        -1,
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(3),
                        Duration.ofSeconds(4),
                        Duration.ofSeconds(5),
                        null,
                        Collections.emptySet());
        FlinkOperatorConfiguration specificNsConfig =
                new FlinkOperatorConfiguration(
                        Duration.ofSeconds(1),
                        -1,
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(3),
                        Duration.ofSeconds(4),
                        Duration.ofSeconds(5),
                        null,
                        Set.of(deployment.getMetadata().getNamespace()));
        FlinkOperatorConfiguration multipleNsConfig =
                new FlinkOperatorConfiguration(
                        Duration.ofSeconds(1),
                        -1,
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(3),
                        Duration.ofSeconds(4),
                        Duration.ofSeconds(5),
                        null,
                        Set.of(deployment.getMetadata().getNamespace(), "ns"));

        Deployment k8sDeployment = new Deployment();
        k8sDeployment.setSpec(new DeploymentSpec());
        k8sDeployment.setStatus(new DeploymentStatus());

        AtomicInteger secondaryResourceAccessed = new AtomicInteger(0);
        Observer allNsObserver = new SessionObserver(flinkService, allNsConfig);
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
                },
                FlinkUtils.getEffectiveConfig(deployment, new Configuration()));

        assertEquals(1, secondaryResourceAccessed.get());

        Observer specificNsObserver = new SessionObserver(flinkService, specificNsConfig);
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
                },
                FlinkUtils.getEffectiveConfig(deployment, new Configuration()));

        assertEquals(2, secondaryResourceAccessed.get());

        Observer multipleNsObserver = new SessionObserver(flinkService, multipleNsConfig);
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
                },
                FlinkUtils.getEffectiveConfig(deployment, new Configuration()));

        assertEquals(3, secondaryResourceAccessed.get());
    }
}
