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

import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link SessionObserver} unit tests. */
public class SessionObserverTest {
    private final Context readyContext = TestUtils.createContextWithReadyJobManagerDeployment();

    @Test
    public void observeSessionCluster() {
        FlinkService flinkService = new TestingFlinkService();
        SessionObserver observer =
                new SessionObserver(
                        flinkService,
                        FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(deployment.getSpec());

        observer.observe(
                deployment,
                readyContext,
                FlinkUtils.getEffectiveConfig(deployment, new Configuration()));

        assertEquals(
                JobManagerDeploymentStatus.DEPLOYED_NOT_READY,
                deployment.getStatus().getJobManagerDeploymentStatus());

        observer.observe(
                deployment,
                readyContext,
                FlinkUtils.getEffectiveConfig(deployment, new Configuration()));

        assertEquals(
                JobManagerDeploymentStatus.READY,
                deployment.getStatus().getJobManagerDeploymentStatus());
    }
}
