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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link SavepointUtils}. */
public class SavepointUtilsTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @Test
    public void testTriggering() {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster(FlinkVersion.v1_15);
        deployment
                .getMetadata()
                .setCreationTimestamp(Instant.now().minus(Duration.ofMinutes(15)).toString());

        deployment.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);

        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);

        assertEquals(
                Optional.empty(),
                SavepointUtils.shouldTriggerSavepoint(
                        deployment, configManager.getObserveConfig(deployment)));

        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL.key(), "10m");
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);

        assertEquals(
                Optional.of(SavepointTriggerType.PERIODIC),
                SavepointUtils.shouldTriggerSavepoint(
                        deployment, configManager.getObserveConfig(deployment)));
        deployment.getStatus().getJobStatus().getSavepointInfo().resetTrigger();

        deployment.getSpec().getJob().setSavepointTriggerNonce(123L);
        assertEquals(
                Optional.of(SavepointTriggerType.MANUAL),
                SavepointUtils.shouldTriggerSavepoint(
                        deployment, configManager.getObserveConfig(deployment)));
    }
}
