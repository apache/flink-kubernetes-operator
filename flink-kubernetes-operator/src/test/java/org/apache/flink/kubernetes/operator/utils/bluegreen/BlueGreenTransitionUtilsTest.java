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

package org.apache.flink.kubernetes.operator.utils.bluegreen;

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionMode;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_GATE_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link BlueGreenTransitionUtils} config validation. */
public class BlueGreenTransitionUtilsTest {

    @Test
    public void testValidate_basicMode_skipsValidation() {
        BlueGreenContext context = buildContext(TransitionMode.BASIC);
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertFalse(result.isPresent());
    }

    @Test
    public void testValidate_advancedMode_needsNoGateConfig() {
        // The job places the gate in its code, so the spec declares nothing about it
        BlueGreenContext context = buildContext(TransitionMode.ADVANCED);
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertFalse(result.isPresent(), result.orElse(""));
    }

    @Test
    public void testValidate_gateTimeoutNotLongerThanDeletionDelay_returnsError() {
        BlueGreenContext context = buildContext(TransitionMode.ADVANCED);
        var configuration = context.getBgDeployment().getSpec().getConfiguration();
        configuration.put(BLUEGREEN_GATE_TIMEOUT.key(), "60000");
        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "60000");

        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertTrue(result.isPresent());
        assertTrue(result.get().contains("must be longer than"), result.get());

        configuration.put(BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key(), "59999");
        assertFalse(BlueGreenTransitionUtils.validateAdvancedModeConfig(context).isPresent());
    }

    @Test
    public void testValidate_statelessUpgradeMode_returnsError() {
        BlueGreenContext context = buildContext(TransitionMode.ADVANCED);
        context.getBgDeployment()
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setUpgradeMode(UpgradeMode.STATELESS);

        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertTrue(result.isPresent());
        assertTrue(result.get().contains("not STATELESS"), result.get());
        assertTrue(result.get().endsWith("Otherwise data loss may occur."), result.get());
    }

    // ==================== Helpers ====================

    private static BlueGreenContext buildContext(TransitionMode transitionMode) {
        var deployment = new FlinkBlueGreenDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName("test-app")
                        .withNamespace("test-ns")
                        .withUid(UUID.randomUUID().toString())
                        .build());

        var flinkDeploymentSpec =
                FlinkDeploymentSpec.builder()
                        .flinkConfiguration(new ConfigObjectNode())
                        .job(JobSpec.builder().upgradeMode(UpgradeMode.LAST_STATE).build())
                        .build();

        var bgDeploymentSpec =
                new FlinkBlueGreenDeploymentSpec(
                        new HashMap<>(),
                        null,
                        transitionMode,
                        FlinkDeploymentTemplateSpec.builder().spec(flinkDeploymentSpec).build());

        deployment.setSpec(bgDeploymentSpec);
        deployment.setStatus(new FlinkBlueGreenDeploymentStatus());
        return new BlueGreenContext(deployment, deployment.getStatus(), null, null, null);
    }
}
