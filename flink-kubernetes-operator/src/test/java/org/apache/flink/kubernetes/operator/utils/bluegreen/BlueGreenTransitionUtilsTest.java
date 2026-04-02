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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link BlueGreenTransitionUtils} config validation. */
public class BlueGreenTransitionUtilsTest {

    @Test
    public void testValidate_basicMode_skipsValidation() {
        BlueGreenContext context = buildContext(TransitionMode.BASIC, new HashMap<>());
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertFalse(result.isPresent());
    }

    @Test
    public void testValidate_advancedMode_missingStrategy_returnsError() {
        BlueGreenContext context = buildContext(TransitionMode.ADVANCED, new HashMap<>());
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertTrue(result.isPresent());
        assertTrue(result.get().contains("bluegreen.gate.strategy"));
    }

    @Test
    public void testValidate_advancedMode_unknownStrategy_returnsError() {
        BlueGreenContext context =
                buildContext(
                        TransitionMode.ADVANCED,
                        Map.of("bluegreen.gate.strategy", "UNKNOWN_STRATEGY"));
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertTrue(result.isPresent());
        assertTrue(result.get().contains("UNKNOWN_STRATEGY"));
    }

    @Test
    public void testValidate_watermarkStrategy_missingExtractorClass_returnsError() {
        BlueGreenContext context =
                buildContext(
                        TransitionMode.ADVANCED, Map.of("bluegreen.gate.strategy", "WATERMARK"));
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertTrue(result.isPresent());
        assertTrue(result.get().contains("bluegreen.gate.watermark.extractor-class"));
    }

    @Test
    public void testValidate_watermarkStrategy_validConfig_returnsEmpty() {
        BlueGreenContext context =
                buildContext(
                        TransitionMode.ADVANCED,
                        Map.of(
                                "bluegreen.gate.strategy", "WATERMARK",
                                "bluegreen.gate.watermark.extractor-class",
                                        "com.example.MyExtractor"));
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertFalse(result.isPresent());
    }

    // ==================== Helpers ====================

    private static BlueGreenContext buildContext(
            TransitionMode transitionMode, Map<String, String> flinkConfig) {
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
                        .job(JobSpec.builder().upgradeMode(UpgradeMode.STATELESS).build())
                        .build();
        flinkDeploymentSpec.setFlinkConfiguration(new HashMap<>(flinkConfig));

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
