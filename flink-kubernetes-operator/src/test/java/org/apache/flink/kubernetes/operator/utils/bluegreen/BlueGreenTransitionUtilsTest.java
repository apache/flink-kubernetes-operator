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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionMode;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_GATE_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    public void testValidate_watermarkStrategy_eachExtractorAlone_returnsEmpty() {
        for (String extractor : BlueGreenTransitionUtils.WATERMARK_EXTRACTOR_KEYS) {
            BlueGreenContext context =
                    buildContext(
                            TransitionMode.ADVANCED,
                            Map.of("bluegreen.gate.strategy", "WATERMARK", extractor, "2"));
            Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
            assertFalse(result.isPresent(), extractor + ": " + result.orElse(""));
        }
    }

    @Test
    public void testValidate_watermarkStrategy_twoExtractors_returnsError() {
        BlueGreenContext context =
                buildContext(
                        TransitionMode.ADVANCED,
                        Map.of(
                                "bluegreen.gate.strategy", "WATERMARK",
                                "bluegreen.gate.watermark.field-path", "eventTime",
                                "bluegreen.gate.watermark.extractor-class",
                                        "com.example.MyExtractor"));
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertTrue(result.isPresent());
        assertTrue(
                result.get().contains("exactly one of 'bluegreen.gate.watermark.extractor-class'"),
                result.get());
        assertTrue(
                result.get()
                        .contains(
                                "found: bluegreen.gate.watermark.extractor-class, "
                                        + "bluegreen.gate.watermark.field-path."),
                result.get());
    }

    @Test
    public void testValidate_programmaticGate_withoutExtractor_returnsEmpty() {
        BlueGreenContext context =
                buildContext(
                        TransitionMode.ADVANCED,
                        Map.of(
                                "bluegreen.gate.strategy", "WATERMARK",
                                "bluegreen.gate.injection.enabled", "false"));
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertFalse(result.isPresent());
    }

    @Test
    public void testValidate_programmaticGate_withExtractor_returnsError() {
        BlueGreenContext context =
                buildContext(
                        TransitionMode.ADVANCED,
                        Map.of(
                                "bluegreen.gate.strategy", "WATERMARK",
                                "bluegreen.gate.injection.enabled", "false",
                                "bluegreen.gate.watermark.field-path", "eventTime"));
        Optional<String> result = BlueGreenTransitionUtils.validateAdvancedModeConfig(context);
        assertTrue(result.isPresent());
        assertTrue(
                result.get().contains("Remove bluegreen.gate.watermark.field-path"), result.get());
    }

    @Test
    public void testValidate_gateTimeoutNotLongerThanDeletionDelay_returnsError() {
        BlueGreenContext context =
                buildContext(
                        TransitionMode.ADVANCED,
                        Map.of(
                                "bluegreen.gate.strategy", "WATERMARK",
                                "bluegreen.gate.watermark.field-path", "eventTime"));
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
        BlueGreenContext context =
                buildContext(
                        TransitionMode.ADVANCED,
                        Map.of(
                                "bluegreen.gate.strategy", "WATERMARK",
                                "bluegreen.gate.watermark.field-path", "eventTime"));
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

    @Test
    public void testInjectGateAgent_missingOperatorImage_failsLoud() {
        FlinkDeployment deployment =
                buildFlinkDeployment(Map.of("bluegreen.gate.strategy", "WATERMARK"));
        ConfigObjectNode flinkConfig = deployment.getSpec().getFlinkConfiguration();

        IllegalStateException ex =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                BlueGreenTransitionUtils.injectGateAgent(
                                        deployment, flinkConfig, null));
        assertTrue(ex.getMessage().contains("OPERATOR_IMAGE"));

        // Must not wire a doomed deployment: no -javaagent flag, no injected init-container.
        assertFalse(flinkConfig.has("env.java.opts.jobmanager"));
        assertNull(deployment.getSpec().getJobManager());
    }

    @Test
    public void testInjectGateAgent_withOperatorImage_wiresAgent() {
        FlinkDeployment deployment =
                buildFlinkDeployment(Map.of("bluegreen.gate.strategy", "WATERMARK"));
        ConfigObjectNode flinkConfig = deployment.getSpec().getFlinkConfiguration();

        BlueGreenTransitionUtils.injectGateAgent(
                deployment, flinkConfig, "test-operator-image:1.15");

        // -javaagent flag is added for the JobManager.
        assertTrue(
                flinkConfig
                        .get("env.java.opts.jobmanager")
                        .asText()
                        .contains("-javaagent:/opt/flink/lib/bluegreen-agent.jar"));

        PodSpec podSpec = deployment.getSpec().getJobManager().getPodTemplate().getSpec();

        // Init-container copies the agent from the operator image's /opt/flink/artifacts.
        Container init =
                podSpec.getInitContainers().stream()
                        .filter(c -> "bluegreen-agent-init".equals(c.getName()))
                        .findFirst()
                        .orElseThrow();
        assertEquals("test-operator-image:1.15", init.getImage());
        String cmd = String.join(" ", init.getCommand());
        assertTrue(cmd.contains("cp /opt/flink/artifacts/bluegreen-agent.jar"));
        assertTrue(cmd.contains("/bluegreen-agent/bluegreen-agent.jar"));

        // Shared emptyDir volume between init- and main-containers.
        assertTrue(
                podSpec.getVolumes().stream().anyMatch(v -> "bluegreen-agent".equals(v.getName())));

        // Main container mounts the agent onto the -javaagent path.
        Container main =
                podSpec.getContainers().stream()
                        .filter(c -> "flink-main-container".equals(c.getName()))
                        .findFirst()
                        .orElseThrow();
        VolumeMount mount =
                main.getVolumeMounts().stream()
                        .filter(m -> "bluegreen-agent".equals(m.getName()))
                        .findFirst()
                        .orElseThrow();
        assertEquals("/opt/flink/lib/bluegreen-agent.jar", mount.getMountPath());
        assertEquals("bluegreen-agent.jar", mount.getSubPath());
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
                        .job(JobSpec.builder().upgradeMode(UpgradeMode.LAST_STATE).build())
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

    private static FlinkDeployment buildFlinkDeployment(Map<String, String> flinkConfig) {
        var spec =
                FlinkDeploymentSpec.builder()
                        .flinkConfiguration(new ConfigObjectNode())
                        .job(JobSpec.builder().upgradeMode(UpgradeMode.STATELESS).build())
                        .build();
        spec.setFlinkConfiguration(new HashMap<>(flinkConfig));
        var deployment = new FlinkDeployment();
        deployment.setSpec(spec);
        return deployment;
    }
}
