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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionMode;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.OperationNotSupportedException;

import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.ACTIVE_DEPLOYMENT_TYPE;
import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.IS_FIRST_DEPLOYMENT;
import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.TRANSITION_STAGE;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.getConfigMap;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.updateConfigMapEntry;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.upsertConfigMap;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getDeploymentDeletionDelay;

/** Utility class for Blue/Green transition stage operations. */
public class BlueGreenTransitionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenTransitionUtils.class);

    /**
     * Test override for the {@code OPERATOR_IMAGE} env lookup used by {@link
     * #prepareTransitionMetadata}. {@code null} means read the environment (the production path).
     */
    private static String operatorImageOverride = null;

    /**
     * Overrides the {@code OPERATOR_IMAGE} env lookup for tests (including tests in other
     * packages). Pass {@code null} to restore the production path of reading the environment
     * variable.
     */
    @VisibleForTesting
    public static void setOperatorImageOverride(String operatorImage) {
        operatorImageOverride = operatorImage;
    }

    @SneakyThrows
    public static TransitionMode getTransitionMode(BlueGreenContext context) {
        TransitionMode transitionMode = context.getBgDeployment().getSpec().getTransitionMode();

        if (transitionMode == null) {
            throw new OperationNotSupportedException("Please specify the TransitionMode");
        }

        return transitionMode;
    }

    public static void prepareTransitionMetadata(
            BlueGreenContext context,
            BlueGreenDeploymentType blueGreenDeploymentType,
            FlinkDeployment flinkDeployment,
            boolean isFirstDeployment) {
        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
            return;
        }

        var transitionDefaultMetadata =
                Map.of(
                        IS_FIRST_DEPLOYMENT.getLabel(), isFirstDeployment ? "true" : "false",
                        GateContextOptions.DEPLOYMENT_DELETION_DELAY.getLabel(),
                                String.valueOf(getDeploymentDeletionDelay(context)),
                        ACTIVE_DEPLOYMENT_TYPE.getLabel(), blueGreenDeploymentType.toString(),
                        TRANSITION_STAGE.getLabel(), TransitionStage.INITIALIZING.toString());

        upsertConfigMap(context, transitionDefaultMetadata);

        // Preparing the FlinkConfiguration for the OutputDecider
        flinkDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        "bluegreen." + ACTIVE_DEPLOYMENT_TYPE.getLabel(),
                        blueGreenDeploymentType.toString());
        flinkDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put("bluegreen.configmap.name", context.getConfigMapName());

        // Auto-inject agent config and init container when a gate strategy is declared
        ConfigObjectNode flinkConfig = flinkDeployment.getSpec().getFlinkConfiguration();
        if (flinkConfig.has("bluegreen.gate.strategy")) {
            String operatorImage =
                    operatorImageOverride != null
                            ? operatorImageOverride
                            : System.getenv("OPERATOR_IMAGE");
            injectGateAgent(flinkDeployment, flinkConfig, operatorImage);
        }
    }

    /**
     * Moves an ADVANCED transition out of INITIALIZING once the new deployment is ready.
     *
     * @return {@code true} if the gate phase started, i.e. the stage moved to TRANSITIONING
     */
    public static boolean moveToFirstTransitionStage(BlueGreenContext context) {
        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
            return false;
        }

        ConfigMap configMap = getConfigMap(context);
        String stage = configMap.getData().get(TRANSITION_STAGE.getLabel());

        if (stage.equals(TransitionStage.INITIALIZING.toString())) {
            TransitionStage nextStage;

            if (context.getDeployments().getNumberOfDeployments() == 2) {
                LOG.info("Stage INITIALIZING to TRANSITIONING");
                nextStage = TransitionStage.TRANSITIONING;
            } else {
                LOG.info(
                        "No transition between deployments detected, Stage INITIALIZING -> CLEAR_TO_TEARDOWN");
                nextStage = TransitionStage.CLEAR_TO_TEARDOWN;
            }

            updateTransitionStage(context, nextStage);
            return nextStage == TransitionStage.TRANSITIONING;
        }

        return false;
    }

    public static boolean isClearToTeardown(BlueGreenContext context) {

        if (TransitionMode.ADVANCED == getTransitionMode(context)) {
            ConfigMap configMap = getConfigMap(context);
            String stage = configMap.getData().get(TRANSITION_STAGE.getLabel());

            if (!stage.equals(TransitionStage.CLEAR_TO_TEARDOWN.toString())) {
                LOG.info("Waiting for CLEAR_TO_TEARDOWN, current stage: " + stage);
                return false;
            }
        }

        return true;
    }

    /**
     * Hands the gate back to the deployment that keeps running after an aborted transition. The
     * ConfigMap is rewritten whole, which drops the strategy entries (e.g. the watermark toggle) of
     * the aborted hand-over, and it is marked as a first deployment: with no counterpart left to
     * hand over to, the remaining gate must pass every record, as it does before any transition.
     *
     * <p>Call it after the reconciliation's other ConfigMap writes. Those update the informer's
     * cached copy under optimistic locking, while this write replaces the ConfigMap
     * unconditionally.
     */
    public static void resetGateOnAbort(
            BlueGreenContext context, FlinkBlueGreenDeploymentState previousState) {

        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
            return;
        }

        var previousDeploymentType =
                previousState.name().contains("BLUE")
                        ? BlueGreenDeploymentType.BLUE
                        : BlueGreenDeploymentType.GREEN;

        upsertConfigMap(
                context,
                Map.of(
                        IS_FIRST_DEPLOYMENT.getLabel(), "true",
                        GateContextOptions.DEPLOYMENT_DELETION_DELAY.getLabel(),
                                String.valueOf(getDeploymentDeletionDelay(context)),
                        ACTIVE_DEPLOYMENT_TYPE.getLabel(), previousDeploymentType.toString(),
                        TRANSITION_STAGE.getLabel(), TransitionStage.FAILING.toString()));
    }

    public static void updateTransitionStageFromJobStatus(
            BlueGreenContext context, JobStatus jobStatus) {

        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
            return;
        }

        // ConfigMap may not exist yet if validation failed before prepareTransitionMetadata ran
        var secondaryConfigMaps = context.getJosdkContext().getSecondaryResources(ConfigMap.class);
        boolean configMapPresent =
                secondaryConfigMaps != null
                        && secondaryConfigMaps.stream()
                                .anyMatch(
                                        cm ->
                                                cm.getMetadata()
                                                        .getName()
                                                        .equals(context.getConfigMapName()));
        if (!configMapPresent) {
            return;
        }

        TransitionStage transitionStage;
        switch (jobStatus) {
            case RUNNING:
                transitionStage = TransitionStage.RUNNING;
                break;
            case FAILING:
                transitionStage = TransitionStage.FAILING;
                break;
            case RECONCILING:
                transitionStage = TransitionStage.INITIALIZING;
                break;
            default:
                throw new RuntimeException("Unsupported JobStatus: " + jobStatus);
        }
        updateTransitionStage(context, transitionStage);
    }

    public static void updateTransitionStage(
            BlueGreenContext context, TransitionStage transitionStage) {
        updateConfigMapEntry(context, TRANSITION_STAGE.getLabel(), transitionStage.toString());
    }

    /**
     * Validates that all required gate configuration properties are present when using ADVANCED
     * mode. Returns an error message if any required property is missing, or {@link
     * Optional#empty()} if the configuration is valid.
     *
     * <p>Required properties for ADVANCED mode:
     *
     * <ul>
     *   <li>{@code bluegreen.gate.strategy} — must be set to a supported value (e.g. WATERMARK)
     *   <li>{@code bluegreen.gate.watermark.field-path} or {@code
     *       bluegreen.gate.watermark.extractor-class} — required when strategy is WATERMARK
     * </ul>
     */
    public static Optional<String> validateAdvancedModeConfig(BlueGreenContext context) {
        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
            return Optional.empty();
        }

        ConfigObjectNode flinkConfig =
                context.getBgDeployment().getSpec().getTemplate().getSpec().getFlinkConfiguration();

        if (!flinkConfig.has("bluegreen.gate.strategy")) {
            return Optional.of(
                    "[BlueGreen] ADVANCED mode requires 'bluegreen.gate.strategy' to be set "
                            + "in flinkConfiguration. Supported values: WATERMARK");
        }

        String strategy = flinkConfig.get("bluegreen.gate.strategy").asText();
        if ("WATERMARK".equals(strategy)) {
            if (!flinkConfig.has("bluegreen.gate.watermark.field-path")
                    && !flinkConfig.has("bluegreen.gate.watermark.extractor-class")) {
                return Optional.of(
                        "[BlueGreen] Gate strategy WATERMARK requires either "
                                + "'bluegreen.gate.watermark.field-path' (dot-notation POJO field, no app code needed) "
                                + "or 'bluegreen.gate.watermark.extractor-class' (custom class) "
                                + "to be set in flinkConfiguration.");
            }
        } else {
            return Optional.of(
                    "[BlueGreen] Unknown gate strategy: '"
                            + strategy
                            + "'. Supported values: WATERMARK");
        }

        return Optional.empty();
    }

    /**
     * Wires the Blue/Green gate agent into the JobManager for ADVANCED-mode gate injection: enables
     * gate injection, adds the {@code -javaagent} flag, and injects the init-container that pulls
     * the agent jar from the operator image.
     *
     * <p>The agent can only be delivered when we know the operator image (the init-container copies
     * the jar from {@code /opt/flink/artifacts} in that image). If {@code operatorImage} is
     * null/empty we cannot deliver it, and adding the {@code -javaagent} flag anyway would
     * guarantee a cryptic JobManager crash ("bluegreen-agent.jar" missing). We fail loudly instead
     * of silently wiring a doomed deployment — a missing OPERATOR_IMAGE means the operator's Helm
     * chart is misconfigured.
     */
    @VisibleForTesting
    static void injectGateAgent(
            FlinkDeployment flinkDeployment, ConfigObjectNode flinkConfig, String operatorImage) {
        if (operatorImage == null || operatorImage.isEmpty()) {
            String strategy =
                    flinkConfig.has("bluegreen.gate.strategy")
                            ? flinkConfig.get("bluegreen.gate.strategy").asText()
                            : "(unset)";
            throw new IllegalStateException(
                    "[BlueGreen] A gate strategy is declared ('bluegreen.gate.strategy'="
                            + strategy
                            + ") but the OPERATOR_IMAGE environment variable is not set on the"
                            + " operator. The Blue/Green gate agent cannot be injected, so the"
                            + " JobManager would fail to start with a missing -javaagent. This is a"
                            + " misconfigured operator deployment: the Helm chart must set"
                            + " OPERATOR_IMAGE on the operator container. Refusing to deploy a job"
                            + " that would crash-loop.");
        }

        if (!flinkConfig.has("bluegreen.gate.injection.enabled")) {
            flinkConfig.put("bluegreen.gate.injection.enabled", "true");
        }

        final String agentFlag = "-javaagent:/opt/flink/lib/bluegreen-agent.jar";
        String existingOpts =
                flinkConfig.has("env.java.opts.jobmanager")
                        ? flinkConfig.get("env.java.opts.jobmanager").asText()
                        : "";
        if (!existingOpts.contains(agentFlag)) {
            flinkConfig.put(
                    "env.java.opts.jobmanager",
                    existingOpts.isEmpty() ? agentFlag : existingOpts + " " + agentFlag);
        }

        injectAgentInitContainer(flinkDeployment, operatorImage);
    }

    private static void injectAgentInitContainer(
            FlinkDeployment flinkDeployment, String operatorImage) {
        var spec = flinkDeployment.getSpec();

        if (spec.getJobManager() == null) {
            spec.setJobManager(new JobManagerSpec());
        }
        JobManagerSpec jmSpec = spec.getJobManager();

        if (jmSpec.getPodTemplate() == null) {
            jmSpec.setPodTemplate(new PodTemplateSpec());
        }
        PodTemplateSpec podTemplate = jmSpec.getPodTemplate();
        if (podTemplate.getSpec() == null) {
            podTemplate.setSpec(new PodSpec());
        }
        PodSpec podSpec = podTemplate.getSpec();

        podSpec.getVolumes()
                .add(
                        new VolumeBuilder()
                                .withName("bluegreen-agent")
                                .withNewEmptyDir()
                                .endEmptyDir()
                                .build());

        podSpec.getInitContainers()
                .add(
                        new ContainerBuilder()
                                .withName("bluegreen-agent-init")
                                .withImage(operatorImage)
                                .withCommand(
                                        "sh",
                                        "-c",
                                        // Fail with an actionable message instead of a cryptic
                                        // `cp: cannot stat` if the operator image was built without
                                        // the agent jar (see the Dockerfile agent-baking COPY).
                                        "if [ ! -f /opt/flink/artifacts/bluegreen-agent.jar ]; then"
                                                + " echo '[BlueGreen] FATAL:"
                                                + " /opt/flink/artifacts/bluegreen-agent.jar missing"
                                                + " from operator image — built without the gate"
                                                + " agent (check Dockerfile agent baking).';"
                                                + " exit 1; fi;"
                                                + " cp /opt/flink/artifacts/bluegreen-agent.jar"
                                                + " /bluegreen-agent/bluegreen-agent.jar")
                                .withVolumeMounts(
                                        new VolumeMountBuilder()
                                                .withName("bluegreen-agent")
                                                .withMountPath("/bluegreen-agent")
                                                .build())
                                .build());

        VolumeMount agentMount =
                new VolumeMountBuilder()
                        .withName("bluegreen-agent")
                        .withMountPath("/opt/flink/lib/bluegreen-agent.jar")
                        .withSubPath("bluegreen-agent.jar")
                        .build();

        boolean foundMain = false;
        for (Container c : podSpec.getContainers()) {
            if ("flink-main-container".equals(c.getName())) {
                c.getVolumeMounts().add(agentMount);
                foundMain = true;
                break;
            }
        }
        if (!foundMain) {
            podSpec.getContainers()
                    .add(
                            new ContainerBuilder()
                                    .withName("flink-main-container")
                                    .withVolumeMounts(agentMount)
                                    .build());
        }
    }
}
