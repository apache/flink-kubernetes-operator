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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateMetrics;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionMode;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.ACTIVE_DEPLOYMENT_TYPE;
import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.IS_FIRST_DEPLOYMENT;
import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.TRANSITION_STAGE;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.BLUEGREEN_GATE_TIMEOUT;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.getConfigMap;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.updateConfigMapEntry;
import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenKubernetesService.upsertConfigMap;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getDeploymentDeletionDelay;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getGateTimeout;

/** Utility class for Blue/Green transition stage operations. */
public class BlueGreenTransitionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenTransitionUtils.class);

    private static final String GATE_INJECTION_ENABLED = "bluegreen.gate.injection.enabled";

    // The WATERMARK gate's cutover point (WatermarkGateContext.WATERMARK_TOGGLE_VALUE)
    private static final String WATERMARK_TOGGLE_VALUE = "watermark-toggle-value";

    /** The WATERMARK gate's extraction strategies. A job sets exactly one. */
    @VisibleForTesting
    static final List<String> WATERMARK_EXTRACTOR_KEYS =
            List.of(
                    "bluegreen.gate.watermark.extractor-class",
                    "bluegreen.gate.watermark.field-index",
                    "bluegreen.gate.watermark.field-path");

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
            injectGateAgent(
                    flinkDeployment,
                    flinkConfig,
                    operatorImage,
                    mergesPodTemplateArraysByName(context, flinkDeployment));
        }
    }

    /**
     * The {@code kubernetes.operator.pod-template.merge-arrays-by-name} the deployment is deployed
     * with: its own flinkConfiguration over the operator's defaults.
     */
    private static boolean mergesPodTemplateArraysByName(
            BlueGreenContext context, FlinkDeployment flinkDeployment) {
        var spec = flinkDeployment.getSpec();
        Configuration conf =
                context.getCtxFactory()
                        .getConfigManager()
                        .getDefaultConfig(
                                flinkDeployment.getMetadata().getNamespace(),
                                spec.getFlinkVersion());
        conf.addAll(spec.getFlinkConfiguration().asConfiguration());
        return conf.get(KubernetesOperatorConfigOptions.POD_TEMPLATE_MERGE_BY_NAME);
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
     * <p>Call it after the reconciliation's other ConfigMap writes: those patch single entries,
     * while this write replaces the whole ConfigMap.
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
     * Whether the gates agreed on a cutover point in the current transition: from then on, the
     * standby skips the records at or after it. Written by the WATERMARK gate, see
     * WatermarkGateContext in the bluegreen client.
     */
    public static boolean isCutoverSet(BlueGreenContext context) {
        return getConfigMap(context).getData().containsKey(WATERMARK_TOGGLE_VALUE);
    }

    /**
     * Whether every gate subtask of the standby reports that it has nothing left to emit ({@link
     * GateMetrics#HAND_OVER_DONE}). The first standby subtask past the toggle sets
     * CLEAR_TO_TEARDOWN, but the others may still owe records. Always true outside ADVANCED mode.
     */
    public static boolean isHandOverDone(BlueGreenContext context, FlinkDeployment standby) {
        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
            return true;
        }
        var name = standby.getMetadata().getName();
        var jobId = standby.getStatus().getJobStatus().getJobId();
        if (jobId == null) {
            LOG.info("Waiting for the job of '{}' to report its gates", name);
            return false;
        }
        try {
            var ctx =
                    context.getCtxFactory().getResourceContext(standby, context.getJosdkContext());
            var done =
                    ctx.getFlinkService()
                            .getMinSubtaskMetrics(
                                    ctx.getObserveConfig(), jobId, GateMetrics.HAND_OVER_DONE);
            var pending =
                    done.entrySet().stream()
                            .filter(gate -> gate.getValue() < 1)
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());
            if (done.isEmpty() || !pending.isEmpty()) {
                LOG.info(
                        "Waiting for the gates of '{}' to finish the hand-over: {}",
                        name,
                        done.isEmpty() ? "none reported yet" : "pending " + pending);
                return false;
            }
            return true;
        } catch (Exception e) {
            LOG.warn("Could not read the gate metrics of '{}', retrying", name, e);
            return false;
        }
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
     *   <li>exactly one of {@link #WATERMARK_EXTRACTOR_KEYS} when strategy is WATERMARK and the
     *       gate is injected. With {@code bluegreen.gate.injection.enabled=false} the extractor
     *       passed to {@code WatermarkGateProcessFunction.create} is the extraction strategy, so
     *       none of them may be set: they would be ignored.
     *   <li>a gate timeout longer than the deployment deletion delay, and an upgrade mode other
     *       than STATELESS
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
        if (!"WATERMARK".equals(strategy)) {
            return Optional.of(
                    "[BlueGreen] Unknown gate strategy: '"
                            + strategy
                            + "'. Supported values: WATERMARK");
        }

        List<String> extractors =
                WATERMARK_EXTRACTOR_KEYS.stream()
                        .filter(flinkConfig::has)
                        .collect(Collectors.toList());
        boolean injected =
                !flinkConfig.has(GATE_INJECTION_ENABLED)
                        || Boolean.parseBoolean(flinkConfig.get(GATE_INJECTION_ENABLED).asText());
        if (injected && extractors.size() != 1) {
            return Optional.of(
                    "[BlueGreen] Gate strategy WATERMARK requires exactly one of "
                            + "'bluegreen.gate.watermark.extractor-class' (custom class), "
                            + "'bluegreen.gate.watermark.field-index' (column position, for SQL / "
                            + "Table API jobs) or 'bluegreen.gate.watermark.field-path' "
                            + "(dot-notation field, no app code needed) in flinkConfiguration, "
                            + "found: "
                            + (extractors.isEmpty() ? "none" : String.join(", ", extractors))
                            + ".");
        }
        if (!injected && !extractors.isEmpty()) {
            return Optional.of(
                    "[BlueGreen] With '"
                            + GATE_INJECTION_ENABLED
                            + "' set to false, the extractor passed to "
                            + "WatermarkGateProcessFunction.create(...) is the gate's extraction "
                            + "strategy. Remove "
                            + String.join(", ", extractors)
                            + " from flinkConfiguration: it would be ignored.");
        }

        long gateTimeout = getGateTimeout(context);
        long deletionDelay = getDeploymentDeletionDelay(context);
        if (gateTimeout <= deletionDelay) {
            return Optional.of(
                    "[BlueGreen] "
                            + BLUEGREEN_GATE_TIMEOUT.key()
                            + " ("
                            + gateTimeout
                            + " ms) must be longer than "
                            + BLUEGREEN_DEPLOYMENT_DELETION_DELAY.key()
                            + " ("
                            + deletionDelay
                            + " ms): the cutover point is the watermark plus that delay, so the"
                            + " gate cannot clear sooner.");
        }

        var job = context.getBgDeployment().getSpec().getTemplate().getSpec().getJob();
        if (job != null && job.getUpgradeMode() == UpgradeMode.STATELESS) {
            return Optional.of(
                    "[BlueGreen] ADVANCED mode needs the SAVEPOINT or LAST_STATE upgrade mode, not"
                            + " STATELESS: the new deployment must start from the transition"
                            + " savepoint of the one it replaces, and an abort after the cutover"
                            + " point redeploys the survivor from it. Otherwise data loss may"
                            + " occur.");
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
            FlinkDeployment flinkDeployment,
            ConfigObjectNode flinkConfig,
            String operatorImage,
            boolean mergeArraysByName) {
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

        if (!flinkConfig.has(GATE_INJECTION_ENABLED)) {
            flinkConfig.put(GATE_INJECTION_ENABLED, "true");
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

        injectAgentInitContainer(flinkDeployment, operatorImage, mergeArraysByName);
    }

    private static void injectAgentInitContainer(
            FlinkDeployment flinkDeployment, String operatorImage, boolean mergeArraysByName) {
        var spec = flinkDeployment.getSpec();

        if (spec.getJobManager() == null) {
            spec.setJobManager(new JobManagerSpec());
        }
        JobManagerSpec jmSpec = spec.getJobManager();

        // The JobManager pod template is merged into the common one at deployment, by array
        // position unless kubernetes.operator.pod-template.merge-arrays-by-name is set, so entries
        // appended to it could be merged into the common template's entries at the same positions.
        // Merging the common template in first, the same way, makes the JobManager template begin
        // with the common template's entries, which that merge leaves as they are, and puts the
        // agent's after them.
        PodTemplateSpec podTemplate =
                FlinkUtils.mergePodTemplates(
                        spec.getPodTemplate(), jmSpec.getPodTemplate(), mergeArraysByName);
        if (podTemplate == null) {
            podTemplate = new PodTemplateSpec();
        }
        jmSpec.setPodTemplate(podTemplate);
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
