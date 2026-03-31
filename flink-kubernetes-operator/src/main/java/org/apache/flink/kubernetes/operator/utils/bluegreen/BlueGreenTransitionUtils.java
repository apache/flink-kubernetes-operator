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
                        IS_FIRST_DEPLOYMENT.getLabel(),
                        isFirstDeployment ? "true" : "false",
                        GateContextOptions.DEPLOYMENT_DELETION_DELAY.getLabel(),
                        String.valueOf(getDeploymentDeletionDelay(context)),
                        ACTIVE_DEPLOYMENT_TYPE.getLabel(),
                        blueGreenDeploymentType.toString(),
                        TRANSITION_STAGE.getLabel(),
                        TransitionStage.INITIALIZING.toString());

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

            String operatorImage = System.getenv("OPERATOR_IMAGE");
            if (operatorImage != null && !operatorImage.isEmpty()) {
                injectAgentInitContainer(flinkDeployment, operatorImage);
            } else {
                LOG.warn(
                        "[BlueGreen] OPERATOR_IMAGE env var not set — "
                                + "agent JAR init-container will not be injected");
            }
        }
    }

    public static void moveToFirstTransitionStage(BlueGreenContext context) {
        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
            return;
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
        }
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

    public static void rollbackActiveDeploymentType(
            BlueGreenContext context, FlinkBlueGreenDeploymentState previousState) {

        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
            return;
        }

        var previousDeploymentType =
                previousState.name().contains("BLUE")
                        ? BlueGreenDeploymentType.BLUE
                        : BlueGreenDeploymentType.GREEN;

        updateConfigMapEntry(
                context, ACTIVE_DEPLOYMENT_TYPE.getLabel(), previousDeploymentType.toString());
    }

    public static void updateTransitionStageFromJobStatus(
            BlueGreenContext context, JobStatus jobStatus) {

        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
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
                                        "cp /opt/flink/artifacts/bluegreen-agent.jar"
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
