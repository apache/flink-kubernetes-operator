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
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;

import io.fabric8.kubernetes.api.model.ConfigMap;
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
        TransitionMode transitionMode =
                context.getBgDeployment().getSpec().getTemplate().getTransitionMode();

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
}
