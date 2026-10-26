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
import org.apache.flink.kubernetes.operator.api.bluegreen.GateMetrics;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionMode;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;

import io.fabric8.kubernetes.api.model.ConfigMap;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.OperationNotSupportedException;

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

    // The WATERMARK gate's cutover point (WatermarkGateContext.WATERMARK_TOGGLE_VALUE)
    private static final String WATERMARK_TOGGLE_VALUE = "watermark-toggle-value";

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

        // The keys WatermarkGateProcessFunction.create reads from the job's configuration
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
     * Validates the configuration of a deployment in the ADVANCED transition mode: the gate timeout
     * must be longer than the deployment deletion delay, and the upgrade mode other than STATELESS.
     * Returns an error message if it is invalid, or {@link Optional#empty()} if it is valid.
     */
    public static Optional<String> validateAdvancedModeConfig(BlueGreenContext context) {
        if (TransitionMode.ADVANCED != getTransitionMode(context)) {
            return Optional.empty();
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
}
