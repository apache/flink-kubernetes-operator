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

package org.apache.flink.kubernetes.operator.bluegreen.client;

import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateContext;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateKubernetesService;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

/** Watermark based GateProcessFunction (streaming). */
public class WatermarkGateProcessFunction<I> extends GateProcessFunction<I>
        implements Serializable {

    private final Function<I, Long> watermarkExtractor;

    protected WatermarkGateContext currentWatermarkGateContext;

    protected Boolean waitingForWatermark = false;

    WatermarkGateProcessFunction(
            BlueGreenDeploymentType blueGreenDeploymentType,
            String namespace,
            String configMapName,
            Function<I, Long> watermarkExtractor) {
        this(blueGreenDeploymentType, namespace, configMapName, watermarkExtractor, null);
    }

    WatermarkGateProcessFunction(
            BlueGreenDeploymentType blueGreenDeploymentType,
            String namespace,
            String configMapName,
            Function<I, Long> watermarkExtractor,
            GateKubernetesService gateKubernetesService) {
        super(blueGreenDeploymentType, namespace, configMapName, gateKubernetesService);

        Preconditions.checkNotNull(watermarkExtractor);

        this.watermarkExtractor = watermarkExtractor;
    }

    public static <I> WatermarkGateProcessFunction<I> create(
            Map<String, String> flinkConfig, Function<I, Long> watermarkExtractor) {
        return new WatermarkGateProcessFunction<I>(
                BlueGreenDeploymentType.valueOf(
                        flinkConfig.get("bluegreen.active-deployment-type")),
                flinkConfig.get("kubernetes.namespace"),
                flinkConfig.get("bluegreen.configmap.name"),
                watermarkExtractor);
    }

    @Override
    protected void onContextUpdate(GateContext baseContext, Map<String, String> data) {
        var fetchedWatermarkContext = WatermarkGateContext.create(baseContext, data);
        logInfo("Refreshing WatermarkGateContext with data: " + data);

        if (currentWatermarkGateContext == null) {
            logInfo("currentWatermarkGateContext INITIALIZED: " + fetchedWatermarkContext);
            currentWatermarkGateContext = fetchedWatermarkContext;
        } else if (!currentWatermarkGateContext.equals(fetchedWatermarkContext)) {
            logInfo("currentWatermarkGateContext UPDATED: " + fetchedWatermarkContext);
            currentWatermarkGateContext = fetchedWatermarkContext;
        }
    }

    @Override
    protected void processElementActive(
            I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws IllegalAccessException {
        Long wmToggleValue = currentWatermarkGateContext.getWatermarkToggleValue();
        if (wmToggleValue != null) {
            Long extractedWatermark = watermarkExtractor.apply(value);
            if (wmToggleValue <= extractedWatermark) {
                // Normal
                out.collect(value);
            } else {
                // Waiting for WM
                logInfo(
                        " -- Waiting to Reach WM: "
                                + (wmToggleValue - extractedWatermark)
                                + " ms - ");
            }
        } else {
            // Transitioning to Active
            var currentGateStage = currentWatermarkGateContext.getBaseContext().getGateStage();
            if (currentGateStage == TransitionStage.TRANSITIONING) {
                logInfo(" -- Waiting for WM to be set - ");
                notifyWaitingForWatermark();
            } else {
                logInfo("Waiting for the TRANSITIONING state, current: " + currentGateStage);
            }
        }
    }

    @Override
    protected void processElementStandby(
            I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws IllegalAccessException {
        if (currentWatermarkGateContext.getWatermarkToggleValue() != null) {
            var watermarkToggleValue = currentWatermarkGateContext.getWatermarkToggleValue();

            if (getWatermarkBoundary(ctx) <= watermarkToggleValue) {
                if (watermarkToggleValue > watermarkExtractor.apply(value)) {
                    // Should still output the element
                    out.collect(value);
                } else {
                    // Went past the Watermark toggle value: BLOCK ELEMENT
                    logInfo(" -- Past WM -- ");
                }
            } else {
                // Went past the Watermark Boundary: BLOCK ELEMENT
                logInfo(" -- Past WM Boundary -- ");
                notifyClearToTeardown();
            }
        } else {
            // This ACTIVE job is transitioning to STANDBY, output elements
            out.collect(value);
            // Set the watermark when the other new job is ready
            updateWatermarkInConfigMap(ctx);
        }
    }

    private long getWatermarkBoundary(ProcessFunction<I, I>.Context ctx) {
        return ctx.timerService().currentWatermark() > 0
                ? ctx.timerService().currentWatermark()
                : ctx.timerService().currentProcessingTime();
    }

    private void updateWatermarkInConfigMap(ProcessFunction<I, I>.Context ctx)
            throws IllegalAccessException {
        var curWmCtx = currentWatermarkGateContext;

        if (getRuntimeContext().getIndexOfThisSubtask() == subtaskIndexGuide
                && curWmCtx.getWatermarkGateStage() == WatermarkGateStage.WAITING_FOR_WATERMARK) {
            var nextWatermarkToggleValue =
                    getWatermarkBoundary(ctx)
                            + curWmCtx.getBaseContext().getDeploymentTeardownDelayMs();

            // Setting the value in advance to avoid subsequent elements in this subtask from
            // setting it
            // while the changes get reflected in Kubernetes
            currentWatermarkGateContext.setWatermarkToggleValue(nextWatermarkToggleValue);

            logInfo("Updating the ConfigMap Watermark value to: " + nextWatermarkToggleValue);
            updateConfigMapCustomEntries(
                    Map.of(
                            WatermarkGateContext.WATERMARK_TOGGLE_VALUE,
                                    Long.toString(nextWatermarkToggleValue),
                            WatermarkGateContext.WATERMARK_STAGE,
                                    WatermarkGateStage.WATERMARK_SET.toString()));
            logInfo("Watermark updated!");
        }
    }

    private void notifyWaitingForWatermark() throws IllegalAccessException {
        if (!waitingForWatermark
                && getRuntimeContext().getIndexOfThisSubtask() == subtaskIndexGuide) {
            logInfo("Setting " + WatermarkGateStage.WAITING_FOR_WATERMARK);
            updateConfigMapCustomEntries(
                    Map.of(
                            WatermarkGateContext.WATERMARK_STAGE,
                            WatermarkGateStage.WAITING_FOR_WATERMARK.toString()));
            logInfo(WatermarkGateStage.WAITING_FOR_WATERMARK + " set!");
            waitingForWatermark = true;
        }
    }
}
