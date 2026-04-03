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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateContext;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateKubernetesService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.TRANSITION_STAGE;
import static org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage.CLEAR_TO_TEARDOWN;

/** Base class for ProcessFunction (streaming) based Gate implementations. */
abstract class GateProcessFunction<I> extends ProcessFunction<I, I> implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(GateProcessFunction.class);

    protected final BlueGreenDeploymentType blueGreenDeploymentType;

    // How long (ms) to wait after a write condition is first detected before writing to the
    // ConfigMap. This window gives other subtasks a chance to observe the write via the informer,
    // reducing duplicate K8s API calls.
    private static final long WRITE_DEDUP_DELAY_MS = 500L;

    // Processing time (ms) when a pending write was first requested; -1 means no write pending.
    private long pendingWriteSince = -1L;
    // Set by notifyClearToTeardown() so that maybePerformPendingWrite() performs the teardown
    // write even if the pending write was originally scheduled for a different operation.
    private boolean pendingClearToTeardown = false;

    private GateKubernetesService gateKubernetesService;
    protected GateContext baseContext;
    private String namespace;
    private String configMapName;

    protected abstract void onContextUpdate(GateContext baseContext, Map<String, String> data);

    public GateProcessFunction(
            BlueGreenDeploymentType blueGreenDeploymentType,
            String namespace,
            String configMapName) {
        Preconditions.checkArgument(
                blueGreenDeploymentType == BlueGreenDeploymentType.BLUE
                        || blueGreenDeploymentType == BlueGreenDeploymentType.GREEN,
                "Invalid deployment type: " + blueGreenDeploymentType);

        this.blueGreenDeploymentType = blueGreenDeploymentType;
        this.namespace = namespace;
        this.configMapName = configMapName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        setKubernetesEnvironment();
        processConfigMap(gateKubernetesService.parseConfigMap());
    }

    /**
     * Records the current processing time as the start of a pending write window. No-op if a write
     * is already pending. The actual write is performed by {@link #maybePerformPendingWrite} on the
     * next element processed after the dedup delay has elapsed.
     */
    protected final void scheduleWriteTimer(Context ctx) {
        if (pendingWriteSince < 0) {
            pendingWriteSince = ctx.timerService().currentProcessingTime();
        }
    }

    protected abstract void processElementActive(
            I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws IllegalAccessException;

    protected abstract void processElementStandby(
            I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws IllegalAccessException;

    @Override
    public void processElement(I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws Exception {
        maybePerformPendingWrite(ctx);
        switch (baseContext.getOutputMode()) {
            case ACTIVE:
                processElementActive(value, ctx, out);
                break;
            case STANDBY:
                processElementStandby(value, ctx, out);
                break;
            default:
                String error = "Invalid OutputMode caught";
                logger.error(error);
                throw new IllegalStateException(error);
        }
    }

    private void maybePerformPendingWrite(Context ctx) throws Exception {
        if (pendingWriteSince < 0) {
            return;
        }
        long now = ctx.timerService().currentProcessingTime();
        if (now - pendingWriteSince < WRITE_DEDUP_DELAY_MS) {
            return;
        }
        pendingWriteSince = -1L;
        boolean handled = handleScheduledWrite(ctx);
        if (!handled && pendingClearToTeardown) {
            pendingClearToTeardown = false;
            if (baseContext.getGateStage() != CLEAR_TO_TEARDOWN) {
                logInfo("Writing " + CLEAR_TO_TEARDOWN + " to ConfigMap");
                gateKubernetesService.updateConfigMapEntries(
                        Map.of(TRANSITION_STAGE.getLabel(), CLEAR_TO_TEARDOWN.toString()));
                logInfo(CLEAR_TO_TEARDOWN + " set!");
            } else {
                logInfo(CLEAR_TO_TEARDOWN + " already set, skipping");
            }
        }
    }

    private void setKubernetesEnvironment() {
        this.gateKubernetesService = new GateKubernetesService(namespace, configMapName);

        logInfo("Preparing Informers...");
        var resourceEventHandler =
                new ResourceEventHandler<ConfigMap>() {
                    @Override
                    public void onAdd(ConfigMap obj) {
                        logger.warn("Unexpected ConfigMap added: " + obj);
                    }

                    @Override
                    public void onUpdate(ConfigMap oldObj, ConfigMap newObj) {
                        if (!oldObj.equals(newObj)) {
                            var oldState = oldObj.getData().get(TRANSITION_STAGE.getLabel());
                            var newState = newObj.getData().get(TRANSITION_STAGE.getLabel());

                            logInfo("Update notification 1: " + oldState + " to " + newState);

                            processConfigMap(newObj);
                        }
                    }

                    @Override
                    public void onDelete(ConfigMap obj, boolean deletedFinalStateUnknown) {
                        logger.error(
                                "ConfigMap deleted: "
                                        + obj
                                        + ", final state unknown: "
                                        + deletedFinalStateUnknown);
                    }
                };

        gateKubernetesService.setInformers(resourceEventHandler);
        logInfo("Informers set!");
    }

    private void processConfigMap(ConfigMap configMap) {
        this.baseContext = GateContext.create(configMap.getData(), blueGreenDeploymentType);

        // Filtering the "custom" entries only
        var baseKeys =
                Arrays.stream(GateContextOptions.values())
                        .map(GateContextOptions::getLabel)
                        .collect(Collectors.toSet());
        var allConfigMapKeys = new HashSet<>(configMap.getData().keySet());
        // Set difference:
        allConfigMapKeys.removeAll(baseKeys);

        var filteredData =
                configMap.getData().entrySet().stream()
                        .filter(kvp -> allConfigMapKeys.contains(kvp.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        onContextUpdate(baseContext, filteredData);
    }

    protected final void notifyClearToTeardown(Context ctx) {
        pendingClearToTeardown = true;
        scheduleWriteTimer(ctx);
    }

    /**
     * Hook for subclasses to handle watermark-specific ConfigMap writes when the dedup window
     * elapses. Returns {@code true} if the write was handled (base-class CLEAR_TO_TEARDOWN logic is
     * skipped), {@code false} to fall through.
     */
    protected boolean handleScheduledWrite(Context ctx) throws Exception {
        return false;
    }

    protected final void updateConfigMapCustomEntries(Map<String, String> customEntries)
            throws Exception {
        // Validating only "custom" entries/keys can be updated
        var keysToUpdate = customEntries.keySet();
        var baseContextKeys =
                Arrays.stream(GateContextOptions.values())
                        .map(GateContextOptions::getLabel)
                        .collect(Collectors.toCollection(HashSet::new));
        // Set intersection:
        baseContextKeys.retainAll(keysToUpdate);

        if (!baseContextKeys.isEmpty()) {
            var error = "Attempted to update read-only base keys" + baseContextKeys;
            logger.error(error);
            throw new IllegalAccessException(error);
        }
        logInfo("Updating custom entries: " + customEntries);
        int maxRetries = 3;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                gateKubernetesService.updateConfigMapEntries(customEntries);
                return;
            } catch (KubernetesClientException e) {
                if (e.getCode() == 409 && attempt < maxRetries) {
                    logInfo(
                            "ConfigMap update conflict on attempt "
                                    + (attempt + 1)
                                    + ", retrying...");
                } else {
                    throw e;
                }
            }
        }
    }

    // Temporary "utility" function for development
    protected void logInfo(String message) {
        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        logger.error("[BlueGreen Gate-" + subtaskIdx + "]:" + message);
    }
}
