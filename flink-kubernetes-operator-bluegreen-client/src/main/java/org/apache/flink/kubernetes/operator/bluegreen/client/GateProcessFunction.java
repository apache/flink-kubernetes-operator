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

    // TODO: make this configurable? This cannot be a constant
    protected final int subtaskIndexGuide = 1;

    protected GateKubernetesService gateKubernetesService;
    protected Boolean clearToTeardown = false;
    protected GateContext baseContext;
    private String namespace;
    private String configMapName;

    protected abstract void onContextUpdate(GateContext baseContext, Map<String, String> data);

    public GateProcessFunction(
            BlueGreenDeploymentType blueGreenDeploymentType,
            String namespace,
            String configMapName,
            GateKubernetesService gateKubernetesService) {
        Preconditions.checkArgument(
                blueGreenDeploymentType == BlueGreenDeploymentType.BLUE
                        || blueGreenDeploymentType == BlueGreenDeploymentType.GREEN,
                "Invalid deployment type: " + blueGreenDeploymentType);

        this.blueGreenDeploymentType = blueGreenDeploymentType;
        this.namespace = namespace;
        this.configMapName = configMapName;
        this.gateKubernetesService = gateKubernetesService;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Only create service if not injected
        if (gateKubernetesService == null) {
            this.gateKubernetesService = new GateKubernetesService(namespace, configMapName);
        }

        // Always set up informers, whether service was injected or created
        setKubernetesEnvironment();
        processConfigMap(gateKubernetesService.parseConfigMap());
    }

    protected abstract void processElementActive(
            I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws IllegalAccessException;

    protected abstract void processElementStandby(
            I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws IllegalAccessException;

    @Override
    public void processElement(I value, ProcessFunction<I, I>.Context ctx, Collector<I> out)
            throws IllegalAccessException {
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

    private void setKubernetesEnvironment() {
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

    protected void notifyClearToTeardown() {
        // Notify only once
        if (!clearToTeardown && getRuntimeContext().getIndexOfThisSubtask() == subtaskIndexGuide) {
            logInfo("Setting " + CLEAR_TO_TEARDOWN);
            performConfigMapUpdate(
                    Map.of(TRANSITION_STAGE.getLabel(), CLEAR_TO_TEARDOWN.toString()));
            logInfo(CLEAR_TO_TEARDOWN + " set!");
            this.clearToTeardown = true;
        }
    }

    /**
     * Template method for updating ConfigMap entries. Override in tests to avoid actual K8s
     * updates.
     */
    protected void performConfigMapUpdate(Map<String, String> updates) {
        gateKubernetesService.updateConfigMapEntries(updates);
    }

    protected void updateConfigMapCustomEntries(Map<String, String> customEntries)
            throws IllegalAccessException {
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
        performConfigMapUpdate(customEntries);
    }

    // Temporary "utility" function for development
    protected void logInfo(String message) {
        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        logger.error("[BlueGreen Gate-" + subtaskIdx + "]:" + message);
    }
}
