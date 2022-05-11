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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.Mode;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.informer.InformerManager;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The factory to create reconciler based on app mode. */
public class ReconcilerFactory {

    private final KubernetesClient kubernetesClient;
    private final FlinkService flinkService;
    private final FlinkConfigManager configManager;
    private final Map<Mode, Reconciler<FlinkDeployment>> reconcilerMap;
    private final InformerManager informerManager;

    public ReconcilerFactory(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager,
            InformerManager informerManager) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
        this.configManager = configManager;
        this.reconcilerMap = new ConcurrentHashMap<>();
        this.informerManager = informerManager;
    }

    public Reconciler<FlinkDeployment> getOrCreate(FlinkDeployment flinkApp) {
        return reconcilerMap.computeIfAbsent(
                Mode.getMode(flinkApp),
                mode -> {
                    switch (mode) {
                        case SESSION:
                            return new SessionReconciler(
                                    kubernetesClient, flinkService, configManager, informerManager);
                        case APPLICATION:
                            return new ApplicationReconciler(
                                    kubernetesClient, flinkService, configManager);
                        default:
                            throw new UnsupportedOperationException(
                                    String.format("Unsupported running mode: %s", mode));
                    }
                });
    }
}
