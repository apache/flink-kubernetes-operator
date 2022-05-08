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

package org.apache.flink.kubernetes.operator.observer.deployment;

import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.Mode;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.StatusHelper;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The factory to create the observer based ob the {@link FlinkDeployment} mode. */
public class ObserverFactory {

    private final KubernetesClient kubernetesClient;
    private final FlinkService flinkService;
    private final FlinkConfigManager configManager;
    private final StatusHelper<FlinkDeploymentStatus> statusHelper;
    private final Map<Mode, Observer<FlinkDeployment>> observerMap;

    public ObserverFactory(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager,
            StatusHelper<FlinkDeploymentStatus> statusHelper) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
        this.configManager = configManager;
        this.statusHelper = statusHelper;
        this.observerMap = new ConcurrentHashMap<>();
    }

    public Observer<FlinkDeployment> getOrCreate(FlinkDeployment flinkApp) {
        return observerMap.computeIfAbsent(
                Mode.getMode(flinkApp),
                mode -> {
                    switch (mode) {
                        case SESSION:
                            return new SessionObserver(
                                    kubernetesClient, flinkService, configManager);
                        case APPLICATION:
                            return new ApplicationObserver(
                                    kubernetesClient, flinkService, configManager, statusHelper);
                        default:
                            throw new UnsupportedOperationException(
                                    String.format("Unsupported running mode: %s", mode));
                    }
                });
    }
}
