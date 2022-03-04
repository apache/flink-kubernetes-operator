/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The factory to create reconciler based on app mode. */
public class FlinkReconcilerFactory {

    private final KubernetesClient kubernetesClient;
    private final FlinkService flinkService;
    private final FlinkOperatorConfiguration operatorConfiguration;
    private final Map<Mode, FlinkReconciler> reconcilerMap;

    public FlinkReconcilerFactory(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkOperatorConfiguration operatorConfiguration) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
        this.operatorConfiguration = operatorConfiguration;
        this.reconcilerMap = new ConcurrentHashMap<>();
    }

    public FlinkReconciler getOrCreate(FlinkDeployment flinkApp) {
        return reconcilerMap.computeIfAbsent(
                getMode(flinkApp),
                mode -> {
                    switch (mode) {
                        case SESSION:
                            return new SessionReconciler(
                                    kubernetesClient, flinkService, operatorConfiguration);
                        case APPLICATION:
                            return new JobReconciler(
                                    kubernetesClient, flinkService, operatorConfiguration);
                        default:
                            throw new UnsupportedOperationException(
                                    String.format("Unsupported running mode: %s", mode));
                    }
                });
    }

    private Mode getMode(FlinkDeployment flinkApp) {
        return flinkApp.getSpec().getJob() != null ? Mode.APPLICATION : Mode.SESSION;
    }

    enum Mode {
        APPLICATION,
        SESSION
    }
}
