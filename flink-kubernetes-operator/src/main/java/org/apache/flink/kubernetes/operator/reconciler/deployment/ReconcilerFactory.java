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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.Mode;
import org.apache.flink.kubernetes.operator.reconciler.Reconciler;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** The factory to create reconciler based on app mode. */
public class ReconcilerFactory {

    private final KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager;
    private final EventRecorder eventRecorder;
    private final StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> deploymentStatusRecorder;
    private final JobAutoScalerFactory autoscalerFactory;
    private final Map<Tuple2<Mode, KubernetesDeploymentMode>, Reconciler<FlinkDeployment>>
            reconcilerMap;

    public ReconcilerFactory(
            KubernetesClient kubernetesClient,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder,
            StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> deploymentStatusRecorder,
            JobAutoScalerFactory autoscalerFactory) {
        this.kubernetesClient = kubernetesClient;
        this.configManager = configManager;
        this.eventRecorder = eventRecorder;
        this.deploymentStatusRecorder = deploymentStatusRecorder;
        this.autoscalerFactory = autoscalerFactory;
        this.reconcilerMap = new ConcurrentHashMap<>();
    }

    public Reconciler<FlinkDeployment> getOrCreate(FlinkDeployment flinkApp) {
        return reconcilerMap.computeIfAbsent(
                Tuple2.of(
                        Mode.getMode(flinkApp),
                        KubernetesDeploymentMode.getDeploymentMode(flinkApp)),
                modes -> {
                    switch (modes.f0) {
                        case SESSION:
                            return new SessionReconciler(
                                    kubernetesClient, eventRecorder, deploymentStatusRecorder);
                        case APPLICATION:
                            return new ApplicationReconciler(
                                    kubernetesClient,
                                    eventRecorder,
                                    deploymentStatusRecorder,
                                    autoscalerFactory);
                        default:
                            throw new UnsupportedOperationException(
                                    String.format("Unsupported running mode: %s", modes.f0));
                    }
                });
    }
}
