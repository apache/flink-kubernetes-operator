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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.JobAutoScalerImpl;
import org.apache.flink.autoscaler.RestApiMetricsCollector;
import org.apache.flink.autoscaler.ScalingExecutor;
import org.apache.flink.autoscaler.ScalingMetricEvaluator;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.kubernetes.operator.autoscaler.state.ConfigMapStore;
import org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.resources.ClusterResourceManager;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import static org.apache.flink.kubernetes.operator.utils.AutoscalerUtils.discoverOrDefault;

/** The factory of {@link JobAutoScaler}. */
public class AutoscalerFactory {

    @SuppressWarnings("unchecked")
    public static JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> create(
            KubernetesClient client,
            EventRecorder eventRecorder,
            ClusterResourceManager clusterResourceManager,
            FlinkConfigManager configManager) {

        var conf = configManager.getDefaultConfig();

        AutoScalerStateStore<ResourceID, KubernetesJobAutoScalerContext> stateStore =
                discoverOrDefault(
                        conf,
                        AutoScalerStateStore.class,
                        () -> new KubernetesAutoScalerStateStore(new ConfigMapStore(client)));

        AutoScalerEventHandler<ResourceID, KubernetesJobAutoScalerContext> eventHandler =
                discoverOrDefault(
                        conf,
                        AutoScalerEventHandler.class,
                        () -> new KubernetesAutoScalerEventHandler(eventRecorder));

        ScalingRealizer<ResourceID, KubernetesJobAutoScalerContext> scalingRealizer =
                discoverOrDefault(conf, ScalingRealizer.class, KubernetesScalingRealizer::new);

        return new JobAutoScalerImpl<>(
                new RestApiMetricsCollector<>(),
                new ScalingMetricEvaluator(),
                new ScalingExecutor<>(eventHandler, stateStore, clusterResourceManager),
                eventHandler,
                scalingRealizer,
                stateStore);
    }
}
