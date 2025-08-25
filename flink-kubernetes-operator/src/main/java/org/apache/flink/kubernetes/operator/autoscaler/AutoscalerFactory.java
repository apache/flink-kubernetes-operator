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
import org.apache.flink.autoscaler.realizer.FlinkAutoscalerScalingRealizer;
import org.apache.flink.autoscaler.realizer.ScalingRealizer;
import org.apache.flink.kubernetes.operator.autoscaler.state.ConfigMapStore;
import org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.resources.ClusterResourceManager;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.PluginDiscoveryUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/** The factory of {@link JobAutoScaler}. */
public class AutoscalerFactory {

    private static Logger LOG = LoggerFactory.getLogger(AutoscalerFactory.class);

    public static JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> create(
            KubernetesClient client,
            EventRecorder eventRecorder,
            ClusterResourceManager clusterResourceManager,
            FlinkConfigManager configManager) {

        var stateStore = new KubernetesAutoScalerStateStore(new ConfigMapStore(client));
        var eventHandler = new KubernetesAutoScalerEventHandler(eventRecorder);

        Set<FlinkAutoscalerScalingRealizer> flinkAutoscalerScalingRealizers =
                PluginDiscoveryUtils.discoverResources(
                        configManager, FlinkAutoscalerScalingRealizer.class);

        flinkAutoscalerScalingRealizers.forEach(
                realizer -> {
                    LOG.info(
                            "Discovered resource from plugin directory {}",
                            realizer.getClass().getName());
                });

        ScalingRealizer scalingRealizer = new KubernetesScalingRealizer();

        if (!flinkAutoscalerScalingRealizers.isEmpty()) {
            scalingRealizer = flinkAutoscalerScalingRealizers.stream().findFirst().get();
        }

        return new JobAutoScalerImpl<>(
                new RestApiMetricsCollector<>(),
                new ScalingMetricEvaluator(),
                new ScalingExecutor<>(eventHandler, stateStore, clusterResourceManager),
                eventHandler,
                scalingRealizer,
                stateStore);
    }
}
