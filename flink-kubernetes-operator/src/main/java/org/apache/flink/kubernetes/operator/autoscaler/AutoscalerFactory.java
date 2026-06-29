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
import org.apache.flink.autoscaler.ScalingExecutorPlugin;
import org.apache.flink.autoscaler.ScalingMetricEvaluator;
import org.apache.flink.autoscaler.alignment.ParallelismAlignmentMode;
import org.apache.flink.autoscaler.metrics.ScalingMetricsEvaluatorPlugin;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.kubernetes.operator.autoscaler.state.ConfigMapStore;
import org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore;
import org.apache.flink.kubernetes.operator.resources.ClusterResourceManager;
import org.apache.flink.kubernetes.operator.utils.AutoscalerUtils;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import java.util.Collection;

/** The factory of {@link JobAutoScaler}. */
public class AutoscalerFactory {

    public static JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> create(
            KubernetesClient client,
            EventRecorder eventRecorder,
            ClusterResourceManager clusterResourceManager,
            PluginManager pluginManager) {

        var stateStore = new KubernetesAutoScalerStateStore(new ConfigMapStore(client));
        var eventHandler = new KubernetesAutoScalerEventHandler(eventRecorder);

        // Autoscaler SPI integrations sharing the operator's process-wide plugin manager.
        Collection<ScalingMetricsEvaluatorPlugin> customEvaluators =
                AutoscalerUtils.discoverCustomEvaluators(pluginManager);
        Collection<ScalingExecutorPlugin<ResourceID, KubernetesJobAutoScalerContext>>
                customExecutors = AutoscalerUtils.discoverCustomScalingExecutors(pluginManager);
        Collection<ParallelismAlignmentMode> customAlignmentModes =
                AutoscalerUtils.discoverCustomAlignmentModes(pluginManager);

        return new JobAutoScalerImpl<>(
                new RestApiMetricsCollector<>(),
                new ScalingMetricEvaluator(customEvaluators),
                new ScalingExecutor<>(
                        eventHandler,
                        stateStore,
                        clusterResourceManager,
                        customExecutors,
                        customAlignmentModes),
                eventHandler,
                new KubernetesScalingRealizer(),
                stateStore);
    }
}
