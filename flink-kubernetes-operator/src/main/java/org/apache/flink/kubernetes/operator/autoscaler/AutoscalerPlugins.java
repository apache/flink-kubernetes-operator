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

import org.apache.flink.autoscaler.ScalingExecutorPlugin;
import org.apache.flink.autoscaler.alignment.ParallelismAlignmentMode;
import org.apache.flink.autoscaler.metrics.ScalingMetricsEvaluatorPlugin;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.kubernetes.operator.utils.AutoscalerUtils;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;

import java.util.Collection;

/** The autoscaler SPI implementations discovered from the operator's plugin manager. */
@Getter
public class AutoscalerPlugins {

    private final Collection<ScalingMetricsEvaluatorPlugin> evaluators;
    private final Collection<ScalingExecutorPlugin<ResourceID>> scalingExecutors;
    private final Collection<ParallelismAlignmentMode> alignmentModes;

    private AutoscalerPlugins(
            Collection<ScalingMetricsEvaluatorPlugin> evaluators,
            Collection<ScalingExecutorPlugin<ResourceID>> scalingExecutors,
            Collection<ParallelismAlignmentMode> alignmentModes) {
        this.evaluators = evaluators;
        this.scalingExecutors = scalingExecutors;
        this.alignmentModes = alignmentModes;
    }

    /**
     * Discovers all autoscaler plugins from the given plugin manager.
     *
     * @param pluginManager The shared operator plugin manager used for discovery.
     * @return The discovered plugins.
     */
    public static AutoscalerPlugins discover(PluginManager pluginManager) {
        return new AutoscalerPlugins(
                AutoscalerUtils.discoverCustomEvaluators(pluginManager),
                AutoscalerUtils.discoverCustomScalingExecutors(pluginManager),
                AutoscalerUtils.discoverCustomAlignmentModes(pluginManager));
    }
}
