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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.autoscaler.metrics.FlinkAutoscalerEvaluator;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Autoscaler related utility methods for Operator. */
public class AutoscalerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerUtils.class);

    /**
     * Discovers custom metric evaluators for autoscaler.
     *
     * @param conf Base FlinkConfigManager configuration
     * @return The list of discovered custom metric evaluators.
     */
    public static Collection<FlinkAutoscalerEvaluator> discoverCustomEvaluators(
            Configuration conf) {
        List<FlinkAutoscalerEvaluator> customEvaluators = new ArrayList<>();
        PluginUtils.createPluginManagerFromRootFolder(conf)
                .load(FlinkAutoscalerEvaluator.class)
                .forEachRemaining(
                        customEvaluator -> {
                            LOG.info(
                                    "Discovered custom metric evaluator for autoscaler from plugin directory[{}]: {}.",
                                    System.getenv()
                                            .getOrDefault(
                                                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                                    ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS),
                                    customEvaluator.getClass().getName());
                            customEvaluators.add(customEvaluator);
                        });
        return customEvaluators;
    }
}
