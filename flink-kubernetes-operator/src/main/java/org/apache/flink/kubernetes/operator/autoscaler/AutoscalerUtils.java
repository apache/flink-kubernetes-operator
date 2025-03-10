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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.autoscaler.metrics.CustomEvaluator;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Autoscaler related utility methods for Operator. */
public class AutoscalerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerUtils.class);

    /**
     * discovers custom evaluator's for autoscaler.
     *
     * @param configManager Flink Config manager
     * @return A map of discovered custom evaluators, where the key is the fully qualified class
     *     name of the custom evaluator and the value is the corresponding instance.
     */
    public static Map<String, CustomEvaluator> discoverCustomEvaluators(
            FlinkConfigManager configManager) {
        var conf = configManager.getDefaultConfig();
        Map<String, CustomEvaluator> customEvaluators = new HashMap<>();

        PluginUtils.createPluginManagerFromRootFolder(conf)
                .load(CustomEvaluator.class)
                .forEachRemaining(
                        customEvaluator -> {
                            String customEvaluatorClass = customEvaluator.getClass().getName();
                            LOG.info(
                                    "Discovered custom evaluator for autoscaler from plugin directory[{}]: {}.",
                                    System.getenv()
                                            .getOrDefault(
                                                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                                    ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS),
                                    customEvaluatorClass);
                            customEvaluator.configure(conf);
                            customEvaluators.put(customEvaluatorClass, customEvaluator);
                        });
        return customEvaluators;
    }
}
