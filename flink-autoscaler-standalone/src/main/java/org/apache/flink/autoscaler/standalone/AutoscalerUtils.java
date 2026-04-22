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

package org.apache.flink.autoscaler.standalone;

import org.apache.flink.autoscaler.metrics.FlinkAutoscalerEvaluator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** Autoscaler related utility methods for the standalone autoscaler. */
public class AutoscalerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerUtils.class);

    /**
     * Discovers custom evaluators for the standalone autoscaler via Java's {@link ServiceLoader}
     * mechanism. Implementations must be registered under {@code
     * META-INF/services/org.apache.flink.autoscaler.metrics.FlinkAutoscalerEvaluator} on the
     * classpath.
     *
     * @return A map of discovered custom evaluators keyed by the name returned by {@link
     *     FlinkAutoscalerEvaluator#getName()}. If two implementations report the same name, the
     *     later one overwrites the earlier one and a warning is logged.
     */
    public static Map<String, FlinkAutoscalerEvaluator> discoverCustomEvaluators() {
        Map<String, FlinkAutoscalerEvaluator> customEvaluators = new HashMap<>();
        ServiceLoader.load(FlinkAutoscalerEvaluator.class)
                .forEach(
                        customEvaluator -> {
                            String customEvaluatorName = customEvaluator.getName();
                            LOG.info(
                                    "Discovered custom evaluator via ServiceLoader: {} (name={}).",
                                    customEvaluator.getClass().getName(),
                                    customEvaluatorName);
                            if (customEvaluators.containsKey(customEvaluatorName)) {
                                LOG.warn(
                                        "Duplicate custom evaluator name [{}] detected. Overwriting existing [{}] with [{}].",
                                        customEvaluatorName,
                                        customEvaluators
                                                .get(customEvaluatorName)
                                                .getClass()
                                                .getName(),
                                        customEvaluator.getClass().getName());
                            }
                            customEvaluators.put(customEvaluatorName, customEvaluator);
                        });
        return customEvaluators;
    }
}
