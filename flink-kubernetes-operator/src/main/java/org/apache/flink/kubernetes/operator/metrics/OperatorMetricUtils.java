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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.util.MetricUtils;

/** Utility class for flink based operator metrics. */
public class OperatorMetricUtils {

    private static final String ENV_HOSTNAME = "HOSTNAME";
    private static final String ENV_OPERATOR_NAME = "OPERATOR_NAME";
    private static final String ENV_OPERATOR_NAMESPACE = "OPERATOR_NAMESPACE";

    public static void initOperatorMetrics(Configuration configuration) {
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
        MetricRegistry metricRegistry = createMetricRegistry(configuration, pluginManager);
        KubernetesOperatorMetricGroup operatorMetricGroup =
                KubernetesOperatorMetricGroup.create(
                        metricRegistry,
                        configuration,
                        System.getenv().getOrDefault(ENV_OPERATOR_NAMESPACE, "default"),
                        System.getenv().getOrDefault(ENV_OPERATOR_NAME, "flink-operator"),
                        System.getenv().getOrDefault(ENV_HOSTNAME, "localhost"));
        MetricGroup statusGroup = operatorMetricGroup.addGroup("Status");
        MetricUtils.instantiateStatusMetrics(statusGroup);
    }

    private static MetricRegistryImpl createMetricRegistry(
            Configuration configuration, PluginManager pluginManager) {
        return new MetricRegistryImpl(
                MetricRegistryConfiguration.fromConfiguration(configuration, Long.MAX_VALUE),
                ReporterSetup.fromConfiguration(configuration, pluginManager));
    }
}
