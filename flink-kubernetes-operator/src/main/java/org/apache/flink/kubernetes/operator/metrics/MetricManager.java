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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.metrics.lifecycle.LifecycleMetrics;

import io.fabric8.kubernetes.client.CustomResource;

import java.util.ArrayList;
import java.util.List;

/** Metric manager for Operator managed custom resources. */
public class MetricManager<CR extends CustomResource<?, ?>> {
    private final List<CustomResourceMetrics<CR>> registeredMetrics = new ArrayList<>();

    public void onUpdate(CR cr) {
        registeredMetrics.forEach(m -> m.onUpdate(cr));
    }

    public void onRemove(CR cr) {
        registeredMetrics.forEach(m -> m.onRemove(cr));
    }

    public void register(CustomResourceMetrics<CR> metrics) {
        registeredMetrics.add(metrics);
    }

    public static MetricManager<FlinkDeployment> createFlinkDeploymentMetricManager(
            Configuration conf, KubernetesOperatorMetricGroup metricGroup) {
        MetricManager<FlinkDeployment> metricManager = new MetricManager<>();
        registerFlinkDeploymentMetrics(conf, metricGroup, metricManager);
        registerLifecycleMetrics(conf, metricGroup, metricManager);
        return metricManager;
    }

    public static MetricManager<FlinkSessionJob> createFlinkSessionJobMetricManager(
            Configuration conf, KubernetesOperatorMetricGroup metricGroup) {
        MetricManager<FlinkSessionJob> metricManager = new MetricManager<>();
        registerFlinkSessionJobMetrics(conf, metricGroup, metricManager);
        registerLifecycleMetrics(conf, metricGroup, metricManager);
        return metricManager;
    }

    public static MetricManager<FlinkStateSnapshot> createFlinkStateSnapshotMetricManager(
            Configuration conf, KubernetesOperatorMetricGroup metricGroup) {
        MetricManager<FlinkStateSnapshot> metricManager = new MetricManager<>();
        registerFlinkStateSnapshotMetrics(conf, metricGroup, metricManager);
        return metricManager;
    }

    private static void registerFlinkDeploymentMetrics(
            Configuration conf,
            KubernetesOperatorMetricGroup metricGroup,
            MetricManager<FlinkDeployment> metricManager) {
        if (conf.get(KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED)) {
            metricManager.register(new FlinkDeploymentMetrics(metricGroup, conf));
        }
    }

    private static void registerFlinkSessionJobMetrics(
            Configuration conf,
            KubernetesOperatorMetricGroup metricGroup,
            MetricManager<FlinkSessionJob> metricManager) {
        if (conf.get(KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED)) {
            metricManager.register(new FlinkSessionJobMetrics(metricGroup, conf));
        }
    }

    private static void registerFlinkStateSnapshotMetrics(
            Configuration conf,
            KubernetesOperatorMetricGroup metricGroup,
            MetricManager<FlinkStateSnapshot> metricManager) {
        if (conf.get(KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED)) {
            metricManager.register(new FlinkStateSnapshotMetrics(metricGroup, conf));
        }
    }

    private static <CR extends AbstractFlinkResource<?, ?>> void registerLifecycleMetrics(
            Configuration conf,
            KubernetesOperatorMetricGroup metricGroup,
            MetricManager<CR> metricManager) {
        if (conf.get(KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED)
                && conf.get(KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED)) {
            metricManager.register(new LifecycleMetrics<>(conf, metricGroup));
        }
    }

    @VisibleForTesting
    public List<CustomResourceMetrics<CR>> getRegisteredMetrics() {
        return registeredMetrics;
    }
}
