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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.metrics.lifecycle.LifecycleMetrics;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Metric manager for Operator managed custom resources. */
public class MetricManager<CR extends AbstractFlinkResource<?, ?>> {
    private final KubernetesOperatorMetricGroup opMetricGroup;
    private final FlinkConfigManager configManager;
    private final Map<String, CustomResourceMetrics> metrics = new ConcurrentHashMap<>();

    private final LifecycleMetrics<CR> lifeCycleMetrics;

    public MetricManager(
            KubernetesOperatorMetricGroup opMetricGroup, FlinkConfigManager configManager) {
        this.opMetricGroup = opMetricGroup;
        this.configManager = configManager;

        if (configManager
                .getDefaultConfig()
                .get(KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED)) {
            this.lifeCycleMetrics =
                    new LifecycleMetrics<>(configManager, Clock.systemDefaultZone(), opMetricGroup);
        } else {
            this.lifeCycleMetrics = null;
        }
    }

    public void onUpdate(CR cr) {
        getCustomResourceMetrics(cr).onUpdate(cr);
        if (lifeCycleMetrics != null) {
            lifeCycleMetrics.onUpdate(cr);
        }
    }

    public void onRemove(CR cr) {
        getCustomResourceMetrics(cr).onRemove(cr);
        if (lifeCycleMetrics != null) {
            lifeCycleMetrics.onRemove(cr);
        }
    }

    private CustomResourceMetrics getCustomResourceMetrics(CR cr) {
        return metrics.computeIfAbsent(
                cr.getMetadata().getNamespace(), k -> getCustomResourceMetricsImpl(cr));
    }

    private CustomResourceMetrics getCustomResourceMetricsImpl(CR cr) {
        var namespaceMg =
                opMetricGroup.createResourceNamespaceGroup(
                        configManager.getDefaultConfig(), cr.getMetadata().getNamespace());
        if (cr instanceof FlinkDeployment) {
            return new FlinkDeploymentMetrics(namespaceMg);
        } else if (cr instanceof FlinkSessionJob) {
            return new FlinkSessionJobMetrics(namespaceMg);
        } else {
            throw new IllegalArgumentException("Unknown CustomResource");
        }
    }

    @VisibleForTesting
    public LifecycleMetrics<CR> getLifeCycleMetrics() {
        return lifeCycleMetrics;
    }
}
