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
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.GroupVersionKind;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link Metrics} to monitor and forward JOSDK metrics to {@link MetricRegistry}.
 */
public class OperatorJosdkMetrics implements Metrics {
    private static final String OPERATOR_SDK_GROUP = "JOSDK";
    private static final String RECONCILIATION = "Reconciliation";
    private static final String RESOURCE = "Resource";
    private static final String EVENT = "Event";

    private final KubernetesOperatorMetricGroup operatorMetricGroup;
    private final FlinkConfigManager configManager;
    private final Clock clock;

    private final Map<ResourceKey, ResourceMetrics> resourceMetrics = new ConcurrentHashMap<>();
    private final Map<List<String>, Histogram> histograms = new ConcurrentHashMap<>();

    public OperatorJosdkMetrics(
            KubernetesOperatorMetricGroup operatorMetricGroup, FlinkConfigManager configManager) {
        this.operatorMetricGroup = operatorMetricGroup;
        this.configManager = configManager;
        this.clock = SystemClock.getInstance();
    }

    @Override
    public <T> T timeControllerExecution(ControllerExecution<T> execution) throws Exception {
        long startTime = clock.relativeTimeNanos();
        try {
            T result = execution.execute();
            histogram(execution, execution.successTypeName(result)).update(toSeconds(startTime));
            return result;
        } catch (Exception e) {
            histogram(execution, "failed").update(toSeconds(startTime));
            throw e;
        }
    }

    @Override
    public void eventReceived(Event event, Map<String, Object> metadata) {
        if (event instanceof ResourceEvent) {
            var action = ((ResourceEvent) event).getAction();
            resourceMetrics.compute(
                    getResourceKey(event.getRelatedCustomResourceID(), metadata),
                    (key, metrics) -> {
                        if (metrics == null) {
                            metrics = newResourceMetrics(key);
                        }
                        metrics.counter(RESOURCE, EVENT).inc();
                        metrics.counter(RESOURCE, EVENT, action.name()).inc();
                        return metrics;
                    });
        }
    }

    @Override
    public void cleanupDone(ResourceID resourceID, Map<String, Object> metadata) {
        resourceMetrics.computeIfPresent(
                getResourceKey(resourceID, metadata),
                (key, metrics) -> {
                    metrics.close();
                    return null;
                });
    }

    @Override
    public void reconciliationSubmitted(
            HasMetadata resource, RetryInfo retryInfoNullable, Map<String, Object> metadata) {
        resourceMetrics.compute(
                getResourceKey(ResourceID.fromResource(resource), metadata),
                (key, metrics) -> {
                    if (metrics == null) {
                        metrics = newResourceMetrics(key);
                    }
                    metrics.counter(RECONCILIATION).inc();
                    if (retryInfoNullable != null) {
                        metrics.counter(RECONCILIATION, "retries").inc();
                    }
                    return metrics;
                });
    }

    @Override
    public void reconciliationFinished(
            HasMetadata resource, RetryInfo retryInfoNullable, Map<String, Object> metadata) {
        // Update-only: a completion callback may race a concurrent delete and arrive after
        // cleanupDone(). If the key was already removed, computeIfPresent skips the lambda so
        // metrics are never re-registered for a cleaned-up resource.
        resourceMetrics.computeIfPresent(
                getResourceKey(ResourceID.fromResource(resource), metadata),
                (key, metrics) -> {
                    metrics.counter(RECONCILIATION, "finished").inc();
                    return metrics;
                });
    }

    @Override
    public void reconciliationFailed(
            HasMetadata resource,
            RetryInfo retryInfoNullable,
            Exception exception,
            Map<String, Object> metadata) {
        // Update-only, see reconciliationFinished().
        resourceMetrics.computeIfPresent(
                getResourceKey(ResourceID.fromResource(resource), metadata),
                (key, metrics) -> {
                    metrics.counter(RECONCILIATION, "failed").inc();
                    return metrics;
                });
    }

    private Histogram histogram(ControllerExecution<?> execution, String name) {
        var groups = getHistoGroups(execution, name);
        return histograms.computeIfAbsent(
                groups,
                k -> {
                    var key = getResourceKey(execution.resourceID(), execution.metadata());
                    MetricGroup group =
                            operatorMetricGroup
                                    .createResourceNamespaceGroup(
                                            configManager.getDefaultConfig(),
                                            key.resourceClass(),
                                            key.resourceID().getNamespace().orElse("default"))
                                    .addGroup(OPERATOR_SDK_GROUP);
                    for (String mg : groups) {
                        group = group.addGroup(mg);
                    }
                    var finalGroup = group;
                    return finalGroup.histogram(
                            "TimeSeconds",
                            OperatorMetricUtils.createHistogram(
                                    configManager.getOperatorConfiguration()));
                });
    }

    private List<String> getHistoGroups(ControllerExecution<?> execution, String name) {
        return List.of(execution.name(), name);
    }

    private long toSeconds(long startTime) {
        return TimeUnit.NANOSECONDS.toSeconds(clock.relativeTimeNanos() - startTime);
    }

    private ResourceKey getResourceKey(ResourceID resourceID, Map<String, Object> metadata) {
        Class<? extends CustomResource<?, ?>> resourceClass =
                getResourceClass(metadata)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Unknown resource kind for " + resourceID));
        return new ResourceKey(resourceClass, resourceID);
    }

    private ResourceMetrics newResourceMetrics(ResourceKey key) {
        var resourceID = key.resourceID();
        var resourceGroup =
                operatorMetricGroup
                        .createResourceNamespaceGroup(
                                configManager.getDefaultConfig(),
                                key.resourceClass(),
                                resourceID.getNamespace().orElse("default"))
                        .createResourceGroup(
                                configManager.getDefaultConfig(), resourceID.getName());
        return new ResourceMetrics(resourceGroup);
    }

    private Optional<Class<? extends CustomResource<?, ?>>> getResourceClass(
            Map<String, Object> metadata) {
        var resourceGvk = (GroupVersionKind) metadata.get(Constants.RESOURCE_GVK_KEY);

        if (resourceGvk == null) {
            return Optional.empty();
        }

        Class<? extends CustomResource<?, ?>> resourceClass;

        if (resourceGvk.getKind().equals(FlinkDeployment.class.getSimpleName())) {
            resourceClass = FlinkDeployment.class;
        } else if (resourceGvk.getKind().equals(FlinkSessionJob.class.getSimpleName())) {
            resourceClass = FlinkSessionJob.class;
        } else if (resourceGvk.getKind().equals(FlinkStateSnapshot.class.getSimpleName())) {
            resourceClass = FlinkStateSnapshot.class;
        } else if (resourceGvk.getKind().equals(FlinkBlueGreenDeployment.class.getSimpleName())) {
            resourceClass = FlinkBlueGreenDeployment.class;
        } else {
            return Optional.empty();
        }

        return Optional.of(resourceClass);
    }

    @VisibleForTesting
    int getResourceMetricsCacheSize() {
        return resourceMetrics.size();
    }

    /** Cache key that fully identifies a reconciled resource by its kind and {@link ResourceID}. */
    private record ResourceKey(
            Class<? extends CustomResource<?, ?>> resourceClass, ResourceID resourceID) {}

    /** Holds the metric group and counters owned by a single reconciled resource. */
    private static final class ResourceMetrics {
        private final KubernetesResourceMetricGroup group;
        // Only accessed from within resourceMetrics.compute*/computeIfPresent lambdas, which are
        // serialized per ResourceKey by the backing ConcurrentHashMap, so a plain HashMap is safe.
        private final Map<List<String>, Counter> counters = new HashMap<>();

        private ResourceMetrics(KubernetesResourceMetricGroup group) {
            this.group = group;
        }

        private Counter counter(String... names) {
            return counters.computeIfAbsent(
                    List.of(names),
                    k -> {
                        MetricGroup metricGroup = group.addGroup(OPERATOR_SDK_GROUP);
                        for (String name : names) {
                            metricGroup = metricGroup.addGroup(name);
                        }
                        return OperatorMetricUtils.synchronizedCounter(
                                metricGroup.counter("Count"));
                    });
        }

        private void close() {
            group.close();
            counters.clear();
        }
    }
}
