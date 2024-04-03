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

import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.GroupVersionKind;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;

import java.util.ArrayList;
import java.util.Arrays;
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

    private final Map<ResourceID, KubernetesResourceNamespaceMetricGroup> resourceNsMetricGroups =
            new ConcurrentHashMap<>();
    private final Map<ResourceID, KubernetesResourceMetricGroup> resourceMetricGroups =
            new ConcurrentHashMap<>();

    private final Map<List<String>, Histogram> histograms = new ConcurrentHashMap<>();
    private final Map<List<String>, Counter> counters = new ConcurrentHashMap<>();

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
    public void receivedEvent(Event event, Map<String, Object> metadata) {
        if (event instanceof ResourceEvent) {
            var action = ((ResourceEvent) event).getAction();
            counter(getResourceMg(event.getRelatedCustomResourceID(), metadata), RESOURCE, EVENT)
                    .inc();
            counter(
                            getResourceMg(event.getRelatedCustomResourceID(), metadata),
                            RESOURCE,
                            EVENT,
                            action.name())
                    .inc();
        }
    }

    @Override
    public void cleanupDoneFor(ResourceID resourceID, Map<String, Object> metadata) {
        counter(getResourceMg(resourceID, metadata), RECONCILIATION, "cleanup").inc();
    }

    @Override
    public void reconcileCustomResource(
            ResourceID resourceID, RetryInfo retryInfoNullable, Map<String, Object> metadata) {
        counter(getResourceMg(resourceID, metadata), RECONCILIATION).inc();

        if (retryInfoNullable != null) {
            counter(getResourceMg(resourceID, metadata), RECONCILIATION, "retries").inc();
        }
    }

    @Override
    public void finishedReconciliation(ResourceID resourceID, Map<String, Object> metadata) {
        counter(getResourceMg(resourceID, metadata), RECONCILIATION, "finished").inc();
    }

    @Override
    public void failedReconciliation(
            ResourceID resourceID, Exception exception, Map<String, Object> metadata) {
        counter(getResourceMg(resourceID, metadata), RECONCILIATION, "failed").inc();
    }

    @Override
    public <T extends Map<?, ?>> T monitorSizeOf(T map, String name) {
        operatorMetricGroup.addGroup(OPERATOR_SDK_GROUP).addGroup(name).gauge("size", map::size);
        return map;
    }

    private Histogram histogram(ControllerExecution<?> execution, String name) {
        var groups = getHistoGroups(execution, name);
        return histograms.computeIfAbsent(
                groups,
                k -> {
                    var group =
                            getResourceNsMg(execution.resourceID(), execution.metadata())
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

    private Counter counter(MetricGroup parent, String... names) {
        var key = new ArrayList<String>(parent.getScopeComponents().length + names.length);
        Arrays.stream(parent.getScopeComponents()).forEach(key::add);
        Arrays.stream(names).forEach(key::add);

        return counters.computeIfAbsent(
                key,
                s -> {
                    MetricGroup group = parent.addGroup(OPERATOR_SDK_GROUP);
                    for (String name : names) {
                        group = group.addGroup(name);
                    }
                    var finalGroup = group;
                    return OperatorMetricUtils.synchronizedCounter(finalGroup.counter("Count"));
                });
    }

    private KubernetesResourceNamespaceMetricGroup getResourceNsMg(
            ResourceID resourceID, Map<String, Object> metadata) {
        Class<? extends CustomResource<?, ?>> resourceClass =
                getResourceClass(metadata)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Unknown resource kind for " + resourceID));

        return resourceNsMetricGroups.computeIfAbsent(
                resourceID,
                rid ->
                        operatorMetricGroup.createResourceNamespaceGroup(
                                configManager.getDefaultConfig(),
                                resourceClass,
                                rid.getNamespace().orElse("default")));
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
        } else {
            return Optional.empty();
        }

        return Optional.of(resourceClass);
    }

    private KubernetesResourceMetricGroup getResourceMg(
            ResourceID resourceID, Map<String, Object> metadata) {
        return resourceMetricGroups.computeIfAbsent(
                resourceID,
                rid ->
                        getResourceNsMg(rid, metadata)
                                .createResourceGroup(
                                        configManager.getDefaultConfig(), rid.getName()));
    }
}
