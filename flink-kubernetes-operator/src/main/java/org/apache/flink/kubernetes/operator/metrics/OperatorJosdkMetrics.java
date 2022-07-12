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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    private static final int WINDOW_SIZE = 1000;

    private final KubernetesOperatorMetricGroup operatorMetricGroup;
    private final Configuration conf;
    private final Clock clock;

    private final Map<ResourceID, KubernetesResourceNamespaceMetricGroup> resourceNsMetricGroups =
            new ConcurrentHashMap<>();
    private final Map<ResourceID, KubernetesResourceMetricGroup> resourceMetricGroups =
            new ConcurrentHashMap<>();

    private final Map<String, Histogram> histograms = new ConcurrentHashMap<>();
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    private static final Map<String, String> CONTROLLERS =
            Map.of(
                    FlinkDeploymentController.class.getSimpleName().toLowerCase(),
                    "FlinkDeployment",
                    FlinkSessionJob.class.getSimpleName().toLowerCase(),
                    "FlinkSessionJob");

    public OperatorJosdkMetrics(
            KubernetesOperatorMetricGroup operatorMetricGroup, Configuration conf) {
        this.operatorMetricGroup = operatorMetricGroup;
        this.conf = conf;
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
    public void receivedEvent(Event event) {
        if (event instanceof ResourceEvent) {
            var action = ((ResourceEvent) event).getAction();
            counter(
                            getResourceMg(event.getRelatedCustomResourceID()),
                            Collections.emptyList(),
                            RESOURCE,
                            EVENT)
                    .inc();
            counter(
                            getResourceMg(event.getRelatedCustomResourceID()),
                            Collections.emptyList(),
                            RESOURCE,
                            EVENT,
                            action.name())
                    .inc();
        }
    }

    @Override
    public void cleanupDoneFor(ResourceID resourceID) {
        counter(getResourceMg(resourceID), Collections.emptyList(), RECONCILIATION, "cleanup")
                .inc();
    }

    @Override
    public void reconcileCustomResource(ResourceID resourceID, RetryInfo retryInfoNullable) {
        counter(getResourceMg(resourceID), Collections.emptyList(), RECONCILIATION).inc();

        if (retryInfoNullable != null) {
            counter(getResourceMg(resourceID), Collections.emptyList(), RECONCILIATION, "retries")
                    .inc();
        }
    }

    @Override
    public void finishedReconciliation(ResourceID resourceID) {
        counter(getResourceMg(resourceID), Collections.emptyList(), RECONCILIATION, "finished")
                .inc();
    }

    @Override
    public void failedReconciliation(ResourceID resourceID, Exception exception) {
        counter(getResourceMg(resourceID), Collections.emptyList(), RECONCILIATION, "failed").inc();
    }

    @Override
    public <T extends Map<?, ?>> T monitorSizeOf(T map, String name) {
        operatorMetricGroup.addGroup(name).gauge("size", map::size);
        return map;
    }

    private Histogram histogram(ControllerExecution<?> execution, String name) {
        MetricGroup group = operatorMetricGroup.addGroup(OPERATOR_SDK_GROUP);
        for (String metricGroup :
                Arrays.asList(
                        CONTROLLERS.get(execution.controllerName().toLowerCase()),
                        execution.name(),
                        name)) {
            group = group.addGroup(metricGroup);
        }
        var finalGroup = group;
        return histograms.computeIfAbsent(
                String.join(".", group.getScopeComponents()),
                s ->
                        finalGroup.histogram(
                                "TimeSeconds", new DescriptiveStatisticsHistogram(WINDOW_SIZE)));
    }

    private long toSeconds(long startTime) {
        return TimeUnit.NANOSECONDS.toSeconds(clock.relativeTimeNanos() - startTime);
    }

    private Counter counter(
            MetricGroup parent, List<Tuple2<String, String>> additionalTags, String... names) {
        MetricGroup group = parent.addGroup(OPERATOR_SDK_GROUP);
        for (String name : names) {
            group = group.addGroup(name);
        }
        for (Tuple2<String, String> tag : additionalTags) {
            group = group.addGroup(tag.f0, tag.f1);
        }
        var finalGroup = group;
        return counters.computeIfAbsent(
                String.join(".", group.getScopeComponents()), s -> finalGroup.counter("Count"));
    }

    private KubernetesResourceNamespaceMetricGroup getResourceNsMg(ResourceID resourceID) {
        return resourceNsMetricGroups.computeIfAbsent(
                resourceID,
                rid ->
                        operatorMetricGroup.createResourceNamespaceGroup(
                                conf, rid.getNamespace().orElse("default")));
    }

    private KubernetesResourceMetricGroup getResourceMg(ResourceID resourceID) {
        return resourceMetricGroups.computeIfAbsent(
                resourceID,
                rid -> getResourceNsMg(rid).createResourceNamespaceGroup(conf, rid.getName()));
    }
}
