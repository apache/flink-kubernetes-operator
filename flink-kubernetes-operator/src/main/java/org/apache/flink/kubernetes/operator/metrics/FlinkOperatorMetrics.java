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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricRegistry;

import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link Metrics} to monitor the operations of {@link Operator} and forward
 * metrics to {@link MetricRegistry}.
 */
public class FlinkOperatorMetrics implements Metrics {

    private final MetricGroup metricGroup;
    private final Map<String, MetricGroup> metricGroups;
    private final Map<String, Counter> counters;

    private static final String RECONCILIATIONS = "reconciliations.";
    public static final String COUNT = "count";

    public FlinkOperatorMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.counters = new ConcurrentHashMap<>();
        this.metricGroups = new ConcurrentHashMap<>();
    }

    public <T> T timeControllerExecution(ControllerExecution<T> execution) throws Exception {
        MetricGroup executionGroup =
                metricGroup(metricGroup, "controller", execution.controllerName(), "execution");
        try {
            T result = execution.execute();
            counter(executionGroup, execution.name(), "success", execution.successTypeName(result));
            return result;
        } catch (Exception e) {
            counter(executionGroup, execution.name(), "failure", e.getClass().getSimpleName());
            throw e;
        }
    }

    public void receivedEvent(Event event) {
        incrementCounter(
                event.getRelatedCustomResourceID(),
                "events.received",
                "event",
                event.getClass().getSimpleName());
    }

    @Override
    public void cleanupDoneFor(ResourceID resourceID) {
        incrementCounter(resourceID, "events.delete");
    }

    @Override
    public void reconcileCustomResource(ResourceID resourceID, RetryInfo retryInfoNullable) {
        Optional<RetryInfo> retryInfo = Optional.ofNullable(retryInfoNullable);
        incrementCounter(
                resourceID,
                RECONCILIATIONS + "started",
                RECONCILIATIONS + "retries.number",
                "" + retryInfo.map(RetryInfo::getAttemptCount).orElse(0),
                RECONCILIATIONS + "retries.last",
                "" + retryInfo.map(RetryInfo::isLastAttempt).orElse(true));
    }

    @Override
    public void finishedReconciliation(ResourceID resourceID) {
        incrementCounter(resourceID, RECONCILIATIONS + "success");
    }

    public void failedReconciliation(ResourceID resourceID, Exception exception) {
        var cause = exception.getCause();
        if (cause == null) {
            cause = exception;
        } else if (cause instanceof RuntimeException) {
            cause = cause.getCause() != null ? cause.getCause() : cause;
        }
        incrementCounter(
                resourceID,
                RECONCILIATIONS + "failed",
                "exception",
                cause.getClass().getSimpleName());
    }

    public <T extends Map<?, ?>> T monitorSizeOf(T map, String name) {
        metricGroup.gauge(name + ".size", map::size);
        return map;
    }

    private void incrementCounter(
            ResourceID resourceID, String counterName, String... additionalTags) {
        incrementCounter(metricGroup, resourceID, counterName);
        if (additionalTags != null && additionalTags.length > 0) {
            counter(metricGroup, String.join(".", additionalTags));
            incrementCounter(
                    metricGroup(metricGroup, String.join(".", additionalTags)),
                    resourceID,
                    counterName);
        }
    }

    private void incrementCounter(
            MetricGroup metricGroup, ResourceID resourceID, String counterName) {
        counter(
                metricGroup,
                String.join(".", "counter", counterName),
                String.join(".", "resource", resourceID.getName()),
                String.join(".", "namespace", resourceID.getNamespace().orElse("")),
                String.join(
                        ".",
                        "scope",
                        resourceID.getNamespace().isPresent() ? "namespace" : "cluster"));
    }

    private MetricGroup metricGroup(MetricGroup parentGroup, String... groupNames) {
        MetricGroup childGroup = parentGroup;
        for (String groupName : groupNames) {
            String groupIdentifier = childGroup.getMetricIdentifier(groupName);
            if (metricGroups.containsKey(groupIdentifier)) {
                childGroup = metricGroups.get(groupIdentifier);
            } else {
                childGroup = childGroup.addGroup(groupName);
                metricGroups.put(groupIdentifier, childGroup);
            }
        }
        return childGroup;
    }

    private void counter(MetricGroup parentGroup, String... counterMetrics) {
        MetricGroup childGroup = parentGroup;
        for (String counterMetric : counterMetrics) {
            String metricIdentifier = childGroup.getMetricIdentifier(counterMetric);
            childGroup = metricGroup(childGroup, counterMetric);
            MetricGroup counterGroup = childGroup;
            counters.computeIfAbsent(metricIdentifier, m -> counterGroup.counter(COUNT)).inc();
        }
    }
}
