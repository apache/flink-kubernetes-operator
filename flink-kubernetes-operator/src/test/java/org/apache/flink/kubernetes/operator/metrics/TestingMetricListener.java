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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.metrics.ViewUpdater;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.client.CustomResource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/** Utility class for metrics testing. */
public class TestingMetricListener {
    public static final String DELIMITER = ".";
    private static final String NAMESPACE = "test-op-ns";
    private static final String NAME = "test-op-name";
    private static final String HOST = "test-op-host";
    private final KubernetesOperatorMetricGroup metricGroup;
    private final Map<String, Metric> metrics = new HashMap();
    private final ScheduledExecutorService executor;
    private Configuration configuration;
    private ViewUpdater viewUpdater;

    public TestingMetricListener(Configuration configuration) {
        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("Flink-TestingMetricRegistry"));

        TestingMetricRegistry registry =
                TestingMetricRegistry.builder()
                        .setDelimiter(DELIMITER.charAt(0))
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    this.metrics.put(group.getMetricIdentifier(name), metric);
                                    if (metric instanceof View) {
                                        if (viewUpdater == null) {
                                            viewUpdater = new ViewUpdater(executor);
                                        }
                                        viewUpdater.notifyOfAddedView((View) metric);
                                    }
                                })
                        .build();
        this.metricGroup =
                KubernetesOperatorMetricGroup.create(
                        registry, configuration, NAMESPACE, NAME, HOST);
        this.configuration = configuration;
    }

    public KubernetesOperatorMetricGroup getMetricGroup() {
        return this.metricGroup;
    }

    public Optional<Counter> getCounter(String identifier) {
        return this.getMetric(identifier);
    }

    public Optional<Histogram> getHistogram(String identifier) {
        return this.getMetric(identifier);
    }

    public Optional<Meter> getMeter(String identifier) {
        return this.getMetric(identifier);
    }

    public <T> Optional<Gauge<T>> getGauge(String identifier) {
        return Optional.ofNullable((Gauge<T>) this.metrics.get(identifier));
    }

    private <T extends Metric> Optional<T> getMetric(String identifier) {
        return Optional.ofNullable((T) this.metrics.get(identifier));
    }

    public String getMetricId(String... identifiers) {
        return metricGroup.getMetricIdentifier(String.join(DELIMITER, identifiers));
    }

    public String getNamespaceMetricId(
            Class<? extends CustomResource<?, ?>> resourceClass,
            String resourceNs,
            String... identifiers) {
        return metricGroup
                .createResourceNamespaceGroup(configuration, resourceClass, resourceNs)
                .getMetricIdentifier(String.join(DELIMITER, identifiers));
    }

    public String getResourceMetricId(
            Class<? extends CustomResource<?, ?>> resourceClass,
            String resourceNs,
            String resourceName,
            String... identifiers) {
        return metricGroup
                .createResourceNamespaceGroup(configuration, resourceClass, resourceNs)
                .createResourceGroup(configuration, resourceName)
                .getMetricIdentifier(String.join(DELIMITER, identifiers));
    }

    public int size() {
        return metrics.size();
    }
}
