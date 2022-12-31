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
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.util.MetricUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.K8S_OP_CONF_PREFIX;

/** Utility class for flink based operator metrics. */
public class OperatorMetricUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorMetricUtils.class);

    private static final String OPERATOR_METRICS_PREFIX = K8S_OP_CONF_PREFIX + "metrics.";
    private static final String METRICS_PREFIX = "metrics.";

    public static KubernetesOperatorMetricGroup initOperatorMetrics(Configuration defaultConfig) {
        Configuration metricConfig = createMetricConfig(defaultConfig);
        LOG.info("Initializing operator metrics using conf: {}", metricConfig);
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(metricConfig);
        MetricRegistry metricRegistry = createMetricRegistry(metricConfig, pluginManager);
        KubernetesOperatorMetricGroup operatorMetricGroup =
                KubernetesOperatorMetricGroup.create(
                        metricRegistry,
                        metricConfig,
                        EnvUtils.getOrDefault(EnvUtils.ENV_OPERATOR_NAMESPACE, "default"),
                        EnvUtils.getOrDefault(
                                EnvUtils.ENV_OPERATOR_NAME, "flink-kubernetes-operator"),
                        EnvUtils.getOrDefault(EnvUtils.ENV_HOSTNAME, "localhost"));

        if (defaultConfig.getBoolean(
                KubernetesOperatorMetricOptions.OPERATOR_JVM_METRICS_ENABLED)) {
            MetricGroup statusGroup = operatorMetricGroup.addGroup("Status");
            MetricUtils.instantiateStatusMetrics(statusGroup);
        }
        return operatorMetricGroup;
    }

    public static KubernetesResourceMetricGroup createResourceMetricGroup(
            KubernetesOperatorMetricGroup operatorMetricGroup,
            FlinkConfigManager configManager,
            AbstractFlinkResource<?, ?> resource) {
        return operatorMetricGroup
                .createResourceNamespaceGroup(
                        configManager.getDefaultConfig(),
                        resource.getClass(),
                        resource.getMetadata().getNamespace())
                .createResourceGroup(
                        configManager.getDefaultConfig(), resource.getMetadata().getName());
    }

    @VisibleForTesting
    protected static Configuration createMetricConfig(Configuration defaultConfig) {
        Map<String, String> metricConf = new HashMap<>();
        defaultConfig
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(OPERATOR_METRICS_PREFIX)) {
                                metricConf.put(
                                        key.replaceFirst(OPERATOR_METRICS_PREFIX, METRICS_PREFIX),
                                        value);
                            } else if (!key.startsWith(METRICS_PREFIX)) {
                                metricConf.put(key, value);
                            }
                        });
        return Configuration.fromMap(metricConf);
    }

    private static MetricRegistryImpl createMetricRegistry(
            Configuration configuration, PluginManager pluginManager) {
        return new MetricRegistryImpl(
                MetricRegistryConfiguration.fromConfiguration(configuration, Long.MAX_VALUE),
                ReporterSetup.fromConfiguration(configuration, pluginManager));
    }

    public static Counter synchronizedCounter(Counter counter) {
        return new SynchronizedCounter(counter);
    }

    public static SynchronizedMeterView synchronizedMeterView(MeterView meterView) {
        return new SynchronizedMeterView(meterView);
    }

    public static Histogram createHistogram(FlinkOperatorConfiguration operatorConfiguration) {
        return new DescriptiveStatisticsHistogram(
                operatorConfiguration.getMetricsHistogramSampleSize());
    }

    /** Thread safe {@link Histogram} wrapper. */
    public static class SynchronizedHistogram implements Histogram {

        private final Histogram delegate;

        public SynchronizedHistogram(Histogram delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void update(long l) {
            delegate.update(l);
        }

        @Override
        public synchronized long getCount() {
            return delegate.getCount();
        }

        @Override
        public synchronized HistogramStatistics getStatistics() {
            return delegate.getStatistics();
        }
    }

    /** Thread safe {@link Counter} wrapper. */
    public static class SynchronizedCounter implements Counter {

        private final Counter delegate;

        public SynchronizedCounter(Counter delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void inc() {
            delegate.inc();
        }

        @Override
        public synchronized void inc(long l) {
            delegate.inc(l);
        }

        @Override
        public synchronized void dec() {
            delegate.dec();
        }

        @Override
        public synchronized void dec(long l) {
            delegate.dec(l);
        }

        @Override
        public synchronized long getCount() {
            return delegate.getCount();
        }
    }

    /** Thread safe {@link MeterView} wrapper. */
    public static class SynchronizedMeterView implements Meter, View {

        private final MeterView delegate;

        public SynchronizedMeterView(MeterView delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void markEvent() {
            delegate.markEvent();
        }

        @Override
        public synchronized void markEvent(long l) {
            delegate.markEvent(l);
        }

        @Override
        public synchronized double getRate() {
            return delegate.getRate();
        }

        @Override
        public synchronized long getCount() {
            return delegate.getCount();
        }

        @Override
        public synchronized void update() {
            delegate.update();
        }
    }
}
