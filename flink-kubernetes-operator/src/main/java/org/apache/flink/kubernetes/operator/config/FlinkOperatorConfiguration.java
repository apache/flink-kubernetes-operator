/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;

import io.javaoperatorsdk.operator.api.config.RetryConfiguration;
import lombok.Value;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Configuration class for operator. */
@Value
public class FlinkOperatorConfiguration {

    private static final String NAMESPACES_SPLITTER_KEY = "\\s*,\\s*";

    Duration reconcileInterval;
    int reconcilerMaxParallelism;
    Duration progressCheckInterval;
    Duration restApiReadyDelay;
    Duration flinkClientTimeout;
    String flinkServiceHostOverride;
    Set<String> watchedNamespaces;
    boolean dynamicNamespacesEnabled;
    boolean josdkMetricsEnabled;
    int metricsHistogramSampleSize;
    boolean kubernetesClientMetricsEnabled;
    Duration flinkCancelJobTimeout;
    Duration flinkShutdownClusterTimeout;
    String artifactsBaseDir;
    Integer savepointHistoryCountThreshold;
    Duration savepointHistoryAgeThreshold;
    RetryConfiguration retryConfiguration;
    String labelSelector;

    public static FlinkOperatorConfiguration fromConfiguration(Configuration operatorConfig) {
        Duration reconcileInterval =
                operatorConfig.get(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL);

        int reconcilerMaxParallelism =
                operatorConfig.getInteger(
                        KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_PARALLELISM);

        Duration restApiReadyDelay =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_OBSERVER_REST_READY_DELAY);

        Duration progressCheckInterval =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_OBSERVER_PROGRESS_CHECK_INTERVAL);

        Duration flinkClientTimeout =
                operatorConfig.get(KubernetesOperatorConfigOptions.OPERATOR_FLINK_CLIENT_TIMEOUT);

        Duration flinkCancelJobTimeout =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_FLINK_CLIENT_CANCEL_TIMEOUT);

        Duration flinkShutdownClusterTimeout =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_RESOURCE_CLEANUP_TIMEOUT);

        String artifactsBaseDir =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_USER_ARTIFACTS_BASE_DIR);

        Integer savepointHistoryCountThreshold =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions
                                .OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT_THRESHOLD);
        Duration savepointHistoryAgeThreshold =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions
                                .OPERATOR_SAVEPOINT_HISTORY_MAX_AGE_THRESHOLD);

        String flinkServiceHostOverride = null;
        if (EnvUtils.get(EnvUtils.ENV_KUBERNETES_SERVICE_HOST).isEmpty()) {
            // not running in k8s, simplify local development
            flinkServiceHostOverride = "localhost";
        }
        var watchedNamespaces =
                new HashSet<>(
                        Arrays.asList(
                                operatorConfig
                                        .get(
                                                KubernetesOperatorConfigOptions
                                                        .OPERATOR_WATCHED_NAMESPACES)
                                        .split(NAMESPACES_SPLITTER_KEY)));

        boolean dynamicNamespacesEnabled =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_DYNAMIC_NAMESPACES_ENABLED);

        boolean josdkMetricsEnabled =
                operatorConfig.get(KubernetesOperatorMetricOptions.OPERATOR_JOSDK_METRICS_ENABLED);

        boolean kubernetesClientMetricsEnabled =
                operatorConfig.get(
                        KubernetesOperatorMetricOptions.OPERATOR_KUBERNETES_CLIENT_METRICS_ENABLED);

        int metricsHistogramSampleSize =
                operatorConfig.get(
                        KubernetesOperatorMetricOptions.OPERATOR_METRICS_HISTOGRAM_SAMPLE_SIZE);

        RetryConfiguration retryConfiguration = new FlinkOperatorRetryConfiguration(operatorConfig);

        String labelSelector =
                operatorConfig.getString(KubernetesOperatorConfigOptions.OPERATOR_LABEL_SELECTOR);

        return new FlinkOperatorConfiguration(
                reconcileInterval,
                reconcilerMaxParallelism,
                progressCheckInterval,
                restApiReadyDelay,
                flinkClientTimeout,
                flinkServiceHostOverride,
                watchedNamespaces,
                dynamicNamespacesEnabled,
                josdkMetricsEnabled,
                metricsHistogramSampleSize,
                kubernetesClientMetricsEnabled,
                flinkCancelJobTimeout,
                flinkShutdownClusterTimeout,
                artifactsBaseDir,
                savepointHistoryCountThreshold,
                savepointHistoryAgeThreshold,
                retryConfiguration,
                labelSelector);
    }

    /** Enables configurable retry mechanism for reconciliation errors. */
    protected static class FlinkOperatorRetryConfiguration implements RetryConfiguration {
        private final int maxAttempts;
        private final long initialInterval;
        private final double intervalMultiplier;

        public FlinkOperatorRetryConfiguration(Configuration operatorConfig) {
            maxAttempts =
                    operatorConfig.getInteger(
                            KubernetesOperatorConfigOptions.OPERATOR_RETRY_MAX_ATTEMPTS);
            initialInterval =
                    operatorConfig
                            .get(KubernetesOperatorConfigOptions.OPERATOR_RETRY_INITIAL_INTERVAL)
                            .toMillis();
            intervalMultiplier =
                    operatorConfig.getDouble(
                            KubernetesOperatorConfigOptions.OPERATOR_RETRY_INTERVAL_MULTIPLIER);
        }

        @Override
        public int getMaxAttempts() {
            return maxAttempts;
        }

        @Override
        public long getInitialInterval() {
            return initialInterval;
        }

        @Override
        public double getIntervalMultiplier() {
            return intervalMultiplier;
        }

        @Override
        public long getMaxInterval() {
            return (long) (initialInterval * Math.pow(intervalMultiplier, maxAttempts));
        }
    }
}
