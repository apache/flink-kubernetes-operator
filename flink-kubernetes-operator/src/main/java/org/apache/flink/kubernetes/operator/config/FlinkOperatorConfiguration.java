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
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.javaoperatorsdk.operator.api.config.LeaderElectionConfiguration;
import io.javaoperatorsdk.operator.processing.event.rate.LinearRateLimiter;
import io.javaoperatorsdk.operator.processing.event.rate.RateLimiter;
import io.javaoperatorsdk.operator.processing.retry.GenericRetry;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_KUBERNETES_SLOW_REQUEST_THRESHOLD;
import static org.apache.flink.kubernetes.operator.utils.EnvUtils.ENV_WATCH_NAMESPACES;

/** Configuration class for operator. */
@Value
public class FlinkOperatorConfiguration {

    private static final String NAMESPACES_SPLITTER_KEY = "\\s*,\\s*";
    private static final String ENV_KUBERNETES_SERVICE_HOST = "KUBERNETES_SERVICE_HOST";

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
    boolean kubernetesClientMetricsHttpResponseCodeGroupsEnabled;
    Duration flinkCancelJobTimeout;
    Duration flinkShutdownClusterTimeout;
    String artifactsBaseDir;
    Integer savepointHistoryCountThreshold;
    Duration savepointHistoryAgeThreshold;
    GenericRetry retryConfiguration;
    RateLimiter<?> rateLimiter;
    boolean exceptionStackTraceEnabled;
    int exceptionStackTraceLengthThreshold;
    int exceptionFieldLengthThreshold;
    int exceptionThrowableCountThreshold;
    Map<String, String> exceptionLabelMapper;
    String labelSelector;
    LeaderElectionConfiguration leaderElectionConfiguration;
    DeletionPropagation deletionPropagation;
    boolean snapshotResourcesEnabled;
    Duration slowRequestThreshold;
    int reportedExceptionEventsMaxCount;
    int reportedExceptionEventsMaxStackTraceLength;

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

        Boolean exceptionStackTraceEnabled =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_ENABLED);
        int exceptionStackTraceLengthThreshold =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_STACK_TRACE_MAX_LENGTH);
        int exceptionFieldLengthThreshold =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_FIELD_MAX_LENGTH);
        int exceptionThrowableCountThreshold =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions
                                .OPERATOR_EXCEPTION_THROWABLE_LIST_MAX_COUNT);
        Map<String, String> exceptionLabelMapper =
                operatorConfig.get(KubernetesOperatorConfigOptions.OPERATOR_EXCEPTION_LABEL_MAPPER);

        String flinkServiceHostOverride = null;
        if (getEnv(ENV_KUBERNETES_SERVICE_HOST).isEmpty()) {
            // not running in k8s, simplify local development
            flinkServiceHostOverride = "localhost";
        }
        Set<String> watchedNamespaces = null;
        if (EnvUtils.get(ENV_WATCH_NAMESPACES).isEmpty()) {
            // if the env var is not set use the config file, the default if neither set is
            // all namespaces
            watchedNamespaces =
                    new HashSet<>(
                            Arrays.asList(
                                    operatorConfig
                                            .get(
                                                    KubernetesOperatorConfigOptions
                                                            .OPERATOR_WATCHED_NAMESPACES)
                                            .split(NAMESPACES_SPLITTER_KEY)));
        } else {
            watchedNamespaces =
                    new HashSet<>(
                            Arrays.asList(
                                    EnvUtils.get(ENV_WATCH_NAMESPACES)
                                            .get()
                                            .split(NAMESPACES_SPLITTER_KEY)));
        }

        boolean dynamicNamespacesEnabled =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_DYNAMIC_NAMESPACES_ENABLED);

        boolean josdkMetricsEnabled =
                operatorConfig.get(KubernetesOperatorMetricOptions.OPERATOR_JOSDK_METRICS_ENABLED);

        boolean kubernetesClientMetricsEnabled =
                operatorConfig.get(
                        KubernetesOperatorMetricOptions.OPERATOR_KUBERNETES_CLIENT_METRICS_ENABLED);

        boolean kubernetesClientMetricsHttpResponseCodeGroupsEnabled =
                operatorConfig.get(
                        KubernetesOperatorMetricOptions
                                .OPERATOR_KUBERNETES_CLIENT_METRICS_HTTP_RESPONSE_CODE_GROUPS_ENABLED);

        int metricsHistogramSampleSize =
                operatorConfig.get(
                        KubernetesOperatorMetricOptions.OPERATOR_METRICS_HISTOGRAM_SAMPLE_SIZE);

        GenericRetry retryConfiguration = getRetryConfig(operatorConfig);
        RateLimiter rateLimiter = getRateLimiter(operatorConfig);

        String labelSelector =
                operatorConfig.getString(KubernetesOperatorConfigOptions.OPERATOR_LABEL_SELECTOR);

        DeletionPropagation deletionPropagation =
                operatorConfig.get(KubernetesOperatorConfigOptions.RESOURCE_DELETION_PROPAGATION);

        boolean snapshotResourcesEnabled =
                operatorConfig.get(KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED);

        Duration slowRequestThreshold =
                operatorConfig.get(OPERATOR_KUBERNETES_SLOW_REQUEST_THRESHOLD);

        int reportedExceptionEventsMaxCount =
                operatorConfig.get(KubernetesOperatorConfigOptions.OPERATOR_EVENT_EXCEPTION_LIMIT);
        int reportedExceptionEventsMaxStackTraceLength =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_EVENT_EXCEPTION_STACKTRACE_LINES);

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
                kubernetesClientMetricsHttpResponseCodeGroupsEnabled,
                flinkCancelJobTimeout,
                flinkShutdownClusterTimeout,
                artifactsBaseDir,
                savepointHistoryCountThreshold,
                savepointHistoryAgeThreshold,
                retryConfiguration,
                rateLimiter,
                exceptionStackTraceEnabled,
                exceptionStackTraceLengthThreshold,
                exceptionFieldLengthThreshold,
                exceptionThrowableCountThreshold,
                exceptionLabelMapper,
                labelSelector,
                getLeaderElectionConfig(operatorConfig),
                deletionPropagation,
                snapshotResourcesEnabled,
                slowRequestThreshold,
                reportedExceptionEventsMaxCount,
                reportedExceptionEventsMaxStackTraceLength);
    }

    private static GenericRetry getRetryConfig(Configuration conf) {
        var genericRetry =
                new GenericRetry()
                        .setMaxAttempts(
                                conf.getInteger(
                                        KubernetesOperatorConfigOptions
                                                .OPERATOR_RETRY_MAX_ATTEMPTS))
                        .setInitialInterval(
                                conf.get(
                                                KubernetesOperatorConfigOptions
                                                        .OPERATOR_RETRY_INITIAL_INTERVAL)
                                        .toMillis())
                        .setIntervalMultiplier(
                                conf.getDouble(
                                        KubernetesOperatorConfigOptions
                                                .OPERATOR_RETRY_INTERVAL_MULTIPLIER));

        if (conf.contains(KubernetesOperatorConfigOptions.OPERATOR_RETRY_MAX_INTERVAL)) {
            genericRetry.setMaxInterval(
                    conf.get(KubernetesOperatorConfigOptions.OPERATOR_RETRY_MAX_INTERVAL)
                            .toMillis());
        } else {
            genericRetry.withoutMaxInterval();
        }
        return genericRetry;
    }

    private static RateLimiter<?> getRateLimiter(Configuration conf) {
        return new LinearRateLimiter(
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_RATE_LIMITER_PERIOD),
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_RATE_LIMITER_LIMIT));
    }

    private static LeaderElectionConfiguration getLeaderElectionConfig(Configuration conf) {
        if (!conf.get(KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_ENABLED)) {
            return null;
        }

        return new LeaderElectionConfiguration(
                conf.getOptional(
                                KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_LEASE_NAME)
                        .orElseThrow(
                                () ->
                                        new IllegalConfigurationException(
                                                KubernetesOperatorConfigOptions
                                                                .OPERATOR_LEADER_ELECTION_LEASE_NAME
                                                                .key()
                                                        + " must be defined when operator leader election is enabled.")),
                null,
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_LEASE_DURATION),
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_RENEW_DEADLINE),
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_RETRY_PERIOD));
    }

    private static Optional<String> getEnv(String key) {
        return Optional.ofNullable(StringUtils.getIfBlank(System.getenv().get(key), () -> null));
    }
}
