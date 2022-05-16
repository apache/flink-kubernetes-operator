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
import org.apache.flink.kubernetes.operator.utils.EnvUtils;

import lombok.Value;

import java.time.Duration;
import java.util.Set;

/** Configuration class for operator. */
@Value
public class FlinkOperatorConfiguration {

    Duration reconcileInterval;
    int reconcilerMaxParallelism;
    Duration progressCheckInterval;
    Duration restApiReadyDelay;
    Duration savepointTriggerGracePeriod;
    Duration flinkClientTimeout;
    String flinkServiceHostOverride;
    Set<String> watchedNamespaces;
    Duration flinkCancelJobTimeout;
    Duration flinkShutdownClusterTimeout;
    String artifactsBaseDir;
    int savepointHistoryMaxCount;
    Duration savepointHistoryMaxAge;

    public static FlinkOperatorConfiguration fromConfiguration(
            Configuration operatorConfig, Set<String> watchedNamespaces) {
        Duration reconcileInterval =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_RECONCILER_RESCHEDULE_INTERVAL);

        int reconcilerMaxParallelism =
                operatorConfig.getInteger(
                        KubernetesOperatorConfigOptions.OPERATOR_RECONCILER_MAX_PARALLELISM);

        Duration restApiReadyDelay =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_OBSERVER_REST_READY_DELAY);

        Duration progressCheckInterval =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_OBSERVER_PROGRESS_CHECK_INTERVAL);

        Duration savepointTriggerGracePeriod =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions
                                .OPERATOR_OBSERVER_SAVEPOINT_TRIGGER_GRACE_PERIOD);

        Duration flinkClientTimeout =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_OBSERVER_FLINK_CLIENT_TIMEOUT);

        Duration flinkCancelJobTimeout =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions
                                .OPERATOR_RECONCILER_FLINK_CANCEL_JOB_TIMEOUT);

        Duration flinkShutdownClusterTimeout =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions
                                .OPERATOR_RECONCILER_FLINK_CLUSTER_SHUTDOWN_TIMEOUT);

        String artifactsBaseDir =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_USER_ARTIFACTS_BASE_DIR);

        int savepointHistoryMaxCount =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT);
        Duration savepointHistoryMaxAge =
                operatorConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE);

        String flinkServiceHostOverride = null;
        if (EnvUtils.get("KUBERNETES_SERVICE_HOST") == null) {
            // not running in k8s, simplify local development
            flinkServiceHostOverride = "localhost";
        }

        return new FlinkOperatorConfiguration(
                reconcileInterval,
                reconcilerMaxParallelism,
                progressCheckInterval,
                restApiReadyDelay,
                savepointTriggerGracePeriod,
                flinkClientTimeout,
                flinkServiceHostOverride,
                watchedNamespaces,
                flinkCancelJobTimeout,
                flinkShutdownClusterTimeout,
                artifactsBaseDir,
                savepointHistoryMaxCount,
                savepointHistoryMaxAge);
    }
}
