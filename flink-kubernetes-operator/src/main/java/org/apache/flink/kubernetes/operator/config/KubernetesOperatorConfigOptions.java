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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import io.javaoperatorsdk.operator.api.config.ConfigurationService;

import java.time.Duration;
import java.util.Map;

/** This class holds configuration constants used by flink operator. */
public class KubernetesOperatorConfigOptions {

    public static final ConfigOption<Duration> OPERATOR_RECONCILER_RESCHEDULE_INTERVAL =
            ConfigOptions.key("kubernetes.operator.reconciler.reschedule.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            "The interval for the controller to reschedule the reconcile process.");

    public static final ConfigOption<Duration> OPERATOR_OBSERVER_REST_READY_DELAY =
            ConfigOptions.key("kubernetes.operator.observer.rest-ready.delay")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "Final delay before deployment is marked ready after port becomes accessible.");

    public static final ConfigOption<Integer> OPERATOR_RECONCILER_MAX_PARALLELISM =
            ConfigOptions.key("kubernetes.operator.reconciler.max.parallelism")
                    .intType()
                    .defaultValue(ConfigurationService.DEFAULT_RECONCILIATION_THREADS_NUMBER)
                    .withDescription(
                            "The maximum number of threads running the reconciliation loop. Use -1 for infinite.");

    public static final ConfigOption<Duration> OPERATOR_OBSERVER_PROGRESS_CHECK_INTERVAL =
            ConfigOptions.key("kubernetes.operator.observer.progress-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The interval for observing status for in-progress operations such as deployment and savepoints.");

    public static final ConfigOption<Duration> OPERATOR_OBSERVER_SAVEPOINT_TRIGGER_GRACE_PERIOD =
            ConfigOptions.key("kubernetes.operator.observer.savepoint.trigger.grace-period")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The interval before a savepoint trigger attempt is marked as unsuccessful.");

    public static final ConfigOption<Duration> OPERATOR_OBSERVER_FLINK_CLIENT_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.observer.flink.client.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The timeout for the observer to wait the flink rest client to return.");

    public static final ConfigOption<Duration> OPERATOR_RECONCILER_FLINK_CANCEL_JOB_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.reconciler.flink.cancel.job.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The timeout for the reconciler to wait for flink to cancel job.");

    public static final ConfigOption<Duration> OPERATOR_RECONCILER_FLINK_CLUSTER_SHUTDOWN_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.reconciler.flink.cluster.shutdown.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            "The timeout for the reconciler to wait for flink to shutdown cluster.");

    public static final ConfigOption<Boolean> DEPLOYMENT_ROLLBACK_ENABLED =
            ConfigOptions.key("kubernetes.operator.deployment.rollback.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable rolling back failed deployment upgrades.");

    public static final ConfigOption<Duration> DEPLOYMENT_READINESS_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.deployment.readiness.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The timeout for deployments to become ready/stable "
                                    + "before being rolled back if rollback is enabled.");

    public static final ConfigOption<String> OPERATOR_USER_ARTIFACTS_BASE_DIR =
            ConfigOptions.key("kubernetes.operator.user.artifacts.base.dir")
                    .stringType()
                    .defaultValue("/opt/flink/artifacts")
                    .withDescription("The base dir to put the session job artifacts.");

    public static final ConfigOption<Boolean> JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT =
            ConfigOptions.key("kubernetes.operator.job.upgrade.ignore-pending-savepoint")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore pending savepoint during job upgrade.");

    public static final ConfigOption<Boolean> OPERATOR_DYNAMIC_CONFIG_ENABLED =
            ConfigOptions.key("kubernetes.operator.dynamic.config.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable on-the-fly config changes through the operator configmap.");

    public static final ConfigOption<Duration> OPERATOR_DYNAMIC_CONFIG_CHECK_INTERVAL =
            ConfigOptions.key("kubernetes.operator.dynamic.config.check.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription("Time interval for checking config changes.");

    public static final ConfigOption<Duration> OPERATOR_CONFIG_CACHE_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.config.cache.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription("Expiration time for cached configs.");

    public static final ConfigOption<Integer> OPERATOR_CONFIG_CACHE_SIZE =
            ConfigOptions.key("kubernetes.operator.config.cache.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Max config cache size.");

    public static final ConfigOption<Boolean> OPERATOR_RECOVER_JM_DEPLOYMENT_ENABLED =
            ConfigOptions.key("kubernetes.operator.reconciler.jm-deployment-recovery.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable recovery of missing/deleted jobmanager deployments.");

    public static final ConfigOption<Integer> OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT =
            ConfigOptions.key("kubernetes.operator.savepoint.history.max.count")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Maximum number of savepoint history entries to retain.");

    public static final ConfigOption<Duration> OPERATOR_SAVEPOINT_HISTORY_MAX_AGE =
            ConfigOptions.key("kubernetes.operator.savepoint.history.max.age")
                    .durationType()
                    .defaultValue(Duration.ofHours(24))
                    .withDescription(
                            "Maximum age for savepoint history entries to retain. Due to lazy clean-up, the most recent savepoint may live longer than the max age.");

    public static final ConfigOption<Map<String, String>> JAR_ARTIFACT_HTTP_HEADER =
            ConfigOptions.key("kubernetes.operator.user.artifacts.http.header")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTP header for HttpArtifactFetcher. The header will be applied when getting the session job artifacts. "
                                    + "Expected format: headerKey1:headerValue1,headerKey2:headerValue2.");
}
