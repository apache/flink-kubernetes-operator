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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.execution.SavepointFormatType;

import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.reconciler.Constants;

import java.time.Duration;
import java.util.Map;

/** This class holds configuration constants used by flink operator. */
public class KubernetesOperatorConfigOptions {

    public static final String SECTION_SYSTEM = "system";
    public static final String SECTION_ADVANCED = "system_advanced";
    public static final String SECTION_DYNAMIC = "dynamic";

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_RECONCILE_INTERVAL =
            ConfigOptions.key("kubernetes.operator.reconcile.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDeprecatedKeys("kubernetes.operator.reconciler.reschedule.interval")
                    .withDescription(
                            "The interval for the controller to reschedule the reconcile process.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_OBSERVER_REST_READY_DELAY =
            ConfigOptions.key("kubernetes.operator.observer.rest-ready.delay")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "Final delay before deployment is marked ready after port becomes accessible.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Integer> OPERATOR_RECONCILE_PARALLELISM =
            ConfigOptions.key("kubernetes.operator.reconcile.parallelism")
                    .intType()
                    .defaultValue(ConfigurationService.DEFAULT_RECONCILIATION_THREADS_NUMBER)
                    .withDeprecatedKeys("kubernetes.operator.reconciler.max.parallelism")
                    .withDescription(
                            "The maximum number of threads running the reconciliation loop. Use -1 for infinite.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_OBSERVER_PROGRESS_CHECK_INTERVAL =
            ConfigOptions.key("kubernetes.operator.observer.progress-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The interval for observing status for in-progress operations such as deployment and savepoints.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> OPERATOR_SAVEPOINT_TRIGGER_GRACE_PERIOD =
            ConfigOptions.key("kubernetes.operator.savepoint.trigger.grace-period")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDeprecatedKeys(
                            "kubernetes.operator.observer.savepoint.trigger.grace-period")
                    .withDescription(
                            "The interval before a savepoint trigger attempt is marked as unsuccessful.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_FLINK_CLIENT_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.flink.client.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDeprecatedKeys("kubernetes.operator.observer.flink.client.timeout")
                    .withDescription(
                            "The timeout for the observer to wait the flink rest client to return.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_FLINK_CLIENT_CANCEL_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.flink.client.cancel.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDeprecatedKeys("kubernetes.operator.reconciler.flink.cancel.job.timeout")
                    .withDescription(
                            "The timeout for the reconciler to wait for flink to cancel job.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_RESOURCE_CLEANUP_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.resource.cleanup.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDeprecatedKeys(
                            "kubernetes.operator.reconciler.flink.cluster.shutdown.timeout")
                    .withDescription(
                            "The timeout for the resource clean up to wait for flink to shutdown cluster.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> DEPLOYMENT_ROLLBACK_ENABLED =
            ConfigOptions.key("kubernetes.operator.deployment.rollback.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable rolling back failed deployment upgrades.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> DEPLOYMENT_READINESS_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.deployment.readiness.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The timeout for deployments to become ready/stable "
                                    + "before being rolled back if rollback is enabled.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<String> OPERATOR_USER_ARTIFACTS_BASE_DIR =
            ConfigOptions.key("kubernetes.operator.user.artifacts.base.dir")
                    .stringType()
                    .defaultValue("/opt/flink/artifacts")
                    .withDescription("The base dir to put the session job artifacts.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT =
            ConfigOptions.key("kubernetes.operator.job.upgrade.ignore-pending-savepoint")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore pending savepoint during job upgrade.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Boolean> OPERATOR_DYNAMIC_CONFIG_ENABLED =
            ConfigOptions.key("kubernetes.operator.dynamic.config.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable on-the-fly config changes through the operator configmap.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_DYNAMIC_CONFIG_CHECK_INTERVAL =
            ConfigOptions.key("kubernetes.operator.dynamic.config.check.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription("Time interval for checking config changes.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_CONFIG_CACHE_TIMEOUT =
            ConfigOptions.key("kubernetes.operator.config.cache.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription("Expiration time for cached configs.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Integer> OPERATOR_CONFIG_CACHE_SIZE =
            ConfigOptions.key("kubernetes.operator.config.cache.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Max config cache size.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_JM_DEPLOYMENT_RECOVERY_ENABLED =
            ConfigOptions.key("kubernetes.operator.jm-deployment-recovery.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys(
                            "kubernetes.operator.reconciler.jm-deployment-recovery.enabled")
                    .withDescription(
                            "Whether to enable recovery of missing/deleted jobmanager deployments.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Integer> OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT =
            ConfigOptions.key("kubernetes.operator.savepoint.history.max.count")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Maximum number of savepoint history entries to retain.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Integer> OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT_THRESHOLD =
            ConfigOptions.key(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT.key() + ".threshold")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum number threshold of savepoint history entries to retain.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> OPERATOR_SAVEPOINT_HISTORY_MAX_AGE =
            ConfigOptions.key("kubernetes.operator.savepoint.history.max.age")
                    .durationType()
                    .defaultValue(Duration.ofHours(24))
                    .withDescription(
                            "Maximum age for savepoint history entries to retain. Due to lazy clean-up, the most recent savepoint may live longer than the max age.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_SAVEPOINT_HISTORY_MAX_AGE_THRESHOLD =
            ConfigOptions.key(OPERATOR_SAVEPOINT_HISTORY_MAX_AGE.key() + ".threshold")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum age threshold for savepoint history entries to retain.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Map<String, String>> JAR_ARTIFACT_HTTP_HEADER =
            ConfigOptions.key("kubernetes.operator.user.artifacts.http.header")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTP header for HttpArtifactFetcher. The header will be applied when getting the session job artifacts. "
                                    + "Expected format: headerKey1:headerValue1,headerKey2:headerValue2.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> PERIODIC_SAVEPOINT_INTERVAL =
            ConfigOptions.key("kubernetes.operator.periodic.savepoint.interval")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription(
                            "Interval at which periodic savepoints will be triggered. "
                                    + "The triggering schedule is not guaranteed, savepoints will be triggered as part of the regular reconcile loop.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<String> OPERATOR_WATCHED_NAMESPACES =
            ConfigOptions.key("kubernetes.operator.watched.namespaces")
                    .stringType()
                    .defaultValue(Constants.WATCH_ALL_NAMESPACES)
                    .withDescription(
                            "Comma separated list of namespaces the operator monitors for custom resources.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<String> OPERATOR_LABEL_SELECTOR =
            ConfigOptions.key("kubernetes.operator.label.selector")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Label selector of the custom resources to be watched. Please see "
                                    + "https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors for the format supported.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Boolean> OPERATOR_DYNAMIC_NAMESPACES_ENABLED =
            ConfigOptions.key("kubernetes.operator.dynamic.namespaces.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enables dynamic change of watched/monitored namespaces.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_RETRY_INITIAL_INTERVAL =
            ConfigOptions.key("kubernetes.operator.retry.initial.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription(
                            "Initial interval of automatic reconcile retries on recoverable errors.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Double> OPERATOR_RETRY_INTERVAL_MULTIPLIER =
            ConfigOptions.key("kubernetes.operator.retry.interval.multiplier")
                    .doubleType()
                    .defaultValue(2.0)
                    .withDescription(
                            "Interval multiplier of automatic reconcile retries on recoverable errors.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Integer> OPERATOR_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("kubernetes.operator.retry.max.attempts")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Max attempts of automatic reconcile retries on recoverable errors.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_JOB_UPGRADE_LAST_STATE_FALLBACK_ENABLED =
            ConfigOptions.key("kubernetes.operator.job.upgrade.last-state-fallback.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enables last-state fallback for savepoint upgrade mode. When the job is not running thus savepoint cannot be triggered but HA metadata is available for last state restore the operator can initiate the upgrade process when the flag is enabled.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<SavepointFormatType> OPERATOR_SAVEPOINT_FORMAT_TYPE =
            ConfigOptions.key("kubernetes.operator.savepoint.format.type")
                    .enumType(SavepointFormatType.class)
                    .defaultValue(SavepointFormatType.DEFAULT)
                    .withDescription(
                            "Type of the binary format in which a savepoint should be taken.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Boolean> OPERATOR_HEALTH_PROBE_ENABLED =
            ConfigOptions.key("kubernetes.operator.health.probe.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enables health probe for the kubernetes operator.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Integer> OPERATOR_HEALTH_PROBE_PORT =
            ConfigOptions.key("kubernetes.operator.health.probe.port")
                    .intType()
                    .defaultValue(8085)
                    .withDescription("The port the health probe will use to expose the status.");
}
