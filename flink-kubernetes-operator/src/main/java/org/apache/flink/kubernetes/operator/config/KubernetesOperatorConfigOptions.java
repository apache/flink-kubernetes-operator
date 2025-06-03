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
import org.apache.flink.kubernetes.operator.api.status.CheckpointType;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.config.LeaderElectionConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Constants;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** This class holds configuration constants used by flink operator. */
public class KubernetesOperatorConfigOptions {

    public static final String K8S_OP_CONF_PREFIX = "kubernetes.operator.";

    private static final String DEFAULT_CONF_PREFIX = K8S_OP_CONF_PREFIX + "default-configuration.";
    public static final String VERSION_CONF_PREFIX = DEFAULT_CONF_PREFIX + "flink-version.";
    public static final String FLINK_VERSION_GREATER_THAN_SUFFIX = "+";
    public static final String NAMESPACE_CONF_PREFIX = DEFAULT_CONF_PREFIX + "namespace.";
    public static final String SECTION_SYSTEM = "system";
    public static final String SECTION_ADVANCED = "system_advanced";
    public static final String SECTION_DYNAMIC = "dynamic";

    public static ConfigOptions.OptionBuilder operatorConfig(String key) {
        return ConfigOptions.key(K8S_OP_CONF_PREFIX + key);
    }

    public static String operatorConfigKey(String key) {
        return K8S_OP_CONF_PREFIX + key;
    }

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_RECONCILE_INTERVAL =
            operatorConfig("reconcile.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDeprecatedKeys(K8S_OP_CONF_PREFIX + "reconciler.reschedule.interval")
                    .withDescription(
                            "The interval for the controller to reschedule the reconcile process.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_OBSERVER_REST_READY_DELAY =
            operatorConfig("observer.rest-ready.delay")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "Final delay before deployment is marked ready after port becomes accessible.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Integer> OPERATOR_RECONCILE_PARALLELISM =
            operatorConfig("reconcile.parallelism")
                    .intType()
                    .defaultValue(ConfigurationService.DEFAULT_RECONCILIATION_THREADS_NUMBER)
                    .withDeprecatedKeys(operatorConfigKey("reconciler.max.parallelism"))
                    .withDescription(
                            "The maximum number of threads running the reconciliation loop. Use -1 for infinite.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_OBSERVER_PROGRESS_CHECK_INTERVAL =
            operatorConfig("observer.progress-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "The interval for observing status for in-progress operations such as deployment and savepoints.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> OPERATOR_SAVEPOINT_TRIGGER_GRACE_PERIOD =
            operatorConfig("savepoint.trigger.grace-period")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDeprecatedKeys(
                            operatorConfigKey("observer.savepoint.trigger.grace-period"))
                    .withDescription(
                            "The interval before a savepoint trigger attempt is marked as unsuccessful.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> OPERATOR_CHECKPOINT_TRIGGER_GRACE_PERIOD =
            operatorConfig("checkpoint.trigger.grace-period")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "The interval before a checkpoint trigger attempt is marked as unsuccessful.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_FLINK_CLIENT_TIMEOUT =
            operatorConfig("flink.client.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDeprecatedKeys(operatorConfigKey("observer.flink.client.timeout"))
                    .withDescription(
                            "The timeout for the observer to wait the flink rest client to return.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_FLINK_CLIENT_CANCEL_TIMEOUT =
            operatorConfig("flink.client.cancel.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDeprecatedKeys(operatorConfigKey("reconciler.flink.cancel.job.timeout"))
                    .withDescription(
                            "The timeout for the reconciler to wait for flink to cancel job.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_RESOURCE_CLEANUP_TIMEOUT =
            operatorConfig("resource.cleanup.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDeprecatedKeys(
                            operatorConfigKey("reconciler.flink.cluster.shutdown.timeout"))
                    .withDescription(
                            "The timeout for the resource clean up to wait for flink to shutdown cluster.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> DEPLOYMENT_ROLLBACK_ENABLED =
            operatorConfig("deployment.rollback.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable rolling back failed deployment upgrades.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> DEPLOYMENT_READINESS_TIMEOUT =
            operatorConfig("deployment.readiness.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription(
                            "The timeout for deployments to become ready/stable "
                                    + "before being rolled back if rollback is enabled.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<String> OPERATOR_USER_ARTIFACTS_BASE_DIR =
            operatorConfig("user.artifacts.base.dir")
                    .stringType()
                    .defaultValue("/opt/flink/artifacts")
                    .withDescription("The base dir to put the session job artifacts.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> JOB_UPGRADE_IGNORE_PENDING_SAVEPOINT =
            operatorConfig("job.upgrade.ignore-pending-savepoint")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore pending savepoint during job upgrade.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> JOB_UPGRADE_INPLACE_SCALING_ENABLED =
            operatorConfig("job.upgrade.inplace-scaling.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable inplace scaling for Flink 1.18+ using the resource requirements API. On failure or earlier Flink versions it falls back to regular full redeployment.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Boolean> OPERATOR_DYNAMIC_CONFIG_ENABLED =
            operatorConfig("dynamic.config.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable on-the-fly config changes through the operator configmap.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_DYNAMIC_CONFIG_CHECK_INTERVAL =
            operatorConfig("dynamic.config.check.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription("Time interval for checking config changes.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_CONFIG_CACHE_TIMEOUT =
            operatorConfig("config.cache.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription("Expiration time for cached configs.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Integer> OPERATOR_CONFIG_CACHE_SIZE =
            operatorConfig("config.cache.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Max config cache size.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_JM_DEPLOYMENT_RECOVERY_ENABLED =
            operatorConfig("jm-deployment-recovery.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys(
                            operatorConfigKey("reconciler.jm-deployment-recovery.enabled"))
                    .withDescription(
                            "Whether to enable recovery of missing/deleted jobmanager deployments.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_JOB_SAVEPOINT_DISPOSE_ON_DELETE =
            operatorConfig("savepoint.dispose-on-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Savepoint data for FlinkStateSnapshot resources created by the operator during upgrades and periodic savepoints will be disposed of automatically when the generated Kubernetes resource is deleted.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<SavepointFormatType> OPERATOR_SAVEPOINT_FORMAT_TYPE =
            operatorConfig("savepoint.format.type")
                    .enumType(SavepointFormatType.class)
                    .defaultValue(SavepointFormatType.DEFAULT)
                    .withDescription(
                            "Type of the binary format in which a savepoint should be taken.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<CheckpointType> OPERATOR_CHECKPOINT_TYPE =
            operatorConfig("checkpoint.type")
                    .enumType(CheckpointType.class)
                    .defaultValue(CheckpointType.FULL)
                    .withDescription("Type of checkpoint.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_SAVEPOINT_CLEANUP_ENABLED =
            operatorConfig("savepoint.cleanup.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            String.format(
                                    "Whether to enable clean up of savepoint FlinkStateSnapshot resources. Savepoint state will be disposed of as well if the snapshot CR spec is configured as such. For automatic savepoints this can be configured via the %s config option.",
                                    OPERATOR_JOB_SAVEPOINT_DISPOSE_ON_DELETE.key()));

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Integer> OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT =
            operatorConfig("savepoint.history.max.count")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Maximum number of savepoint FlinkStateSnapshot resources entries to retain.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Integer> OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT_THRESHOLD =
            ConfigOptions.key(OPERATOR_SAVEPOINT_HISTORY_MAX_COUNT.key() + ".threshold")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum number threshold of savepoint FlinkStateSnapshot resources to retain.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> OPERATOR_SAVEPOINT_HISTORY_MAX_AGE =
            operatorConfig("savepoint.history.max.age")
                    .durationType()
                    .defaultValue(Duration.ofHours(24))
                    .withDescription(
                            "Maximum age for savepoint FlinkStateSnapshot resources to retain. Due to lazy clean-up, the most recent savepoint may live longer than the max age.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_SAVEPOINT_HISTORY_MAX_AGE_THRESHOLD =
            ConfigOptions.key(OPERATOR_SAVEPOINT_HISTORY_MAX_AGE.key() + ".threshold")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum age threshold for FlinkStateSnapshot resources to retain.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Boolean> OPERATOR_EXCEPTION_STACK_TRACE_ENABLED =
            operatorConfig("exception.stacktrace.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable exception stacktrace to be included in CR status error field.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Integer> OPERATOR_EXCEPTION_STACK_TRACE_MAX_LENGTH =
            operatorConfig("exception.stacktrace.max.length")
                    .intType()
                    .defaultValue(2048)
                    .withDescription(
                            "Maximum length of stacktrace to be included in CR status error field.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Integer> OPERATOR_EXCEPTION_FIELD_MAX_LENGTH =
            operatorConfig("exception.field.max.length")
                    .intType()
                    .defaultValue(2048)
                    .withDescription(
                            "Maximum length of each exception field including stack trace to be included in CR status error field.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Integer> OPERATOR_EXCEPTION_THROWABLE_LIST_MAX_COUNT =
            operatorConfig("exception.throwable.list.max.count")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "Maximum number of throwable to be included in CR status error field.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Map<String, String>> OPERATOR_EXCEPTION_LABEL_MAPPER =
            operatorConfig("exception.label.mapper")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Key-Value pair where key is the REGEX to filter through the exception messages and value is the string to be included in CR status error label field if the REGEX matches. Expected format: headerKey1:headerValue1,headerKey2:headerValue2.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Map<String, String>> JAR_ARTIFACT_HTTP_HEADER =
            operatorConfig("user.artifacts.http.header")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTP header for HttpArtifactFetcher. The header will be applied when getting the session job artifacts. "
                                    + "Expected format: headerKey1:headerValue1,headerKey2:headerValue2.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> SNAPSHOT_RESOURCE_ENABLED =
            operatorConfig("snapshot.resource.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Create new FlinkStateSnapshot resources for storing snapshots. "
                                    + "Disable if you wish to use the deprecated mode and save snapshot results to "
                                    + "FlinkDeployment/FlinkSessionJob status fields. The Operator will fallback to "
                                    + "legacy mode during runtime if the CRD is not found, "
                                    + "even if this value is true.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<String> PERIODIC_SAVEPOINT_INTERVAL =
            operatorConfig("periodic.savepoint.interval")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Option to enable automatic savepoint triggering. Can be specified "
                                    + "either as a Duration type (i.e. '10m') or as a cron expression "
                                    + "in Quartz format (6 or 7 positions, see "
                                    + "http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html)."
                                    + "The triggering schedule is not guaranteed, savepoints will be "
                                    + "triggered as part of the regular reconcile loop. "
                                    + "WARNING: not intended to be used together with the cron-based "
                                    + "periodic savepoint triggering");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<String> PERIODIC_CHECKPOINT_INTERVAL =
            operatorConfig("periodic.checkpoint.interval")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Option to enable automatic checkpoint triggering. Can be specified "
                                    + "either as a Duration type (i.e. '10m') or as a cron expression "
                                    + "in Quartz format (6 or 7 positions, see "
                                    + "http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html)."
                                    + "The triggering schedule is not guaranteed, checkpoints will "
                                    + "be triggered as part of the regular reconcile loop. "
                                    + "NOTE: checkpoints are generally managed by Flink. This "
                                    + "setting isn't meant to replace Flink's checkpoint settings, "
                                    + "but to complement them in special cases. For instance, a "
                                    + "full checkpoint might need to be occasionally triggered to "
                                    + "break the chain of incremental checkpoints and consolidate "
                                    + "the partial incremental files. "
                                    + "WARNING: not intended to be used together with the cron-based "
                                    + "periodic checkpoint triggering");

    @SuppressWarnings("unused")
    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<String> PLUGINS_LISTENERS_CLASS =
            operatorConfig("plugins.listeners.<listener-name>.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom plugins listener class, 'listener-name' is the name of the plugin listener, and its value is a fully qualified class name.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<String> OPERATOR_WATCHED_NAMESPACES =
            operatorConfig("watched.namespaces")
                    .stringType()
                    .defaultValue(Constants.WATCH_ALL_NAMESPACES)
                    .withDescription(
                            "Comma separated list of namespaces the operator monitors for custom resources.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<String> OPERATOR_LABEL_SELECTOR =
            operatorConfig("label.selector")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Label selector of the custom resources to be watched. Please see "
                                    + "https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors for the format supported.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Boolean> OPERATOR_DYNAMIC_NAMESPACES_ENABLED =
            operatorConfig("dynamic.namespaces.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enables dynamic change of watched/monitored namespaces.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_RETRY_INITIAL_INTERVAL =
            operatorConfig("retry.initial.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription("Initial interval of retries on unhandled controller errors.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_RETRY_MAX_INTERVAL =
            operatorConfig("retry.max.interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Max interval of retries on unhandled controller errors.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Double> OPERATOR_RETRY_INTERVAL_MULTIPLIER =
            operatorConfig("retry.interval.multiplier")
                    .doubleType()
                    .defaultValue(1.5)
                    .withDescription(
                            "Interval multiplier of retries on unhandled controller errors.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Integer> OPERATOR_RETRY_MAX_ATTEMPTS =
            operatorConfig("retry.max.attempts")
                    .intType()
                    .defaultValue(15)
                    .withDescription("Max attempts of retries on unhandled controller errors.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_RATE_LIMITER_PERIOD =
            operatorConfig("rate-limiter.refresh-period")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(15))
                    .withDescription("Operator rate limiter refresh period for each resource.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Integer> OPERATOR_RATE_LIMITER_LIMIT =
            operatorConfig("rate-limiter.limit")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "Max number of reconcile loops triggered within the rate limiter refresh period for each resource. Setting the limit <= 0 disables the limiter.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_JOB_UPGRADE_LAST_STATE_FALLBACK_ENABLED =
            operatorConfig("job.upgrade.last-state-fallback.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enables last-state fallback for savepoint upgrade mode. When the job is not running thus savepoint cannot be triggered but HA metadata is available for last state restore the operator can initiate the upgrade process when the flag is enabled.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> OPERATOR_JOB_UPGRADE_LAST_STATE_CHECKPOINT_MAX_AGE =
            operatorConfig("job.upgrade.last-state.max.allowed.checkpoint.age")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Max allowed checkpoint age for initiating last-state upgrades on running jobs. If a checkpoint is not available within the desired age (and nothing in progress) a savepoint will be triggered.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_JOB_UPGRADE_LAST_STATE_CANCEL_JOB =
            operatorConfig("job.upgrade.last-state.job-cancel.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Cancel jobs during last-state upgrade. This config is ignored for session jobs where cancel is the only mechanism to perform this type of upgrade.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Boolean> OPERATOR_HEALTH_PROBE_ENABLED =
            operatorConfig("health.probe.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enables health probe for the kubernetes operator.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Integer> OPERATOR_HEALTH_PROBE_PORT =
            operatorConfig("health.probe.port")
                    .intType()
                    .defaultValue(8085)
                    .withDescription("The port the health probe will use to expose the status.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Boolean> OPERATOR_STOP_ON_INFORMER_ERROR =
            operatorConfig("startup.stop-on-informer-error")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether informer errors should stop operator startup. If false, the startup will ignore recoverable errors, caused for example by RBAC issues and will retry periodically.");

    public static final int DEFAULT_TERMINATION_TIMEOUT_SECONDS = 10;

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> OPERATOR_TERMINATION_TIMEOUT =
            operatorConfig("termination.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(DEFAULT_TERMINATION_TIMEOUT_SECONDS))
                    .withDescription(
                            "Operator shutdown timeout before reconciliation threads are killed.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED =
            operatorConfig("cluster.health-check.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable health check for clusters.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW =
            operatorConfig("cluster.health-check.restarts.window")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(2))
                    .withDescription(
                            "The duration of the time window where job restart count measured.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Integer> OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD =
            operatorConfig("cluster.health-check.restarts.threshold")
                    .intType()
                    .defaultValue(64)
                    .withDescription(
                            "The threshold which is checked against job restart count within a configured window. "
                                    + "If the restart count is reaching the threshold then full cluster restart is initiated.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean>
            OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_ENABLED =
                    operatorConfig("cluster.health-check.checkpoint-progress.enabled")
                            .booleanType()
                            .defaultValue(true)
                            .withDescription(
                                    "Whether to enable checkpoint progress health check for clusters.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration>
            OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_WINDOW =
                    operatorConfig("cluster.health-check.checkpoint-progress.window")
                            .durationType()
                            .noDefaultValue()
                            .withDescription(
                                    "If no checkpoints are completed within the defined time window, the job is considered unhealthy. The minimum window size is `max(checkpointingInterval, checkpointTimeout) * (tolerableCheckpointFailures + 2)`, which also serves as the default value when checkpointing is enabled. For example with checkpoint interval 10 minutes and 0 tolerable failures, the default progress check window will be 20 minutes.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_JOB_RESTART_FAILED =
            operatorConfig("job.restart.failed")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to restart failed jobs.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Boolean> OPERATOR_LEADER_ELECTION_ENABLED =
            operatorConfig("leader-election.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable leader election for the operator to allow running standby instances.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<String> OPERATOR_LEADER_ELECTION_LEASE_NAME =
            operatorConfig("leader-election.lease-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Leader election lease name, must be unique for leases in the same namespace.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_LEADER_ELECTION_LEASE_DURATION =
            operatorConfig("leader-election.lease-duration")
                    .durationType()
                    .defaultValue(LeaderElectionConfiguration.LEASE_DURATION_DEFAULT_VALUE)
                    .withDescription("Leader election lease duration.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_LEADER_ELECTION_RENEW_DEADLINE =
            operatorConfig("leader-election.renew-deadline")
                    .durationType()
                    .defaultValue(LeaderElectionConfiguration.RENEW_DEADLINE_DEFAULT_VALUE)
                    .withDescription("Leader election renew deadline.");

    @Documentation.Section(SECTION_SYSTEM)
    public static final ConfigOption<Duration> OPERATOR_LEADER_ELECTION_RETRY_PERIOD =
            operatorConfig("leader-election.retry-period")
                    .durationType()
                    .defaultValue(LeaderElectionConfiguration.RETRY_PERIOD_DEFAULT_VALUE)
                    .withDescription("Leader election retry period.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Duration> OPERATOR_JM_SHUTDOWN_TTL =
            operatorConfig("jm-deployment.shutdown-ttl")
                    .durationType()
                    .defaultValue(Duration.ofDays(1))
                    .withDescription(
                            "Time after which jobmanager pods of terminal application deployments are shut down.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> CANARY_RESOURCE_TIMEOUT =
            operatorConfig("health.canary.resource.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "Allowed max time between spec update and reconciliation for canary resources.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> OPERATOR_JM_STARTUP_PROBE_ENABLED =
            operatorConfig("jm-deployment.startup.probe.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enable job manager startup probe to allow detecting when the jobmanager could not submit the job.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> POD_TEMPLATE_MERGE_BY_NAME =
            operatorConfig("pod-template.merge-arrays-by-name")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Configure the array merge behaviour during pod merging. Arrays can be either merged by position or name matching.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<DeletionPropagation> RESOURCE_DELETION_PROPAGATION =
            operatorConfig("resource.deletion.propagation")
                    .enumType(DeletionPropagation.class)
                    .defaultValue(DeletionPropagation.FOREGROUND)
                    .withDescription("JM/TM Deployment deletion propagation.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> SAVEPOINT_ON_DELETION =
            operatorConfig("job.savepoint-on-deletion")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Indicate whether a savepoint must be taken when deleting a FlinkDeployment or FlinkSessionJob.");

    @Documentation.Section(SECTION_DYNAMIC)
    public static final ConfigOption<Boolean> DRAIN_ON_SAVEPOINT_DELETION =
            operatorConfig("job.drain-on-savepoint-deletion")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Indicate whether the job should be drained when stopping with savepoint.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Duration> REFRESH_CLUSTER_RESOURCE_VIEW =
            operatorConfig("cluster.resource-view.refresh-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(-1))
                    .withDescription(
                            "How often to retrieve Kubernetes cluster resource usage information. This information is used to avoid running out of cluster resources when scaling up resources. Negative values disable the feature.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Integer> OPERATOR_EVENT_EXCEPTION_STACKTRACE_LINES =
            operatorConfig("events.exceptions.stacktrace-lines")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "Maximum number of stack trace lines to include in exception-related Kubernetes event messages.");

    @Documentation.Section(SECTION_ADVANCED)
    public static final ConfigOption<Integer> OPERATOR_EVENT_EXCEPTION_LIMIT =
            operatorConfig("events.exceptions.limit-per-reconciliation")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Maximum number of exception-related Kubernetes events emitted per reconciliation cycle.");
}
