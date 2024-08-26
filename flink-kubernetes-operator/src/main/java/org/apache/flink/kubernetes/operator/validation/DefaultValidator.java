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

package org.apache.flink.kubernetes.operator.validation;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.utils.CalendarUtils;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.Quantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Default validator implementation for {@link FlinkDeployment}. */
public class DefaultValidator implements FlinkResourceValidator {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultValidator.class);

    private static final Pattern DEPLOYMENT_NAME_PATTERN =
            Pattern.compile("[a-z]([-a-z\\d]{0,43}[a-z\\d])?");
    private static final String[] FORBIDDEN_CONF_KEYS =
            new String[] {
                KubernetesConfigOptions.NAMESPACE.key(),
                KubernetesConfigOptions.CLUSTER_ID.key(),
                HighAvailabilityOptions.HA_CLUSTER_ID.key()
            };

    private static final Set<String> ALLOWED_LOG_CONF_KEYS =
            Set.of(Constants.CONFIG_FILE_LOG4J_NAME, Constants.CONFIG_FILE_LOGBACK_NAME);

    private final FlinkConfigManager configManager;

    public DefaultValidator(FlinkConfigManager configManager) {
        this.configManager = configManager;
    }

    @Override
    public Optional<String> validateDeployment(FlinkDeployment deployment) {
        FlinkDeploymentSpec spec = deployment.getSpec();
        Map<String, String> effectiveConfig =
                configManager
                        .getDefaultConfig(
                                deployment.getMetadata().getNamespace(), spec.getFlinkVersion())
                        .toMap();
        if (spec.getFlinkConfiguration() != null) {
            effectiveConfig.putAll(spec.getFlinkConfiguration());
        }
        return firstPresent(
                validateDeploymentName(deployment.getMetadata().getName()),
                validateFlinkVersion(deployment),
                validateFlinkDeploymentConfig(effectiveConfig),
                validateIngress(
                        spec.getIngress(),
                        deployment.getMetadata().getName(),
                        deployment.getMetadata().getNamespace()),
                validateLogConfig(spec.getLogConfiguration()),
                validateJobSpec(spec.getJob(), spec.getTaskManager(), effectiveConfig),
                validateJmSpec(spec.getJobManager(), effectiveConfig),
                validateTmSpec(spec.getTaskManager(), effectiveConfig),
                validateSpecChange(deployment, effectiveConfig),
                validateServiceAccount(spec.getServiceAccount()),
                validateAutoScalerFlinkConfiguration(effectiveConfig));
    }

    @SafeVarargs
    private static Optional<String> firstPresent(Optional<String>... errOpts) {
        for (Optional<String> opt : errOpts) {
            if (opt.isPresent()) {
                return opt;
            }
        }
        return Optional.empty();
    }

    private Optional<String> validateDeploymentName(String name) {
        Matcher matcher = DEPLOYMENT_NAME_PATTERN.matcher(name);
        if (!matcher.matches()) {
            return Optional.of(
                    String.format(
                            "The FlinkDeployment name: %s is invalid, must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123'), and the length must be no more than 45 characters.",
                            name));
        }
        return Optional.empty();
    }

    private Optional<String> validateFlinkVersion(FlinkDeployment deployment) {
        var spec = deployment.getSpec();
        var version = spec.getFlinkVersion();
        if (version == null) {
            return Optional.of("Flink Version must be defined.");
        }

        if (!FlinkVersion.isSupported(version)) {
            return Optional.of(
                    "Flink version " + version + " is not supported by this operator version");
        }

        var lastReconciledSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();

        if (lastReconciledSpec != null
                && lastReconciledSpec.getJob() != null
                && spec.getJob() != null
                && spec.getJob().getUpgradeMode() != UpgradeMode.STATELESS) {
            var lastJob = lastReconciledSpec.getJob();
            if (lastJob.getState() == JobState.SUSPENDED
                    && lastJob.getUpgradeMode() == UpgradeMode.LAST_STATE
                    && lastReconciledSpec.getFlinkVersion() != spec.getFlinkVersion()) {
                return Optional.of(
                        "Changing flinkVersion after last-state suspend is not allowed. Restore your cluster with the current flinkVersion and perform the version upgrade afterwards.");
            }
        }

        return Optional.empty();
    }

    private Optional<String> validateIngress(IngressSpec ingress, String name, String namespace) {
        if (ingress == null) {
            return Optional.empty();
        }
        if (ingress.getTemplate() == null) {
            return Optional.of("Ingress template must be defined");
        }
        try {
            IngressUtils.getIngressUrl(ingress.getTemplate(), name, namespace);
        } catch (ReconciliationException e) {
            return Optional.of(e.getMessage());
        }

        return Optional.empty();
    }

    private Optional<String> validateFlinkDeploymentConfig(Map<String, String> confMap) {
        if (confMap == null) {
            return Optional.empty();
        }
        Configuration conf = Configuration.fromMap(confMap);
        for (String key : FORBIDDEN_CONF_KEYS) {
            if (conf.containsKey(key)) {
                return Optional.of("Forbidden Flink config key: " + key);
            }
        }

        if (conf.get(KubernetesOperatorConfigOptions.DEPLOYMENT_ROLLBACK_ENABLED)
                && !HighAvailabilityMode.isHighAvailabilityModeActivated(conf)) {
            return Optional.of("HA must be enabled for rollback support.");
        }

        if (conf.get(KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED)
                && !conf.get(
                        KubernetesOperatorConfigOptions.OPERATOR_JM_DEPLOYMENT_RECOVERY_ENABLED)) {
            return Optional.of(
                    "Deployment recovery ("
                            + KubernetesOperatorConfigOptions
                                    .OPERATOR_JM_DEPLOYMENT_RECOVERY_ENABLED
                                    .key()
                            + ") must be enabled for job health check ("
                            + KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED
                                    .key()
                            + ") support.");
        }

        return Optional.empty();
    }

    private Optional<String> validateLogConfig(Map<String, String> confMap) {
        if (confMap == null) {
            return Optional.empty();
        }
        for (String key : confMap.keySet()) {
            if (!ALLOWED_LOG_CONF_KEYS.contains(key)) {
                return Optional.of(
                        String.format(
                                "Invalid log config key: %s. Allowed keys are %s",
                                key, ALLOWED_LOG_CONF_KEYS));
            }
        }
        return Optional.empty();
    }

    private Optional<String> validateJobSpec(
            JobSpec job, @Nullable TaskManagerSpec tm, Map<String, String> confMap) {
        if (job == null) {
            return Optional.empty();
        }

        Configuration configuration = Configuration.fromMap(confMap);

        if (job.getUpgradeMode() != UpgradeMode.STATELESS) {
            if (StringUtils.isNullOrWhitespaceOnly(
                    configuration.getString(CheckpointingOptions.CHECKPOINTS_DIRECTORY))) {
                return Optional.of(
                        String.format(
                                "Checkpoint directory[%s] must be defined for last-state and savepoint upgrade modes",
                                CheckpointingOptions.CHECKPOINTS_DIRECTORY.key()));
            }
        }

        if (StringUtils.isNullOrWhitespaceOnly(
                configuration.getString(CheckpointingOptions.SAVEPOINT_DIRECTORY))) {
            if (job.getUpgradeMode() == UpgradeMode.SAVEPOINT) {
                return Optional.of(
                        String.format(
                                "Job could not be upgraded with savepoint while config key[%s] is not set",
                                CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));
            } else if (job.getSavepointTriggerNonce() != null) {
                return Optional.of(
                        String.format(
                                "Savepoint could not be manually triggered for the running job while config key[%s] is not set",
                                CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));
            } else if (configuration.contains(
                    KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL)) {
                return Optional.of(
                        String.format(
                                "Periodic savepoints cannot be enabled when config key[%s] is not set",
                                CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));
            } else if (configuration.get(
                            KubernetesOperatorConfigOptions
                                    .OPERATOR_JOB_UPGRADE_LAST_STATE_CHECKPOINT_MAX_AGE)
                    != null) {
                return Optional.of(
                        String.format(
                                "In order to use max-checkpoint age functionality config key[%s] must be set to allow triggering savepoint upgrades.",
                                CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));
            }
        }

        var tmReplicasDefined = tm != null && tm.getReplicas() != null;

        if (tmReplicasDefined && tm.getReplicas() < 1) {
            return Optional.of("TaskManager replicas must be larger than 0");
        } else if (!tmReplicasDefined && job.getParallelism() < 1) {
            return Optional.of("Job parallelism must be larger than 0");
        }

        return Optional.empty();
    }

    private Optional<String> validateJmSpec(JobManagerSpec jmSpec, Map<String, String> confMap) {
        Configuration conf = Configuration.fromMap(confMap);
        var jmMemoryDefined =
                jmSpec != null
                        && jmSpec.getResource() != null
                        && !StringUtils.isNullOrWhitespaceOnly(jmSpec.getResource().getMemory());
        Optional<String> jmMemoryValidation =
                jmMemoryDefined ? Optional.empty() : validateJmMemoryConfig(conf);

        if (jmSpec == null) {
            return jmMemoryValidation;
        }

        return firstPresent(
                jmMemoryValidation,
                validateResources("JobManager", jmSpec.getResource()),
                validateJmReplicas(jmSpec.getReplicas(), confMap));
    }

    private Optional<String> validateJmMemoryConfig(Configuration conf) {
        try {
            JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
                    conf, JobManagerOptions.JVM_HEAP_MEMORY);
        } catch (Exception e) {
            return Optional.of(
                    "JobManager resource memory must be defined using `spec.jobManager.resource.memory`");
        }

        return Optional.empty();
    }

    private Optional<String> validateJmReplicas(int replicas, Map<String, String> confMap) {
        if (replicas < 1) {
            return Optional.of("JobManager replicas should not be configured less than one.");
        } else if (replicas > 1
                && !HighAvailabilityMode.isHighAvailabilityModeActivated(
                        Configuration.fromMap(confMap))) {
            return Optional.of(
                    "High availability should be enabled when starting standby JobManagers.");
        }
        return Optional.empty();
    }

    private Optional<String> validateTmSpec(TaskManagerSpec tmSpec, Map<String, String> confMap) {
        Configuration conf = Configuration.fromMap(confMap);

        var tmMemoryDefined =
                tmSpec != null
                        && tmSpec.getResource() != null
                        && !StringUtils.isNullOrWhitespaceOnly(tmSpec.getResource().getMemory());
        Optional<String> tmMemoryConfigValidation =
                tmMemoryDefined ? Optional.empty() : validateTmMemoryConfig(conf);

        if (tmSpec == null) {
            return tmMemoryConfigValidation;
        }

        return firstPresent(
                tmMemoryConfigValidation,
                validateResources("TaskManager", tmSpec.getResource()),
                validateTmReplicas(tmSpec));
    }

    private Optional<String> validateTmMemoryConfig(Configuration conf) {
        try {
            TaskExecutorProcessUtils.processSpecFromConfig(conf);
        } catch (Exception e) {
            return Optional.of(
                    "TaskManager resource memory must be defined using `spec.taskManager.resource.memory`");
        }
        return Optional.empty();
    }

    private Optional<String> validateTmReplicas(TaskManagerSpec tmSpec) {
        if (tmSpec.getReplicas() != null && tmSpec.getReplicas() < 1) {
            return Optional.of("TaskManager replicas should not be configured less than one.");
        }
        return Optional.empty();
    }

    private Optional<String> validateResources(String component, Resource resource) {
        if (resource == null) {
            return Optional.empty();
        }

        String memory = resource.getMemory();
        String storage = resource.getEphemeralStorage();
        StringBuilder builder = new StringBuilder();

        if (memory != null) {
            try {
                MemorySize.parse(
                        FlinkConfigBuilder.parseResourceMemoryString(resource.getMemory()));
            } catch (IllegalArgumentException iae) {
                builder.append(component + " resource memory parse error: " + iae.getMessage());
            }
        }

        if (storage != null) {
            try {
                Quantity quantity = Quantity.parse(storage);
                Quantity.getAmountInBytes(quantity);
            } catch (IllegalArgumentException iae) {
                builder.append(
                        component + " resource ephemeral storage parse error: " + iae.getMessage());
            }
        }

        String errorMessage = builder.toString();
        if (!StringUtils.isNullOrWhitespaceOnly(errorMessage)) {
            return Optional.of(errorMessage);
        }

        return Optional.empty();
    }

    private Optional<String> validateSpecChange(
            FlinkDeployment deployment, Map<String, String> effectiveConfig) {
        FlinkDeploymentSpec newSpec = deployment.getSpec();

        if (deployment.getStatus().getReconciliationStatus().isBeforeFirstDeployment()) {
            return Optional.empty();
        }

        FlinkDeploymentSpec oldSpec =
                deployment.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();

        if (newSpec.getJob() != null && oldSpec.getJob() == null) {
            return Optional.of("Cannot switch from session to job cluster");
        }

        if (newSpec.getJob() == null && oldSpec.getJob() != null) {
            return Optional.of("Cannot switch from job to session cluster");
        }

        KubernetesDeploymentMode oldDeploymentMode =
                oldSpec.getMode() == null ? KubernetesDeploymentMode.NATIVE : oldSpec.getMode();

        KubernetesDeploymentMode newDeploymentMode =
                newSpec.getMode() == null ? KubernetesDeploymentMode.NATIVE : newSpec.getMode();

        if (oldDeploymentMode == KubernetesDeploymentMode.NATIVE
                && newDeploymentMode != KubernetesDeploymentMode.NATIVE) {
            return Optional.of(
                    "Cannot switch from native kubernetes to standalone kubernetes cluster");
        }

        if (oldDeploymentMode == KubernetesDeploymentMode.STANDALONE
                && newDeploymentMode != KubernetesDeploymentMode.STANDALONE) {
            return Optional.of(
                    "Cannot switch from standalone kubernetes to native kubernetes cluster");
        }

        JobSpec oldJob = oldSpec.getJob();
        JobSpec newJob = newSpec.getJob();
        if (oldJob != null && newJob != null) {
            if (newJob.getSavepointRedeployNonce() != null
                    && !newJob.getSavepointRedeployNonce()
                            .equals(oldJob.getSavepointRedeployNonce())) {
                if (StringUtils.isNullOrWhitespaceOnly(newJob.getInitialSavepointPath())) {
                    return Optional.of(
                            "InitialSavepointPath must not be empty for savepoint redeployment");
                }
            }
        }

        return Optional.empty();
    }

    // validate session job

    @Override
    public Optional<String> validateSessionJob(
            FlinkSessionJob sessionJob, Optional<FlinkDeployment> sessionOpt) {

        if (sessionOpt.isEmpty()) {
            return validateSessionJobOnly(sessionJob);
        } else {
            return firstPresent(
                    validateSessionJobOnly(sessionJob),
                    validateSessionJobWithCluster(sessionJob, sessionOpt.get()));
        }
    }

    @Override
    public Optional<String> validateStateSnapshot(
            FlinkStateSnapshot snapshot, Optional<AbstractFlinkResource<?, ?>> target) {
        var spec = snapshot.getSpec();

        if ((!spec.isSavepoint() && !spec.isCheckpoint())
                || (spec.isSavepoint() && spec.isCheckpoint())) {
            return Optional.of(
                    "Exactly one of checkpoint or savepoint configurations has to be set.");
        }

        if (spec.isSavepoint() && spec.getSavepoint().getAlreadyExists()) {
            return Optional.empty();
        }

        // The remaining checks are not required if savepoint already exists.
        if (spec.getJobReference() == null) {
            return Optional.of("Job reference must be supplied for this snapshot");
        }

        // If the savepoint has already been processed by the operator, we don't need to check the
        // job reference.
        if (target.isEmpty()
                && (snapshot.getStatus() == null
                        || FlinkStateSnapshotStatus.State.TRIGGER_PENDING.equals(
                                snapshot.getStatus().getState()))) {
            var resourceId = FlinkStateSnapshotUtils.getSnapshotJobReferenceResourceId(snapshot);
            return Optional.of(
                    String.format(
                            "Target for snapshot %s/%s was not found",
                            resourceId.getNamespace().orElse(null), resourceId.getName()));
        }

        return Optional.empty();
    }

    private Optional<String> validateSessionJobOnly(FlinkSessionJob sessionJob) {
        return firstPresent(
                validateDeploymentName(sessionJob.getSpec().getDeploymentName()),
                validateJobNotEmpty(sessionJob),
                validateSpecChange(sessionJob));
    }

    private Optional<String> validateSessionJobWithCluster(
            FlinkSessionJob sessionJob, FlinkDeployment sessionCluster) {
        Map<String, String> effectiveConfig =
                configManager
                        .getDefaultConfig(
                                sessionJob.getMetadata().getNamespace(),
                                sessionCluster.getSpec().getFlinkVersion())
                        .toMap();
        if (sessionCluster.getSpec().getFlinkConfiguration() != null) {
            effectiveConfig.putAll(sessionCluster.getSpec().getFlinkConfiguration());
        }

        if (sessionJob.getSpec().getFlinkConfiguration() != null) {
            effectiveConfig.putAll(sessionJob.getSpec().getFlinkConfiguration());
        }

        return firstPresent(
                validateNotApplicationCluster(sessionCluster),
                validateSessionClusterId(sessionJob, sessionCluster),
                validateJobSpec(sessionJob.getSpec().getJob(), null, effectiveConfig),
                validateAutoScalerFlinkConfiguration(effectiveConfig));
    }

    private Optional<String> validateJobNotEmpty(FlinkSessionJob sessionJob) {
        if (sessionJob.getSpec().getJob() == null) {
            return Optional.of("The job spec should not be empty");
        }
        return Optional.empty();
    }

    private Optional<String> validateNotApplicationCluster(FlinkDeployment session) {
        if (session.getSpec().getJob() != null) {
            return Optional.of("Can not submit session job to application cluster");
        } else {
            return Optional.empty();
        }
    }

    private Optional<String> validateSessionClusterId(
            FlinkSessionJob sessionJob, FlinkDeployment session) {
        var deploymentName = session.getMetadata().getName();
        if (!deploymentName.equals(sessionJob.getSpec().getDeploymentName())) {
            return Optional.of(
                    "The session job's cluster id is not match with the session cluster");
        } else {
            return Optional.empty();
        }
    }

    private Optional<String> validateSpecChange(FlinkSessionJob sessionJob) {
        if (sessionJob.getStatus().getReconciliationStatus().isBeforeFirstDeployment()) {
            return Optional.empty();
        } else {
            var lastReconciledSpec =
                    sessionJob
                            .getStatus()
                            .getReconciliationStatus()
                            .deserializeLastReconciledSpec();
            if (!lastReconciledSpec
                    .getDeploymentName()
                    .equals(sessionJob.getSpec().getDeploymentName())) {
                return Optional.of("The deploymentName can't be changed");
            }
        }

        return Optional.empty();
    }

    private Optional<String> validateServiceAccount(String serviceAccount) {
        if (serviceAccount == null) {
            return Optional.of(
                    "spec.serviceAccount must be defined. If you use helm, its value should be the same with the name of jobServiceAccount.");
        }
        return Optional.empty();
    }

    public static Optional<String> validateAutoScalerFlinkConfiguration(
            Map<String, String> effectiveConfig) {
        if (effectiveConfig == null) {
            return Optional.empty();
        }
        Configuration flinkConfiguration = Configuration.fromMap(effectiveConfig);
        if (!flinkConfiguration.getBoolean(AutoScalerOptions.AUTOSCALER_ENABLED)) {
            return Optional.empty();
        }
        return firstPresent(
                validateNumber(flinkConfiguration, AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.0d),
                validateNumber(flinkConfiguration, AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.0d),
                validateNumber(flinkConfiguration, AutoScalerOptions.TARGET_UTILIZATION, 0.0d),
                validateNumber(
                        flinkConfiguration, AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.0d),
                CalendarUtils.validateExcludedPeriods(flinkConfiguration));
    }

    private static <T extends Number> Optional<String> validateNumber(
            Configuration flinkConfiguration,
            ConfigOption<T> autoScalerConfig,
            Double min,
            Double max) {
        try {
            var configValue = flinkConfiguration.get(autoScalerConfig);
            if (configValue != null) {
                double value = configValue.doubleValue();
                if ((min != null && value < min) || (max != null && value > max)) {
                    return Optional.of(
                            String.format(
                                    "The AutoScalerOption %s is invalid, it should be a value within the range [%s, %s]",
                                    autoScalerConfig.key(),
                                    min != null ? min.toString() : "-Infinity",
                                    max != null ? max.toString() : "+Infinity"));
                }
            }
            return Optional.empty();
        } catch (IllegalArgumentException e) {
            return Optional.of(
                    String.format(
                            "Invalid value in the autoscaler config %s", autoScalerConfig.key()));
        }
    }

    private static <T extends Number> Optional<String> validateNumber(
            Configuration flinkConfiguration, ConfigOption<T> autoScalerConfig, Double min) {
        return validateNumber(flinkConfiguration, autoScalerConfig, min, null);
    }
}
