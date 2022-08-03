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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.crd.spec.Resource;
import org.apache.flink.kubernetes.operator.crd.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Default validator implementation for {@link FlinkDeployment}. */
public class DefaultValidator implements FlinkResourceValidator {
    private static final Pattern DEPLOYMENT_NAME_PATTERN =
            Pattern.compile("[a-z]([-a-z\\d]{0,43}[a-z\\d])?");
    private static final String[] FORBIDDEN_CONF_KEYS =
            new String[] {
                KubernetesConfigOptions.NAMESPACE.key(), KubernetesConfigOptions.CLUSTER_ID.key()
            };

    private static final Set<String> ALLOWED_FLINK_SESSION_JOB_CONF_KEYS =
            Set.of(KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER.key());

    private static final Set<String> ALLOWED_LOG_CONF_KEYS =
            Set.of(Constants.CONFIG_FILE_LOG4J_NAME, Constants.CONFIG_FILE_LOGBACK_NAME);

    private final FlinkConfigManager configManager;

    public DefaultValidator(FlinkConfigManager configManager) {
        this.configManager = configManager;
    }

    @Override
    public Optional<String> validateDeployment(FlinkDeployment deployment) {
        FlinkDeploymentSpec spec = deployment.getSpec();
        Map<String, String> effectiveConfig = configManager.getDefaultConfig().toMap();
        if (spec.getFlinkConfiguration() != null) {
            effectiveConfig.putAll(spec.getFlinkConfiguration());
        }
        return firstPresent(
                validateDeploymentName(deployment.getMetadata().getName()),
                validateFlinkVersion(spec.getFlinkVersion()),
                validateFlinkDeploymentConfig(effectiveConfig),
                validateIngress(
                        spec.getIngress(),
                        deployment.getMetadata().getName(),
                        deployment.getMetadata().getNamespace()),
                validateLogConfig(spec.getLogConfiguration()),
                validateJobSpec(spec.getJob(), spec.getTaskManager(), effectiveConfig),
                validateJmSpec(spec.getJobManager(), effectiveConfig),
                validateTmSpec(spec.getTaskManager()),
                validateSpecChange(deployment, effectiveConfig),
                validateServiceAccount(spec.getServiceAccount()));
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

    private Optional<String> validateFlinkVersion(FlinkVersion version) {
        if (version == null) {
            return Optional.of("Flink Version must be defined.");
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
                && !FlinkUtils.isKubernetesHAActivated(conf)) {
            return Optional.of("Kubernetes HA must be enabled for rollback support.");
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

        if (StringUtils.isNullOrWhitespaceOnly(job.getJarURI())) {
            return Optional.of("Jar URI must be defined");
        }

        Configuration configuration = Configuration.fromMap(confMap);
        if (job.getUpgradeMode() == UpgradeMode.LAST_STATE
                && !FlinkUtils.isKubernetesHAActivated(configuration)) {
            return Optional.of(
                    "Job could not be upgraded with last-state while Kubernetes HA disabled");
        }

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
        if (jmSpec == null) {
            return Optional.empty();
        }

        return firstPresent(
                validateResources("JobManager", jmSpec.getResource()),
                validateJmReplicas(jmSpec.getReplicas(), confMap));
    }

    private Optional<String> validateJmReplicas(int replicas, Map<String, String> confMap) {
        if (replicas < 1) {
            return Optional.of("JobManager replicas should not be configured less than one.");
        } else if (replicas > 1
                && !FlinkUtils.isKubernetesHAActivated(Configuration.fromMap(confMap))) {
            return Optional.of(
                    "Kubernetes High availability should be enabled when starting standby JobManagers.");
        }
        return Optional.empty();
    }

    private Optional<String> validateTmSpec(TaskManagerSpec tmSpec) {
        if (tmSpec == null) {
            return Optional.empty();
        }

        if (tmSpec.getReplicas() != null && tmSpec.getReplicas() < 1) {
            return Optional.of("TaskManager replicas should not be configured less than one.");
        }

        return validateResources("TaskManager", tmSpec.getResource());
    }

    private Optional<String> validateResources(String component, Resource resource) {
        if (resource == null) {
            return Optional.empty();
        }

        String memory = resource.getMemory();
        if (memory == null) {
            return Optional.of(component + " resource memory must be defined.");
        }

        try {
            MemorySize.parse(memory);
        } catch (IllegalArgumentException iae) {
            return Optional.of(component + " resource memory parse error: " + iae.getMessage());
        }

        return Optional.empty();
    }

    private Optional<String> validateSpecChange(
            FlinkDeployment deployment, Map<String, String> effectiveConfig) {
        FlinkDeploymentSpec newSpec = deployment.getSpec();

        if (deployment.getStatus().getReconciliationStatus().isFirstDeployment()) {
            if (newSpec.getJob() != null && !newSpec.getJob().getState().equals(JobState.RUNNING)) {
                return Optional.of("Job must start in running state");
            }

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
            if (StringUtils.isNullOrWhitespaceOnly(
                            effectiveConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY.key()))
                    && deployment.getStatus().getJobManagerDeploymentStatus()
                            != JobManagerDeploymentStatus.MISSING
                    && ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                            deployment, configManager.getObserveConfig(deployment))) {
                return Optional.of(
                        String.format(
                                "Job could not be upgraded to last-state while config key[%s] is not set",
                                CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));
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

    private Optional<String> validateSessionJobOnly(FlinkSessionJob sessionJob) {
        return firstPresent(
                validateDeploymentName(sessionJob.getSpec().getDeploymentName()),
                validateJobNotEmpty(sessionJob),
                validateNotLastStateUpgradeMode(sessionJob),
                validateSpecChange(sessionJob),
                validateFlinkSessionJobConfig(sessionJob.getSpec().getFlinkConfiguration()));
    }

    private Optional<String> validateSessionJobWithCluster(
            FlinkSessionJob sessionJob, FlinkDeployment sessionCluster) {
        Map<String, String> effectiveConfig = configManager.getDefaultConfig().toMap();
        if (sessionCluster.getSpec().getFlinkConfiguration() != null) {
            effectiveConfig.putAll(sessionCluster.getSpec().getFlinkConfiguration());
        }

        return firstPresent(
                validateNotApplicationCluster(sessionCluster),
                validateSessionClusterId(sessionJob, sessionCluster),
                validateJobSpec(sessionJob.getSpec().getJob(), null, effectiveConfig));
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

    private Optional<String> validateNotLastStateUpgradeMode(FlinkSessionJob sessionJob) {
        if (sessionJob.getSpec().getJob().getUpgradeMode() == UpgradeMode.LAST_STATE) {
            return Optional.of(
                    String.format(
                            "The %s upgrade mode is not supported in session job now.",
                            UpgradeMode.LAST_STATE));
        }
        return Optional.empty();
    }

    private Optional<String> validateSpecChange(FlinkSessionJob sessionJob) {
        FlinkSessionJobSpec newSpec = sessionJob.getSpec();

        if (sessionJob.getStatus().getReconciliationStatus().isFirstDeployment()) {
            // New job
            if (newSpec.getJob() != null && !newSpec.getJob().getState().equals(JobState.RUNNING)) {
                return Optional.of("Job must start in running state");
            }

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

    private Optional<String> validateFlinkSessionJobConfig(
            Map<String, String> flinkSessionJobConfig) {
        if (flinkSessionJobConfig == null) {
            return Optional.empty();
        }

        for (String key : flinkSessionJobConfig.keySet()) {
            if (!ALLOWED_FLINK_SESSION_JOB_CONF_KEYS.contains(key)) {
                return Optional.of(
                        String.format(
                                "Invalid session job flinkConfiguration key: %s. Allowed keys are %s",
                                key, ALLOWED_FLINK_SESSION_JOB_CONF_KEYS));
            }
        }
        return Optional.empty();
    }
}
