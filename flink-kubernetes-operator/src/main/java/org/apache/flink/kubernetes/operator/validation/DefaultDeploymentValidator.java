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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.Resource;
import org.apache.flink.kubernetes.operator.crd.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Default validator implementation. */
public class DefaultDeploymentValidator implements FlinkDeploymentValidator {

    private static final String[] FORBIDDEN_CONF_KEYS =
            new String[] {
                KubernetesConfigOptions.NAMESPACE.key(), KubernetesConfigOptions.CLUSTER_ID.key()
            };

    private static final Set<String> ALLOWED_LOG_CONF_KEYS =
            Set.of(Constants.CONFIG_FILE_LOG4J_NAME, Constants.CONFIG_FILE_LOGBACK_NAME);

    @Override
    public Optional<String> validate(FlinkDeployment deployment) {
        FlinkDeploymentSpec spec = deployment.getSpec();
        return firstPresent(
                validateFlinkVersion(spec.getFlinkVersion()),
                validateFlinkConfig(spec.getFlinkConfiguration()),
                validateIngress(
                        spec.getIngress(),
                        deployment.getMetadata().getName(),
                        deployment.getMetadata().getNamespace()),
                validateLogConfig(spec.getLogConfiguration()),
                validateJobSpec(spec.getJob(), spec.getFlinkConfiguration()),
                validateJmSpec(spec.getJobManager(), spec.getFlinkConfiguration()),
                validateTmSpec(spec.getTaskManager()),
                validateSpecChange(deployment));
    }

    private static Optional<String> firstPresent(Optional<String>... errOpts) {
        for (Optional<String> opt : errOpts) {
            if (opt.isPresent()) {
                return opt;
            }
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

    private Optional<String> validateFlinkConfig(Map<String, String> confMap) {
        if (confMap == null) {
            return Optional.empty();
        }
        Configuration conf = Configuration.fromMap(confMap);
        for (String key : FORBIDDEN_CONF_KEYS) {
            if (conf.containsKey(key)) {
                return Optional.of("Forbidden Flink config key: " + key);
            }
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

    private Optional<String> validateJobSpec(JobSpec job, Map<String, String> confMap) {
        if (job == null) {
            return Optional.empty();
        }

        if (job.getParallelism() < 1) {
            return Optional.of("Job parallelism must be larger than 0");
        }

        if (job.getJarURI() == null) {
            return Optional.of("Jar URI must be defined");
        }

        Configuration configuration = Configuration.fromMap(confMap);
        if (job.getUpgradeMode() == UpgradeMode.LAST_STATE
                && !FlinkUtils.isKubernetesHAActivated(configuration)) {
            return Optional.of(
                    "Job could not be upgraded with last-state while Kubernetes HA disabled");
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
            }
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

    private Optional<String> validateSpecChange(FlinkDeployment deployment) {
        FlinkDeploymentSpec newSpec = deployment.getSpec();

        if (deployment.getStatus() == null
                || deployment.getStatus().getReconciliationStatus() == null
                || deployment.getStatus().getReconciliationStatus().getLastReconciledSpec()
                        == null) {
            // New deployment
            if (newSpec.getJob() != null && !newSpec.getJob().getState().equals(JobState.RUNNING)) {
                return Optional.of("Job must start in running state");
            }

            return Optional.empty();
        }

        FlinkDeploymentSpec oldSpec =
                deployment.getStatus().getReconciliationStatus().getLastReconciledSpec();

        if (newSpec.getJob() != null && oldSpec.getJob() == null) {
            return Optional.of("Cannot switch from session to job cluster");
        }

        if (newSpec.getJob() == null && oldSpec.getJob() != null) {
            return Optional.of("Cannot switch from job to session cluster");
        }

        JobSpec oldJob = oldSpec.getJob();
        JobSpec newJob = newSpec.getJob();
        if (oldJob != null && newJob != null) {
            if (oldJob.getState() == JobState.SUSPENDED
                    && newJob.getState() == JobState.RUNNING
                    && newJob.getUpgradeMode() == UpgradeMode.SAVEPOINT
                    && (deployment.getStatus().getJobStatus().getSavepointInfo().getLastSavepoint()
                            == null)) {
                return Optional.of("Cannot perform savepoint restore without a valid savepoint");
            }

            if (StringUtils.isNullOrWhitespaceOnly(
                            newSpec.getFlinkConfiguration()
                                    .get(CheckpointingOptions.SAVEPOINT_DIRECTORY.key()))
                    && deployment.getStatus().getJobManagerDeploymentStatus()
                            != JobManagerDeploymentStatus.MISSING
                    && ReconciliationUtils.isUpgradeModeChangedToLastStateAndHADisabledPreviously(
                            deployment)) {
                return Optional.of(
                        String.format(
                                "Job could not be upgraded to last-state while config key[%s] is not set",
                                CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));
            }
        }

        return Optional.empty();
    }
}
