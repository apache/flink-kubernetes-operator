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

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.configuration.DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH;
import static org.apache.flink.configuration.DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR;
import static org.apache.flink.configuration.DeploymentOptionsInternal.CONF_DIR;
import static org.apache.flink.configuration.WebOptions.CANCEL_ENABLE;
import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE;
import static org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION;
import static org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal.ClusterMode.SESSION;
import static org.apache.flink.kubernetes.operator.utils.FlinkUtils.mergePodTemplates;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;

/** Builder to get effective flink config from {@link FlinkDeployment}. */
public class FlinkConfigBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkConfigBuilder.class);

    public static final ConfigOption<FlinkVersion> FLINK_VERSION =
            ConfigOptions.key("$internal.flink.version")
                    .enumType(FlinkVersion.class)
                    .noDefaultValue();

    protected static final String GENERATED_FILE_PREFIX = "flink_op_generated_";
    protected static final Duration DEFAULT_CHECKPOINTING_INTERVAL = Duration.ofMinutes(5);

    private final String namespace;
    private final String clusterId;
    private final FlinkDeploymentSpec spec;
    private final Configuration effectiveConfig;

    protected FlinkConfigBuilder(FlinkDeployment deployment, Configuration flinkConf) {
        this(
                deployment.getMetadata().getNamespace(),
                deployment.getMetadata().getName(),
                deployment.getSpec(),
                flinkConf);
    }

    protected FlinkConfigBuilder(
            String namespace, String clusterId, FlinkDeploymentSpec spec, Configuration flinkConf) {
        this.namespace = namespace;
        this.clusterId = clusterId;
        this.spec = spec;
        this.effectiveConfig = flinkConf;
    }

    protected FlinkConfigBuilder applyImage() {
        if (!StringUtils.isNullOrWhitespaceOnly(spec.getImage())) {
            String configKey;
            if (spec.getFlinkVersion().isEqualOrNewer(FlinkVersion.v1_17)) {
                configKey = KubernetesConfigOptions.CONTAINER_IMAGE.key();
            } else {
                configKey = "kubernetes.container.image";
            }
            effectiveConfig.setString(configKey, spec.getImage());
        }
        return this;
    }

    protected FlinkConfigBuilder applyImagePullPolicy() {
        if (!StringUtils.isNullOrWhitespaceOnly(spec.getImagePullPolicy())) {
            effectiveConfig.set(
                    KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                    KubernetesConfigOptions.ImagePullPolicy.valueOf(spec.getImagePullPolicy()));
        }
        return this;
    }

    protected FlinkConfigBuilder applyFlinkConfiguration() {
        // Parse config from spec's flinkConfiguration
        if (spec.getFlinkConfiguration() != null && !spec.getFlinkConfiguration().isEmpty()) {
            spec.getFlinkConfiguration().forEach(effectiveConfig::setString);
        }

        // Adapt default rest service type from 1.15+
        setDefaultConf(
                REST_SERVICE_EXPOSED_TYPE, KubernetesConfigOptions.ServiceExposedType.ClusterIP);
        // Set 'web.cancel.enable' to false to avoid users accidentally cancelling jobs.
        setDefaultConf(CANCEL_ENABLE, false);
        effectiveConfig.set(FLINK_VERSION, spec.getFlinkVersion());
        return this;
    }

    protected static void applyJobConfig(String name, Configuration conf, JobSpec jobSpec) {
        // Set 'pipeline.name' to resource name by default for application deployments.
        setDefaultConf(conf, PipelineOptions.NAME, name);

        // With last-state upgrade mode, set the default value of
        // 'execution.checkpointing.interval'
        // to 5 minutes when HA is enabled.
        if (jobSpec.getUpgradeMode() == UpgradeMode.LAST_STATE) {
            setDefaultConf(
                    conf,
                    ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                    DEFAULT_CHECKPOINTING_INTERVAL);
        }
        setDefaultConf(
                conf,
                ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT,
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        if (jobSpec.getAllowNonRestoredState() != null) {
            conf.set(
                    SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE,
                    jobSpec.getAllowNonRestoredState());
        }

        if (jobSpec.getEntryClass() != null) {
            conf.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, jobSpec.getEntryClass());
        }

        if (jobSpec.getArgs() != null) {
            conf.set(ApplicationConfiguration.APPLICATION_ARGS, Arrays.asList(jobSpec.getArgs()));
        }
    }

    protected FlinkConfigBuilder applyLogConfiguration() throws IOException {
        if (spec.getLogConfiguration() != null) {
            String confDir =
                    createLogConfigFiles(
                            spec.getLogConfiguration().get(CONFIG_FILE_LOG4J_NAME),
                            spec.getLogConfiguration().get(CONFIG_FILE_LOGBACK_NAME));
            effectiveConfig.setString(CONF_DIR, confDir);
        }
        return this;
    }

    @VisibleForTesting
    protected static PodTemplateSpec applyResourceRequirementsToPodTemplate(
            PodTemplateSpec podTemplate, ResourceRequirements requirements) {
        if (requirements == null
                || (requirements.getRequests().isEmpty() && requirements.getLimits().isEmpty())) {
            return podTemplate;
        }

        PodTemplateSpec outPodTemplate =
                (podTemplate == null)
                        ? new PodTemplateSpec()
                        : ReconciliationUtils.clone(podTemplate);

        if (outPodTemplate.getSpec() == null) {
            outPodTemplate.setSpec(new PodSpec());
        }
        PodSpec podSpec = outPodTemplate.getSpec();

        Optional<Container> mainContainerOpt =
                podSpec.getContainers().stream()
                        .filter(c -> c.getName().equals(Constants.MAIN_CONTAINER_NAME))
                        .findFirst();

        Container mainContainer;
        if (mainContainerOpt.isPresent()) {
            mainContainer = mainContainerOpt.get();
        } else {
            mainContainer = new Container();
            mainContainer.setName(Constants.MAIN_CONTAINER_NAME);
            podSpec.getContainers().add(mainContainer);
        }

        if (mainContainer.getResources() == null) {
            mainContainer.setResources(new io.fabric8.kubernetes.api.model.ResourceRequirements());
        }

        // Merge requests, overwriting any existing values
        if (!requirements.getRequests().isEmpty()) {
            if (mainContainer.getResources().getRequests() == null) {
                mainContainer.getResources().setRequests(new HashMap<>());
            }
            mainContainer.getResources().getRequests().putAll(requirements.getRequests());
        }

        // Merge limits, overwriting any existing values
        if (!requirements.getLimits().isEmpty()) {
            if (mainContainer.getResources().getLimits() == null) {
                mainContainer.getResources().setLimits(new HashMap<>());
            }
            mainContainer.getResources().getLimits().putAll(requirements.getLimits());
        }

        return outPodTemplate;
    }

    protected FlinkConfigBuilder applyPodTemplate() throws IOException {
        PodTemplateSpec commonPodTemplate = spec.getPodTemplate();
        boolean mergeByName =
                effectiveConfig.get(KubernetesOperatorConfigOptions.POD_TEMPLATE_MERGE_BY_NAME);

        PodTemplateSpec jmPodTemplate;
        if (spec.getJobManager() != null) {
            jmPodTemplate =
                    mergePodTemplates(
                            commonPodTemplate, spec.getJobManager().getPodTemplate(), mergeByName);

            // Prioritize new ResourceRequirements, then fall back to old Resource
            if (spec.getJobManager().getResources() != null) {
                jmPodTemplate =
                        applyResourceRequirementsToPodTemplate(
                                jmPodTemplate, spec.getJobManager().getResources());
            } else {
                jmPodTemplate =
                        applyResourceToPodTemplate(
                                jmPodTemplate, spec.getJobManager().getResource());
            }
        } else {
            jmPodTemplate = ReconciliationUtils.clone(commonPodTemplate);
        }

        if (effectiveConfig.get(
                KubernetesOperatorConfigOptions.OPERATOR_JM_STARTUP_PROBE_ENABLED)) {
            if (jmPodTemplate == null) {
                jmPodTemplate = new PodTemplateSpec();
            }
            FlinkUtils.addStartupProbe(jmPodTemplate);
        }

        String jmTemplateFile = null;
        if (jmPodTemplate != null) {
            jmTemplateFile = createTempFile(jmPodTemplate);
            effectiveConfig.set(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE, jmTemplateFile);
        }

        PodTemplateSpec tmPodTemplate;
        if (spec.getTaskManager() != null) {
            tmPodTemplate =
                    mergePodTemplates(
                            commonPodTemplate, spec.getTaskManager().getPodTemplate(), mergeByName);

            // Prioritize new ResourceRequirements, then fall back to old Resource
            if (spec.getTaskManager().getResources() != null) {
                tmPodTemplate =
                        applyResourceRequirementsToPodTemplate(
                                tmPodTemplate, spec.getTaskManager().getResources());
            } else {
                tmPodTemplate =
                        applyResourceToPodTemplate(
                                tmPodTemplate, spec.getTaskManager().getResource());
            }
        } else {
            tmPodTemplate = ReconciliationUtils.clone(commonPodTemplate);
        }

        if (tmPodTemplate != null) {
            effectiveConfig.set(
                    KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE,
                    tmPodTemplate.equals(jmPodTemplate)
                            ? jmTemplateFile
                            : createTempFile(tmPodTemplate));
        }

        return this;
    }

    protected FlinkConfigBuilder applyServiceAccount() {
        if (spec.getServiceAccount() != null) {
            effectiveConfig.set(
                    KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, spec.getServiceAccount());
        }
        return this;
    }

    protected FlinkConfigBuilder applyJobManagerSpec() {
        if (spec.getJobManager() != null) {
            // Handle new ResourceRequirements first, fall back to deprecated Resource
            if (spec.getJobManager().getResources() != null) {
                setResourceRequirements(spec.getJobManager().getResources(), effectiveConfig, true);
            } else if (spec.getJobManager().getResource() != null) {
                setResource(spec.getJobManager().getResource(), effectiveConfig, true);
            }
            if (spec.getJobManager().getReplicas() > 0) {
                effectiveConfig.set(
                        KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS,
                        spec.getJobManager().getReplicas());
            }
        }
        return this;
    }

    /**
     * Sets Flink configuration from Kubernetes ResourceRequirements, handling both requests (for
     * Flink config values) and limits (for computing limit factors).
     *
     * @param resourceRequirements the Kubernetes resource requirements
     * @param effectiveConfig the Flink configuration to populate
     * @param isJM true for JobManager, false for TaskManager
     */
    private void setResourceRequirements(
            ResourceRequirements resourceRequirements,
            Configuration effectiveConfig,
            boolean isJM) {
        if (resourceRequirements == null) {
            return;
        }

        Map<String, Quantity> requests = resourceRequirements.getRequests();
        Map<String, Quantity> limits = resourceRequirements.getLimits();

        if ((requests == null || requests.isEmpty()) && (limits == null || limits.isEmpty())) {
            return;
        }

        var memoryConfigOption =
                isJM
                        ? JobManagerOptions.TOTAL_PROCESS_MEMORY
                        : TaskManagerOptions.TOTAL_PROCESS_MEMORY;

        // Handle memory from requests
        if (requests != null && requests.containsKey("memory")) {
            String memoryValue = requests.get("memory").toString();
            effectiveConfig.setString(
                    memoryConfigOption.key(), parseResourceMemoryString(memoryValue));
        }

        // Handle CPU from requests
        if (requests != null && requests.containsKey("cpu")) {
            String cpuValue = requests.get("cpu").toString();
            configureCpuFromString(cpuValue, effectiveConfig, isJM);
        }

        // Handle CPU limit factor: limits.cpu / requests.cpu
        if (limits != null
                && limits.containsKey("cpu")
                && requests != null
                && requests.containsKey("cpu")) {
            double requestCpu = requests.get("cpu").getNumericalAmount().doubleValue();
            double limitCpu = limits.get("cpu").getNumericalAmount().doubleValue();
            if (requestCpu > 0) {
                ConfigOption<Double> cpuLimitFactorOption =
                        isJM
                                ? KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT_FACTOR
                                : KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR;
                effectiveConfig.set(cpuLimitFactorOption, limitCpu / requestCpu);
            }
        }

        // Handle memory limit factor: limits.memory / requests.memory
        if (limits != null
                && limits.containsKey("memory")
                && requests != null
                && requests.containsKey("memory")) {
            double requestMemory = requests.get("memory").getNumericalAmount().doubleValue();
            double limitMemory = limits.get("memory").getNumericalAmount().doubleValue();
            if (requestMemory > 0) {
                ConfigOption<Double> memoryLimitFactorOption =
                        isJM
                                ? KubernetesConfigOptions.JOB_MANAGER_MEMORY_LIMIT_FACTOR
                                : KubernetesConfigOptions.TASK_MANAGER_MEMORY_LIMIT_FACTOR;
                effectiveConfig.set(memoryLimitFactorOption, limitMemory / requestMemory);
            }
        }
    }

    protected FlinkConfigBuilder applyTaskManagerSpec() {
        if (spec.getTaskManager() != null) {
            // Handle new ResourceRequirements first, fall back to deprecated Resource
            if (spec.getTaskManager().getResources() != null) {
                setResourceRequirements(
                        spec.getTaskManager().getResources(), effectiveConfig, false);
            } else if (spec.getTaskManager().getResource() != null) {
                setResource(spec.getTaskManager().getResource(), effectiveConfig, false);
            }
            if (spec.getTaskManager().getReplicas() != null
                    && spec.getTaskManager().getReplicas() > 0) {
                effectiveConfig.set(
                        StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS,
                        spec.getTaskManager().getReplicas());
            }
        }

        if (spec.getJob() != null
                && KubernetesDeploymentMode.getDeploymentMode(spec)
                        == KubernetesDeploymentMode.STANDALONE) {
            if (!effectiveConfig.contains(
                    StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS)) {
                effectiveConfig.set(
                        StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS,
                        FlinkUtils.getNumTaskManagers(effectiveConfig, getParallelism()));
            }
        }
        return this;
    }

    protected FlinkConfigBuilder applyJobOrSessionSpec() throws URISyntaxException {
        KubernetesDeploymentMode deploymentMode = KubernetesDeploymentMode.getDeploymentMode(spec);

        if (spec.getJob() != null) {
            JobSpec jobSpec = spec.getJob();
            effectiveConfig.set(
                    DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());

            if (deploymentMode == KubernetesDeploymentMode.NATIVE && jobSpec.getJarURI() != null) {
                effectiveConfig.set(
                        PipelineOptions.JARS,
                        Collections.singletonList(new URI(jobSpec.getJarURI()).toString()));
            }
            effectiveConfig.set(CoreOptions.DEFAULT_PARALLELISM, getParallelism());

            // We need to keep the application clusters around for proper operator behaviour
            effectiveConfig.set(SHUTDOWN_ON_APPLICATION_FINISH, false);
            setDefaultConf(SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true);

            // Generic shared job config logic
            applyJobConfig(clusterId, effectiveConfig, jobSpec);
        } else {
            effectiveConfig.set(
                    DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        }

        if (deploymentMode == KubernetesDeploymentMode.STANDALONE) {
            effectiveConfig.set(DeploymentOptions.TARGET, "remote");
            effectiveConfig.set(
                    StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE,
                    spec.getJob() == null ? SESSION : APPLICATION);

            if (spec.getJob() != null && spec.getJob().getJarURI() != null) {
                effectiveConfig.set(
                        PipelineOptions.CLASSPATHS,
                        Collections.singletonList(getStandaloneJarURI(spec.getJob())));
            }
        }
        return this;
    }

    private String getStandaloneJarURI(JobSpec jobSpec) throws URISyntaxException {
        URI uri = new URI(jobSpec.getJarURI());

        // Running an application job through standalone mode doesn't requires file uri scheme and
        // doesn't accept
        // local scheme which is used for native so convert here to improve compatibilty at the
        // operator layer
        if (uri.getScheme().equals("local")) {
            uri =
                    new URI(
                            "file",
                            uri.getAuthority() == null ? "" : uri.getAuthority(),
                            uri.getPath(),
                            uri.getQuery(),
                            uri.getFragment());
        }

        return uri.toASCIIString();
    }

    private int getParallelism() {
        if (spec.getTaskManager() != null && spec.getTaskManager().getReplicas() != null) {
            if (spec.getJob().getParallelism() > 0) {
                LOG.warn("Job parallelism setting is ignored as TaskManager replicas are set");
            }
            return spec.getTaskManager().getReplicas()
                    * effectiveConfig.get(TaskManagerOptions.NUM_TASK_SLOTS);
        }

        Optional<Integer> maxOverrideParallelism = getMaxParallelismFromOverrideConfig();
        if (maxOverrideParallelism.isPresent() && maxOverrideParallelism.get() > 0) {
            return maxOverrideParallelism.get();
        }

        return spec.getJob().getParallelism();
    }

    private Optional<Integer> getMaxParallelismFromOverrideConfig() {
        return effectiveConfig
                .getOptional(PipelineOptions.PARALLELISM_OVERRIDES)
                .flatMap(
                        overrides ->
                                overrides.values().stream()
                                        .map(Integer::valueOf)
                                        .max(Integer::compareTo));
    }

    protected Configuration build() {

        // Set cluster config
        effectiveConfig.setString(KubernetesConfigOptions.NAMESPACE, namespace);
        effectiveConfig.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);
        if (HighAvailabilityMode.isHighAvailabilityModeActivated(effectiveConfig)) {
            effectiveConfig.setString(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
        }
        return effectiveConfig;
    }

    public static Configuration buildFrom(
            String namespace, String clusterId, FlinkDeploymentSpec spec, Configuration flinkConfig)
            throws IOException, URISyntaxException {
        return new FlinkConfigBuilder(namespace, clusterId, spec, flinkConfig)
                .applyFlinkConfiguration()
                .applyLogConfiguration()
                .applyImage()
                .applyImagePullPolicy()
                .applyServiceAccount()
                .applyPodTemplate()
                .applyJobManagerSpec()
                .applyTaskManagerSpec()
                .applyJobOrSessionSpec()
                .build();
    }

    private <T> void setDefaultConf(ConfigOption<T> option, T value) {
        setDefaultConf(effectiveConfig, option, value);
    }

    private static <T> void setDefaultConf(Configuration conf, ConfigOption<T> option, T value) {
        if (!conf.contains(option)) {
            conf.set(option, value);
        }
    }

    private void setResource(Resource resource, Configuration effectiveConfig, boolean isJM) {
        if (resource != null) {
            var memoryConfigOption =
                    isJM
                            ? JobManagerOptions.TOTAL_PROCESS_MEMORY
                            : TaskManagerOptions.TOTAL_PROCESS_MEMORY;
            if (resource.getMemory() != null) {
                effectiveConfig.setString(
                        memoryConfigOption.key(), parseResourceMemoryString(resource.getMemory()));
            }

            configureCpu(resource, effectiveConfig, isJM);
        }
    }

    public static String parseResourceMemoryString(String memory) {
        try {
            return MemorySize.parse(memory).toString();
        } catch (IllegalArgumentException e) {
            var memoryQuantity = formatMemoryStringForK8sSpec(memory);
            return Quantity.parse(memoryQuantity).getNumericalAmount() + "";
        }
    }

    private static String formatMemoryStringForK8sSpec(String memory) {
        var memoryQuantity = memory.trim().replaceAll("\\s", "").toUpperCase();
        if (memoryQuantity.endsWith("B")) {
            memoryQuantity = memoryQuantity.substring(0, memoryQuantity.length() - 1);
        }
        if (memoryQuantity.endsWith("I")) {
            memoryQuantity = memoryQuantity.substring(0, memoryQuantity.length() - 1) + "i";
        }
        return memoryQuantity;
    }

    private void configureCpu(Resource resource, Configuration conf, boolean isJM) {
        if (resource.getCpu() == null) {
            return;
        }

        boolean newConfKeys = spec.getFlinkVersion().isEqualOrNewer(FlinkVersion.v1_17);
        String configKey = null;
        if (isJM) {
            // Set new config all the time to simplify reading side
            conf.setDouble(KubernetesConfigOptions.JOB_MANAGER_CPU.key(), resource.getCpu());
            if (!newConfKeys) {
                configKey = "kubernetes.jobmanager.cpu";
            }
        } else {
            // Set new config all the time to simplify reading side
            conf.setDouble(KubernetesConfigOptions.TASK_MANAGER_CPU.key(), resource.getCpu());
            if (!newConfKeys) {
                configKey = "kubernetes.taskmanager.cpu";
            }
        }
        if (configKey != null) {
            conf.setDouble(configKey, resource.getCpu());
        }
    }

    private void configureCpuFromString(String cpuValue, Configuration conf, boolean isJM) {
        if (StringUtils.isNullOrWhitespaceOnly(cpuValue)) {
            return;
        }

        try {
            double cpuDouble = parseCpuValue(cpuValue);
            configureCpu(cpuDouble, conf, isJM);
        } catch (IllegalArgumentException e) {
            LOG.warn("Could not parse CPU value '{}'.", cpuValue, e);
        }
    }

    private void configureCpu(Double cpu, Configuration conf, boolean isJM) {
        if (cpu == null) {
            return;
        }

        ConfigOption<Double> cpuConfigOption =
                isJM
                        ? KubernetesConfigOptions.JOB_MANAGER_CPU
                        : KubernetesConfigOptions.TASK_MANAGER_CPU;
        conf.set(cpuConfigOption, cpu);

        if (!spec.getFlinkVersion().isEqualOrNewer(FlinkVersion.v1_17)) {
            String legacyKey = isJM ? "kubernetes.jobmanager.cpu" : "kubernetes.taskmanager.cpu";
            conf.setDouble(legacyKey, cpu);
        }
    }

    private static double parseCpuValue(String cpuValue) {
        if (StringUtils.isNullOrWhitespaceOnly(cpuValue)) {
            throw new IllegalArgumentException("CPU value cannot be empty.");
        }

        String trimmed = cpuValue.trim();
        if (trimmed.endsWith("m")) {
            String numericPart = trimmed.substring(0, trimmed.length() - 1);
            return Double.parseDouble(numericPart) / 1000.0;
        }
        return Double.parseDouble(trimmed);
    }

    @VisibleForTesting
    protected static PodTemplateSpec applyResourceToPodTemplate(
            PodTemplateSpec podTemplate, Resource resource) {
        if (resource == null
                || StringUtils.isNullOrWhitespaceOnly(resource.getEphemeralStorage())) {
            return podTemplate;
        }

        if (podTemplate == null) {
            var newPodTemplate = new PodTemplateSpec();
            newPodTemplate.setSpec(createPodSpecWithResource(resource));
            return newPodTemplate;
        } else if (podTemplate.getSpec() == null) {
            podTemplate.setSpec(createPodSpecWithResource(resource));
            return podTemplate;
        } else {
            boolean hasMainContainer = false;
            for (Container container : podTemplate.getSpec().getContainers()) {
                if (container.getName().equals(Constants.MAIN_CONTAINER_NAME)) {
                    decorateContainerWithEphemeralStorage(
                            container, resource.getEphemeralStorage());
                    hasMainContainer = true;
                }
            }

            if (!hasMainContainer) {
                podTemplate
                        .getSpec()
                        .getContainers()
                        .add(
                                decorateContainerWithEphemeralStorage(
                                        new Container(), resource.getEphemeralStorage()));
            }
        }

        return podTemplate;
    }

    private static PodSpec createPodSpecWithResource(Resource resource) {
        PodSpec spec = new PodSpec();
        spec.getContainers()
                .add(
                        decorateContainerWithEphemeralStorage(
                                new Container(), resource.getEphemeralStorage()));

        return spec;
    }

    private static Container decorateContainerWithEphemeralStorage(
            Container container, String ephemeralStorage) {
        container.setName(Constants.MAIN_CONTAINER_NAME);
        ResourceRequirements resourceRequirements =
                container.getResources() == null
                        ? new ResourceRequirements()
                        : container.getResources();
        resourceRequirements
                .getLimits()
                .put(CrdConstants.EPHEMERAL_STORAGE, Quantity.parse(ephemeralStorage));
        resourceRequirements
                .getRequests()
                .put(CrdConstants.EPHEMERAL_STORAGE, Quantity.parse(ephemeralStorage));
        container.setResources(resourceRequirements);
        return container;
    }

    private static String createLogConfigFiles(String log4jConf, String logbackConf)
            throws IOException {
        File tmpDir = Files.createTempDirectory(GENERATED_FILE_PREFIX + "conf_").toFile();

        if (log4jConf != null) {
            File log4jConfFile = new File(tmpDir.getAbsolutePath(), CONFIG_FILE_LOG4J_NAME);
            Files.write(log4jConfFile.toPath(), log4jConf.getBytes());
        }

        if (logbackConf != null) {
            File logbackConfFile = new File(tmpDir.getAbsolutePath(), CONFIG_FILE_LOGBACK_NAME);
            Files.write(logbackConfFile.toPath(), logbackConf.getBytes());
        }
        return tmpDir.getAbsolutePath();
    }

    private static String createTempFile(PodTemplateSpec podTemplate) throws IOException {
        final File tmp = File.createTempFile(GENERATED_FILE_PREFIX + "podTemplate_", ".yaml");
        Files.write(tmp.toPath(), Serialization.asYaml(podTemplate).getBytes());
        return tmp.getAbsolutePath();
    }

    protected static void cleanupTmpFiles(Configuration configuration) {
        configuration
                .getOptional(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE)
                .ifPresent(FlinkConfigBuilder::deleteSilentlyIfGenerated);
        configuration
                .getOptional(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE)
                .ifPresent(FlinkConfigBuilder::deleteSilentlyIfGenerated);
        configuration
                .getOptional(DeploymentOptionsInternal.CONF_DIR)
                .ifPresent(FlinkConfigBuilder::deleteSilentlyIfGenerated);
    }

    private static void deleteSilentlyIfGenerated(String file) {
        try {
            File localFile = new File(file);
            if (!localFile.getName().startsWith(FlinkConfigBuilder.GENERATED_FILE_PREFIX)) {
                return;
            }
            LOG.debug("Deleting tmp config file {}", localFile);
            FileUtils.deleteFileOrDirectory(localFile);
        } catch (Exception err) {
            LOG.error("Could not clean up file " + file, err);
        }
    }
}
