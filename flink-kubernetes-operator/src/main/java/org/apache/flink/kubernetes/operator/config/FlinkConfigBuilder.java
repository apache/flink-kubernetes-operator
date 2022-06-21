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

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.crd.spec.Resource;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.internal.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;

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

    protected FlinkConfigBuilder(FlinkDeployment deployment, Configuration flinkConfig) {
        this(
                deployment.getMetadata().getNamespace(),
                deployment.getMetadata().getName(),
                deployment.getSpec(),
                flinkConfig);
    }

    protected FlinkConfigBuilder(
            String namespace,
            String clusterId,
            FlinkDeploymentSpec spec,
            Configuration flinkConfig) {
        this.namespace = namespace;
        this.clusterId = clusterId;
        this.spec = spec;
        this.effectiveConfig = new Configuration(flinkConfig);
    }

    protected FlinkConfigBuilder applyImage() {
        if (!StringUtils.isNullOrWhitespaceOnly(spec.getImage())) {
            effectiveConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, spec.getImage());
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

        if (spec.getJob() != null) {
            // With last-state upgrade mode, set the default value of
            // 'execution.checkpointing.interval'
            // to 5 minutes when HA is enabled.
            if (spec.getJob().getUpgradeMode() == UpgradeMode.LAST_STATE) {
                setDefaultConf(
                        ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                        DEFAULT_CHECKPOINTING_INTERVAL);
            }

            // We need to keep the application clusters around for proper operator behaviour
            effectiveConfig.set(SHUTDOWN_ON_APPLICATION_FINISH, false);
            if (HighAvailabilityMode.isHighAvailabilityModeActivated(effectiveConfig)) {
                setDefaultConf(SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true);
            }

            setDefaultConf(
                    ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT,
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }

        effectiveConfig.set(FLINK_VERSION, spec.getFlinkVersion());
        return this;
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

    protected FlinkConfigBuilder applyCommonPodTemplate() throws IOException {
        if (spec.getPodTemplate() != null) {
            effectiveConfig.set(
                    KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE,
                    createTempFile(spec.getPodTemplate()));
        }
        return this;
    }

    protected FlinkConfigBuilder applyIngressDomain() {
        // Web UI
        if (spec.getIngress() != null) {
            effectiveConfig.set(
                    REST_SERVICE_EXPOSED_TYPE,
                    KubernetesConfigOptions.ServiceExposedType.ClusterIP);
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

    protected FlinkConfigBuilder applyJobManagerSpec() throws IOException {
        if (spec.getJobManager() != null) {
            setResource(spec.getJobManager().getResource(), effectiveConfig, true);
            setPodTemplate(
                    spec.getPodTemplate(),
                    spec.getJobManager().getPodTemplate(),
                    effectiveConfig,
                    true);
            if (spec.getJobManager().getReplicas() > 0) {
                effectiveConfig.set(
                        KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS,
                        spec.getJobManager().getReplicas());
            }
        }
        return this;
    }

    protected FlinkConfigBuilder applyTaskManagerSpec() throws IOException {
        if (spec.getTaskManager() != null) {
            setResource(spec.getTaskManager().getResource(), effectiveConfig, false);
            setPodTemplate(
                    spec.getPodTemplate(),
                    spec.getTaskManager().getPodTemplate(),
                    effectiveConfig,
                    false);

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
            final URI uri = new URI(jobSpec.getJarURI());
            effectiveConfig.set(PipelineOptions.JARS, Collections.singletonList(uri.toString()));
            effectiveConfig.set(CoreOptions.DEFAULT_PARALLELISM, getParallelism());

            if (jobSpec.getAllowNonRestoredState() != null) {
                effectiveConfig.set(
                        SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE,
                        jobSpec.getAllowNonRestoredState());
            }

            if (jobSpec.getEntryClass() != null) {
                effectiveConfig.set(
                        ApplicationConfiguration.APPLICATION_MAIN_CLASS, jobSpec.getEntryClass());
            }
        } else {
            effectiveConfig.set(
                    DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        }

        if (deploymentMode == KubernetesDeploymentMode.STANDALONE) {
            effectiveConfig.set(DeploymentOptions.TARGET, "remote");
            effectiveConfig.set(
                    StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE,
                    spec.getJob() == null ? SESSION : APPLICATION);

            if (spec.getJob() != null) {
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

        return spec.getJob().getParallelism();
    }

    protected Configuration build() {

        // Set cluster config
        effectiveConfig.setString(KubernetesConfigOptions.NAMESPACE, namespace);
        effectiveConfig.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);
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
                .applyCommonPodTemplate()
                .applyIngressDomain()
                .applyJobManagerSpec()
                .applyTaskManagerSpec()
                .applyJobOrSessionSpec()
                .build();
    }

    private <T> void setDefaultConf(ConfigOption<T> option, T value) {
        if (!effectiveConfig.contains(option)) {
            effectiveConfig.set(option, value);
        }
    }

    private static void setResource(
            Resource resource, Configuration effectiveConfig, boolean isJM) {
        if (resource != null) {
            final ConfigOption<MemorySize> memoryConfigOption =
                    isJM
                            ? JobManagerOptions.TOTAL_PROCESS_MEMORY
                            : TaskManagerOptions.TOTAL_PROCESS_MEMORY;
            final ConfigOption<Double> cpuConfigOption =
                    isJM
                            ? KubernetesConfigOptions.JOB_MANAGER_CPU
                            : KubernetesConfigOptions.TASK_MANAGER_CPU;
            if (resource.getMemory() != null) {
                effectiveConfig.setString(memoryConfigOption.key(), resource.getMemory());
            }
            if (resource.getCpu() != null) {
                effectiveConfig.setDouble(cpuConfigOption.key(), resource.getCpu());
            }
        }
    }

    private static void setPodTemplate(
            Pod basicPod, Pod appendPod, Configuration effectiveConfig, boolean isJM)
            throws IOException {

        if (basicPod == null && appendPod == null) {
            return;
        }

        // Avoid to create temporary pod template files for JobManager and TaskManager if it is not
        // configured explicitly via .spec.JobManagerSpec.podTemplate or
        // .spec.TaskManagerSpec.podTemplate.
        if (appendPod != null) {
            final ConfigOption<String> podConfigOption =
                    isJM
                            ? KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE
                            : KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE;
            effectiveConfig.setString(
                    podConfigOption, createTempFile(mergePodTemplates(basicPod, appendPod)));
        }
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
        tmpDir.deleteOnExit();
        return tmpDir.getAbsolutePath();
    }

    private static String createTempFile(Pod podTemplate) throws IOException {
        final File tmp = File.createTempFile(GENERATED_FILE_PREFIX + "podTemplate_", ".yaml");
        Files.write(tmp.toPath(), SerializationUtils.dumpAsYaml(podTemplate).getBytes());
        tmp.deleteOnExit();
        return tmp.getAbsolutePath();
    }

    protected static void cleanupTmpFiles(Configuration configuration) {
        configuration
                .getOptional(KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE)
                .ifPresent(FlinkConfigBuilder::deleteSilentlyIfGenerated);
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
