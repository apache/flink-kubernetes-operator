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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.Resource;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.internal.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Collections;

import static org.apache.flink.kubernetes.operator.utils.FlinkUtils.mergePodTemplates;

/** Builder to get effective flink config from {@link FlinkDeployment}. */
public class FlinkConfigBuilder {
    private final FlinkDeployment deploy;
    private final FlinkDeploymentSpec spec;
    private final Configuration effectiveConfig;

    public FlinkConfigBuilder(FlinkDeployment deploy) {
        this.deploy = deploy;
        this.spec = this.deploy.getSpec();
        this.effectiveConfig =
                FlinkUtils.loadConfiguration(
                        System.getenv().get(ConfigConstants.ENV_FLINK_CONF_DIR));
    }

    public FlinkConfigBuilder applyImage() {
        if (!StringUtils.isNullOrWhitespaceOnly(spec.getImage())) {
            effectiveConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, spec.getImage());
        }
        return this;
    }

    public FlinkConfigBuilder applyImagePullPolicy() {
        if (!StringUtils.isNullOrWhitespaceOnly(spec.getImagePullPolicy())) {
            effectiveConfig.set(
                    KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                    KubernetesConfigOptions.ImagePullPolicy.valueOf(spec.getImagePullPolicy()));
        }
        return this;
    }

    public FlinkConfigBuilder applyFlinkConfiguration() {
        // Parse config from spec's flinkConfiguration
        if (spec.getFlinkConfiguration() != null && !spec.getFlinkConfiguration().isEmpty()) {
            spec.getFlinkConfiguration().forEach(effectiveConfig::setString);
        }
        return this;
    }

    public FlinkConfigBuilder applyCommonPodTemplate() throws IOException {
        if (spec.getPodTemplate() != null) {
            effectiveConfig.set(
                    KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE,
                    createTempFile(spec.getPodTemplate()));
        }
        return this;
    }

    public FlinkConfigBuilder applyIngressDomain() {
        // Web UI
        if (spec.getIngressDomain() != null) {
            effectiveConfig.set(
                    KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                    KubernetesConfigOptions.ServiceExposedType.ClusterIP);
        }
        return this;
    }

    public FlinkConfigBuilder applyJobManagerSpec() throws IOException {
        if (spec.getJobManager() != null) {
            if (spec.getJobManager() != null) {
                setResource(spec.getJobManager().getResource(), effectiveConfig, true);
                setPodTemplate(
                        spec.getPodTemplate(),
                        spec.getJobManager().getPodTemplate(),
                        effectiveConfig,
                        true);
            }
        }
        return this;
    }

    public FlinkConfigBuilder applyTaskManagerSpec() throws IOException {
        if (spec.getTaskManager() != null) {
            setResource(spec.getTaskManager().getResource(), effectiveConfig, false);
            setPodTemplate(
                    spec.getPodTemplate(),
                    spec.getTaskManager().getPodTemplate(),
                    effectiveConfig,
                    false);
            if (spec.getTaskManager().getTaskSlots() > 0) {
                effectiveConfig.set(
                        TaskManagerOptions.NUM_TASK_SLOTS, spec.getTaskManager().getTaskSlots());
            }
        }
        return this;
    }

    public FlinkConfigBuilder applyJobOrSessionSpec() throws URISyntaxException {
        if (spec.getJob() != null) {
            effectiveConfig.set(
                    DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
            final URI uri = new URI(spec.getJob().getJarURI());
            effectiveConfig.set(PipelineOptions.JARS, Collections.singletonList(uri.toString()));

            if (spec.getJob().getParallelism() > 0) {
                effectiveConfig.set(
                        CoreOptions.DEFAULT_PARALLELISM, spec.getJob().getParallelism());
            }
        } else {
            effectiveConfig.set(
                    DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        }
        return this;
    }

    public Configuration build() {

        // Set cluster config
        final String namespace = deploy.getMetadata().getNamespace();
        final String clusterId = deploy.getMetadata().getName();
        effectiveConfig.setString(KubernetesConfigOptions.NAMESPACE, namespace);
        effectiveConfig.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);
        return effectiveConfig;
    }

    public static Configuration buildFrom(FlinkDeployment dep)
            throws IOException, URISyntaxException {
        return new FlinkConfigBuilder(dep)
                .applyFlinkConfiguration()
                .applyImage()
                .applyImagePullPolicy()
                .applyCommonPodTemplate()
                .applyIngressDomain()
                .applyJobManagerSpec()
                .applyTaskManagerSpec()
                .applyJobOrSessionSpec()
                .build();
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
            effectiveConfig.setString(memoryConfigOption.key(), resource.getMemory());
            effectiveConfig.setDouble(cpuConfigOption.key(), resource.getCpu());
        }
    }

    private static void setPodTemplate(
            Pod basicPod, Pod appendPod, Configuration effectiveConfig, boolean isJM)
            throws IOException {
        if (basicPod != null) {
            final ConfigOption<String> podConfigOption =
                    isJM
                            ? KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE
                            : KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE;
            effectiveConfig.setString(
                    podConfigOption, createTempFile(mergePodTemplates(basicPod, appendPod)));
        }
    }

    private static String createTempFile(Pod podTemplate) throws IOException {
        final File tmp = File.createTempFile("podTemplate_", ".yaml");
        Files.write(tmp.toPath(), SerializationUtils.dumpAsYaml(podTemplate).getBytes());
        tmp.deleteOnExit();
        return tmp.getAbsolutePath();
    }
}
