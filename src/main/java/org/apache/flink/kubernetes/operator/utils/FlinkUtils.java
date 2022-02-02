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

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.internal.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.Executors;

/** Flink Utility methods used by the operator. */
public class FlinkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUtils.class);

    public static Configuration getEffectiveConfig(FlinkDeployment flinkApp) {
        String namespace = flinkApp.getMetadata().getNamespace();
        String clusterId = flinkApp.getMetadata().getName();
        FlinkDeploymentSpec spec = flinkApp.getSpec();

        try {
            String flinkConfDir = System.getenv().get(ConfigConstants.ENV_FLINK_CONF_DIR);
            Configuration effectiveConfig =
                    flinkConfDir != null
                            ? GlobalConfiguration.loadConfiguration(flinkConfDir)
                            : new Configuration();

            effectiveConfig.setString(KubernetesConfigOptions.NAMESPACE, namespace);
            effectiveConfig.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);

            if (spec.getIngressDomain() != null) {
                effectiveConfig.set(
                        KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                        KubernetesConfigOptions.ServiceExposedType.ClusterIP);
            }

            if (spec.getJob() != null) {
                effectiveConfig.set(
                        DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
            } else {
                effectiveConfig.set(
                        DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
            }

            if (!StringUtils.isNullOrWhitespaceOnly(spec.getImage())) {
                effectiveConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE, spec.getImage());
            }

            if (!StringUtils.isNullOrWhitespaceOnly(spec.getImagePullPolicy())) {
                effectiveConfig.set(
                        KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                        KubernetesConfigOptions.ImagePullPolicy.valueOf(spec.getImagePullPolicy()));
            }

            if (spec.getFlinkConfiguration() != null && !spec.getFlinkConfiguration().isEmpty()) {
                spec.getFlinkConfiguration().forEach(effectiveConfig::setString);
            }

            // Pod template
            if (spec.getPodTemplate() != null) {
                effectiveConfig.set(
                        KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE,
                        createTempFile(spec.getPodTemplate()));
            }

            if (spec.getJobManager() != null) {
                if (spec.getJobManager().getResource() != null) {
                    effectiveConfig.setString(
                            JobManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                            spec.getJobManager().getResource().getMemory());
                    effectiveConfig.set(
                            KubernetesConfigOptions.JOB_MANAGER_CPU,
                            spec.getJobManager().getResource().getCpu());
                }

                if (spec.getJobManager().getPodTemplate() != null) {
                    effectiveConfig.set(
                            KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE,
                            createTempFile(spec.getJobManager().getPodTemplate()));
                }
            }

            if (spec.getTaskManager() != null) {
                if (spec.getTaskManager().getTaskSlots() > 0) {
                    effectiveConfig.set(
                            TaskManagerOptions.NUM_TASK_SLOTS,
                            spec.getTaskManager().getTaskSlots());
                }

                if (spec.getTaskManager().getResource() != null) {
                    effectiveConfig.setString(
                            TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                            spec.getTaskManager().getResource().getMemory());
                    effectiveConfig.set(
                            KubernetesConfigOptions.TASK_MANAGER_CPU,
                            spec.getTaskManager().getResource().getCpu());
                }

                if (spec.getTaskManager().getPodTemplate() != null) {
                    effectiveConfig.set(
                            KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE,
                            createTempFile(spec.getTaskManager().getPodTemplate()));
                }
            }

            if (spec.getJob() != null) {
                final URI uri = new URI(spec.getJob().getJarURI());
                effectiveConfig.set(
                        PipelineOptions.JARS, Collections.singletonList(uri.toString()));

                if (spec.getJob().getParallelism() > 0) {
                    effectiveConfig.set(
                            CoreOptions.DEFAULT_PARALLELISM, spec.getJob().getParallelism());
                }
            }

            return effectiveConfig;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    private static String createTempFile(Pod podTemplate) throws IOException {
        File tmp = File.createTempFile("podTemplate_", ".yaml");
        Files.write(tmp.toPath(), SerializationUtils.dumpAsYaml(podTemplate).getBytes());
        tmp.deleteOnExit();
        return tmp.getAbsolutePath();
    }

    public static ClusterClient<String> getRestClusterClient(Configuration config)
            throws Exception {
        final String clusterId = config.get(KubernetesConfigOptions.CLUSTER_ID);
        final String namespace = config.get(KubernetesConfigOptions.NAMESPACE);
        final int port = config.getInteger(RestOptions.PORT);
        final String host =
                config.getString(
                        RestOptions.ADDRESS, String.format("%s-rest.%s", clusterId, namespace));
        final String restServerAddress = String.format("http://%s:%s", host, port);
        LOG.info("Creating RestClusterClient({})", restServerAddress);
        return new RestClusterClient<>(
                config, clusterId, (c, e) -> new StandaloneClientHAServices(restServerAddress));
    }

    public static JobStatus convert(JobStatusMessage message) {
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobId(message.getJobId().toHexString());
        jobStatus.setJobName(message.getJobName());
        jobStatus.setState(message.getJobState().name());
        return jobStatus;
    }

    public static void deleteCluster(FlinkDeployment flinkApp, KubernetesClient kubernetesClient) {
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(flinkApp.getMetadata().getNamespace())
                .withName(flinkApp.getMetadata().getName())
                .cascading(true)
                .delete();
    }

    /** We need this due to the buggy flink kube cluster client behaviour for now. */
    public static void waitForClusterShutdown(
            KubernetesClient kubernetesClient, Configuration effectiveConfig)
            throws InterruptedException {
        Fabric8FlinkKubeClient flinkKubeClient =
                new Fabric8FlinkKubeClient(
                        effectiveConfig,
                        (NamespacedKubernetesClient) kubernetesClient,
                        Executors.newSingleThreadExecutor());
        for (int i = 0; i < 60; i++) {
            if (!flinkKubeClient
                    .getRestEndpoint(effectiveConfig.get(KubernetesConfigOptions.CLUSTER_ID))
                    .isPresent()) {
                break;
            }
            LOG.info("Waiting for cluster shutdown... ({})", i);
            Thread.sleep(1000);
        }
    }
}
