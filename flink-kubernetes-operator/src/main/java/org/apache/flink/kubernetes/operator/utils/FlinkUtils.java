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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.internal.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;

/** Flink Utility methods used by the operator. */
public class FlinkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUtils.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Configuration getEffectiveConfig(FlinkDeployment flinkApp) {
        String namespace = flinkApp.getMetadata().getNamespace();
        String clusterId = flinkApp.getMetadata().getName();
        FlinkDeploymentSpec spec = flinkApp.getSpec();

        try {
            String flinkConfDir = System.getenv().get(ConfigConstants.ENV_FLINK_CONF_DIR);
            Configuration effectiveConfig = loadConfiguration(flinkConfDir);

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
                            createTempFile(
                                    mergePodTemplates(
                                            spec.getPodTemplate(),
                                            spec.getJobManager().getPodTemplate())));
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
                            createTempFile(
                                    mergePodTemplates(
                                            spec.getPodTemplate(),
                                            spec.getTaskManager().getPodTemplate())));
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

    public static Configuration loadConfiguration(String confDir) {
        Configuration configuration =
                confDir != null
                        ? GlobalConfiguration.loadConfiguration(confDir)
                        : new Configuration();
        return configuration;
    }

    private static String createTempFile(Pod podTemplate) throws IOException {
        File tmp = File.createTempFile("podTemplate_", ".yaml");
        Files.write(tmp.toPath(), SerializationUtils.dumpAsYaml(podTemplate).getBytes());
        tmp.deleteOnExit();
        return tmp.getAbsolutePath();
    }

    public static Pod mergePodTemplates(Pod toPod, Pod fromPod) {
        if (fromPod == null) {
            return toPod;
        } else if (toPod == null) {
            return fromPod;
        }
        JsonNode node1 = MAPPER.valueToTree(toPod);
        JsonNode node2 = MAPPER.valueToTree(fromPod);
        mergeInto(node1, node2);
        try {
            return MAPPER.treeToValue(node1, Pod.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void mergeInto(JsonNode toNode, JsonNode fromNode) {
        Iterator<String> fieldNames = fromNode.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode toChildNode = toNode.get(fieldName);
            JsonNode fromChildNode = fromNode.get(fieldName);

            if (toChildNode != null && toChildNode.isArray() && fromChildNode.isArray()) {
                // TODO: does merging arrays even make sense or should it just override?
                for (int i = 0; i < fromChildNode.size(); i++) {
                    JsonNode updatedChildNode = fromChildNode.get(i);
                    if (toChildNode.size() <= i) {
                        // append new node
                        ((ArrayNode) toChildNode).add(updatedChildNode);
                    }
                    mergeInto(toChildNode.get(i), updatedChildNode);
                }
            } else if (toChildNode != null && toChildNode.isObject()) {
                mergeInto(toChildNode, fromChildNode);
            } else {
                if (toNode instanceof ObjectNode) {
                    ((ObjectNode) toNode).replace(fieldName, fromChildNode);
                }
            }
        }
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
}
