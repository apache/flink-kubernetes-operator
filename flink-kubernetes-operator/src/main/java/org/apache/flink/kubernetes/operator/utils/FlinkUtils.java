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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

/** Flink Utility methods used by the operator. */
public class FlinkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUtils.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String CR_GENERATION_LABEL = "flinkdeployment.flink.apache.org/generation";

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

    public static void deleteJobGraphInKubernetesHA(
            String clusterId, String namespace, KubernetesClient kubernetesClient) {
        // The HA ConfigMap names have been changed from 1.15, so we use the labels to filter out
        // them and delete job graph key
        final Map<String, String> haConfigMapLabels =
                KubernetesUtils.getConfigMapLabels(
                        clusterId, Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);
        final ConfigMapList configMaps =
                kubernetesClient
                        .configMaps()
                        .inNamespace(namespace)
                        .withLabels(haConfigMapLabels)
                        .list();

        boolean shouldUpdate = false;
        for (ConfigMap configMap : configMaps.getItems()) {
            if (configMap.getData() == null || configMap.getData().isEmpty()) {
                continue;
            }
            final boolean isDeleted =
                    configMap.getData().entrySet().removeIf(FlinkUtils::isJobGraphKey);
            if (isDeleted) {
                shouldUpdate = true;
                LOG.info("Job graph in ConfigMap {} is deleted", configMap.getMetadata().getName());
            }
        }
        if (shouldUpdate) {
            kubernetesClient.resourceList(configMaps).inNamespace(namespace).createOrReplace();
        }
    }

    public static boolean isHaMetadataAvailable(
            Configuration conf, KubernetesClient kubernetesClient) {

        String clusterId = conf.get(KubernetesConfigOptions.CLUSTER_ID);
        String namespace = conf.get(KubernetesConfigOptions.NAMESPACE);

        var haConfigMapLabels =
                KubernetesUtils.getConfigMapLabels(
                        clusterId, Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY);

        var configMaps =
                kubernetesClient
                        .configMaps()
                        .inNamespace(namespace)
                        .withLabels(haConfigMapLabels)
                        .list()
                        .getItems();

        return configMaps.stream().anyMatch(Predicate.not(ConfigMap::isMarkedForDeletion));
    }

    private static boolean isJobGraphKey(Map.Entry<String, String> entry) {
        return entry.getKey().startsWith(Constants.JOB_GRAPH_STORE_KEY_PREFIX);
    }

    public static boolean isKubernetesHAActivated(Configuration configuration) {
        return configuration
                .get(HighAvailabilityOptions.HA_MODE)
                .equalsIgnoreCase(KubernetesHaServicesFactory.class.getCanonicalName());
    }

    public static boolean clusterShutdownDisabled(FlinkDeploymentSpec spec) {
        return spec.getFlinkVersion().isNewerVersionThan(FlinkVersion.v1_14);
    }

    public static int getNumTaskManagers(Configuration conf) {
        int parallelism = conf.get(CoreOptions.DEFAULT_PARALLELISM);
        return getNumTaskManagers(conf, parallelism);
    }

    public static int getNumTaskManagers(Configuration conf, int parallelism) {
        int taskSlots = conf.get(TaskManagerOptions.NUM_TASK_SLOTS);
        return (parallelism + taskSlots - 1) / taskSlots;
    }

    public static void setGenerationAnnotation(Configuration conf, Long generation) {
        if (generation == null) {
            return;
        }
        var labels =
                new HashMap<>(
                        conf.getOptional(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS)
                                .orElse(Collections.emptyMap()));
        labels.put(CR_GENERATION_LABEL, generation.toString());
        conf.set(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS, labels);
    }

    /**
     * The jobID's lower part is the resource uid, the higher part is the resource generation.
     *
     * @param meta the meta of the resource.
     * @return the generated jobID.
     */
    public static JobID generateSessionJobFixedJobID(ObjectMeta meta) {
        return generateSessionJobFixedJobID(meta.getUid(), meta.getGeneration());
    }

    /**
     * The jobID's lower part is the resource uid, the higher part is the resource generation.
     *
     * @param uid the uid of the resource.
     * @param generation the generation of the resource.
     * @return the generated jobID.
     */
    public static JobID generateSessionJobFixedJobID(String uid, Long generation) {
        return new JobID(
                Preconditions.checkNotNull(uid).hashCode(), Preconditions.checkNotNull(generation));
    }
}
