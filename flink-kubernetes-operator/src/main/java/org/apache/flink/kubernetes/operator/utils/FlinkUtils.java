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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.config.DefaultConfig;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;

/** Flink Utility methods used by the operator. */
public class FlinkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUtils.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static DefaultConfig loadDefaultConfig() {
        Configuration operatorConfig =
                FlinkUtils.loadConfiguration(EnvUtils.get(EnvUtils.ENV_FLINK_OPERATOR_CONF_DIR));
        Configuration flinkConfig =
                FlinkUtils.loadConfiguration(EnvUtils.get(EnvUtils.ENV_FLINK_CONF_DIR));
        return new DefaultConfig(operatorConfig, flinkConfig);
    }

    public static Configuration getEffectiveConfig(
            FlinkDeployment flinkApp, Configuration flinkConfig) {
        return getEffectiveConfig(flinkApp.getMetadata(), flinkApp.getSpec(), flinkConfig);
    }

    public static Configuration getEffectiveConfig(
            ObjectMeta meta, FlinkDeploymentSpec spec, Configuration flinkConfig) {
        try {
            final Configuration effectiveConfig =
                    FlinkConfigBuilder.buildFrom(meta, spec, flinkConfig);
            LOG.debug("Effective config: {}", effectiveConfig);
            return effectiveConfig;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    public static Configuration loadConfiguration(String confDir) {
        return confDir != null
                ? GlobalConfiguration.loadConfiguration(confDir)
                : new Configuration();
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

    public static void deleteCluster(
            FlinkDeployment flinkApp,
            KubernetesClient kubernetesClient,
            boolean deleteHaConfigmaps) {
        deleteCluster(
                flinkApp.getMetadata().getNamespace(),
                flinkApp.getMetadata().getName(),
                kubernetesClient,
                deleteHaConfigmaps);
    }

    /**
     * Delete Flink kubernetes cluster by deleting the kubernetes resources directly. Optionally
     * allows deleting the native kubernetes HA resources as well.
     *
     * @param namespace Namespace where the Flink cluster is deployed
     * @param clusterId ClusterId of the Flink cluster
     * @param kubernetesClient Kubernetes client
     * @param deleteHaConfigmaps Flag to indicate whether k8s HA metadata should be removed as well
     */
    public static void deleteCluster(
            String namespace,
            String clusterId,
            KubernetesClient kubernetesClient,
            boolean deleteHaConfigmaps) {
        LOG.info("Deleting Flink cluster resources");
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(KubernetesUtils.getDeploymentName(clusterId))
                .cascading(true)
                .delete();

        if (deleteHaConfigmaps) {
            // We need to wait for cluster shutdown otherwise HA configmaps might be recreated
            waitForClusterShutdown(kubernetesClient, namespace, clusterId);
            kubernetesClient
                    .configMaps()
                    .inNamespace(namespace)
                    .withLabels(
                            KubernetesUtils.getConfigMapLabels(
                                    clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                    .delete();
        }
    }

    /** Wait until the FLink cluster has completely shut down. */
    public static void waitForClusterShutdown(
            KubernetesClient kubernetesClient, String namespace, String clusterId) {

        boolean jobManagerRunning = true;
        boolean serviceRunning = true;

        for (int i = 0; i < 60; i++) {
            if (jobManagerRunning) {
                PodList jmPodList = getJmPodList(kubernetesClient, namespace, clusterId);

                if (jmPodList == null || jmPodList.getItems().isEmpty()) {
                    jobManagerRunning = false;
                }
            }

            if (serviceRunning) {
                Service service =
                        kubernetesClient
                                .services()
                                .inNamespace(namespace)
                                .withName(
                                        ExternalServiceDecorator.getExternalServiceName(clusterId))
                                .fromServer()
                                .get();
                if (service == null) {
                    serviceRunning = false;
                }
            }

            if (!jobManagerRunning && !serviceRunning) {
                break;
            }
            LOG.info("Waiting for cluster shutdown... ({})", i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("Cluster shutdown completed.");
    }

    /** Wait until the FLink cluster has completely shut down. */
    public static void waitForClusterShutdown(
            KubernetesClient kubernetesClient, Configuration conf) {
        FlinkUtils.waitForClusterShutdown(
                kubernetesClient,
                conf.getString(KubernetesConfigOptions.NAMESPACE),
                conf.getString(KubernetesConfigOptions.CLUSTER_ID));
    }

    public static PodList getJmPodList(
            KubernetesClient kubernetesClient, String namespace, String clusterId) {
        return kubernetesClient
                .pods()
                .inNamespace(namespace)
                .withLabels(KubernetesUtils.getJobManagerSelectors(clusterId))
                .list();
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

        configMaps
                .getItems()
                .forEach(
                        configMap -> {
                            final boolean isDeleted =
                                    configMap
                                            .getData()
                                            .entrySet()
                                            .removeIf(FlinkUtils::isJobGraphKey);
                            if (isDeleted) {
                                LOG.info(
                                        "Job graph in ConfigMap {} is deleted",
                                        configMap.getMetadata().getName());
                            }
                        });
        kubernetesClient.resourceList(configMaps).inNamespace(namespace).createOrReplace();
    }

    private static boolean isJobGraphKey(Map.Entry<String, String> entry) {
        return entry.getKey().startsWith(Constants.JOB_GRAPH_STORE_KEY_PREFIX);
    }

    public static boolean isKubernetesHAActivated(Configuration configuration) {
        return configuration
                .get(HighAvailabilityOptions.HA_MODE)
                .equalsIgnoreCase(KubernetesHaServicesFactory.class.getCanonicalName());
    }
}
