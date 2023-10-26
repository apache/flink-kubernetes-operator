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
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.rest.messages.DashboardConfigurationHeaders;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HTTPGetAction;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;

/** Flink Utility methods used by the operator. */
public class FlinkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUtils.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String CR_GENERATION_LABEL = "flinkdeployment.flink.apache.org/generation";

    public static Pod mergePodTemplates(Pod toPod, Pod fromPod, boolean mergeArraysByName) {
        if (fromPod == null) {
            return ReconciliationUtils.clone(toPod);
        } else if (toPod == null) {
            return ReconciliationUtils.clone(fromPod);
        }
        JsonNode node1 = MAPPER.valueToTree(toPod);
        JsonNode node2 = MAPPER.valueToTree(fromPod);
        mergeInto(node1, node2, mergeArraysByName);
        try {
            return MAPPER.treeToValue(node1, Pod.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void mergeInto(JsonNode toNode, JsonNode fromNode, boolean mergeArraysByName) {
        Iterator<String> fieldNames = fromNode.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode toChildNode = toNode.get(fieldName);
            JsonNode fromChildNode = fromNode.get(fieldName);

            if (toChildNode != null && toChildNode.isArray() && fromChildNode.isArray()) {
                mergeArray((ArrayNode) toChildNode, (ArrayNode) fromChildNode, mergeArraysByName);
            } else if (toChildNode != null && toChildNode.isObject()) {
                mergeInto(toChildNode, fromChildNode, mergeArraysByName);
            } else {
                if (toNode instanceof ObjectNode) {
                    ((ObjectNode) toNode).replace(fieldName, fromChildNode);
                }
            }
        }
    }

    private static void mergeArray(
            ArrayNode toChildNode, ArrayNode fromChildNode, boolean mergeArraysByName) {
        if (namesDefined(toChildNode) && namesDefined(fromChildNode) && mergeArraysByName) {
            var toGrouped = groupByName(toChildNode);
            var fromGrouped = groupByName(fromChildNode);
            fromGrouped.forEach(
                    (name, fromElement) ->
                            toGrouped.compute(
                                    name,
                                    (n, toElement) -> {
                                        if (toElement == null) {
                                            return fromElement;
                                        }
                                        mergeInto(toElement, fromElement, mergeArraysByName);
                                        return toElement;
                                    }));

            toChildNode.removeAll();
            toGrouped.values().forEach(toChildNode::add);
        } else {
            for (int i = 0; i < fromChildNode.size(); i++) {
                JsonNode updatedChildNode = fromChildNode.get(i);
                if (toChildNode.size() <= i) {
                    // append new node
                    toChildNode.add(updatedChildNode);
                }
                mergeInto(toChildNode.get(i), updatedChildNode, mergeArraysByName);
            }
        }
    }

    private static boolean namesDefined(ArrayNode node) {
        var it = node.elements();
        while (it.hasNext()) {
            var next = it.next();
            if (!next.has("name")) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, ObjectNode> groupByName(ArrayNode node) {
        var out = new LinkedHashMap<String, ObjectNode>();
        node.elements().forEachRemaining(e -> out.put((e.get("name").asText()), (ObjectNode) e));
        return out;
    }

    public static void addStartupProbe(Pod pod) {
        var spec = pod.getSpec();
        if (spec == null) {
            spec = new PodSpec();
            pod.setSpec(spec);
        }

        var containers = spec.getContainers();
        if (containers == null) {
            containers = new ArrayList<>();
            spec.setContainers(containers);
        }

        var mainContainer =
                containers.stream()
                        .filter(c -> Constants.MAIN_CONTAINER_NAME.equals(c.getName()))
                        .findAny()
                        .orElseGet(
                                () -> {
                                    var c = new Container();
                                    c.setName(Constants.MAIN_CONTAINER_NAME);
                                    var containersCopy =
                                            new ArrayList<>(pod.getSpec().getContainers());
                                    containersCopy.add(c);
                                    pod.getSpec().setContainers(containersCopy);
                                    return c;
                                });

        if (mainContainer.getStartupProbe() == null) {
            var probe = new Probe();
            probe.setFailureThreshold(Integer.MAX_VALUE);
            probe.setPeriodSeconds(1);

            var configGet = new HTTPGetAction();
            configGet.setPath(
                    DashboardConfigurationHeaders.getInstance().getTargetRestEndpointURL());
            configGet.setPort(new IntOrString(Constants.REST_PORT_NAME));
            probe.setHttpGet(configGet);

            mainContainer.setStartupProbe(probe);
        }
    }

    public static void deleteZookeeperHAMetadata(Configuration conf) {
        try (var curator = ZooKeeperUtils.startCuratorFramework(conf, exception -> {})) {
            try {
                curator.asCuratorFramework().delete().deletingChildrenIfNeeded().forPath("/");
            } catch (Exception e) {
                LOG.error(
                        "Could not delete HA Metadata at path {} in Zookeeper",
                        ZooKeeperUtils.generateZookeeperPath(
                                conf.get(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT),
                                conf.get(HighAvailabilityOptions.HA_CLUSTER_ID)),
                        e);
            }
        }
    }

    public static void deleteKubernetesHAMetadata(
            String clusterId, String namespace, KubernetesClient kubernetesClient) {
        kubernetesClient
                .configMaps()
                .inNamespace(namespace)
                .withLabels(
                        KubernetesUtils.getConfigMapLabels(
                                clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                .delete();
    }

    public static void deleteJobGraphInZookeeperHA(Configuration conf) throws Exception {
        try (var curator = ZooKeeperUtils.startCuratorFramework(conf, exception -> {})) {
            ZooKeeperUtils.deleteZNode(
                    curator.asCuratorFramework(),
                    conf.get(HighAvailabilityOptions.HA_ZOOKEEPER_JOBGRAPHS_PATH));
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

    public static boolean isZookeeperHaMetadataAvailable(Configuration conf) {
        try (var curator = ZooKeeperUtils.startCuratorFramework(conf, exception -> {})) {
            if (curator.asCuratorFramework().checkExists().forPath(ZooKeeperUtils.getJobsPath())
                    != null) {
                return curator.asCuratorFramework()
                                .getChildren()
                                .forPath(ZooKeeperUtils.getJobsPath())
                                .size()
                        != 0;
            }
            return false;
        } catch (Exception e) {
            LOG.error(
                    "Could not check whether the HA metadata exists at path {} in Zookeeper",
                    ZooKeeperUtils.generateZookeeperPath(
                            conf.get(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT),
                            conf.get(HighAvailabilityOptions.HA_CLUSTER_ID),
                            ZooKeeperUtils.getJobsPath()),
                    e);
        }

        return false;
    }

    public static boolean isKubernetesHaMetadataAvailable(
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

        return configMaps.stream().anyMatch(FlinkUtils::isValidHaConfigMap);
    }

    private static boolean isValidHaConfigMap(ConfigMap cm) {
        if (cm.isMarkedForDeletion()) {
            return false;
        }

        var name = cm.getMetadata().getName();
        if (name.endsWith("-config-map")) {
            return !name.endsWith("-cluster-config-map");
        }

        return name.endsWith("-jobmanager-leader");
    }

    private static boolean isJobGraphKey(Map.Entry<String, String> entry) {
        return entry.getKey().startsWith(Constants.JOB_GRAPH_STORE_KEY_PREFIX);
    }

    public static boolean isZookeeperHAActivated(Configuration configuration) {
        return HighAvailabilityMode.fromConfig(configuration)
                .equals(HighAvailabilityMode.ZOOKEEPER);
    }

    public static boolean isKubernetesHAActivated(Configuration configuration) {
        String haMode = configuration.get(HighAvailabilityOptions.HA_MODE);
        return haMode.equalsIgnoreCase(KubernetesHaServicesFactory.class.getCanonicalName())
                // Hardcoded config value should be removed when upgrading Flink dependency to 1.16
                || haMode.equalsIgnoreCase("kubernetes");
    }

    public static int getNumTaskManagers(Configuration conf) {
        int parallelism = conf.get(CoreOptions.DEFAULT_PARALLELISM);
        return getNumTaskManagers(conf, parallelism);
    }

    public static int getNumTaskManagers(Configuration conf, int parallelism) {
        int taskSlots = conf.get(TaskManagerOptions.NUM_TASK_SLOTS);
        return (parallelism + taskSlots - 1) / taskSlots;
    }

    public static Double calculateClusterCpuUsage(Configuration conf, int taskManagerReplicas) {
        var jmTotalCpu =
                conf.getDouble(KubernetesConfigOptions.JOB_MANAGER_CPU)
                        * conf.getDouble(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT_FACTOR)
                        * conf.get(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS);

        var tmTotalCpu =
                conf.getDouble(KubernetesConfigOptions.TASK_MANAGER_CPU, 1)
                        * conf.getDouble(KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR)
                        * taskManagerReplicas;

        return tmTotalCpu + jmTotalCpu;
    }

    public static Long calculateClusterMemoryUsage(Configuration conf, int taskManagerReplicas) {
        var clusterSpec = new KubernetesClusterClientFactory().getClusterSpecification(conf);

        var jmParameters = new KubernetesJobManagerParameters(conf, clusterSpec);
        var jmTotalMemory =
                Math.round(
                        jmParameters.getJobManagerMemoryMB()
                                * Math.pow(1024, 2)
                                * jmParameters.getJobManagerMemoryLimitFactor()
                                * jmParameters.getReplicas());

        var tmTotalMemory =
                Math.round(
                        clusterSpec.getTaskManagerMemoryMB()
                                * Math.pow(1024, 2)
                                * conf.getDouble(
                                        KubernetesConfigOptions.TASK_MANAGER_MEMORY_LIMIT_FACTOR)
                                * taskManagerReplicas);

        return tmTotalMemory + jmTotalMemory;
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
                UUID.fromString(Preconditions.checkNotNull(uid)).getMostSignificantBits(),
                Preconditions.checkNotNull(generation));
    }

    /**
     * Check if the jobmanager pod has never successfully started. This is an important check to
     * determine whether it is possible that the job has started and taken any checkpoints that we
     * are unaware of.
     *
     * <p>The way we check this is by using the availability condition transition timestamp. If the
     * deployment never transitioned out of the unavailable state, we can assume that the JM never
     * started.
     *
     * @param context Resource context
     * @return True only if we are sure that the jobmanager pod never started
     */
    public static boolean jmPodNeverStarted(Context<?> context) {
        Optional<Deployment> depOpt = context.getSecondaryResource(Deployment.class);
        if (depOpt.isPresent()) {
            Deployment deployment = depOpt.get();
            for (DeploymentCondition condition : deployment.getStatus().getConditions()) {
                if (condition.getType().equals("Available")) {
                    var createTs = deployment.getMetadata().getCreationTimestamp();
                    if ("False".equals(condition.getStatus())
                            && createTs.equals(condition.getLastTransitionTime())) {
                        return true;
                    }
                }
            }
        }

        // If unsure, return false to be on the safe side
        return false;
    }
}
