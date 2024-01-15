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

package org.apache.flink.kubernetes.operator.resources;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.resources.ResourceCheck;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A cluster resource manager which provides a view over the allocatable resources within a
 * Kubernetes cluster and allows to simulate scheduling pods with a defined number of required
 * resources.
 *
 * <p>The goal is to provide a good indicator for whether resources needed for autoscaling are going
 * to be available. This is achieved by pulling the node resource usage from the Kubernetes cluster
 * at a regular configurable interval, after which we use this data to simulate adding / removing
 * resources (pods). Note that this is merely a (pretty good) heuristic because the Kubernetes
 * scheduler has the final saying. However, we prevent 99% of the scenarios after pipeline outages
 * which can lead to massive scale up where all pipelines may be scaled up at the same time and
 * exhaust the number of available resources.
 *
 * <p>The simulation can run on a fixed set of Kubernetes nodes. Additionally, if we detect that the
 * cluster is using the Kubernetes Cluster Autoscaler, we will use this data to extrapolate the
 * number of nodes to the maximum defined nodes in the autoscaler configuration.
 *
 * <p>We currently track CPU and memory. Ephemeral storage is missing because there is no easy way
 * to get node statics on free storage.
 */
public class ClusterResourceManager implements ResourceCheck {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterResourceManager.class);

    /** ConfigMap name of the Kubernetes Cluster Autoscaler. */
    static final String CLUSTER_AUTOSCALER_CONFIG_MAP = "cluster-autoscaler-status";

    /** EKS specific node group information. Code still works without this label. */
    static final String LABEL_NODE_GROUP = "eks.amazonaws.com/nodegroup";

    private final Duration refreshInterval;
    private final KubernetesClient kubernetesClient;

    @VisibleForTesting ClusterResourceView clusterResourceView;

    public static ClusterResourceManager of(Configuration config, KubernetesClient client) {
        return new ClusterResourceManager(
                config.get(KubernetesOperatorConfigOptions.REFRESH_CLUSTER_RESOURCE_VIEW), client);
    }

    public ClusterResourceManager(Duration refreshInterval, KubernetesClient kubernetesClient) {
        this.refreshInterval = refreshInterval;
        this.kubernetesClient = kubernetesClient;
    }

    @Override
    public synchronized boolean trySchedule(
            int currentInstances,
            int newInstances,
            double cpuPerInstance,
            MemorySize memoryPerInstance) {

        if (refreshInterval.isNegative()) {
            // Feature disabled
            return true;
        }

        if (shouldRefreshView(clusterResourceView, refreshInterval)) {
            try {
                clusterResourceView = createResourceView(kubernetesClient);
            } catch (KubernetesClientException e) {
                if (e.getCode() == 403) {
                    LOG.warn(
                            "No permission to retrieve node resource usage. Resource check disabled.");
                    return true;
                }
                throw e;
            }
        }

        return trySchedule(
                clusterResourceView,
                currentInstances,
                newInstances,
                cpuPerInstance,
                memoryPerInstance);
    }

    /**
     * Simple check whether the new resource requirements can be scheduled. Note: This is not a
     * full-blown scheduler. We may return false-negatives, i.e. we may indicate scheduling is not
     * possible when it actually is. This is still better than false positives where we suffer from
     * downtime due to non-schedulable pods.
     */
    private static boolean trySchedule(
            ClusterResourceView resourceView,
            int currentInstances,
            int newInstances,
            double cpuPerInstance,
            MemorySize memoryPerInstance) {

        resourceView.cancelPending();

        for (int i = 0; i < currentInstances; i++) {
            resourceView.release(cpuPerInstance, memoryPerInstance);
        }

        for (int i = 0; i < newInstances; i++) {
            if (!resourceView.tryReserve(cpuPerInstance, memoryPerInstance)) {
                return false;
            }
        }

        resourceView.commit();

        return true;
    }

    private boolean shouldRefreshView(
            ClusterResourceView clusterResourceView, Duration refreshInterval) {
        return clusterResourceView == null
                || Instant.now()
                        .isAfter(clusterResourceView.getCreationTime().plus(refreshInterval));
    }

    private static ClusterResourceView createResourceView(KubernetesClient kubernetesClient) {
        List<KubernetesNodeResourceInfo> nodes = new ArrayList<>();

        for (Node item : kubernetesClient.nodes().list().getItems()) {

            String nodeName = item.getMetadata().getName();
            String nodeGroup = item.getMetadata().getLabels().get(LABEL_NODE_GROUP);

            Map<String, Quantity> usage =
                    kubernetesClient.top().nodes().metrics(nodeName).getUsage();
            Map<String, Quantity> allocatable = item.getStatus().getAllocatable();

            KubernetesResource cpuInfo = getResourceInfo("cpu", allocatable, usage);
            KubernetesResource memInfo = getResourceInfo("memory", allocatable, usage);
            // Ephemeral storage is currently missing because there is no easy way to get node
            // statics on free storage.

            nodes.add(new KubernetesNodeResourceInfo(nodeName, nodeGroup, cpuInfo, memInfo));
        }

        try {
            addClusterAutoscalableNodes(kubernetesClient, nodes);
        } catch (ClusterAutoscalerUnavailableException e) {
            LOG.info("No cluster autoscaler information available: {}", e.getMessage());
        }

        return new ClusterResourceView(nodes);
    }

    private static void addClusterAutoscalableNodes(
            KubernetesClient kubernetesClient, List<KubernetesNodeResourceInfo> nodes)
            throws ClusterAutoscalerUnavailableException {
        Map<String, Integer> nodeGroupsWithMaxSize = getMaxClusterSizeByNodeGroup(kubernetesClient);

        Map<String, List<KubernetesNodeResourceInfo>> nodeGroupsWithCurrentSize = new HashMap<>();
        for (KubernetesNodeResourceInfo node : nodes) {
            List<KubernetesNodeResourceInfo> nodeGroupInfos =
                    nodeGroupsWithCurrentSize.computeIfAbsent(
                            node.getNodeGroup(), key -> new ArrayList<>());
            nodeGroupInfos.add(node);
        }

        // Add the extra number of nodes which can be added via cluster autoscaling
        for (Map.Entry<String, Integer> entry : nodeGroupsWithMaxSize.entrySet()) {
            String nodeGroup = entry.getKey();
            int nodeGroupMaxSize = entry.getValue();
            List<KubernetesNodeResourceInfo> nodeGroupNodes =
                    nodeGroupsWithCurrentSize.get(nodeGroup);
            if (nodeGroupNodes == null) {
                // Node group not autoscaled
                continue;
            }
            int nodeGroupCurrentSize = nodeGroupNodes.size();

            for (int i = nodeGroupCurrentSize; i < nodeGroupMaxSize; i++) {
                KubernetesNodeResourceInfo exmplaryNode = nodeGroupNodes.get(0);
                nodes.add(
                        new KubernetesNodeResourceInfo(
                                "future-node-" + i,
                                exmplaryNode.getNodeGroup(),
                                new KubernetesResource(exmplaryNode.getCpu().getAllocatable(), 0),
                                new KubernetesResource(
                                        exmplaryNode.getMemory().getAllocatable(), 0)));
            }
        }
    }

    private static Map<String, Integer> getMaxClusterSizeByNodeGroup(
            KubernetesClient kubernetesClient) throws ClusterAutoscalerUnavailableException {

        ConfigMap configMap = new ConfigMap();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName(CLUSTER_AUTOSCALER_CONFIG_MAP);
        metadata.setNamespace("kube-system");
        configMap.setMetadata(metadata);
        configMap = kubernetesClient.configMaps().resource(configMap).get();
        if (configMap == null) {
            LOG.info("ConfigMap {} not found", CLUSTER_AUTOSCALER_CONFIG_MAP);
            throw new ClusterAutoscalerUnavailableException(
                    "ConfigMap not found " + CLUSTER_AUTOSCALER_CONFIG_MAP);
        }
        String status = configMap.getData().get("status");
        if (status == null) {
            throw new RuntimeException(
                    "status field not found in " + CLUSTER_AUTOSCALER_CONFIG_MAP);
        }

        Matcher matcher = Pattern.compile("Name:\\s*(\\S+)\n.*\n.*maxSize=(\\d+)").matcher(status);
        Map<String, Integer> nodeGroupsBySize = new HashMap<>();
        while (matcher.find()) {
            String nodeGroupName = matcher.group(1);
            int numNodes = Integer.parseInt(matcher.group(2));
            Integer existingValue = nodeGroupsBySize.put(nodeGroupName, numNodes);
            LOG.debug("Extracted nodeGroup {} maxSize: {}", nodeGroupName, nodeGroupsBySize);
            Preconditions.checkState(
                    existingValue == null, "NodeGroup %s found twice", nodeGroupName);
        }

        if (nodeGroupsBySize.isEmpty()) {
            throw new RuntimeException("Cluster size could not be determined");
        }

        return nodeGroupsBySize;
    }

    @VisibleForTesting
    void refresh() {
        clusterResourceView = null;
    }

    private static KubernetesResource getResourceInfo(
            String type, Map<String, Quantity> allocatableMap, Map<String, Quantity> usageMap) {
        Quantity allocatableQuantity = Preconditions.checkNotNull(allocatableMap.get(type));
        Quantity usageQuantity = Preconditions.checkNotNull(usageMap.get(type));

        double allocatable = allocatableQuantity.getNumericalAmount().doubleValue();
        double usage = usageQuantity.getNumericalAmount().doubleValue();
        return new KubernetesResource(allocatable, usage);
    }

    private static class ClusterAutoscalerUnavailableException extends Exception {
        public ClusterAutoscalerUnavailableException(String message) {
            super(message);
        }
    }
}
