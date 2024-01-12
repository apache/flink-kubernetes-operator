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

import org.apache.flink.configuration.MemorySize;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.metrics.v1beta1.NodeMetrics;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.resources.ClusterResourceManager.CLUSTER_AUTOSCALER_CONFIG_MAP;
import static org.apache.flink.kubernetes.operator.resources.ClusterResourceManager.LABEL_NODE_GROUP;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link ClusterResourceManager}. */
@EnableKubernetesMockClient(crud = true)
public class ClusterResourceManagerTest {

    KubernetesClient kubernetesClient;

    ClusterResourceManager manager;

    @BeforeEach
    void beforeEach() {
        manager = new ClusterResourceManager(Duration.ofHours(1), kubernetesClient);
    }

    @Test
    void testNoResources() {
        var currentInstances = 0;
        var newInstances = 1;
        var cpu = 0.5;
        var memory = MemorySize.parse("128 mb");

        // Cluster is at size zero and no autoscaling information is available
        assertThat(manager.trySchedule(currentInstances, newInstances, cpu, memory)).isFalse();
    }

    @Test
    void testFixedAmountOfResources() {
        var currentInstances = 0;
        var newInstances = 1;
        var cpu = 0.5;
        var memory = MemorySize.parse("128 mb");

        createNodes(newInstances, cpu, memory);

        // Free capacity just fits the requested resources
        assertThat(manager.trySchedule(currentInstances, newInstances, cpu, memory)).isTrue();
        // No further capacity
        assertThat(manager.trySchedule(currentInstances, newInstances, cpu, memory)).isFalse();
        // But we can schedule if we free resources first
        assertThat(manager.trySchedule(newInstances, newInstances, cpu, memory)).isTrue();
    }

    @Test
    void testMoreResourcesAvailable() {
        var currentInstances = 0;
        var newInstances = 10;
        var cpu = 1;
        var memory = MemorySize.parse("1024 mb");

        createNodes(1, cpu, memory);
        assertThat(manager.trySchedule(currentInstances, newInstances, cpu, memory)).isFalse();

        createClusterAutoscalerConfigMap();
        manager.refresh();
        assertThat(manager.trySchedule(currentInstances, newInstances, cpu, memory)).isTrue();
    }

    @Test
    void testAllOrNothing() {
        var currentInstances = 0;
        var newInstances = 1;
        var cpu = 1;
        var memory = MemorySize.parse("1024 mb");

        createNodes(newInstances, cpu, memory);

        assertThat(manager.trySchedule(currentInstances, newInstances * 2, cpu, memory)).isFalse();
        assertThat(manager.trySchedule(currentInstances, newInstances, cpu, memory)).isTrue();
        assertThat(manager.trySchedule(currentInstances, newInstances, cpu, memory)).isFalse();
    }

    @Test
    void testCaching() {
        var currentInstances = 0;
        var newInstances = 1;
        var cpu = 1;
        var memory = MemorySize.parse("1024 mb");

        createNodes(newInstances, cpu, memory);

        assertThat(manager.clusterResourceView).isNull();
        assertThat(manager.trySchedule(currentInstances, newInstances, cpu, memory)).isTrue();
        assertThat(manager.clusterResourceView).isNotNull();

        var resourceViewBackup = manager.clusterResourceView;
        assertThat(manager.trySchedule(currentInstances, newInstances, cpu, memory)).isFalse();
        assertThat(manager.clusterResourceView).isSameAs(resourceViewBackup);
    }

    @Test
    void testRefresh() {
        // Refresh every time
        manager = new ClusterResourceManager(Duration.ZERO, kubernetesClient);

        assertThat(manager.clusterResourceView).isNull();
        assertThat(manager.trySchedule(0, 1, 1, MemorySize.parse("128 mb"))).isFalse();
        assertThat(manager.clusterResourceView).isNotNull();

        var resourceViewBackup = manager.clusterResourceView;
        assertThat(manager.trySchedule(0, 1, 1, MemorySize.parse("128 mb"))).isFalse();
        assertThat(manager.clusterResourceView).isNotSameAs(resourceViewBackup);
    }

    @Test
    void testDisabled() {
        manager = new ClusterResourceManager(Duration.ofSeconds(-1), kubernetesClient);
        assertThat(
                        manager.trySchedule(
                                0, Integer.MAX_VALUE, Double.MAX_VALUE, MemorySize.MAX_VALUE))
                .isTrue();
    }

    private void createNodes(int numNodes, double cpuPerNode, MemorySize memPerNode) {
        for (int i = 1; i <= numNodes; i++) {
            createNode("node" + i, cpuPerNode, memPerNode);
        }
    }

    private void createNode(String name, double cpuPerNode, MemorySize memPerNode) {
        Node node = new Node();
        node.setMetadata(new ObjectMeta());
        node.getMetadata().setName(name);
        node.getMetadata().setLabels(Map.of(LABEL_NODE_GROUP, "node-group-2"));

        node.setStatus(new NodeStatus());
        node.getStatus().setAllocatable(createResourceMap(cpuPerNode, memPerNode));
        kubernetesClient.resource(node).create();

        NodeMetrics nodeMetrics = new NodeMetrics();
        nodeMetrics.setMetadata(new ObjectMeta());
        nodeMetrics.getMetadata().setName(name);
        nodeMetrics.setUsage(createResourceMap(0, MemorySize.ZERO));
        kubernetesClient.resource(nodeMetrics).create();
    }

    private void createClusterAutoscalerConfigMap() {
        ConfigMap configMap = new ConfigMap();
        configMap.setMetadata(new ObjectMeta());
        configMap.getMetadata().setName(CLUSTER_AUTOSCALER_CONFIG_MAP);
        configMap.getMetadata().setNamespace("kube-system");
        configMap.setData(Map.of("status", CLUSTER_AUTOSCALING_STATUS));

        kubernetesClient.resource(configMap).create();
    }

    private static Map<String, Quantity> createResourceMap(double cpu, MemorySize memory) {
        return Map.of(
                "cpu",
                Quantity.fromNumericalAmount(BigDecimal.valueOf(cpu), null),
                "memory",
                Quantity.fromNumericalAmount(BigDecimal.valueOf(memory.getBytes()), null));
    }

    static final String CLUSTER_AUTOSCALING_STATUS =
            "    Cluster-autoscaler status at 2024-01-05 14:42:56.660050258 +0000 UTC:\n"
                    + "    Cluster-wide:\n"
                    + "      Health:      Healthy (ready=82 unready=0 notStarted=0 longNotStarted=0 registered=82 longUnregistered=0)\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2023-12-24 02:19:08.392928745 +0000 UTC m=+72.882198411\n"
                    + "      ScaleUp:     NoActivity (ready=82 registered=82)\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2024-01-05 04:54:28.692544653 +0000 UTC m=+1046193.181814402\n"
                    + "      ScaleDown:   NoCandidates (candidates=0)\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2024-01-05 07:49:23.659004365 +0000 UTC m=+1056688.148274040\n"
                    + "\n"
                    + "    NodeGroups:\n"
                    + "      Name:        node-group-1\n"
                    + "      Health:      Healthy (ready=27 unready=0 notStarted=0 longNotStarted=0 registered=27 longUnregistered=0 cloudProviderTarget=27\n"
                    + "(minSize=10, maxSize=100))\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2023-12-24 02:19:08.392928745 +0000 UTC m=+72.882198411\n"
                    + "      ScaleUp:     NoActivity (ready=27 cloudProviderTarget=27)\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2023-12-24 02:19:08.392928745 +0000 UTC m=+72.882198411\n"
                    + "      ScaleDown:   NoCandidates (candidates=0)\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2024-01-03 19:34:56.195698291 +0000 UTC m=+926220.684967979\n"
                    + "\n"
                    + "      Name:        node-group-2\n"
                    + "      Health:      Healthy (ready=30 unready=0 notStarted=0 longNotStarted=0 registered=30 longUnregistered=0 cloudProviderTarget=30\n"
                    + "(minSize=10, maxSize=100))\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2023-12-24 02:19:08.392928745 +0000 UTC m=+72.882198411\n"
                    + "      ScaleUp:     NoActivity (ready=30 cloudProviderTarget=30)\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2023-12-24 02:19:08.392928745 +0000 UTC m=+72.882198411\n"
                    + "      ScaleDown:   NoCandidates (candidates=0)\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2024-01-03 19:39:59.613210811 +0000 UTC m=+926524.102480488\n"
                    + "\n"
                    + "      Name:        node-group-3\n"
                    + "      Health:      Healthy (ready=25 unready=0 notStarted=0 longNotStarted=0 registered=25 longUnregistered=0 cloudProviderTarget=25\n"
                    + "(minSize=1, maxSize=100))\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2023-12-24 02:19:08.392928745 +0000 UTC m=+72.882198411\n"
                    + "      ScaleUp:     NoActivity (ready=25 cloudProviderTarget=25)\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2024-01-05 04:54:28.692544653 +0000 UTC m=+1046193.181814402\n"
                    + "      ScaleDown:   NoCandidates (candidates=0)\n"
                    + "                   LastProbeTime:      2024-01-05 14:42:56.011239739 +0000 UTC m=+1081500.500509416\n"
                    + "                   LastTransitionTime: 2024-01-05 07:49:23.659004365 +0000 UTC m=+1056688.148274040";
}
