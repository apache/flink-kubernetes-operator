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

package org.apache.flink.kubernetes.operator.standalone;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.kubeclient.Fabric8FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link KubernetesStandaloneClusterDescriptor unit tests */
@EnableKubernetesMockClient(crud = true)
public class KubernetesStandaloneClusterDescriptorTest {

    private KubernetesStandaloneClusterDescriptor clusterDescriptor;
    KubernetesMockServer mockServer;
    private NamespacedKubernetesClient kubernetesClient;
    private FlinkStandaloneKubeClient flinkKubeClient;
    private Configuration flinkConfig = new Configuration();

    @BeforeEach
    public void setup() {
        flinkConfig = TestUtils.createTestFlinkConfig();
        kubernetesClient = mockServer.createClient().inNamespace(TestUtils.TEST_NAMESPACE);
        flinkKubeClient =
                new Fabric8FlinkStandaloneKubeClient(
                        flinkConfig, kubernetesClient, Executors.newDirectExecutorService());

        clusterDescriptor = new KubernetesStandaloneClusterDescriptor(flinkConfig, flinkKubeClient);
    }

    @Test
    public void testDeploySessionCluster() throws Exception {
        ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();

        flinkConfig.setString(BlobServerOptions.PORT, String.valueOf(0));
        flinkConfig.setString(TaskManagerOptions.RPC_PORT, String.valueOf(0));
        flinkConfig.setString(RestOptions.BIND_PORT, String.valueOf(0));

        var clusterClientProvider = clusterDescriptor.deploySessionCluster(clusterSpecification);

        List<Deployment> deployments =
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .list()
                        .getItems();
        String expectedJMDeploymentName = TestUtils.CLUSTER_ID;
        String expectedTMDeploymentName = TestUtils.CLUSTER_ID + "-taskmanager";

        assertEquals(2, deployments.size());
        assertThat(
                deployments.stream()
                        .map(d -> d.getMetadata().getName())
                        .collect(Collectors.toList()),
                containsInAnyOrder(expectedJMDeploymentName, expectedTMDeploymentName));
        assertEquals(
                flinkConfig.get(BlobServerOptions.PORT),
                String.valueOf(Constants.BLOB_SERVER_PORT));
        assertEquals(
                flinkConfig.get(TaskManagerOptions.RPC_PORT),
                String.valueOf(Constants.TASK_MANAGER_RPC_PORT));
        assertEquals(flinkConfig.get(RestOptions.BIND_PORT), String.valueOf(Constants.REST_PORT));

        Deployment jmDeployment =
                deployments.stream()
                        .filter(d -> d.getMetadata().getName().equals(expectedJMDeploymentName))
                        .findFirst()
                        .orElse(null);
        assertTrue(
                jmDeployment.getSpec().getTemplate().getSpec().getContainers().stream()
                        .anyMatch(c -> c.getArgs().contains("jobmanager")));

        var clusterClient = clusterClientProvider.getClusterClient();

        String expectedWebUrl =
                String.format(
                        "http://%s:%d",
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                TestUtils.CLUSTER_ID, TestUtils.TEST_NAMESPACE),
                        Constants.REST_PORT);
        assertEquals(expectedWebUrl, clusterClient.getWebInterfaceURL());
    }

    @Test
    public void testDeployApplicationCluster() throws Exception {
        ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();

        flinkConfig.setString(BlobServerOptions.PORT, String.valueOf(0));
        flinkConfig.setString(TaskManagerOptions.RPC_PORT, String.valueOf(0));
        flinkConfig.setString(RestOptions.BIND_PORT, String.valueOf(0));

        var clusterClientProvider =
                clusterDescriptor.deployApplicationCluster(
                        clusterSpecification,
                        ApplicationConfiguration.fromConfiguration(flinkConfig));

        List<Deployment> deployments =
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .list()
                        .getItems();
        String expectedJMDeploymentName = TestUtils.CLUSTER_ID;
        String expectedTMDeploymentName = TestUtils.CLUSTER_ID + "-taskmanager";

        assertEquals(2, deployments.size());
        assertThat(
                deployments.stream()
                        .map(d -> d.getMetadata().getName())
                        .collect(Collectors.toList()),
                containsInAnyOrder(expectedJMDeploymentName, expectedTMDeploymentName));
        assertEquals(
                flinkConfig.get(BlobServerOptions.PORT),
                String.valueOf(Constants.BLOB_SERVER_PORT));
        assertEquals(
                flinkConfig.get(TaskManagerOptions.RPC_PORT),
                String.valueOf(Constants.TASK_MANAGER_RPC_PORT));
        assertEquals(flinkConfig.get(RestOptions.BIND_PORT), String.valueOf(Constants.REST_PORT));

        Deployment jmDeployment =
                deployments.stream()
                        .filter(d -> d.getMetadata().getName().equals(expectedJMDeploymentName))
                        .findFirst()
                        .orElse(null);
        assertTrue(
                jmDeployment.getSpec().getTemplate().getSpec().getContainers().stream()
                        .anyMatch(c -> c.getArgs().contains("standalone-job")));

        var clusterClient = clusterClientProvider.getClusterClient();

        String expectedWebUrl =
                String.format(
                        "http://%s:%d",
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                TestUtils.CLUSTER_ID, TestUtils.TEST_NAMESPACE),
                        Constants.REST_PORT);
        assertEquals(expectedWebUrl, clusterClient.getWebInterfaceURL());
    }
}
