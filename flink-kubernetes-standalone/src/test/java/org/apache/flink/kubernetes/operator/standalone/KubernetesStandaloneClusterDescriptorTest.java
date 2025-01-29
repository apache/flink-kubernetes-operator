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
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.kubeclient.Fabric8FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVar;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.Deployment;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.Config;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.ConfigBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils.JM_ENV_VALUE;
import static org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils.TEST_NAMESPACE;
import static org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils.TM_ENV_VALUE;
import static org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils.USER_ENV_VAR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @link KubernetesStandaloneClusterDescriptor unit tests
 */
@EnableKubernetesMockClient(crud = true, https = false)
public class KubernetesStandaloneClusterDescriptorTest {
    KubernetesMockServer mockWebServer;
    private KubernetesStandaloneClusterDescriptor clusterDescriptor;
    private FlinkStandaloneKubeClient flinkKubeClient;
    private Configuration flinkConfig = new Configuration();

    @BeforeEach
    public void setup() {
        flinkConfig = TestUtils.createTestFlinkConfig();
        flinkKubeClient =
                new Fabric8FlinkStandaloneKubeClient(
                        flinkConfig, getClient(), Executors.newSingleThreadScheduledExecutor());

        clusterDescriptor = new KubernetesStandaloneClusterDescriptor(flinkConfig, flinkKubeClient);
    }

    @Test
    public void testDeploySessionCluster() throws Exception {
        ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();

        flinkConfig.setString(BlobServerOptions.PORT, String.valueOf(0));
        flinkConfig.setString(TaskManagerOptions.RPC_PORT, String.valueOf(0));
        flinkConfig.setString(RestOptions.BIND_PORT, String.valueOf(0));
        flinkConfig.setString(
                ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + USER_ENV_VAR,
                JM_ENV_VALUE);
        flinkConfig.setString(
                ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX + USER_ENV_VAR,
                TM_ENV_VALUE);

        var clusterClientProvider = clusterDescriptor.deploySessionCluster(clusterSpecification);

        List<Deployment> deployments =
                getClient()
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
        List<EnvVar> envVars =
                jmDeployment.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertTrue(envVars.contains(new EnvVar(USER_ENV_VAR, JM_ENV_VALUE, null)));

        Deployment tmDeployment =
                deployments.stream()
                        .filter(d -> d.getMetadata().getName().equals(expectedTMDeploymentName))
                        .findFirst()
                        .orElse(null);
        envVars = tmDeployment.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertTrue(envVars.contains(new EnvVar(USER_ENV_VAR, TM_ENV_VALUE, null)));

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
                getClient()
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

    @Test
    public void testMainContainerArgsIntegrity() throws Exception {
        ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();

        clusterDescriptor.deployApplicationCluster(
                clusterSpecification,
                new ApplicationConfiguration(new String[] {"--test", "123"}, "test"));
        List<Deployment> deployments =
                getClient()
                        .apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .list()
                        .getItems();
        String expectedJMDeploymentName = TestUtils.CLUSTER_ID;

        Deployment jmDeployment =
                deployments.stream()
                        .filter(d -> d.getMetadata().getName().equals(expectedJMDeploymentName))
                        .findFirst()
                        .orElse(null);
        assertTrue(
                jmDeployment.getSpec().getTemplate().getSpec().getContainers().stream()
                        .anyMatch(c -> c.getArgs().contains("standalone-job")));

        assertTrue(
                jmDeployment.getSpec().getTemplate().getSpec().getContainers().stream()
                        .anyMatch(c -> c.getArgs().contains("123")));
    }

    private NamespacedKubernetesClient getClient() {
        var config =
                new ConfigBuilder(Config.empty())
                        .withMasterUrl(mockWebServer.url("/").toString())
                        .withHttp2Disable(true)
                        .build();
        return new DefaultKubernetesClient(config).inNamespace(TEST_NAMESPACE);
    }
}
