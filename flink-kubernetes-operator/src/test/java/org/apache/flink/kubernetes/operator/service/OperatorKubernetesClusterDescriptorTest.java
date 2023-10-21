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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.kubeclient.Fabric8FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.operator.kubeclient.FlinkStandaloneKubeClient;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.Deployment;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.Config;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.ConfigBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_DEPLOYMENT_NAME;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.TEST_NAMESPACE;
import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link KubernetesStandaloneClusterDescriptor unit tests */
@EnableKubernetesMockClient(crud = true, https = false)
public class OperatorKubernetesClusterDescriptorTest {
    KubernetesMockServer mockWebServer;
    private OperatorKubernetesClusterDescriptor clusterDescriptor;
    private FlinkStandaloneKubeClient flinkKubeClient;

    KubernetesClient client;
    private Configuration flinkConfig = new Configuration();

    @BeforeEach
    public void setup() {
        flinkConfig = createTestFlinkConfig();
        flinkKubeClient =
                new Fabric8FlinkStandaloneKubeClient(
                        flinkConfig, getClient(), Executors.newDirectExecutorService());
        clusterDescriptor =
                new OperatorKubernetesClusterDescriptor(flinkConfig, flinkKubeClient, null);
    }

    @Test
    public void testDeploySessionCluster() throws Exception {
        var cluster = TestUtils.buildSessionCluster();

        flinkConfig.setString(BlobServerOptions.PORT, String.valueOf(0));
        flinkConfig.setString(TaskManagerOptions.RPC_PORT, String.valueOf(0));
        flinkConfig.setString(RestOptions.BIND_PORT, String.valueOf(0));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        var clusterSpecification =
                new KubernetesClusterClientFactory().getClusterSpecification(flinkConfig);

        var clusterClientProvider = clusterDescriptor.deploySessionCluster(clusterSpecification);

        List<Deployment> deployments =
                getClient().apps().deployments().inNamespace(TEST_NAMESPACE).list().getItems();
        String expectedJMDeploymentName = TEST_DEPLOYMENT_NAME;

        assertEquals(1, deployments.size());
        assertThat(
                deployments.stream()
                        .map(d -> d.getMetadata().getName())
                        .collect(Collectors.toList()),
                containsInAnyOrder(expectedJMDeploymentName));
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
                        .anyMatch(
                                c ->
                                        c.getArgs()
                                                .contains(
                                                        "kubernetes-jobmanager.sh kubernetes-session ")));

        var clusterClient = clusterClientProvider.getClusterClient();

        String expectedWebUrl =
                String.format(
                        "http://%s:%d",
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                TEST_DEPLOYMENT_NAME, TEST_NAMESPACE),
                        Constants.REST_PORT);
        assertEquals(expectedWebUrl, clusterClient.getWebInterfaceURL());
    }

    @Test
    public void testDeployApplicationCluster() throws Exception {
        ClusterSpecification clusterSpecification =
                new ClusterSpecification.ClusterSpecificationBuilder()
                        .setMasterMemoryMB(2048)
                        .setTaskManagerMemoryMB(1024)
                        .setSlotsPerTaskManager(2)
                        .createClusterSpecification();

        flinkConfig.setString(BlobServerOptions.PORT, String.valueOf(0));
        flinkConfig.setString(TaskManagerOptions.RPC_PORT, String.valueOf(0));
        flinkConfig.setString(RestOptions.BIND_PORT, String.valueOf(0));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///path/of/user.jar"));

        var clusterClientProvider =
                clusterDescriptor.deployApplicationCluster(
                        clusterSpecification,
                        ApplicationConfiguration.fromConfiguration(flinkConfig));

        List<Deployment> deployments =
                getClient().apps().deployments().inNamespace(TEST_NAMESPACE).list().getItems();
        String expectedJMDeploymentName = TEST_DEPLOYMENT_NAME;

        assertEquals(1, deployments.size());
        assertThat(
                deployments.stream()
                        .map(d -> d.getMetadata().getName())
                        .collect(Collectors.toList()),
                containsInAnyOrder(expectedJMDeploymentName));
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
                        .anyMatch(
                                c ->
                                        c.getArgs()
                                                .contains(
                                                        "kubernetes-jobmanager.sh kubernetes-application ")));

        var clusterClient = clusterClientProvider.getClusterClient();

        String expectedWebUrl =
                String.format(
                        "http://%s:%d",
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                TEST_DEPLOYMENT_NAME, TEST_NAMESPACE),
                        Constants.REST_PORT);
        assertEquals(expectedWebUrl, clusterClient.getWebInterfaceURL());
    }

    private NamespacedKubernetesClient getClient() {
        var config =
                new ConfigBuilder(Config.empty())
                        .withMasterUrl(mockWebServer.url("/").toString())
                        .withHttp2Disable(true)
                        .build();
        return new DefaultKubernetesClient(config).inNamespace(TEST_NAMESPACE);
    }

    public static Configuration createTestFlinkConfig() {
        Configuration configuration = new Configuration();
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);
        configuration.set(FLINK_VERSION, FlinkVersion.v1_18);
        configuration.set(KubernetesConfigOptions.JOB_MANAGER_CPU, 2.0);
        configuration.set(KubernetesConfigOptions.TASK_MANAGER_CPU, 4.0);
        configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1000));
        configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1000));
        return configuration;
    }
}
