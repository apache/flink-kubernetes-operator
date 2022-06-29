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

package org.apache.flink.kubernetes.operator.kubeclient;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.operator.kubeclient.factory.StandaloneKubernetesJobManagerFactory;
import org.apache.flink.kubernetes.operator.kubeclient.factory.StandaloneKubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesJobManagerParameters;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** @link Fabric8FlinkStandaloneKubeClient unit tests */
@EnableKubernetesMockClient(crud = true)
public class Fabric8FlinkStandaloneKubeClientTest {
    private static final String NAMESPACE = "test";

    KubernetesMockServer mockServer;
    protected NamespacedKubernetesClient kubernetesClient;
    private FlinkStandaloneKubeClient flinkKubeClient;
    private StandaloneKubernetesTaskManagerParameters taskManagerParameters;
    private Deployment tmDeployment;
    private ClusterSpecification clusterSpecification;
    private Configuration flinkConfig = new Configuration();

    @BeforeEach
    public final void setup() {
        flinkConfig = TestUtils.createTestFlinkConfig();
        kubernetesClient = mockServer.createClient();

        flinkKubeClient =
                new Fabric8FlinkStandaloneKubeClient(
                        flinkConfig, kubernetesClient, Executors.newDirectExecutorService());
        clusterSpecification = TestUtils.createClusterSpecification();

        taskManagerParameters =
                new StandaloneKubernetesTaskManagerParameters(flinkConfig, clusterSpecification);

        tmDeployment =
                StandaloneKubernetesTaskManagerFactory.buildKubernetesTaskManagerDeployment(
                        new FlinkPod.Builder().build(), taskManagerParameters);
    }

    @Test
    public void testCreateTaskManagerDeployment() {
        flinkKubeClient.createTaskManagerDeployment(tmDeployment);

        final List<Deployment> resultedDeployments =
                kubernetesClient.apps().deployments().inNamespace(NAMESPACE).list().getItems();
        assertEquals(1, resultedDeployments.size());
    }

    @Test
    public void testStopAndCleanupCluster() throws Exception {
        ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();
        StandaloneKubernetesJobManagerParameters jmParameters =
                new StandaloneKubernetesJobManagerParameters(flinkConfig, clusterSpecification);
        KubernetesJobManagerSpecification jmSpec =
                StandaloneKubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        new FlinkPod.Builder().build(), jmParameters);

        flinkKubeClient.createJobManagerComponent(jmSpec);
        flinkKubeClient.createTaskManagerDeployment(tmDeployment);

        List<Deployment> resultedDeployments =
                kubernetesClient.apps().deployments().inNamespace(NAMESPACE).list().getItems();
        assertEquals(2, resultedDeployments.size());

        flinkKubeClient.stopAndCleanupCluster(taskManagerParameters.getClusterId());

        resultedDeployments =
                kubernetesClient.apps().deployments().inNamespace(NAMESPACE).list().getItems();
        assertEquals(0, resultedDeployments.size());
    }
}
