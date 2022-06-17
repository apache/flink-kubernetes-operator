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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/** @link StandaloneFlinkService unit tests */
@EnableKubernetesMockClient(crud = true)
public class StandaloneFlinkServiceTest {
    KubernetesMockServer mockServer;

    private NamespacedKubernetesClient kubernetesClient;
    StandaloneFlinkService flinkStandaloneService;
    Configuration configuration = new Configuration();

    @BeforeEach
    public void setup() {
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TestUtils.TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);

        kubernetesClient = mockServer.createClient().inAnyNamespace();
        flinkStandaloneService =
                new StandaloneFlinkService(kubernetesClient, new FlinkConfigManager(configuration));

        ExecutorService executorService =
                Executors.newFixedThreadPool(
                        1, new ExecutorThreadFactory("flink-kubeclient-io-for-standalone-service"));
    }

    @Test
    public void testDeleteClusterDeployment() throws Exception {
        FlinkDeployment flinkDeployment = TestUtils.buildSessionCluster();
        configuration = buildConfig(flinkDeployment, configuration);

        createDeployments();

        List<Deployment> deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(2, deployments.size());

        flinkStandaloneService.deleteClusterDeployment(
                flinkDeployment.getMetadata(), flinkDeployment.getStatus(), false);

        deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(0, deployments.size());
    }

    @Test
    public void testDeleteClusterDeploymentWithHADelete() throws Exception {
        FlinkDeployment flinkDeployment = TestUtils.buildSessionCluster();
        configuration = buildConfig(flinkDeployment, configuration);

        createDeployments();

        List<Deployment> deployments = kubernetesClient.apps().deployments().list().getItems();
        assertEquals(2, deployments.size());

        flinkStandaloneService.deleteClusterDeployment(
                flinkDeployment.getMetadata(), flinkDeployment.getStatus(), true);

        deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(0, deployments.size());
    }

    private Configuration buildConfig(FlinkDeployment flinkDeployment, Configuration configuration)
            throws Exception {
        return configuration =
                FlinkConfigBuilder.buildFrom(
                        flinkDeployment.getMetadata().getNamespace(),
                        flinkDeployment.getMetadata().getName(),
                        flinkDeployment.getSpec(),
                        configuration);
    }

    private void createDeployments() {
        Deployment jmDeployment = new Deployment();
        ObjectMeta jmMetadata = new ObjectMeta();
        jmMetadata.setName(
                StandaloneKubernetesUtils.getJobManagerDeploymentName(
                        TestUtils.TEST_DEPLOYMENT_NAME));
        jmDeployment.setMetadata(jmMetadata);
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(TestUtils.TEST_NAMESPACE)
                .create(jmDeployment);

        Deployment tmDeployment = new Deployment();
        ObjectMeta tmMetadata = new ObjectMeta();
        tmMetadata.setName(
                StandaloneKubernetesUtils.getTaskManagerDeploymentName(
                        TestUtils.TEST_DEPLOYMENT_NAME));
        tmDeployment.setMetadata(tmMetadata);
        kubernetesClient
                .apps()
                .deployments()
                .inNamespace(TestUtils.TEST_NAMESPACE)
                .create(tmDeployment);
    }
}
