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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.Mode;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link StandaloneFlinkService unit tests */
@EnableKubernetesMockClient(crud = true)
public class StandaloneFlinkServiceTest {
    KubernetesMockServer mockServer;

    private NamespacedKubernetesClient kubernetesClient;

    TestStandaloneFlinkService flinkStandaloneService;
    Configuration configuration = new Configuration();

    class TestStandaloneFlinkService extends StandaloneFlinkService {
        int nbCall = 0;

        public TestStandaloneFlinkService(
                KubernetesClient kubernetesClient, FlinkConfigManager configManager) {
            super(kubernetesClient, configManager);
        }

        @Override
        protected PodList getTmPodList(String namespace, String clusterId) {
            nbCall++;
            PodList podList = new PodList();
            if (nbCall == 1) {
                Pod pod = new Pod();
                podList.setItems(List.of(pod));
            }
            return podList;
        }
    }

    @BeforeEach
    public void setup() {
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TestUtils.TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);
        configuration.set(OPERATOR_HEALTH_PROBE_PORT, 80);

        kubernetesClient = mockServer.createClient().inAnyNamespace();
        flinkStandaloneService =
                new TestStandaloneFlinkService(
                        kubernetesClient, new FlinkConfigManager(configuration));
    }

    @Test
    public void testDeleteClusterDeployment() throws Exception {
        FlinkDeployment flinkDeployment = TestUtils.buildSessionCluster();
        configuration = buildConfig(flinkDeployment, configuration);

        createDeployments(flinkDeployment);

        List<Deployment> deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(2, deployments.size());

        var requestsBeforeDelete = mockServer.getRequestCount();
        flinkStandaloneService.deleteClusterDeployment(
                flinkDeployment.getMetadata(), flinkDeployment.getStatus(), configuration, false);

        assertEquals(2, mockServer.getRequestCount() - requestsBeforeDelete);
        assertTrue(mockServer.getLastRequest().getPath().contains("taskmanager"));

        deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(0, deployments.size());
    }

    @Test
    public void testDeleteClusterDeploymentWithHADelete() throws Exception {
        FlinkDeployment flinkDeployment = TestUtils.buildSessionCluster();
        configuration = buildConfig(flinkDeployment, configuration);

        createDeployments(flinkDeployment);

        List<Deployment> deployments = kubernetesClient.apps().deployments().list().getItems();
        assertEquals(2, deployments.size());

        flinkStandaloneService.deleteClusterDeployment(
                flinkDeployment.getMetadata(), flinkDeployment.getStatus(), configuration, true);

        assertEquals(2, flinkStandaloneService.nbCall);

        deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(0, deployments.size());
    }

    @Test
    public void testTMReplicaScaleApplication() throws Exception {
        var flinkDeployment = TestUtils.buildApplicationCluster();
        var clusterId = flinkDeployment.getMetadata().getName();
        var namespace = flinkDeployment.getMetadata().getNamespace();
        flinkDeployment.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);

        // Add parallelism change, verify it is honoured in reactive mode
        flinkDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        JobManagerOptions.SCHEDULER_MODE.key(),
                        SchedulerExecutionMode.REACTIVE.name());
        flinkDeployment.getSpec().getJob().setParallelism(4);
        createDeployments(flinkDeployment);
        assertTrue(
                flinkStandaloneService.scale(
                        flinkDeployment.getMetadata(),
                        flinkDeployment.getSpec().getJob(),
                        buildConfig(flinkDeployment, configuration)));
        assertEquals(
                2,
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(namespace)
                        .withName(StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId))
                        .get()
                        .getSpec()
                        .getReplicas());

        // Add parallelism and replica change, verify if replica change is honoured in reactive mode
        flinkDeployment.getSpec().getJob().setParallelism(100);
        flinkDeployment.getSpec().getTaskManager().setReplicas(2);
        assertTrue(
                flinkStandaloneService.scale(
                        flinkDeployment.getMetadata(),
                        flinkDeployment.getSpec().getJob(),
                        buildConfig(flinkDeployment, configuration)));
        assertEquals(
                2,
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(namespace)
                        .withName(StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId))
                        .get()
                        .getSpec()
                        .getReplicas());

        // Verify that any change in parallelism doesnt scale the cluster without reactive mode
        flinkDeployment.getSpec().getJob().setParallelism(100);
        flinkDeployment
                .getSpec()
                .getFlinkConfiguration()
                .remove(JobManagerOptions.SCHEDULER_MODE.key());
        assertFalse(
                flinkStandaloneService.scale(
                        flinkDeployment.getMetadata(),
                        flinkDeployment.getSpec().getJob(),
                        buildConfig(flinkDeployment, configuration)));

        // Add replicas and verify that the scaling is not honoured as reactive mode not enabled
        flinkDeployment.getSpec().getTaskManager().setReplicas(10);
        assertFalse(
                flinkStandaloneService.scale(
                        flinkDeployment.getMetadata(),
                        flinkDeployment.getSpec().getJob(),
                        buildConfig(flinkDeployment, configuration)));
    }

    @Test
    public void testTMReplicaScaleSession() throws Exception {
        var flinkDeployment = TestUtils.buildSessionCluster();
        var clusterId = flinkDeployment.getMetadata().getName();
        var namespace = flinkDeployment.getMetadata().getNamespace();
        flinkDeployment.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);
        // Add replicas
        flinkDeployment.getSpec().getTaskManager().setReplicas(3);
        createDeployments(flinkDeployment);
        assertTrue(
                flinkStandaloneService.scale(
                        flinkDeployment.getMetadata(),
                        flinkDeployment.getSpec().getJob(),
                        buildConfig(flinkDeployment, configuration)));

        assertEquals(
                3,
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(namespace)
                        .withName(StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId))
                        .get()
                        .getSpec()
                        .getReplicas());

        // Scale the replica count of the task managers
        flinkDeployment.getSpec().getTaskManager().setReplicas(10);
        createDeployments(flinkDeployment);
        assertTrue(
                flinkStandaloneService.scale(
                        flinkDeployment.getMetadata(),
                        flinkDeployment.getSpec().getJob(),
                        buildConfig(flinkDeployment, configuration)));

        assertEquals(
                10,
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(namespace)
                        .withName(StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId))
                        .get()
                        .getSpec()
                        .getReplicas());
    }

    @Test
    public void testSubmitSessionClusterConfigRemoval() throws Exception {
        TestingStandaloneFlinkService service =
                new TestingStandaloneFlinkService(flinkStandaloneService);
        service.submitSessionCluster(configuration);
        assertFalse(service.getRuntimeConfig().containsKey(OPERATOR_HEALTH_PROBE_PORT.key()));
    }

    @Test
    public void testDeployApplicationClusterConfigRemoval() throws Exception {
        var flinkDeployment = TestUtils.buildApplicationCluster();
        TestingStandaloneFlinkService service =
                new TestingStandaloneFlinkService(flinkStandaloneService);
        service.deployApplicationCluster(flinkDeployment.getSpec().getJob(), configuration);
        assertFalse(service.getRuntimeConfig().containsKey(OPERATOR_HEALTH_PROBE_PORT.key()));
    }

    class TestingStandaloneFlinkService extends StandaloneFlinkService {
        private Configuration runtimeConfig;

        public TestingStandaloneFlinkService(StandaloneFlinkService service) {
            super(service.kubernetesClient, service.configManager);
        }

        public Configuration getRuntimeConfig() {
            return this.runtimeConfig;
        }

        @Override
        protected void submitClusterInternal(Configuration conf, Mode mode) {
            this.runtimeConfig = conf;
        }
    }

    private Configuration buildConfig(FlinkDeployment flinkDeployment, Configuration configuration)
            throws Exception {
        return FlinkConfigBuilder.buildFrom(
                flinkDeployment.getMetadata().getNamespace(),
                flinkDeployment.getMetadata().getName(),
                flinkDeployment.getSpec(),
                configuration);
    }

    private void createDeployments(AbstractFlinkResource cr) {
        Deployment jmDeployment = new Deployment();
        ObjectMeta jmMetadata = new ObjectMeta();
        jmMetadata.setName(
                StandaloneKubernetesUtils.getJobManagerDeploymentName(cr.getMetadata().getName()));
        jmDeployment.setMetadata(jmMetadata);
        kubernetesClient
                .resource(jmDeployment)
                .inNamespace(cr.getMetadata().getNamespace())
                .createOrReplace();

        Deployment tmDeployment = new Deployment();
        ObjectMeta tmMetadata = new ObjectMeta();
        tmMetadata.setName(
                StandaloneKubernetesUtils.getTaskManagerDeploymentName(cr.getMetadata().getName()));
        tmDeployment.setMetadata(tmMetadata);
        tmDeployment.setSpec(new DeploymentSpec());
        kubernetesClient
                .resource(tmDeployment)
                .inNamespace(cr.getMetadata().getNamespace())
                .createOrReplace();
    }
}
