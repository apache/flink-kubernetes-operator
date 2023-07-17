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
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.artifact.ArtifactManager;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.Mode;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
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
    StandaloneFlinkService flinkStandaloneService;
    FlinkConfigManager configManager;
    FlinkDeployment flinkDeployment;

    @BeforeEach
    public void setup() {
        var configuration = new Configuration();
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TestUtils.TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);
        configuration.set(OPERATOR_HEALTH_PROBE_PORT, 80);

        configManager = new FlinkConfigManager(configuration);

        kubernetesClient = mockServer.createClient().inAnyNamespace();
        flinkStandaloneService =
                new StandaloneFlinkService(
                        kubernetesClient,
                        new ArtifactManager(configManager),
                        Executors.newDirectExecutorService(),
                        configManager.getOperatorConfiguration());
        flinkDeployment = TestUtils.buildSessionCluster();
        flinkDeployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(flinkDeployment.getSpec(), flinkDeployment);
        createDeployments(flinkDeployment);
    }

    @Test
    public void testDeleteClusterDeployment() throws Exception {
        List<Deployment> deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(2, deployments.size());

        var requestsBeforeDelete = mockServer.getRequestCount();
        flinkStandaloneService.deleteClusterDeployment(
                flinkDeployment.getMetadata(),
                flinkDeployment.getStatus(),
                new Configuration(),
                false);

        assertEquals(2, mockServer.getRequestCount() - requestsBeforeDelete);
        assertTrue(mockServer.getLastRequest().getPath().contains("taskmanager"));

        deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(0, deployments.size());
    }

    @Test
    public void testDeleteClusterDeploymentWithHADelete() {
        var service = new TestingStandaloneFlinkService(flinkStandaloneService);
        createDeployments(flinkDeployment);

        List<Deployment> deployments = kubernetesClient.apps().deployments().list().getItems();
        assertEquals(2, deployments.size());

        service.deleteClusterDeployment(
                flinkDeployment.getMetadata(),
                flinkDeployment.getStatus(),
                new Configuration(),
                true);

        assertEquals(2, service.nbCall);

        deployments = kubernetesClient.apps().deployments().list().getItems();

        assertEquals(0, deployments.size());
    }

    @Test
    public void testTMReplicaScaleApplication() {
        flinkDeployment.getSpec().setJob(new JobSpec());
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
        flinkDeployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(flinkDeployment.getSpec(), flinkDeployment);
        flinkDeployment.getSpec().getTaskManager().setReplicas(5);
        var ctx =
                new FlinkResourceContextFactory(
                                kubernetesClient,
                                configManager,
                                TestUtils.createTestMetricGroup(new Configuration()),
                                null)
                        .getResourceContext(flinkDeployment, TestUtils.createEmptyContext());
        assertEquals(
                FlinkService.ScalingResult.SCALING_TRIGGERED,
                flinkStandaloneService.scale(ctx, ctx.getDeployConfig(flinkDeployment.getSpec())));
        assertEquals(
                5,
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(namespace)
                        .withName(StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId))
                        .get()
                        .getSpec()
                        .getReplicas());

        flinkDeployment
                .getSpec()
                .getFlinkConfiguration()
                .remove(JobManagerOptions.SCHEDULER_MODE.key());
        flinkDeployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(flinkDeployment.getSpec(), flinkDeployment);
        ctx =
                new FlinkResourceContextFactory(
                                kubernetesClient,
                                configManager,
                                TestUtils.createTestMetricGroup(new Configuration()),
                                null)
                        .getResourceContext(flinkDeployment, TestUtils.createEmptyContext());

        // Add replicas and verify that the scaling is not honoured as reactive mode not enabled
        flinkDeployment.getSpec().getTaskManager().setReplicas(10);
        assertEquals(
                FlinkService.ScalingResult.CANNOT_SCALE,
                flinkStandaloneService.scale(ctx, ctx.getDeployConfig(flinkDeployment.getSpec())));
    }

    @Test
    public void testTMReplicaScaleSession() {
        var clusterId = flinkDeployment.getMetadata().getName();
        var namespace = flinkDeployment.getMetadata().getNamespace();
        flinkDeployment.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);
        // Add replicas
        flinkDeployment.getSpec().getTaskManager().setReplicas(3);
        flinkDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        JobManagerOptions.SCHEDULER_MODE.key(),
                        SchedulerExecutionMode.REACTIVE.name());
        flinkDeployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(flinkDeployment.getSpec(), flinkDeployment);

        var ctx =
                new FlinkResourceContextFactory(
                                kubernetesClient,
                                configManager,
                                TestUtils.createTestMetricGroup(new Configuration()),
                                null)
                        .getResourceContext(flinkDeployment, TestUtils.createEmptyContext());
        assertEquals(
                FlinkService.ScalingResult.SCALING_TRIGGERED,
                flinkStandaloneService.scale(ctx, ctx.getDeployConfig(flinkDeployment.getSpec())));

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
        assertEquals(
                FlinkService.ScalingResult.SCALING_TRIGGERED,
                flinkStandaloneService.scale(ctx, ctx.getDeployConfig(flinkDeployment.getSpec())));

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
        service.submitSessionCluster(
                configManager.getDeployConfig(
                        flinkDeployment.getMetadata(), flinkDeployment.getSpec()));
        assertFalse(service.getRuntimeConfig().containsKey(OPERATOR_HEALTH_PROBE_PORT.key()));
    }

    @Test
    public void testDeployApplicationClusterConfigRemoval() throws Exception {
        TestingStandaloneFlinkService service =
                new TestingStandaloneFlinkService(flinkStandaloneService);
        service.deployApplicationCluster(
                flinkDeployment.getSpec().getJob(),
                configManager.getDeployConfig(
                        flinkDeployment.getMetadata(), flinkDeployment.getSpec()));
        assertFalse(service.getRuntimeConfig().containsKey(OPERATOR_HEALTH_PROBE_PORT.key()));
    }

    class TestingStandaloneFlinkService extends StandaloneFlinkService {
        Configuration runtimeConfig;
        int nbCall = 0;

        public TestingStandaloneFlinkService(StandaloneFlinkService service) {
            super(
                    service.kubernetesClient,
                    service.artifactManager,
                    service.executorService,
                    service.operatorConfig);
        }

        public Configuration getRuntimeConfig() {
            return this.runtimeConfig;
        }

        @Override
        protected void submitClusterInternal(Configuration conf, Mode mode) {
            this.runtimeConfig = conf;
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
