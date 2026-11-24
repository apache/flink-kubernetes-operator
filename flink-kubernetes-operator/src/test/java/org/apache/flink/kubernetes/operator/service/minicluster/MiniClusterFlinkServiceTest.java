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

package org.apache.flink.kubernetes.operator.service.minicluster;

import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingClusterClient;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.kubernetes.operator.service.CustomDashboardConfiguration;
import org.apache.flink.kubernetes.operator.service.CustomDashboardConfigurationHeaders;
import org.apache.flink.kubernetes.operator.service.SuspendMode;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.KubernetesClientException;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MiniClusterFlinkService}. */
@EnableKubernetesMockClient(crud = true)
class MiniClusterFlinkServiceTest {

    private KubernetesClient kubernetesClient;

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private FlinkMiniCluster miniCluster;
    private Configuration deployConfig;
    private MiniClusterFlinkService service;

    /** Whether creating the resources fails after the Deployment has been created. */
    private boolean failAfterCreatingDeployment;

    @BeforeEach
    void setup() {
        miniCluster = TestUtils.buildMiniCluster();
        deployConfig = deployConfig();
        failAfterCreatingDeployment = false;
        service =
                new MiniClusterFlinkService(
                        kubernetesClient,
                        null,
                        Executors.newDirectExecutorService(),
                        FlinkOperatorConfiguration.fromConfiguration(new Configuration())) {
                    @Override
                    public RestClusterClient<String> getClusterClient(Configuration conf)
                            throws Exception {
                        var client = new TestingClusterClient<String>(conf);
                        client.setRequestProcessor(
                                (headers, parameters, body) -> {
                                    if (headers instanceof CustomDashboardConfigurationHeaders) {
                                        var dashboard = new CustomDashboardConfiguration();
                                        dashboard.setFlinkVersion("2.4.0");
                                        dashboard.setFlinkRevision("abc");
                                        return CompletableFuture.completedFuture(dashboard);
                                    }
                                    if (headers instanceof TaskManagersHeaders) {
                                        return CompletableFuture.completedFuture(
                                                new TaskManagersInfo(
                                                        List.of(
                                                                taskManagerInfo(),
                                                                taskManagerInfo())));
                                    }
                                    return CompletableFuture.failedFuture(
                                            new AssertionError("Unexpected request " + headers));
                                });
                        return client;
                    }

                    @Override
                    protected FlinkKubeClient createFlinkKubeClient(Configuration conf) {
                        if (!failAfterCreatingDeployment) {
                            return TestingMiniClusterFlinkService.createFlinkKubeClient(
                                    kubernetesClient, conf);
                        }
                        return new Fabric8FlinkKubeClient(
                                conf,
                                TestingMiniClusterFlinkService.createShadedKubernetesClient(
                                        kubernetesClient, conf),
                                java.util.concurrent.Executors.newSingleThreadScheduledExecutor()) {
                            @Override
                            public void createJobManagerComponent(
                                    KubernetesJobManagerSpecification spec) {
                                super.createJobManagerComponent(spec);
                                throw new KubernetesClientException(
                                        "Failed after creating the Deployment");
                            }
                        };
                    }
                };
    }

    /** The deploy config of the MiniCluster, naming it as the owner as the reconciler does. */
    private Configuration deployConfig() {
        var conf = configManager.getDeployConfig(miniCluster.getMetadata(), miniCluster.getSpec());
        conf.set(
                KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE,
                List.of(
                        Map.of(
                                "apiVersion", miniCluster.getApiVersion(),
                                "kind", miniCluster.getKind(),
                                "name", name(),
                                "uid", miniCluster.getMetadata().getUid())));
        return conf;
    }

    @Test
    void testDeployAndDelete() throws Exception {
        enableKubernetesHa();
        service.submitMiniCluster(miniCluster, miniCluster.getSpec(), deployConfig, false);
        createHaConfigMap();

        assertThat(deployment()).isNotNull();
        assertThat(
                        kubernetesClient
                                .services()
                                .inNamespace(namespace())
                                .withName(name() + "-rest")
                                .get())
                .isNotNull();
        assertThat(
                        kubernetesClient
                                .configMaps()
                                .inNamespace(namespace())
                                .withName("flink-config-" + name())
                                .get())
                .isNotNull();

        var status = miniCluster.getStatus();
        status.setPodDeploymentStatus(JobManagerDeploymentStatus.READY);
        service.deleteMiniCluster(miniCluster.getMetadata(), status, deployConfig, true);

        // The Service and ConfigMap are owned by the Deployment (see testDeployOwnership), so
        // Kubernetes deletes them with it
        assertThat(deployment()).isNull();
        assertThat(haConfigMap()).isNull();
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.MISSING);
    }

    @Test
    void testDeleteLeavesDeploymentOwnedByAnotherResource() {
        // e.g. the JobManager Deployment of a FlinkDeployment with the same name, which the
        // MiniCluster failed to deploy over
        enableKubernetesHa();
        kubernetesClient
                .apps()
                .deployments()
                .resource(
                        new DeploymentBuilder()
                                .withNewMetadata()
                                .withName(name())
                                .withNamespace(namespace())
                                .addNewOwnerReference()
                                .withApiVersion("flink.apache.org/v1beta1")
                                .withKind("FlinkDeployment")
                                .withName(name())
                                .withUid("flinkdeployment-uid")
                                .endOwnerReference()
                                .endMetadata()
                                .build())
                .create();
        createHaConfigMap();

        var status = miniCluster.getStatus();
        service.deleteMiniCluster(miniCluster.getMetadata(), status, deployConfig, true);

        assertThat(deployment()).isNotNull();
        assertThat(haConfigMap()).isNotNull();
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.MISSING);
    }

    @Test
    void testDeleteRemovesHaMetadataOfMissingDeployment() {
        enableKubernetesHa();
        createHaConfigMap();

        service.deleteMiniCluster(
                miniCluster.getMetadata(), miniCluster.getStatus(), deployConfig, true);

        assertThat(haConfigMap()).isNull();
    }

    @Test
    void testDeleteForceDeletesStuckPod() {
        var service = serviceWithShortCleanupTimeout();
        createTerminatingDeployment();
        createStuckPod();

        service.deleteMiniCluster(
                miniCluster.getMetadata(), miniCluster.getStatus(), deployConfig, false);

        assertThat(deployment()).isNull();
        assertThat(forceDeletedEvents())
                .singleElement()
                .satisfies(
                        event -> assertThat(event.getInvolvedObject().getName()).isEqualTo(name()));
    }

    @Test
    void testDeleteDoesNotForceDeleteStuckPodWhenDisabled() {
        var service = serviceWithShortCleanupTimeout();
        deployConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_RESOURCE_CLEANUP_FORCE_DELETE_STUCK_PODS,
                false);
        createTerminatingDeployment();
        createStuckPod();

        assertThatThrownBy(
                        () ->
                                service.deleteMiniCluster(
                                        miniCluster.getMetadata(),
                                        miniCluster.getStatus(),
                                        deployConfig,
                                        false))
                .isInstanceOf(KubernetesClientTimeoutException.class);
        assertThat(deployment()).isNotNull();
        assertThat(forceDeletedEvents()).isEmpty();
    }

    @Test
    void testHaCleanupLeavesMiniClusterConfigMap() throws Exception {
        // Flink labels its HA ConfigMaps as for a native cluster, and the MiniCluster labels its
        // own ConfigMap with a different type, so the HA cleanup only deletes the HA metadata
        service.submitMiniCluster(miniCluster, miniCluster.getSpec(), deployConfig, false);
        createHaConfigMap();

        FlinkUtils.deleteKubernetesHAMetadata(name(), namespace(), kubernetesClient);

        assertThat(haConfigMap()).isNull();
        assertThat(configMap()).isNotNull();
    }

    @Test
    void testFailedDeployDeletesItsDeployment() throws Exception {
        failAfterCreatingDeployment = true;

        assertThatThrownBy(
                        () ->
                                service.submitMiniCluster(
                                        miniCluster, miniCluster.getSpec(), deployConfig, false))
                .isInstanceOf(KubernetesClientException.class)
                .hasMessageContaining("Failed after creating the Deployment");
        assertThat(deployment()).isNull();
        // Kubernetes deletes the Service and ConfigMap with the Deployment, but in the background,
        // so a retry may find them still there (as it does here, without garbage collection)
        assertThat(service()).isNotNull();
        assertThat(configMap()).isNotNull();

        // Without the Deployment left behind, a retry can deploy the MiniCluster, replacing the
        // Service and ConfigMap
        failAfterCreatingDeployment = false;
        service.submitMiniCluster(miniCluster, miniCluster.getSpec(), deployConfig, false);
        assertThat(deployment()).isNotNull();
    }

    @Test
    void testDeployOwnership() throws Exception {
        service.submitMiniCluster(miniCluster, miniCluster.getSpec(), deployConfig, false);

        // Deleting the MiniCluster resource deletes the Deployment, which deletes the rest
        var deployment = deployment();
        assertThat(deployment.getMetadata().getOwnerReferences())
                .singleElement()
                .satisfies(
                        ref ->
                                assertThat(ref.getUid())
                                        .isEqualTo(miniCluster.getMetadata().getUid()));
        var deploymentUid = deployment.getMetadata().getUid();
        assertThat(deploymentUid).isNotNull();
        assertThat(
                        List.<HasMetadata>of(
                                kubernetesClient
                                        .services()
                                        .inNamespace(namespace())
                                        .withName(name() + "-rest")
                                        .get(),
                                kubernetesClient
                                        .configMaps()
                                        .inNamespace(namespace())
                                        .withName("flink-config-" + name())
                                        .get()))
                .allSatisfy(
                        resource ->
                                assertThat(resource.getMetadata().getOwnerReferences())
                                        .singleElement()
                                        .satisfies(
                                                ref ->
                                                        assertThat(ref.getUid())
                                                                .isEqualTo(deploymentUid)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeployStartupProbe(boolean startupProbeEnabled) throws Exception {
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        KubernetesOperatorConfigOptions.OPERATOR_JM_STARTUP_PROBE_ENABLED.key(),
                        String.valueOf(startupProbeEnabled));
        deployConfig = deployConfig();

        service.submitMiniCluster(miniCluster, miniCluster.getSpec(), deployConfig, false);

        var mainContainer =
                deployment().getSpec().getTemplate().getSpec().getContainers().stream()
                        .filter(c -> Constants.MAIN_CONTAINER_NAME.equals(c.getName()))
                        .findFirst()
                        .orElseThrow();
        assertThat(mainContainer.getStartupProbe() != null)
                .as("startup probe added to the MiniCluster container")
                .isEqualTo(startupProbeEnabled);
    }

    @Test
    void testDeployFailsOverExistingDeployment() throws Exception {
        // An existing Deployment is not replaced, as for the JobManager of a FlinkDeployment
        kubernetesClient
                .apps()
                .deployments()
                .resource(
                        new DeploymentBuilder()
                                .withNewMetadata()
                                .withName(name())
                                .withNamespace(namespace())
                                .addToLabels("existing", "true")
                                .endMetadata()
                                .build())
                .create();

        assertThatThrownBy(
                        () ->
                                service.submitMiniCluster(
                                        miniCluster, miniCluster.getSpec(), deployConfig, false))
                .isInstanceOfSatisfying(
                        KubernetesClientException.class,
                        e -> assertThat(e.getCode()).isEqualTo(409));
        assertThat(deployment().getMetadata().getLabels()).containsEntry("existing", "true");
    }

    @Test
    void testPodList() {
        kubernetesClient
                .pods()
                .resource(
                        new PodBuilder()
                                .withNewMetadata()
                                .withName("minicluster-pod")
                                .withNamespace(namespace())
                                .withLabels(MiniClusterPodFactory.getSelectorLabels(name()))
                                .endMetadata()
                                .build())
                .create();
        kubernetesClient
                .pods()
                .resource(
                        new PodBuilder()
                                .withNewMetadata()
                                .withName("other-pod")
                                .withNamespace(namespace())
                                .addToLabels("app", name())
                                .endMetadata()
                                .build())
                .create();

        assertThat(service.getMiniClusterPodList(miniCluster).getItems())
                .extracting(p -> p.getMetadata().getName())
                .containsExactly("minicluster-pod");
    }

    @Test
    void testClusterInfo() throws Exception {
        // A MiniCluster config has none of the JobManager and TaskManager memory settings that the
        // resource usage of a distributed cluster is derived from
        var clusterInfo = service.getClusterInfo(deployConfig, null);

        assertThat(clusterInfo.getInfo())
                .containsEntry(DashboardConfiguration.FIELD_NAME_FLINK_VERSION, "2.4.0")
                .containsEntry(DashboardConfiguration.FIELD_NAME_FLINK_REVISION, "abc")
                .doesNotContainKeys(
                        AbstractFlinkService.FIELD_NAME_TOTAL_CPU,
                        AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY);
        assertThat(clusterInfo.getTaskManagerReplicas()).isEqualTo(2);
    }

    private static TaskManagerInfo taskManagerInfo() {
        return new TaskManagerInfo(
                ResourceID.generate(),
                "",
                0,
                0,
                0L,
                0,
                0,
                ResourceProfile.UNKNOWN,
                ResourceProfile.UNKNOWN,
                new HardwareDescription(1, 0L, 0L, 0L),
                new TaskExecutorMemoryConfiguration(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
                null);
    }

    @Test
    void testUnsupportedOperations() throws Exception {
        assertThat(service.scale(null, deployConfig)).isFalse();
        assertThatThrownBy(() -> service.submitSessionCluster(deployConfig))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                service.submitApplicationCluster(
                                        miniCluster.getSpec().getJob(), deployConfig, false))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                service.cancelJob(
                                        TestUtils.buildApplicationCluster(),
                                        SuspendMode.STATELESS,
                                        deployConfig))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                service.cancelSessionJob(
                                        TestUtils.buildSessionJob(),
                                        SuspendMode.STATELESS,
                                        deployConfig))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * A service that waits only briefly for the MiniCluster Deployment to be deleted. Once a stuck
     * pod is force deleted, the Deployment it was holding up is deleted, as Kubernetes does.
     */
    private MiniClusterFlinkService serviceWithShortCleanupTimeout() {
        var operatorConf = new Configuration();
        operatorConf.set(
                KubernetesOperatorConfigOptions.OPERATOR_RESOURCE_CLEANUP_TIMEOUT,
                Duration.ofMillis(500));
        return new MiniClusterFlinkService(
                kubernetesClient,
                null,
                Executors.newDirectExecutorService(),
                FlinkOperatorConfiguration.fromConfiguration(operatorConf)) {
            @Override
            protected void forceDeleteStuckPods(Map<String, String> labels, Deployment deployment) {
                super.forceDeleteStuckPods(labels, deployment);
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(namespace())
                        .withName(name())
                        .edit(
                                d -> {
                                    d.getMetadata().setFinalizers(List.of());
                                    return d;
                                });
            }
        };
    }

    /**
     * Creates a MiniCluster Deployment whose deletion waits for its pod to go, as with foreground
     * deletion.
     */
    private void createTerminatingDeployment() {
        kubernetesClient
                .apps()
                .deployments()
                .resource(
                        new DeploymentBuilder()
                                .withNewMetadata()
                                .withName(name())
                                .withNamespace(namespace())
                                .withFinalizers("test.io/wait-for-pod")
                                .addNewOwnerReference()
                                .withApiVersion(miniCluster.getApiVersion())
                                .withKind(miniCluster.getKind())
                                .withName(name())
                                .withUid(miniCluster.getMetadata().getUid())
                                .endOwnerReference()
                                .endMetadata()
                                .build())
                .create();
    }

    /**
     * Creates a MiniCluster pod stuck Terminating past its deletion deadline, as a pod on an
     * unreachable node is. The finalizer keeps it Terminating.
     */
    private void createStuckPod() {
        var pod =
                new PodBuilder()
                        .withNewMetadata()
                        .withName("minicluster-pod")
                        .withNamespace(namespace())
                        .withLabels(MiniClusterPodFactory.getSelectorLabels(name()))
                        .withFinalizers("test.io/block")
                        .endMetadata()
                        .build();
        kubernetesClient.resource(pod).create();
        kubernetesClient.resource(pod).delete();
    }

    private List<Event> forceDeletedEvents() {
        return kubernetesClient.v1().events().inNamespace(namespace()).list().getItems().stream()
                .filter(event -> "ForceDeleted".equals(event.getReason()))
                .collect(Collectors.toList());
    }

    private void enableKubernetesHa() {
        deployConfig.set(HighAvailabilityOptions.HA_MODE, "kubernetes");
    }

    /** Creates a ConfigMap of Kubernetes HA metadata named after the MiniCluster. */
    private void createHaConfigMap() {
        kubernetesClient
                .configMaps()
                .resource(
                        new ConfigMapBuilder()
                                .withNewMetadata()
                                .withName(name() + "-cluster-config-map")
                                .withNamespace(namespace())
                                .withLabels(KubernetesUtils.getCommonLabels(name()))
                                .addToLabels(
                                        Constants.LABEL_CONFIGMAP_TYPE_KEY,
                                        Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY)
                                .endMetadata()
                                .build())
                .create();
    }

    private io.fabric8.kubernetes.api.model.Service service() {
        return kubernetesClient
                .services()
                .inNamespace(namespace())
                .withName(name() + "-rest")
                .get();
    }

    private ConfigMap configMap() {
        return kubernetesClient
                .configMaps()
                .inNamespace(namespace())
                .withName("flink-config-" + name())
                .get();
    }

    private ConfigMap haConfigMap() {
        return kubernetesClient
                .configMaps()
                .inNamespace(namespace())
                .withName(name() + "-cluster-config-map")
                .get();
    }

    private Deployment deployment() {
        return kubernetesClient
                .apps()
                .deployments()
                .inNamespace(namespace())
                .withName(name())
                .get();
    }

    private String name() {
        return miniCluster.getMetadata().getName();
    }

    private String namespace() {
        return miniCluster.getMetadata().getNamespace();
    }
}
