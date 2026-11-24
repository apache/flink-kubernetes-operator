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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.TestingStatusRecorder;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.FlinkMiniClusterStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.health.CanaryResourceManager;
import org.apache.flink.kubernetes.operator.observer.minicluster.MiniClusterObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.MiniClusterReconciler;
import org.apache.flink.kubernetes.operator.service.minicluster.TestingMiniClusterFlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.operator.utils.OperatorPluginUtils;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;

import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link FlinkMiniClusterController} tests. */
@EnableKubernetesMockClient(crud = true)
class FlinkMiniClusterControllerTest {

    private KubernetesClient kubernetesClient;

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private TestingMiniClusterFlinkService flinkService;
    private FlinkResourceEventCollector eventCollector;
    private FlinkMiniClusterController controller;

    /** Whether the MiniCluster pod is reported as available by its Deployment. */
    private boolean podAvailable;

    @BeforeEach
    void setup() {
        flinkService = new TestingMiniClusterFlinkService(kubernetesClient);
        eventCollector = new FlinkResourceEventCollector();
        var eventRecorder =
                new EventRecorder(eventCollector, new FlinkStateSnapshotEventCollector());
        var statusRecorder = new TestingStatusRecorder<FlinkMiniCluster, FlinkMiniClusterStatus>();
        controller =
                new FlinkMiniClusterController(
                        ValidatorUtils.discoverValidators(
                                configManager,
                                OperatorPluginUtils.createPluginManager(
                                        configManager.getDefaultConfig())),
                        new TestingFlinkResourceContextFactory(
                                configManager,
                                TestUtils.createTestMetricGroup(new Configuration()),
                                flinkService.getDelegate(),
                                flinkService,
                                eventRecorder),
                        new MiniClusterReconciler(eventRecorder, statusRecorder),
                        new MiniClusterObserver(eventRecorder),
                        statusRecorder,
                        eventRecorder,
                        new CanaryResourceManager<>(configManager),
                        configManager);
    }

    @Test
    void testReconcileLifecycle() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();

        reconcile(miniCluster);
        assertThat(miniCluster.getStatus().getReconciliationStatus().getState())
                .isEqualTo(ReconciliationState.DEPLOYED);
        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.DEPLOYING);
        assertThat(miniCluster.getStatus().getJobStatus().getState())
                .isEqualTo(JobStatus.RECONCILING);
        assertThat(kubernetesClient.resource(deployment(miniCluster)).get()).isNotNull();

        // pod starting...
        reconcile(miniCluster);
        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.DEPLOYING);

        // pod available (REST port reachable)
        podAvailable = true;
        reconcile(miniCluster);
        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.DEPLOYED_NOT_READY);

        // job running (REST API ready)
        reconcile(miniCluster);
        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.READY);
        assertThat(miniCluster.getStatus().getJobStatus().getState()).isEqualTo(JobStatus.RUNNING);
        assertThat(miniCluster.getStatus().getJobStatus().getJobId()).isNotNull();
        assertThat(miniCluster.getStatus().getReconciliationStatus().getState())
                .isEqualTo(ReconciliationState.DEPLOYED);
        assertThat(miniCluster.getStatus().getReconciliationStatus().isLastReconciledSpecStable())
                .isTrue();
        assertThat(miniCluster.getStatus().getLifecycleState())
                .isEqualTo(ResourceLifecycleState.STABLE);
        assertThat(miniCluster.getStatus().getError()).isNull();
    }

    @Test
    void testRunningCondition() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        assertThat(miniCluster.getStatus().getConditions()).isEmpty();

        reconcile(miniCluster);
        assertThat(miniCluster.getStatus().getConditions())
                .singleElement()
                .satisfies(
                        c -> {
                            assertThat(c.getType()).isEqualTo("Running");
                            assertThat(c.getStatus()).isEqualTo("False");
                            assertThat(c.getReason()).isEqualTo("Reconciling");
                        });

        podAvailable = true;
        reconcile(miniCluster);
        reconcile(miniCluster);
        assertThat(miniCluster.getStatus().getJobStatus().getState()).isEqualTo(JobStatus.RUNNING);
        assertThat(miniCluster.getStatus().getConditions())
                .singleElement()
                .satisfies(
                        c -> {
                            assertThat(c.getType()).isEqualTo("Running");
                            assertThat(c.getStatus()).isEqualTo("True");
                            assertThat(c.getReason()).isEqualTo("Running");
                        });

        var transitionTime = miniCluster.getStatus().getConditions().get(0).getLastTransitionTime();
        Thread.sleep(5);
        reconcile(miniCluster);
        assertThat(miniCluster.getStatus().getConditions())
                .singleElement()
                .satisfies(c -> assertThat(c.getLastTransitionTime()).isEqualTo(transitionTime));
    }

    @Test
    void testValidationError() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster(FlinkVersion.v2_0, JobState.RUNNING);

        reconcile(miniCluster);

        assertThat(flinkService.getDeployCount()).isZero();
        assertThat(miniCluster.getStatus().getError()).contains("requires Flink version");
        assertThat(eventCollector.events)
                .anySatisfy(
                        e ->
                                assertThat(e.getReason())
                                        .isEqualTo(EventRecorder.Reason.ValidationError.name()));
    }

    @Test
    void testContainerError() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        reconcile(miniCluster);
        flinkService.setPodList(podWaitingFor("ErrImagePull"));

        reconcile(miniCluster);

        var status = miniCluster.getStatus();
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.ERROR);
        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.RECONCILING);
        assertThat(status.getError()).contains("ErrImagePull");
        assertThat(eventCollector.events)
                .anySatisfy(e -> assertThat(e.getReason()).isEqualTo("ErrImagePull"));
    }

    @Test
    void testUpgrade() throws Exception {
        var miniCluster = runningMiniCluster();
        var firstJobId = miniCluster.getStatus().getJobStatus().getJobId();

        miniCluster.getSpec().setRestartNonce(1L);
        miniCluster.getMetadata().setGeneration(2L);
        reconcile(miniCluster);
        reconcile(miniCluster);

        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(miniCluster.getStatus().getJobStatus().getJobId()).isNotEqualTo(firstJobId);
        assertThat(
                        deployment(miniCluster)
                                .getMetadata()
                                .getAnnotations()
                                .get("flinkdeployment.flink.apache.org/generation"))
                .isEqualTo("2");
    }

    @Test
    void testIngressIsRemovedWithSpec() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster
                .getSpec()
                .setIngress(IngressSpec.builder().template("{{name}}.example.com").build());
        reconcile(miniCluster);
        podAvailable = true;
        reconcile(miniCluster);
        reconcile(miniCluster);
        assertThat(ingress()).isPresent();

        miniCluster.getSpec().setIngress(null);
        reconcile(miniCluster);
        reconcile(miniCluster);

        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(ingress()).isEmpty();
    }

    @Test
    void testCleanup() throws Exception {
        var miniCluster = runningMiniCluster();

        var deleteControl = controller.cleanup(miniCluster, context(miniCluster));

        assertThat(deleteControl.isRemoveFinalizer()).isTrue();
        assertThat(miniCluster.getStatus().getLifecycleState())
                .isEqualTo(ResourceLifecycleState.DELETED);
        assertThat(kubernetesClient.resource(deploymentFor(miniCluster)).get()).isNull();
        assertThat(eventCollector.events)
                .anySatisfy(
                        e ->
                                assertThat(e.getReason())
                                        .isEqualTo(EventRecorder.Reason.Cleanup.name()));
    }

    private FlinkMiniCluster runningMiniCluster() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        reconcile(miniCluster);
        podAvailable = true;
        reconcile(miniCluster);
        reconcile(miniCluster);
        assertThat(miniCluster.getStatus().getJobStatus().getState()).isEqualTo(JobStatus.RUNNING);
        return miniCluster;
    }

    private void reconcile(FlinkMiniCluster miniCluster) throws Exception {
        var copy = ReconciliationUtils.clone(miniCluster);
        controller.reconcile(copy, context(copy));
        miniCluster.setStatus(copy.getStatus());
    }

    private Context<FlinkMiniCluster> context(FlinkMiniCluster miniCluster) {
        return new TestUtils.TestingContext<>() {
            @Override
            public <R> Optional<R> getSecondaryResource(Class<R> type, String name) {
                if (type == Deployment.class) {
                    return Optional.ofNullable(deployment(miniCluster)).map(type::cast);
                }
                if (HasMetadata.class.isAssignableFrom(type)
                        && type.getSimpleName().equals("Ingress")) {
                    return ingress().map(type::cast);
                }
                return Optional.empty();
            }

            @Override
            public KubernetesClient getClient() {
                return kubernetesClient;
            }
        };
    }

    /** The Deployment of the MiniCluster, with the status a Deployment controller would give it. */
    private Deployment deployment(FlinkMiniCluster miniCluster) {
        var deployment = kubernetesClient.resource(deploymentFor(miniCluster)).get();
        if (deployment != null) {
            deployment.setStatus(
                    new DeploymentStatusBuilder()
                            .withReplicas(1)
                            .withAvailableReplicas(podAvailable ? 1 : 0)
                            .build());
        }
        return deployment;
    }

    private static Deployment deploymentFor(FlinkMiniCluster miniCluster) {
        var deployment = new Deployment();
        deployment.setMetadata(
                new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
                        .withName(miniCluster.getMetadata().getName())
                        .withNamespace(miniCluster.getMetadata().getNamespace())
                        .build());
        return deployment;
    }

    private Optional<HasMetadata> ingress() {
        if (IngressUtils.ingressInNetworkingV1(kubernetesClient)) {
            return Optional.ofNullable(
                    kubernetesClient
                            .network()
                            .v1()
                            .ingresses()
                            .inNamespace(TestUtils.TEST_NAMESPACE)
                            .withName(TestUtils.TEST_MINI_CLUSTER_NAME)
                            .get());
        }
        return Optional.ofNullable(
                kubernetesClient
                        .network()
                        .v1beta1()
                        .ingresses()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .withName(TestUtils.TEST_MINI_CLUSTER_NAME)
                        .get());
    }

    private static PodList podWaitingFor(String reason) {
        var containerStatus =
                new ContainerStatusBuilder()
                        .withName("flink-main-container")
                        .withNewState()
                        .withNewWaiting()
                        .withReason(reason)
                        .withMessage(reason + " for image")
                        .endWaiting()
                        .endState()
                        .build();
        var pod =
                new PodBuilder()
                        .withNewMetadata()
                        .withName("pod")
                        .endMetadata()
                        .withNewStatus()
                        .withContainerStatuses(containerStatus)
                        .endStatus()
                        .build();
        var podList = new PodList();
        podList.setItems(new ArrayList<>(List.of(pod)));
        return podList;
    }
}
