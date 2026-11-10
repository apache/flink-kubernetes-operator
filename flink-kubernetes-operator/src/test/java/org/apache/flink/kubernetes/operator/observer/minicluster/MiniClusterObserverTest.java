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

package org.apache.flink.kubernetes.operator.observer.minicluster;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingStatusRecorder;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkMiniClusterStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthEvaluator;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.MiniClusterReconciler;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.kubernetes.operator.service.minicluster.TestingMiniClusterFlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MiniClusterObserver}. */
@EnableKubernetesMockClient(crud = true)
class MiniClusterObserverTest {

    private static final String NUM_RESTARTS_METRIC_NAME = "numRestarts";
    private static final String NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME =
            "numberOfCompletedCheckpoints";

    private KubernetesClient kubernetesClient;

    private FlinkConfigManager configManager;
    private TestingMiniClusterFlinkService flinkService;
    private FlinkResourceEventCollector eventCollector;
    private EventRecorder eventRecorder;
    private MiniClusterObserver observer;

    /** The Deployment of the MiniCluster, as the informer would return it, or null if missing. */
    private Deployment deployment;

    @BeforeEach
    void setup() {
        configManager = new FlinkConfigManager(new Configuration());
        flinkService = new TestingMiniClusterFlinkService(kubernetesClient);
        eventCollector = new FlinkResourceEventCollector();
        eventRecorder = new EventRecorder(eventCollector, new FlinkStateSnapshotEventCollector());
        observer = new MiniClusterObserver(eventRecorder);
    }

    @Test
    void testNotObservedBeforeFirstDeployment() {
        var miniCluster = TestUtils.buildMiniCluster();

        observer.observe(context(miniCluster));

        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.MISSING);
        assertThat(miniCluster.getStatus().getError()).isNull();
    }

    @Test
    void testPodDeploymentStatusTransitions() throws Exception {
        var miniCluster = deployedMiniCluster();
        var status = miniCluster.getStatus();
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.DEPLOYING);

        // The pod is starting
        deployment = createdDeployment(false);
        observer.observe(context(miniCluster));
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.DEPLOYING);

        // The pod is available, but the REST port is not yet reachable
        deployment = createdDeployment(true);
        flinkService.getDelegate().setPortReady(false);
        observer.observe(context(miniCluster));
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.DEPLOYING);

        // The REST port is reachable
        flinkService.getDelegate().setPortReady(true);
        observer.observe(context(miniCluster));
        assertThat(status.getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.DEPLOYED_NOT_READY);
        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.RECONCILING);

        // The REST API is ready, and the job is observed through it
        observer.observe(context(miniCluster));
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.READY);
        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.RUNNING);
        assertThat(status.getClusterInfo()).containsAllEntriesOf(TestingFlinkService.CLUSTER_INFO);
        // The CPU and memory of the MiniCluster pod, from the 1 CPU and 2Gi of its requests
        assertThat(status.getClusterInfo())
                .containsEntry(AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "1.0")
                .containsEntry(
                        AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY,
                        String.valueOf(2L * 1024 * 1024 * 1024));
        assertThat(status.getError()).isNull();
    }

    @Test
    void testResourceUsagePrefersLimits() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster
                .getSpec()
                .setResources(
                        new ResourceRequirementsBuilder()
                                .addToRequests("cpu", new Quantity("500m"))
                                .addToRequests("memory", new Quantity("1Gi"))
                                .addToLimits("cpu", new Quantity("2"))
                                .build());
        var reconciler =
                new MiniClusterReconciler(
                        eventRecorder,
                        new TestingStatusRecorder<FlinkMiniCluster, FlinkMiniClusterStatus>());
        reconciler.reconcile(context(miniCluster));
        deployment = createdDeployment(true);
        observer.observe(context(miniCluster));
        observer.observe(context(miniCluster));

        assertThat(miniCluster.getStatus().getClusterInfo())
                .containsEntry(AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "2.0")
                .containsEntry(
                        AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY,
                        String.valueOf(1024L * 1024 * 1024));
    }

    @ParameterizedTest
    @CsvSource({"512m, 536870912", "1g, 1073741824", "2Gi, 2147483648"})
    void testResourceUsageReadsMemoryAsTheContainerGetsIt(String memory, long bytes)
            throws Exception {
        var miniCluster =
                runningMiniCluster(
                        mc ->
                                mc.getSpec()
                                        .setResources(
                                                new ResourceRequirementsBuilder()
                                                        .addToRequests("cpu", new Quantity("1"))
                                                        .addToRequests(
                                                                "memory", new Quantity(memory))
                                                        .build()));

        assertThat(miniCluster.getStatus().getClusterInfo())
                .containsEntry(AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY, String.valueOf(bytes));
    }

    @Test
    void testResourceUsageIncludesPodTemplateResources() throws Exception {
        var miniCluster =
                runningMiniCluster(
                        mc -> {
                            mc.getSpec()
                                    .setResources(
                                            new ResourceRequirementsBuilder()
                                                    .addToRequests("cpu", new Quantity("1"))
                                                    .build());
                            mc.getSpec()
                                    .setPodTemplate(
                                            new PodTemplateSpecBuilder()
                                                    .withNewSpec()
                                                    .addNewContainer()
                                                    .withName("flink-main-container")
                                                    .withNewResources()
                                                    .addToLimits("memory", new Quantity("3Gi"))
                                                    .endResources()
                                                    .endContainer()
                                                    .endSpec()
                                                    .build());
                        });

        assertThat(miniCluster.getStatus().getClusterInfo())
                .containsEntry(AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "1.0")
                .containsEntry(
                        AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY,
                        String.valueOf(3L * 1024 * 1024 * 1024));
    }

    @Test
    void testMissingDeploymentIsAnError() throws Exception {
        var miniCluster = deployedMiniCluster();
        deployment = null;

        observer.observe(context(miniCluster));

        var status = miniCluster.getStatus();
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.MISSING);
        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.RECONCILING);
        assertThat(status.getError()).contains("Missing MiniCluster deployment");
        assertThat(eventCollector.events)
                .anySatisfy(
                        e ->
                                assertThat(e.getReason())
                                        .isEqualTo(EventRecorder.Reason.Missing.name()));

        // Only reported when the Deployment goes missing
        eventCollector.events.clear();
        observer.observe(context(miniCluster));
        assertThat(eventCollector.events).isEmpty();
    }

    @Test
    void testRestApiTimeoutFindsMissingDeploymentOfRunningMiniCluster() throws Exception {
        var miniCluster = runningMiniCluster();
        var status = miniCluster.getStatus();
        assertThat(status.getReconciliationStatus().isLastReconciledSpecStable()).isTrue();
        deployment = null;
        flinkService.setJobStatusException(new TimeoutException("REST API timed out"));

        observer.observe(context(miniCluster));

        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.MISSING);
        assertThat(status.getError()).contains("Missing MiniCluster deployment");
    }

    @Test
    void testContainerErrorIsAnError() throws Exception {
        var miniCluster = deployedMiniCluster();
        deployment = createdDeployment(false);
        flinkService.setPodList(podWaitingFor("ImagePullBackOff"));

        assertThatThrownBy(() -> observer.observe(context(miniCluster)))
                .isInstanceOf(DeploymentFailedException.class)
                .hasMessageContaining("ImagePullBackOff");
        assertThat(miniCluster.getStatus().getJobStatus().getState())
                .isEqualTo(JobStatus.RECONCILING);

        // Once the pod is in error, it is not reported again, so that the spec can be updated
        miniCluster.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.ERROR);
        observer.observe(context(miniCluster));
        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.ERROR);
    }

    @Test
    void testFailedPodCreationIsAnError() throws Exception {
        var miniCluster = deployedMiniCluster();
        deployment = createdDeployment(false);
        var condition = new DeploymentCondition();
        condition.setType("ReplicaFailure");
        condition.setReason("FailedCreate");
        condition.setMessage("exceeded quota");
        deployment.getStatus().setConditions(List.of(condition));

        assertThatThrownBy(() -> observer.observe(context(miniCluster)))
                .isInstanceOf(DeploymentFailedException.class)
                .hasMessageContaining("exceeded quota");
    }

    @Test
    void testErrorsAreClearedWhenHealthy() throws Exception {
        var miniCluster = runningMiniCluster();
        miniCluster.getStatus().setError("earlier error");
        miniCluster.getStatus().getReconciliationStatus().markReconciledSpecAsStable();

        observer.observe(context(miniCluster));

        assertThat(miniCluster.getStatus().getError()).isNull();
    }

    @Test
    void testTerminalJobWithPodRunning() throws Exception {
        var miniCluster = runningMiniCluster();
        var job = flinkService.getDelegate().listJobs().get(0);
        flinkService.getDelegate().cancelJob(job.f1.getJobId(), false);

        observer.observe(context(miniCluster));

        // The MiniCluster stays up with the job in its terminal state, which is not an error
        var status = miniCluster.getStatus();
        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.CANCELED);
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.READY);
        assertThat(status.getError()).isNull();
    }

    @Test
    void testRestApiUnreachableDuringPodRestart() throws Exception {
        var miniCluster = runningMiniCluster();
        flinkService.setJobStatusException(new ConnectException("Connection refused"));

        observer.observe(context(miniCluster));

        // The pod is restarting: the job is reconciling, but nothing has failed
        var status = miniCluster.getStatus();
        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.RECONCILING);
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.READY);
        assertThat(status.getError()).isNull();
        assertThat(eventCollector.events)
                .noneSatisfy(e -> assertThat(e.getType()).isEqualTo("Warning"));

        // When the job is back, under the same JobID, it is observed as running again
        flinkService.setJobStatusException(null);
        observer.observe(context(miniCluster));
        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void testRestApiTimeoutObservesPodAgain() throws Exception {
        var miniCluster = runningMiniCluster();
        deployment = createdDeployment(false);
        flinkService.getDelegate().setPortReady(false);

        observer.observe(context(miniCluster));

        // The Deployment is still there, while Kubernetes restarts its pod
        var status = miniCluster.getStatus();
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.DEPLOYING);
        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.RECONCILING);
        assertThat(status.getError()).isNull();
    }

    @Test
    void testSuspendedMiniClusterIsNotObserved() throws Exception {
        var miniCluster = runningMiniCluster();
        miniCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        ReconciliationUtils.updateStatusForDeployedSpec(
                miniCluster,
                configManager.getDeployConfig(miniCluster.getMetadata(), miniCluster.getSpec()));
        miniCluster.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        deployment = null;

        observer.observe(context(miniCluster));

        assertThat(miniCluster.getStatus().getError()).isNull();
        assertThat(eventCollector.events).isEmpty();
    }

    @Test
    void testAlreadyUpgraded() throws Exception {
        var miniCluster = deployedMiniCluster();
        var status = miniCluster.getStatus();
        // An upgrade of a MiniCluster that was deployed before
        ReconciliationUtils.updateLastReconciledSpec(
                miniCluster, (spec, meta) -> meta.setFirstDeployment(false));
        status.getReconciliationStatus().setState(ReconciliationState.UPGRADING);
        status.setPodDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        var generation = ReconciliationUtils.getUpgradeTargetGeneration(miniCluster).toString();

        // A Deployment for an older generation of the resource is not the upgrade
        deployment = createdDeployment(false);
        deployment.getMetadata().setAnnotations(Map.of(FlinkUtils.CR_GENERATION_LABEL, "0"));
        observer.observe(context(miniCluster));
        assertThat(status.getReconciliationStatus().getState())
                .isEqualTo(ReconciliationState.UPGRADING);

        deployment.getMetadata().setAnnotations(Map.of(FlinkUtils.CR_GENERATION_LABEL, generation));
        observer.observe(context(miniCluster));
        assertThat(status.getReconciliationStatus().getState())
                .isEqualTo(ReconciliationState.DEPLOYED);
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.DEPLOYING);
    }

    @Test
    void testUnhealthyJobIsRecordedAndRestarted() throws Exception {
        flinkService.getDelegate().setMetricValue(NUM_RESTARTS_METRIC_NAME, "0");
        flinkService.getDelegate().setMetricValue(NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME, "1");
        var miniCluster =
                runningMiniCluster(
                        mc -> {
                            var conf = mc.getSpec().getFlinkConfiguration();
                            conf.put(OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED.key(), "true");
                            conf.put(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD.key(), "64");
                            mc.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
                        });
        var clusterInfo = miniCluster.getStatus().getClusterInfo();
        assertThat(ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo))
                .satisfies(info -> assertThat(info.getHealthResult().isHealthy()).isTrue());

        flinkService.getDelegate().setMetricValue(NUM_RESTARTS_METRIC_NAME, "100");
        observer.observe(context(miniCluster));
        assertThat(ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo))
                .satisfies(info -> assertThat(info.getHealthResult().isHealthy()).isFalse());

        // The reconciler acts on the health that the observer recorded
        reconciler().reconcile(context(miniCluster));
        assertThat(eventCollector.events)
                .anySatisfy(
                        e ->
                                assertThat(e.getReason())
                                        .isEqualTo(
                                                EventRecorder.Reason.RestartUnhealthyJob.name()));
        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
    }

    @Test
    void testUpgradedJobBecomesStableOnceRunning() throws Exception {
        var miniCluster = runningMiniCluster();
        var status = miniCluster.getStatus();
        miniCluster.getSpec().getJob().setArgs(new String[] {"upgraded"});
        miniCluster.getMetadata().setGeneration(2L);

        var reconciler = reconciler();
        reconciler.reconcile(context(miniCluster));
        reconciler.reconcile(context(miniCluster));
        assertThat(status.getReconciliationStatus().isLastReconciledSpecStable()).isFalse();

        deployment = createdDeployment(true);
        observer.observe(context(miniCluster));
        observer.observe(context(miniCluster));
        observer.observe(context(miniCluster));

        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.RUNNING);
        assertThat(status.getReconciliationStatus().isLastReconciledSpecStable()).isTrue();
        assertThat(status.getReconciliationStatus().deserializeLastStableSpec().getJob().getArgs())
                .containsExactly("upgraded");
    }

    private MiniClusterReconciler reconciler() {
        return new MiniClusterReconciler(
                eventRecorder,
                new TestingStatusRecorder<FlinkMiniCluster, FlinkMiniClusterStatus>());
    }

    /** A MiniCluster that has been deployed, with its Deployment created. */
    private FlinkMiniCluster deployedMiniCluster() throws Exception {
        return deployedMiniCluster(miniCluster -> {});
    }

    /** A MiniCluster that has been deployed, with its Deployment created. */
    private FlinkMiniCluster deployedMiniCluster(Consumer<FlinkMiniCluster> customise)
            throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        customise.accept(miniCluster);
        var reconciler =
                new MiniClusterReconciler(
                        eventRecorder,
                        new TestingStatusRecorder<FlinkMiniCluster, FlinkMiniClusterStatus>());
        reconciler.reconcile(context(miniCluster));
        deployment = createdDeployment(false);
        eventCollector.events.clear();
        return miniCluster;
    }

    /** A MiniCluster that has been deployed, with its pod ready and its job running. */
    private FlinkMiniCluster runningMiniCluster() throws Exception {
        return runningMiniCluster(miniCluster -> {});
    }

    /** A MiniCluster that has been deployed, with its pod ready and its job running. */
    private FlinkMiniCluster runningMiniCluster(Consumer<FlinkMiniCluster> customise)
            throws Exception {
        var miniCluster = deployedMiniCluster(customise);
        deployment = createdDeployment(true);
        observer.observe(context(miniCluster));
        observer.observe(context(miniCluster));
        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.READY);
        assertThat(miniCluster.getStatus().getJobStatus().getState()).isEqualTo(JobStatus.RUNNING);
        eventCollector.events.clear();
        return miniCluster;
    }

    /**
     * The Deployment that the reconciler created for the MiniCluster, as the informer would return
     * it once its pod is starting, or once it is ready.
     */
    private Deployment createdDeployment(boolean ready) {
        var created =
                kubernetesClient
                        .apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .withName(TestUtils.TEST_MINI_CLUSTER_NAME)
                        .get();
        assertThat(created).isNotNull();
        created.setStatus(TestUtils.createDeployment(ready).getStatus());
        return created;
    }

    private FlinkResourceContext<FlinkMiniCluster> context(FlinkMiniCluster miniCluster) {
        var copy = ReconciliationUtils.clone(miniCluster);
        copy.setStatus(miniCluster.getStatus());
        var josdkContext =
                new TestUtils.TestingContext<FlinkMiniCluster>() {
                    @Override
                    public <R> Optional<R> getSecondaryResource(Class<R> type, String name) {
                        return type == Deployment.class
                                ? Optional.ofNullable(type.cast(deployment))
                                : Optional.empty();
                    }

                    @Override
                    public KubernetesClient getClient() {
                        return kubernetesClient;
                    }
                };
        return new TestingFlinkResourceContextFactory(
                        configManager,
                        TestUtils.createTestMetricGroup(configManager.getDefaultConfig()),
                        flinkService.getDelegate(),
                        flinkService,
                        eventRecorder)
                .getResourceContext(copy, josdkContext);
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
