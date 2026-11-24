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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.TestingStatusRecorder;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkMiniClusterStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.UpgradeFailureException;
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthEvaluator;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthResult;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.SuspendMode;
import org.apache.flink.kubernetes.operator.service.minicluster.TestingMiniClusterFlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractJobReconciler.LAST_STATE_DUMMY_SP_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MiniClusterReconciler}. */
@EnableKubernetesMockClient(crud = true)
class MiniClusterReconcilerTest {

    private KubernetesClient kubernetesClient;

    private FlinkConfigManager configManager;
    private TestingMiniClusterFlinkService flinkService;
    private FlinkResourceEventCollector eventCollector;
    private MiniClusterReconciler reconciler;

    @BeforeEach
    void setup() {
        configManager = new FlinkConfigManager(new Configuration());
        flinkService = new TestingMiniClusterFlinkService(kubernetesClient);
        eventCollector = new FlinkResourceEventCollector();
        reconciler =
                new MiniClusterReconciler(
                        new EventRecorder(eventCollector, new FlinkStateSnapshotEventCollector()),
                        new TestingStatusRecorder<FlinkMiniCluster, FlinkMiniClusterStatus>());
    }

    @Test
    void testFirstDeployment() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();

        reconciler.reconcile(context(miniCluster));

        var status = miniCluster.getStatus();
        var jobId = status.getJobStatus().getJobId();
        assertThat(jobId).isNotNull();
        assertThat(status.getJobStatus().getState()).isEqualTo(JobStatus.RECONCILING);
        assertThat(status.getPodDeploymentStatus()).isEqualTo(JobManagerDeploymentStatus.DEPLOYING);
        assertThat(status.getReconciliationStatus().getState())
                .isEqualTo(ReconciliationState.DEPLOYED);

        assertThat(deployment(miniCluster)).isNotNull();
        assertThat(service(miniCluster)).isNotNull();
        assertThat(configMap(miniCluster)).isNotNull();
        assertThat(deployment(miniCluster).getMetadata().getOwnerReferences())
                .singleElement()
                .satisfies(
                        ref ->
                                assertThat(ref.getUid())
                                        .isEqualTo(miniCluster.getMetadata().getUid()));
        // The launcher is given the JobID recorded in the status
        assertThat(deployment(miniCluster).getSpec().getTemplate().getSpec().getContainers())
                .first()
                .satisfies(c -> assertThat(c.getArgs()).containsSubsequence("--job-id", jobId));
        assertThat(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH))
                .isNull();
        assertThat(eventCollector.events)
                .anySatisfy(
                        e ->
                                assertThat(e.getReason())
                                        .isEqualTo(EventRecorder.Reason.Submit.name()));
    }

    @Test
    void testSuspendedResourceIsNotDeployed() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster.getSpec().getJob().setState(JobState.SUSPENDED);

        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeployCount()).isZero();
        assertThat(deployment(miniCluster)).isNull();
    }

    @Test
    void testInitialSavepointPath() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster.getSpec().getJob().setInitialSavepointPath("file:///initial-savepoint");

        reconciler.reconcile(context(miniCluster));

        // The restore path reaches the launcher through the config file
        assertThat(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH))
                .isEqualTo("file:///initial-savepoint");
        assertThat(
                        configMap(miniCluster)
                                .getData()
                                .get("config.yaml")
                                .contains("file:///initial-savepoint"))
                .isTrue();
        assertThat(deployment(miniCluster).getSpec().getTemplate().getSpec().getContainers())
                .first()
                .satisfies(c -> assertThat(c.getArgs()).noneMatch(a -> a.contains("Savepoint")));
    }

    @Test
    void testJobIdIsKeptOnlyForLastStateDeployments() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        var ctx = context(miniCluster);
        var spec = miniCluster.getSpec();

        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.empty(), false);
        var firstJobId = miniCluster.getStatus().getJobStatus().getJobId();
        assertThat(
                        flinkService
                                .getSubmittedConf()
                                .get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID))
                .isEqualTo(firstJobId);

        terminate(miniCluster);
        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.empty(), true);
        assertThat(miniCluster.getStatus().getJobStatus().getJobId()).isEqualTo(firstJobId);

        terminate(miniCluster);
        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.empty(), false);
        assertThat(miniCluster.getStatus().getJobStatus().getJobId()).isNotEqualTo(firstJobId);
    }

    @Test
    void testUserJobIdIsUsed() throws Exception {
        var jobId = new JobID().toHexString();
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key(), jobId);

        reconciler.reconcile(context(miniCluster));

        assertThat(miniCluster.getStatus().getJobStatus().getJobId()).isEqualTo(jobId);
    }

    @Test
    void testRestorePath() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(SavepointConfigOptions.SAVEPOINT_PATH.key(), "file:///stale");
        var ctx = context(miniCluster);
        var spec = miniCluster.getSpec();

        // Stateless deployments start without state, even if a restore path is configured
        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.empty(), false);
        assertThat(flinkService.getSubmittedConf().contains(SavepointConfigOptions.SAVEPOINT_PATH))
                .isFalse();

        terminate(miniCluster);
        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.of("file:///sp"), false);
        assertThat(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH))
                .isEqualTo("file:///sp");

        // Last-state deployments restore from the HA metadata, with a dummy restore path in case
        // the HA metadata has been removed
        terminate(miniCluster);
        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.empty(), true);
        assertThat(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH))
                .isEqualTo(LAST_STATE_DUMMY_SP_PATH);
    }

    @Test
    void testEachDeploymentHasItsOwnResultStores() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        var ctx = context(miniCluster);
        var spec = miniCluster.getSpec();

        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.empty(), false);
        var firstPath = flinkService.getSubmittedConf().get(JobResultStoreOptions.STORAGE_PATH);
        var firstAppPath =
                flinkService
                        .getSubmittedConf()
                        .get(AbstractClusterJobReconciler.APPLICATION_RESULT_STORE_STORAGE_PATH);

        terminate(miniCluster);
        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.empty(), false);

        var haStorage =
                spec.getFlinkConfiguration().get(HighAvailabilityOptions.HA_STORAGE_PATH.key());
        assertThat(firstPath).startsWith(haStorage.asText() + "/job-result-store/");
        assertThat(flinkService.getSubmittedConf().get(JobResultStoreOptions.STORAGE_PATH))
                .isNotEqualTo(firstPath);
        assertThat(firstAppPath).startsWith(haStorage.asText() + "/application-result-store/");
        assertThat(
                        flinkService
                                .getSubmittedConf()
                                .get(
                                        AbstractClusterJobReconciler
                                                .APPLICATION_RESULT_STORE_STORAGE_PATH))
                .isNotEqualTo(firstAppPath);
    }

    @Test
    void testTerminatedMiniClusterIsDeletedBeforeRedeploying() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        var ctx = context(miniCluster);
        var spec = miniCluster.getSpec();

        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.empty(), false);
        terminate(miniCluster);
        reconciler.deploy(ctx, spec, ctx.getDeployConfig(spec), Optional.empty(), false);

        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
        assertThat(flinkService.getDeployCount()).isEqualTo(2);
    }

    @Test
    void testStatelessUpgrade() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.STATELESS);
        var firstJobId = miniCluster.getStatus().getJobStatus().getJobId();

        miniCluster.getSpec().setRestartNonce(1L);
        reconciler.reconcile(context(miniCluster));

        // The job is stopped first, and redeployed by the next reconciliation
        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
        assertThat(flinkService.getDeployCount()).isEqualTo(1);
        assertThat(miniCluster.getStatus().getReconciliationStatus().getState())
                .isEqualTo(ReconciliationState.UPGRADING);

        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(miniCluster.getStatus().getJobStatus().getJobId()).isNotEqualTo(firstJobId);
        assertThat(flinkService.getSubmittedConf().contains(SavepointConfigOptions.SAVEPOINT_PATH))
                .isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testConfigOnlyChangeIsRedeployed(boolean logConfiguration) throws Exception {
        // The new config is only used by a new pod, so a change to it alone must not be recorded
        // as deployed while the pod runs with the old config
        var miniCluster = runningMiniCluster(UpgradeMode.STATELESS);
        assertThat(configMap(miniCluster).getData().values())
                .noneMatch(v -> v.contains("changed-"));
        String configFile;
        if (logConfiguration) {
            configFile = "log4j-console.properties";
            miniCluster.getSpec().setLogConfiguration(Map.of(configFile, "changed-log-config"));
        } else {
            configFile = "config.yaml";
            miniCluster.getSpec().getFlinkConfiguration().put("pipeline.name", "changed-config");
        }

        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
        assertThat(flinkService.getDeployCount()).isEqualTo(1);

        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(configMap(miniCluster).getData().get(configFile)).contains("changed-");
    }

    @Test
    void testInsufficientMemoryIsWarnedOnDeploy() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        miniCluster
                .getSpec()
                .setResources(
                        new ResourceRequirementsBuilder()
                                .addToRequests("cpu", new Quantity("1"))
                                .addToRequests("memory", new Quantity("128Mi"))
                                .build());

        deployStable(miniCluster);

        assertThat(insufficientMemoryEvents(miniCluster))
                .singleElement()
                .satisfies(
                        e -> {
                            assertThat(e.getType()).isEqualTo(EventRecorder.Type.Warning.name());
                            assertThat(e.getMessage()).contains("spec.resources");
                            assertThat(e.getCount()).isEqualTo(1);
                        });

        // Only deploying checks the memory
        reconciler.reconcile(context(miniCluster));
        assertThat(insufficientMemoryEvents(miniCluster))
                .singleElement()
                .satisfies(e -> assertThat(e.getCount()).isEqualTo(1));

        // Each deploy updates the same event
        miniCluster.getSpec().setRestartNonce(1L);
        reconciler.reconcile(context(miniCluster));
        reconciler.reconcile(context(miniCluster));
        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(insufficientMemoryEvents(miniCluster))
                .singleElement()
                .satisfies(e -> assertThat(e.getCount()).isEqualTo(2));
    }

    @Test
    void testSufficientMemoryIsNotWarned() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();

        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeployCount()).isEqualTo(1);
        assertThat(insufficientMemoryEvents(miniCluster)).isEmpty();
    }

    @Test
    void testSavepointUpgrade() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.SAVEPOINT);

        miniCluster.getSpec().setRestartNonce(1L);
        reconciler.reconcile(context(miniCluster));

        // Stopping the job with a savepoint leaves the MiniCluster running, as it does for the
        // JobManager of an application FlinkDeployment, until the job is redeployed
        assertThat(miniCluster.getStatus().getJobStatus().getUpgradeSavepointPath())
                .isEqualTo("savepoint_0");
        assertThat(flinkService.getDeleteCount()).isZero();

        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH))
                .isEqualTo("savepoint_0");
    }

    @Test
    void testLastStateUpgrade() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.LAST_STATE);
        var firstJobId = miniCluster.getStatus().getJobStatus().getJobId();

        miniCluster.getSpec().setRestartNonce(1L);
        reconciler.reconcile(context(miniCluster));
        reconciler.reconcile(context(miniCluster));

        // The MiniCluster is deleted without cancelling the job, keeping its HA metadata, and the
        // job is resubmitted under the same JobID to be recovered from it
        assertThat(flinkService.getDelegate().getCancelJobCallCount()).isZero();
        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(miniCluster.getStatus().getJobStatus().getJobId()).isEqualTo(firstJobId);
        assertThat(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH))
                .isEqualTo(LAST_STATE_DUMMY_SP_PATH);
    }

    @Test
    void testSuspendWithSavepoint() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.SAVEPOINT);

        miniCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        reconciler.reconcile(context(miniCluster));

        assertThat(miniCluster.getStatus().getJobStatus().getUpgradeSavepointPath())
                .isEqualTo("savepoint_0");
        assertThat(miniCluster.getStatus().getJobStatus().getState()).isEqualTo(JobStatus.FINISHED);
        assertThat(flinkService.getDeployCount()).isEqualTo(1);
        assertThat(
                        miniCluster
                                .getStatus()
                                .getReconciliationStatus()
                                .deserializeLastReconciledSpec()
                                .getJob()
                                .getState())
                .isEqualTo(JobState.SUSPENDED);
    }

    @Test
    void testStatelessSuspendDeletesMiniCluster() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.STATELESS);

        miniCluster.getSpec().getJob().setState(JobState.SUSPENDED);
        reconciler.reconcile(context(miniCluster));

        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.MISSING);
        assertMiniClusterDeleted(miniCluster);
    }

    @Test
    void testCleanupAfterFailedJob() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.STATELESS);

        reconciler.cleanupAfterFailedJob(context(miniCluster));

        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.MISSING);
        assertMiniClusterDeleted(miniCluster);
    }

    @Test
    void testFailedJobIsRestartedWhenConfigured() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.STATELESS);
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.OPERATOR_JOB_RESTART_FAILED.key(), "true");
        markReconciled(miniCluster);
        miniCluster.getStatus().getJobStatus().setState(JobStatus.FAILED);

        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(miniCluster.getStatus().getJobStatus().getState())
                .isEqualTo(JobStatus.RECONCILING);
    }

    @Test
    void testFlinkVersionUpgradeUsesSavepoint() throws Exception {
        // HA metadata may not be compatible across Flink versions, so a last-state upgrade of
        // the Flink version uses a savepoint instead
        var miniCluster = TestUtils.buildMiniCluster(FlinkVersion.v2_3, JobState.RUNNING);
        miniCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        reconciler.reconcile(context(miniCluster));
        var status = miniCluster.getStatus();
        status.setPodDeploymentStatus(JobManagerDeploymentStatus.READY);
        status.getJobStatus().setState(JobStatus.RUNNING);
        status.getReconciliationStatus().markReconciledSpecAsStable();

        miniCluster.getSpec().setFlinkVersion(FlinkVersion.v2_4);
        reconciler.reconcile(context(miniCluster));
        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH))
                .isEqualTo("savepoint_0");
    }

    @Test
    void testUnstableUpgradeIsRolledBack() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        enableRollback(miniCluster);
        miniCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        var stableParallelism = miniCluster.getSpec().getJob().getParallelism();
        deployStable(miniCluster);

        miniCluster.getSpec().getJob().setParallelism(stableParallelism + 1);
        reconciler.reconcile(context(miniCluster));
        reconciler.reconcile(context(miniCluster));
        assertThat(flinkService.getDeployCount()).isEqualTo(2);

        // The upgraded job doesn't become stable within the readiness timeout
        advanceClockPastReadinessTimeout();
        reconcileUntil(miniCluster, ReconciliationState.ROLLED_BACK);

        assertThat(flinkService.getDeployCount()).isEqualTo(3);
        assertThat(ReconciliationUtils.getDeployedSpec(miniCluster).getJob().getParallelism())
                .isEqualTo(stableParallelism);
        assertThat(flinkService.getSubmittedConf().get(CoreOptions.DEFAULT_PARALLELISM))
                .isEqualTo(stableParallelism);
    }

    @Test
    void testFlinkVersionUpgradeIsNotRolledBack() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster(FlinkVersion.v2_3, JobState.RUNNING);
        enableRollback(miniCluster);
        miniCluster.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        deployStable(miniCluster);

        miniCluster.getSpec().setFlinkVersion(FlinkVersion.v2_4);
        reconciler.reconcile(context(miniCluster));
        reconciler.reconcile(context(miniCluster));
        assertThat(flinkService.getDeployCount()).isEqualTo(2);

        advanceClockPastReadinessTimeout();
        reconciler.reconcile(context(miniCluster));
        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        var reconciliationStatus = miniCluster.getStatus().getReconciliationStatus();
        assertThat(reconciliationStatus.getState()).isEqualTo(ReconciliationState.DEPLOYED);
        assertThat(reconciliationStatus.deserializeLastReconciledSpec().getFlinkVersion())
                .isEqualTo(FlinkVersion.v2_4);
    }

    @Test
    void testUnhealthyJobIsRestarted() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.STATELESS);
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED.key(),
                        "true");
        markReconciled(miniCluster);
        var clusterInfo = miniCluster.getStatus().getClusterInfo();
        ClusterHealthEvaluator.setLastValidClusterHealthInfo(clusterInfo, unhealthy("unhealthy"));

        reconciler.reconcile(context(miniCluster));

        assertThat(eventCollector.events)
                .anySatisfy(
                        e -> {
                            assertThat(e.getReason())
                                    .isEqualTo(EventRecorder.Reason.RestartUnhealthyJob.name());
                            assertThat(e.getMessage()).isEqualTo("unhealthy");
                        });
        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo)).isNull();
    }

    @Test
    void testHealthInfoIsClearedOnDeploy() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        var clusterInfo = miniCluster.getStatus().getClusterInfo();
        ClusterHealthEvaluator.setLastValidClusterHealthInfo(clusterInfo, unhealthy("stale"));

        reconciler.reconcile(context(miniCluster));

        assertThat(ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo)).isNull();
    }

    @Test
    void testMissingDeploymentIsRecovered() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.LAST_STATE);
        var jobId = miniCluster.getStatus().getJobStatus().getJobId();
        kubernetesClient.resource(deployment(miniCluster)).delete();
        flinkService.getDelegate().listJobs().clear();
        miniCluster.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.MISSING);

        reconciler.reconcile(context(miniCluster));

        assertThat(eventCollector.events)
                .anySatisfy(
                        e ->
                                assertThat(e.getReason())
                                        .isEqualTo(EventRecorder.Reason.RecoverDeployment.name()));
        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(deployment(miniCluster)).isNotNull();
        assertThat(miniCluster.getStatus().getJobStatus().getJobId()).isEqualTo(jobId);
    }

    @Test
    void testLastStateUpgradeFromHaMetadataWhenJobIsNotRunning() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.LAST_STATE);
        var jobId = miniCluster.getStatus().getJobStatus().getJobId();
        // The pod is failing, so the job is neither running nor cancellable
        miniCluster.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.ERROR);
        miniCluster.getStatus().getJobStatus().setState(JobStatus.RECONCILING);

        miniCluster.getSpec().setRestartNonce(1L);
        reconciler.reconcile(context(miniCluster));
        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeployCount()).isEqualTo(2);
        assertThat(miniCluster.getStatus().getJobStatus().getJobId()).isEqualTo(jobId);
        assertThat(
                        miniCluster
                                .getStatus()
                                .getReconciliationStatus()
                                .deserializeLastReconciledSpec()
                                .getRestartNonce())
                .isEqualTo(1L);
    }

    @Test
    void testUpgradeFailsWhenPodIsMissingWithoutHaMetadata() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.LAST_STATE);
        flinkService.getDelegate().setHaDataAvailable(false);
        miniCluster.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.MISSING);
        miniCluster.getStatus().getJobStatus().setState(JobStatus.RECONCILING);

        miniCluster.getSpec().setRestartNonce(1L);

        assertThatThrownBy(() -> reconciler.reconcile(context(miniCluster)))
                .isInstanceOf(UpgradeFailureException.class)
                .hasMessageContaining(CommonStatus.MSG_MANUAL_RESTORE_REQUIRED);
        assertThat(flinkService.getDeployCount()).isEqualTo(1);
    }

    @Test
    void testPodThatNeverStartedIsDeletedForUpgrade() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.SAVEPOINT);
        var jobSpec = miniCluster.getSpec().getJob();
        jobSpec.setState(JobState.SUSPENDED);
        reconciler.reconcile(context(miniCluster));
        jobSpec.setState(JobState.RUNNING);
        reconciler.reconcile(context(miniCluster));
        var deleteCount = flinkService.getDeleteCount();
        var deployCount = flinkService.getDeployCount();

        // The redeployed pod never starts, and there is no HA metadata to restore from
        flinkService.getDelegate().setHaDataAvailable(false);
        miniCluster.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
        Context<FlinkMiniCluster> neverStarted =
                TestUtils.createContextWithInProgressDeployment(kubernetesClient);

        jobSpec.setEntryClass("newClass");
        reconciler.reconcile(context(miniCluster, neverStarted));
        reconciler.reconcile(context(miniCluster, neverStarted));

        assertThat(flinkService.getDeleteCount()).isEqualTo(deleteCount + 1);
        assertThat(flinkService.getDeployCount()).isEqualTo(deployCount + 1);
        assertThat(flinkService.getSubmittedConf().get(SavepointConfigOptions.SAVEPOINT_PATH))
                .isEqualTo("savepoint_0");
    }

    @Test
    void testTerminatedMiniClusterIsDeletedAfterTtl() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.STATELESS);
        var jobStatus = miniCluster.getStatus().getJobStatus();
        jobStatus.setState(JobStatus.FINISHED);
        jobStatus.setUpdateTime(String.valueOf(System.currentTimeMillis()));

        reconciler.reconcile(context(miniCluster));
        assertThat(flinkService.getDeleteCount()).isZero();

        reconciler.clock =
                Clock.fixed(
                        Instant.now()
                                .plus(
                                        KubernetesOperatorConfigOptions.OPERATOR_JM_SHUTDOWN_TTL
                                                .defaultValue())
                                .plus(Duration.ofMinutes(1)),
                        ZoneId.systemDefault());
        reconciler.reconcile(context(miniCluster));

        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
        assertThat(miniCluster.getStatus().getPodDeploymentStatus())
                .isEqualTo(JobManagerDeploymentStatus.MISSING);
    }

    @Test
    void testIngress() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster
                .getSpec()
                .setIngress(IngressSpec.builder().template("{{name}}.example.com").build());

        reconciler.reconcile(context(miniCluster));

        // The Ingress routes to the REST Service of the MiniCluster
        String backendService;
        io.fabric8.kubernetes.api.model.HasMetadata ingress;
        if (IngressUtils.ingressInNetworkingV1(kubernetesClient)) {
            var v1 =
                    kubernetesClient
                            .network()
                            .v1()
                            .ingresses()
                            .inNamespace(TestUtils.TEST_NAMESPACE)
                            .withName(TestUtils.TEST_MINI_CLUSTER_NAME)
                            .get();
            ingress = v1;
            backendService =
                    v1.getSpec()
                            .getRules()
                            .get(0)
                            .getHttp()
                            .getPaths()
                            .get(0)
                            .getBackend()
                            .getService()
                            .getName();
        } else {
            var v1beta1 =
                    kubernetesClient
                            .network()
                            .v1beta1()
                            .ingresses()
                            .inNamespace(TestUtils.TEST_NAMESPACE)
                            .withName(TestUtils.TEST_MINI_CLUSTER_NAME)
                            .get();
            ingress = v1beta1;
            backendService =
                    v1beta1.getSpec()
                            .getRules()
                            .get(0)
                            .getHttp()
                            .getPaths()
                            .get(0)
                            .getBackend()
                            .getServiceName();
        }
        assertThat(ingress.getMetadata().getOwnerReferences())
                .singleElement()
                .satisfies(ref -> assertThat(ref.getKind()).isEqualTo("FlinkMiniCluster"));
        assertThat(backendService).isEqualTo(TestUtils.TEST_MINI_CLUSTER_NAME + "-rest");
    }

    @Test
    void testDeletingRunningJob() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.STATELESS);

        var deleteControl = reconciler.cleanup(context(miniCluster));

        assertThat(deleteControl.isRemoveFinalizer()).isTrue();
        assertMiniClusterDeleted(miniCluster);
    }

    @Test
    void testDeletingWithSavepoint() throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.SAVEPOINT);
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.SAVEPOINT_ON_DELETION.key(), "true");
        markReconciled(miniCluster);

        reconciler.cleanup(context(miniCluster));

        assertThat(miniCluster.getStatus().getJobStatus().getUpgradeSavepointPath())
                .isEqualTo("savepoint_0");
    }

    @Test
    void testDeletingLeavesSameNamedFlinkDeployment() throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(HighAvailabilityOptions.HA_MODE.key(), "kubernetes");
        // The JobManager Deployment and HA metadata of a FlinkDeployment with the same name
        var flinkDeployment = TestUtils.buildApplicationCluster();
        flinkDeployment.getMetadata().setName(miniCluster.getMetadata().getName());
        flinkDeployment.getMetadata().setNamespace(miniCluster.getMetadata().getNamespace());
        kubernetesClient
                .apps()
                .deployments()
                .resource(
                        new DeploymentBuilder()
                                .withNewMetadata()
                                .withName(flinkDeployment.getMetadata().getName())
                                .withNamespace(flinkDeployment.getMetadata().getNamespace())
                                .addNewOwnerReference()
                                .withApiVersion(flinkDeployment.getApiVersion())
                                .withKind(flinkDeployment.getKind())
                                .withName(flinkDeployment.getMetadata().getName())
                                .withUid(flinkDeployment.getMetadata().getUid())
                                .endOwnerReference()
                                .endMetadata()
                                .build())
                .create();
        createHaConfigMap(miniCluster);

        assertThatThrownBy(() -> reconciler.reconcile(context(miniCluster)))
                .isInstanceOfSatisfying(
                        org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client
                                .KubernetesClientException.class,
                        e -> assertThat(e.getCode()).isEqualTo(409));
        reconciler.cleanup(context(miniCluster));

        assertThat(deployment(miniCluster).getMetadata().getOwnerReferences())
                .singleElement()
                .satisfies(
                        ref ->
                                assertThat(ref.getUid())
                                        .isEqualTo(flinkDeployment.getMetadata().getUid()));
        assertThat(
                        kubernetesClient
                                .configMaps()
                                .inNamespace(miniCluster.getMetadata().getNamespace())
                                .withName(
                                        miniCluster.getMetadata().getName() + "-cluster-config-map")
                                .get())
                .isNotNull();
    }

    @Test
    void testDeletingBeforeDeployment() {
        var miniCluster = TestUtils.buildMiniCluster();

        var deleteControl = reconciler.cleanup(context(miniCluster));

        assertThat(deleteControl.isRemoveFinalizer()).isTrue();
        assertThat(flinkService.getDeleteCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @MethodSource("cancelJobModes")
    void testCancelJobModes(
            SuspendMode suspendMode,
            boolean pending,
            JobStatus jobState,
            boolean miniClusterDeleted,
            boolean haMetadataDeleted)
            throws Exception {
        var miniCluster = runningMiniCluster(UpgradeMode.SAVEPOINT);
        var haConfigMap = createHaConfigMap(miniCluster);

        assertThat(reconciler.cancelJob(context(miniCluster), suspendMode)).isEqualTo(pending);

        var status = miniCluster.getStatus();
        assertThat(status.getJobStatus().getState()).isEqualTo(jobState);
        assertThat(status.getJobStatus().getUpgradeSavepointPath())
                .isEqualTo(suspendMode == SuspendMode.SAVEPOINT ? "savepoint_0" : null);
        assertThat(deployment(miniCluster) == null).isEqualTo(miniClusterDeleted);
        assertThat(status.getPodDeploymentStatus())
                .isEqualTo(
                        miniClusterDeleted
                                ? JobManagerDeploymentStatus.MISSING
                                : JobManagerDeploymentStatus.READY);
        assertThat(kubernetesClient.resource(haConfigMap).get() == null)
                .isEqualTo(haMetadataDeleted);
    }

    private static Stream<Arguments> cancelJobModes() {
        return Stream.of(
                Arguments.of(SuspendMode.NOOP, false, JobStatus.FINISHED, false, false),
                Arguments.of(SuspendMode.SAVEPOINT, false, JobStatus.FINISHED, false, false),
                Arguments.of(SuspendMode.CANCEL, true, JobStatus.CANCELLING, false, false),
                Arguments.of(SuspendMode.STATELESS, false, JobStatus.FINISHED, true, true),
                Arguments.of(SuspendMode.LAST_STATE, false, JobStatus.FINISHED, true, false));
    }

    /** Creates a ConfigMap holding Kubernetes HA metadata for the MiniCluster. */
    private ConfigMap createHaConfigMap(FlinkMiniCluster miniCluster) {
        var name = miniCluster.getMetadata().getName();
        return kubernetesClient
                .resource(
                        new ConfigMapBuilder()
                                .withNewMetadata()
                                .withName(name + "-cluster-config-map")
                                .withNamespace(miniCluster.getMetadata().getNamespace())
                                .withLabels(KubernetesUtils.getCommonLabels(name))
                                .addToLabels(
                                        Constants.LABEL_CONFIGMAP_TYPE_KEY,
                                        Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY)
                                .endMetadata()
                                .build())
                .create();
    }

    /** Deploys a MiniCluster, and marks its pod and job as running. */
    private FlinkMiniCluster runningMiniCluster(UpgradeMode upgradeMode) throws Exception {
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster.getSpec().getJob().setUpgradeMode(upgradeMode);
        deployStable(miniCluster);
        return miniCluster;
    }

    private static final Duration READINESS_TIMEOUT = Duration.ofSeconds(10);

    private static void enableRollback(FlinkMiniCluster miniCluster) {
        var conf = miniCluster.getSpec().getFlinkConfiguration();
        conf.put(KubernetesOperatorConfigOptions.DEPLOYMENT_ROLLBACK_ENABLED.key(), "true");
        conf.put(
                KubernetesOperatorConfigOptions.DEPLOYMENT_READINESS_TIMEOUT.key(),
                READINESS_TIMEOUT.toSeconds() + "s");
    }

    /** Deploys a MiniCluster, and marks its pod and job as running and its spec as stable. */
    private void deployStable(FlinkMiniCluster miniCluster) throws Exception {
        reconciler.reconcile(context(miniCluster));
        var status = miniCluster.getStatus();
        status.setPodDeploymentStatus(JobManagerDeploymentStatus.READY);
        status.getJobStatus().setState(JobStatus.RUNNING);
        status.getReconciliationStatus().markReconciledSpecAsStable();
    }

    private void advanceClockPastReadinessTimeout() {
        reconciler.clock =
                Clock.fixed(
                        Instant.now().plus(READINESS_TIMEOUT).plusSeconds(5),
                        ZoneId.systemDefault());
    }

    /** Reconciles until the reconciliation reaches the given state, failing after a few tries. */
    private void reconcileUntil(FlinkMiniCluster miniCluster, ReconciliationState state)
            throws Exception {
        for (int i = 0; i < 5; i++) {
            reconciler.reconcile(context(miniCluster));
            if (miniCluster.getStatus().getReconciliationStatus().getState() == state) {
                return;
            }
        }
        assertThat(miniCluster.getStatus().getReconciliationStatus().getState()).isEqualTo(state);
    }

    private List<Event> insufficientMemoryEvents(FlinkMiniCluster miniCluster) {
        return kubernetesClient
                .v1()
                .events()
                .inNamespace(miniCluster.getMetadata().getNamespace())
                .list()
                .getItems()
                .stream()
                .filter(e -> EventRecorder.Reason.InsufficientMemory.name().equals(e.getReason()))
                .collect(Collectors.toList());
    }

    private static ClusterHealthInfo unhealthy(String error) {
        var clusterHealthInfo = new ClusterHealthInfo();
        clusterHealthInfo.setTimeStamp(System.currentTimeMillis());
        clusterHealthInfo.setNumRestarts(2);
        clusterHealthInfo.setHealthResult(ClusterHealthResult.error(error));
        return clusterHealthInfo;
    }

    /** Records the current spec as reconciled, so that changing it is not an upgrade. */
    private static void markReconciled(FlinkMiniCluster miniCluster) {
        miniCluster
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(miniCluster.getSpec(), miniCluster);
    }

    /** Marks the job of a deployed MiniCluster as terminated, with its pod still running. */
    private static void terminate(FlinkMiniCluster miniCluster) {
        miniCluster.getStatus().getJobStatus().setState(JobStatus.FINISHED);
        miniCluster.getStatus().setPodDeploymentStatus(JobManagerDeploymentStatus.READY);
    }

    /**
     * Context for reconciling a MiniCluster. As the operator reads the resource afresh for every
     * reconciliation, the context is given a copy of it, sharing only its status.
     */
    private FlinkResourceContext<FlinkMiniCluster> context(FlinkMiniCluster miniCluster) {
        return context(miniCluster, flinkService.getContext());
    }

    private FlinkResourceContext<FlinkMiniCluster> context(
            FlinkMiniCluster miniCluster, Context<FlinkMiniCluster> josdkContext) {
        var copy = ReconciliationUtils.clone(miniCluster);
        copy.setStatus(miniCluster.getStatus());
        var ctxFactory =
                new TestingFlinkResourceContextFactory(
                        configManager,
                        TestUtils.createTestMetricGroup(configManager.getDefaultConfig()),
                        flinkService.getDelegate(),
                        flinkService,
                        new EventRecorder(eventCollector, new FlinkStateSnapshotEventCollector()));
        return ctxFactory.getResourceContext(copy, josdkContext);
    }

    private io.fabric8.kubernetes.api.model.apps.Deployment deployment(
            FlinkMiniCluster miniCluster) {
        return kubernetesClient
                .apps()
                .deployments()
                .inNamespace(miniCluster.getMetadata().getNamespace())
                .withName(miniCluster.getMetadata().getName())
                .get();
    }

    private io.fabric8.kubernetes.api.model.Service service(FlinkMiniCluster miniCluster) {
        return kubernetesClient
                .services()
                .inNamespace(miniCluster.getMetadata().getNamespace())
                .withName(miniCluster.getMetadata().getName() + "-rest")
                .get();
    }

    private io.fabric8.kubernetes.api.model.ConfigMap configMap(FlinkMiniCluster miniCluster) {
        return kubernetesClient
                .configMaps()
                .inNamespace(miniCluster.getMetadata().getNamespace())
                .withName("flink-config-" + miniCluster.getMetadata().getName())
                .get();
    }

    /**
     * Asserts the MiniCluster Deployment is deleted. The resources it owns are deleted with it by
     * Kubernetes, which the mock server doesn't do.
     */
    private void assertMiniClusterDeleted(FlinkMiniCluster miniCluster) {
        assertThat(deployment(miniCluster)).isNull();
    }
}
