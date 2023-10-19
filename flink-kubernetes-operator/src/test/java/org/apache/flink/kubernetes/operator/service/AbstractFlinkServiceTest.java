/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingClusterClient;
import org.apache.flink.kubernetes.operator.TestingRestClient;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.artifact.ArtifactManager;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.observer.CheckpointFetchResult;
import org.apache.flink.kubernetes.operator.observer.SavepointFetchResult;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.JobMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDisposalRequest;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarRunRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadResponseBody;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.TriFunction;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** @link FlinkService unit tests */
@EnableKubernetesMockClient(crud = true)
public class AbstractFlinkServiceTest {

    @TempDir Path tempDir;
    File testJar;

    private KubernetesClient client;
    private final Configuration configuration = new Configuration();

    private final FlinkConfigManager configManager = new FlinkConfigManager(configuration);
    private FlinkOperatorConfiguration operatorConfig;
    private ExecutorService executorService;

    private ArtifactManager artifactManager;

    @BeforeEach
    public void setup() {
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TestUtils.TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);
        configuration.set(FLINK_VERSION, FlinkVersion.v1_18);
        operatorConfig = FlinkOperatorConfiguration.fromConfiguration(configuration);
        executorService = Executors.newDirectExecutorService();
        testJar = tempDir.resolve("test.jar").toFile();
        artifactManager =
                new ArtifactManager(configManager) {
                    @Override
                    public File fetch(
                            String jarURI, Configuration flinkConfiguration, String targetDirStr)
                            throws IOException {
                        Files.writeString(testJar.toPath(), "test");
                        return testJar;
                    }
                };
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void sessionJobSubmissionTest(FlinkVersion flinkVersion) throws Exception {
        var jarRuns = new ArrayList<JarRunRequestBody>();
        var flinkService =
                getTestingService(
                        (h, p, b) -> {
                            if (b instanceof JarRunRequestBody) {
                                jarRuns.add((JarRunRequestBody) b);
                                return CompletableFuture.completedFuture(null);
                            } else if (h instanceof JarUploadHeaders) {
                                return CompletableFuture.completedFuture(
                                        new JarUploadResponseBody("test"));
                            } else if (h instanceof JarDeleteHeaders) {
                                return CompletableFuture.completedFuture(null);
                            }

                            throw new UnsupportedOperationException("Unknown request");
                        });
        var session = TestUtils.buildSessionCluster(flinkVersion);
        session.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(session.getSpec(), session);

        var job = TestUtils.buildSessionJob();
        var deployConf = configManager.getSessionJobConfig(session, job.getSpec());
        flinkService.submitJobToSessionCluster(job.getMetadata(), job.getSpec(), deployConf, null);

        // Make sure that deploy conf was passed to jar run
        if (flinkVersion.isNewerVersionThan(FlinkVersion.v1_16)) {
            assertEquals(deployConf.toMap(), jarRuns.get(0).getFlinkConfiguration().toMap());
        } else {
            assertTrue(jarRuns.get(0).getFlinkConfiguration().toMap().isEmpty());
        }
    }

    @Test
    public void jarRunErrorHandlingTest() throws Exception {
        List<JarRunRequestBody> jarRuns = new ArrayList<>();
        AtomicBoolean deleted = new AtomicBoolean(false);
        var flinkService =
                getTestingService(
                        (h, p, b) -> {
                            if (b instanceof JarRunRequestBody) {
                                jarRuns.add((JarRunRequestBody) b);
                                return CompletableFuture.failedFuture(
                                        new Exception("RunException"));
                            } else if (h instanceof JarDeleteHeaders) {
                                deleted.set(true);
                                return CompletableFuture.failedFuture(
                                        new Exception("DeleteException"));
                            }

                            fail();
                            return null;
                        });

        var job = TestUtils.buildSessionJob();
        var jobId = new JobID();

        assertThrows(
                FlinkRuntimeException.class,
                () ->
                        flinkService.runJar(
                                job.getSpec().getJob(),
                                jobId,
                                new JarUploadResponseBody("test"),
                                configuration,
                                null));
        assertEquals(jobId, jarRuns.get(0).getJobId());
        assertTrue(deleted.get());
    }

    private TestingService getTestingService(
            TriFunction<
                            MessageHeaders<?, ?, ?>,
                            MessageParameters,
                            RequestBody,
                            CompletableFuture<ResponseBody>>
                    requestProcessor)
            throws Exception {
        var testingClusterClient = new TestingClusterClient<String>(configuration);
        testingClusterClient.setRequestProcessor(requestProcessor);
        var testingRestClient = new TestingRestClient(configuration);
        testingRestClient.setRequestProcessor(requestProcessor);
        return new TestingService(testingClusterClient, testingRestClient);
    }

    @Test
    public void cancelJobWithStatelessUpgradeModeTest() throws Exception {
        final TestingClusterClient<String> testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        final CompletableFuture<JobID> cancelFuture = new CompletableFuture<>();
        testingClusterClient.setCancelFunction(
                jobID -> {
                    cancelFuture.complete(jobID);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        var flinkService = new TestingService(testingClusterClient);

        JobID jobID = JobID.generate();
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        JobStatus jobStatus = deployment.getStatus().getJobStatus();
        jobStatus.setJobId(jobID.toHexString());
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        deployment.getStatus().getJobStatus().setState("RUNNING");
        flinkService.cancelJob(
                deployment,
                UpgradeMode.STATELESS,
                configManager.getObserveConfig(deployment),
                false);
        assertTrue(cancelFuture.isDone());
        assertEquals(jobID, cancelFuture.get());
        assertNull(jobStatus.getSavepointInfo().getLastSavepoint());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void cancelJobWithSavepointUpgradeModeTest(boolean deleteAfterSavepoint)
            throws Exception {
        var testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        CompletableFuture<Tuple3<JobID, Boolean, String>> stopWithSavepointFuture =
                new CompletableFuture<>();
        var savepointPath = "file:///path/of/svp-1";
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointPath);
        testingClusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDir) -> {
                    stopWithSavepointFuture.complete(
                            new Tuple3<>(jobID, advanceToEndOfEventTime, savepointDir));
                    return CompletableFuture.completedFuture(savepointPath);
                });

        var flinkService = new TestingService(testingClusterClient);

        JobID jobID = JobID.generate();
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), savepointPath);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        JobStatus jobStatus = deployment.getStatus().getJobStatus();
        jobStatus.setJobId(jobID.toHexString());
        jobStatus.setState(org.apache.flink.api.common.JobStatus.RUNNING.name());
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        flinkService.cancelJob(
                deployment,
                UpgradeMode.SAVEPOINT,
                configManager.getObserveConfig(deployment),
                deleteAfterSavepoint);
        assertTrue(stopWithSavepointFuture.isDone());
        assertEquals(jobID, stopWithSavepointFuture.get().f0);
        assertFalse(stopWithSavepointFuture.get().f1);
        assertEquals(savepointPath, stopWithSavepointFuture.get().f2);
        assertEquals(savepointPath, jobStatus.getSavepointInfo().getLastSavepoint().getLocation());

        assertEquals(jobStatus.getState(), org.apache.flink.api.common.JobStatus.FINISHED.name());
        assertEquals(
                deployment.getStatus().getJobManagerDeploymentStatus(),
                deleteAfterSavepoint
                        ? JobManagerDeploymentStatus.MISSING
                        : JobManagerDeploymentStatus.READY);
        if (deleteAfterSavepoint) {
            assertEquals(List.of(deployment.getMetadata()), flinkService.deleted);
        } else {
            assertTrue(flinkService.deleted.isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void cancelJobWithDrainOnSavepointUpgradeModeTest(boolean drainOnSavepoint)
            throws Exception {
        var testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        CompletableFuture<Tuple3<JobID, Boolean, String>> stopWithSavepointFuture =
                new CompletableFuture<>();
        var savepointPath = "file:///path/of/svp-1";
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointPath);

        testingClusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDir) -> {
                    stopWithSavepointFuture.complete(
                            new Tuple3<>(jobID, advanceToEndOfEventTime, savepointDir));
                    return CompletableFuture.completedFuture(savepointPath);
                });

        var flinkService = new TestingService(testingClusterClient);

        JobID jobID = JobID.generate();
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), savepointPath);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        JobStatus jobStatus = deployment.getStatus().getJobStatus();
        jobStatus.setJobId(jobID.toHexString());
        jobStatus.setState(org.apache.flink.api.common.JobStatus.RUNNING.name());
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        if (drainOnSavepoint) {
            deployment
                    .getSpec()
                    .getFlinkConfiguration()
                    .put(KubernetesOperatorConfigOptions.SAVEPOINT_ON_DELETION.key(), "true");
            deployment
                    .getSpec()
                    .getFlinkConfiguration()
                    .put(KubernetesOperatorConfigOptions.DRAIN_ON_SAVEPOINT_DELETION.key(), "true");
        }

        flinkService.cancelJob(
                deployment,
                UpgradeMode.SAVEPOINT,
                configManager.getObserveConfig(deployment),
                true);
        assertTrue(stopWithSavepointFuture.isDone());
        assertEquals(jobID, stopWithSavepointFuture.get().f0);
        assertEquals(savepointPath, jobStatus.getSavepointInfo().getLastSavepoint().getLocation());
        assertEquals(jobStatus.getState(), org.apache.flink.api.common.JobStatus.FINISHED.name());

        if (drainOnSavepoint) {
            assertTrue(stopWithSavepointFuture.get().f1);
        } else {
            assertFalse(stopWithSavepointFuture.get().f1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void cancelSessionJobWithDrainOnSavepointUpgradeModeTest(boolean drainOnSavepoint)
            throws Exception {
        var testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        CompletableFuture<Tuple3<JobID, Boolean, String>> stopWithSavepointFuture =
                new CompletableFuture<>();
        var savepointPath = "file:///path/of/svp-1";
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointPath);

        testingClusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDir) -> {
                    stopWithSavepointFuture.complete(
                            new Tuple3<>(jobID, advanceToEndOfEventTime, savepointDir));
                    return CompletableFuture.completedFuture(savepointPath);
                });

        var flinkService = new TestingService(testingClusterClient);

        JobID jobID = JobID.generate();
        var session = TestUtils.buildSessionCluster(configuration.get(FLINK_VERSION));
        session.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(session.getSpec(), session);
        var job = TestUtils.buildSessionJob();

        job.getSpec()
                .getFlinkConfiguration()
                .put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), savepointPath);

        JobStatus jobStatus = job.getStatus().getJobStatus();
        jobStatus.setJobId(jobID.toHexString());
        jobStatus.setState(org.apache.flink.api.common.JobStatus.RUNNING.name());
        ReconciliationUtils.updateStatusForDeployedSpec(job, new Configuration());

        if (drainOnSavepoint) {
            job.getSpec()
                    .getFlinkConfiguration()
                    .put(KubernetesOperatorConfigOptions.SAVEPOINT_ON_DELETION.key(), "true");
            job.getSpec()
                    .getFlinkConfiguration()
                    .put(KubernetesOperatorConfigOptions.DRAIN_ON_SAVEPOINT_DELETION.key(), "true");
        }
        var deployConf = configManager.getSessionJobConfig(session, job.getSpec());

        flinkService.cancelSessionJob(job, UpgradeMode.SAVEPOINT, deployConf);
        assertTrue(stopWithSavepointFuture.isDone());
        assertEquals(jobID, stopWithSavepointFuture.get().f0);
        assertEquals(savepointPath, jobStatus.getSavepointInfo().getLastSavepoint().getLocation());
        assertEquals(jobStatus.getState(), org.apache.flink.api.common.JobStatus.FINISHED.name());

        if (drainOnSavepoint) {
            assertTrue(stopWithSavepointFuture.get().f1);
        } else {
            assertFalse(stopWithSavepointFuture.get().f1);
        }
    }

    @Test
    public void cancelJobWithLastStateUpgradeModeTest() throws Exception {
        var deployment = TestUtils.buildApplicationCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        var testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        var flinkService = new TestingService(testingClusterClient);

        JobID jobID = JobID.generate();
        JobStatus jobStatus = deployment.getStatus().getJobStatus();
        jobStatus.setJobId(jobID.toHexString());

        flinkService.cancelJob(
                deployment,
                UpgradeMode.LAST_STATE,
                configManager.getObserveConfig(deployment),
                false);
        assertNull(jobStatus.getSavepointInfo().getLastSavepoint());
    }

    @Test
    public void deletionPropagationTest() {
        var propagation = new ArrayList<DeletionPropagation>();
        TestingService flinkService =
                new TestingService(null) {
                    @Override
                    protected void deleteClusterInternal(
                            ObjectMeta meta,
                            Configuration conf,
                            boolean deleteHaData,
                            DeletionPropagation deletionPropagation) {
                        propagation.add(deletionPropagation);
                    }
                };

        flinkService.deleteClusterDeployment(
                new ObjectMeta(), new FlinkDeploymentStatus(), configuration, true);
        assertEquals(DeletionPropagation.FOREGROUND, propagation.get(0));

        configuration.set(
                KubernetesOperatorConfigOptions.RESOURCE_DELETION_PROPAGATION,
                DeletionPropagation.BACKGROUND);
        operatorConfig = FlinkOperatorConfiguration.fromConfiguration(configuration);

        flinkService =
                new TestingService(null) {
                    @Override
                    protected void deleteClusterInternal(
                            ObjectMeta meta,
                            Configuration conf,
                            boolean deleteHaData,
                            DeletionPropagation deletionPropagation) {
                        propagation.add(deletionPropagation);
                    }
                };
        flinkService.deleteClusterDeployment(
                new ObjectMeta(), new FlinkDeploymentStatus(), configuration, true);
        assertEquals(DeletionPropagation.BACKGROUND, propagation.get(1));
    }

    @Test
    public void triggerSavepointTest() throws Exception {
        CompletableFuture<Tuple3<JobID, String, Boolean>> triggerSavepointFuture =
                new CompletableFuture<>();
        String savepointPath = "file:///path/of/svp";
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointPath);
        var flinkService =
                getTestingService(
                        (headers, parameters, requestBody) -> {
                            triggerSavepointFuture.complete(
                                    new Tuple3<>(
                                            ((SavepointTriggerMessageParameters) parameters)
                                                    .jobID.getValue(),
                                            ((SavepointTriggerRequestBody) requestBody)
                                                    .getTargetDirectory()
                                                    .get(),
                                            ((SavepointTriggerRequestBody) requestBody)
                                                    .isCancelJob()));
                            return CompletableFuture.completedFuture(
                                    new TriggerResponse(new TriggerId()));
                        });

        var jobID = JobID.generate();
        var flinkDeployment = TestUtils.buildApplicationCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(flinkDeployment, new Configuration());
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobId(jobID.toString());
        flinkDeployment.getStatus().setJobStatus(jobStatus);
        flinkService.triggerSavepoint(
                flinkDeployment.getStatus().getJobStatus().getJobId(),
                SnapshotTriggerType.MANUAL,
                flinkDeployment.getStatus().getJobStatus().getSavepointInfo(),
                configuration);
        assertTrue(triggerSavepointFuture.isDone());
        assertEquals(jobID, triggerSavepointFuture.get().f0);
        assertEquals(savepointPath, triggerSavepointFuture.get().f1);
        assertFalse(triggerSavepointFuture.get().f2);
    }

    @Test
    public void testTriggerCheckpoint() throws Exception {
        final CompletableFuture<JobID> triggerCheckpointFuture = new CompletableFuture<>();
        var flinkService =
                getTestingService(
                        (headers, parameters, requestBody) -> {
                            triggerCheckpointFuture.complete(
                                    ((CheckpointTriggerMessageParameters) parameters)
                                            .jobID.getValue());
                            return CompletableFuture.completedFuture(
                                    new TriggerResponse(new TriggerId()));
                        });

        final JobID jobID = JobID.generate();
        final FlinkDeployment flinkDeployment = TestUtils.buildApplicationCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(flinkDeployment, new Configuration());
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobId(jobID.toString());
        flinkDeployment.getStatus().setJobStatus(jobStatus);
        flinkService.triggerCheckpoint(
                flinkDeployment.getStatus().getJobStatus().getJobId(),
                SnapshotTriggerType.MANUAL,
                flinkDeployment.getStatus().getJobStatus().getCheckpointInfo(),
                configuration);
        assertTrue(triggerCheckpointFuture.isDone());
        assertEquals(jobID, triggerCheckpointFuture.get());
    }

    @Test
    public void disposeSavepointTest() throws Exception {
        var savepointPath = "file:///path/of/svp";
        var tested = new AtomicBoolean(false);
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointPath);
        var flinkService =
                getTestingService(
                        (h, p, r) -> {
                            if (r instanceof SavepointDisposalRequest) {
                                var dr = (SavepointDisposalRequest) r;
                                assertEquals(savepointPath, dr.getSavepointPath());
                                tested.set(true);
                                return CompletableFuture.completedFuture(null);
                            }
                            fail("unknown request");
                            return null;
                        });
        flinkService.disposeSavepoint(savepointPath, configuration);
        assertTrue(tested.get());
    }

    @Test
    public void nativeSavepointFormatTest() throws Exception {
        final TestingClusterClient<String> testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        final String savepointPath = "file:///path/of/svp";
        final CompletableFuture<Tuple4<JobID, String, Boolean, SavepointFormatType>>
                triggerSavepointFuture = new CompletableFuture<>();
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointPath);
        testingClusterClient.setRequestProcessor(
                (headers, parameters, requestBody) -> {
                    triggerSavepointFuture.complete(
                            new Tuple4<>(
                                    ((SavepointTriggerMessageParameters) parameters)
                                            .jobID.getValue(),
                                    ((SavepointTriggerRequestBody) requestBody)
                                            .getTargetDirectory()
                                            .get(),
                                    ((SavepointTriggerRequestBody) requestBody).isCancelJob(),
                                    ((SavepointTriggerRequestBody) requestBody).getFormatType()));
                    return CompletableFuture.completedFuture(new TriggerResponse(new TriggerId()));
                });
        final CompletableFuture<Tuple3<JobID, SavepointFormatType, String>>
                stopWithSavepointFuture = new CompletableFuture<>();
        testingClusterClient.setStopWithSavepointFormat(
                (id, formatType, savepointDir) -> {
                    stopWithSavepointFuture.complete(new Tuple3<>(id, formatType, savepointDir));
                    return CompletableFuture.completedFuture(savepointPath);
                });

        var flinkService = new TestingService(testingClusterClient);

        final JobID jobID = JobID.generate();
        final FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), savepointPath);
        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        JobStatus jobStatus = deployment.getStatus().getJobStatus();
        jobStatus.setJobId(jobID.toHexString());
        jobStatus.setState(org.apache.flink.api.common.JobStatus.RUNNING.name());
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        jobStatus.setJobId(jobID.toString());
        deployment.getStatus().setJobStatus(jobStatus);
        flinkService.triggerSavepoint(
                deployment.getStatus().getJobStatus().getJobId(),
                SnapshotTriggerType.MANUAL,
                deployment.getStatus().getJobStatus().getSavepointInfo(),
                new Configuration(configuration)
                        .set(OPERATOR_SAVEPOINT_FORMAT_TYPE, SavepointFormatType.NATIVE));
        assertTrue(triggerSavepointFuture.isDone());
        assertEquals(jobID, triggerSavepointFuture.get().f0);
        assertEquals(savepointPath, triggerSavepointFuture.get().f1);
        assertFalse(triggerSavepointFuture.get().f2);
        assertEquals(SavepointFormatType.NATIVE, triggerSavepointFuture.get().f3);

        flinkService.cancelJob(
                deployment,
                UpgradeMode.SAVEPOINT,
                new Configuration(configManager.getObserveConfig(deployment))
                        .set(OPERATOR_SAVEPOINT_FORMAT_TYPE, SavepointFormatType.NATIVE),
                false);
        assertTrue(stopWithSavepointFuture.isDone());
        assertEquals(jobID, stopWithSavepointFuture.get().f0);
        assertEquals(SavepointFormatType.NATIVE, stopWithSavepointFuture.get().f1);
        assertEquals(savepointPath, stopWithSavepointFuture.get().f2);
    }

    @Test
    public void getLastCheckpointTest() throws Exception {
        ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

        var responseContainer = new ArrayList<CheckpointHistoryWrapper>();
        var flinkService =
                getTestingService(
                        (headers, parameters, requestBody) -> {
                            if (headers instanceof CustomCheckpointingStatisticsHeaders) {
                                return CompletableFuture.completedFuture(responseContainer.get(0));
                            }
                            fail("unknown request");
                            return null;
                        });

        String responseWithHistory =
                "{\"counts\":{\"restored\":1,\"total\":79,\"in_progress\":0,\"completed\":69,\"failed\":10},\"summary\":{\"checkpointed_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"state_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"end_to_end_duration\":{\"min\":14,\"max\":117,\"avg\":24,\"p50\":22,\"p90\":32,\"p95\":40.5,\"p99\":117,\"p999\":117},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0},\"processed_data\":{\"min\":0,\"max\":1274,\"avg\":280,\"p50\":112,\"p90\":840,\"p95\":1071,\"p99\":1274,\"p999\":1274},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0}},\"latest\":{\"completed\":{\"className\":\"completed\",\"id\":96,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212837604,\"latest_ack_timestamp\":1653212837621,\"checkpointed_size\":28437,\"state_size\":28437,\"end_to_end_duration\":17,\"alignment_buffered\":0,\"processed_data\":560,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-96\",\"discarded\":false},\"savepoint\":{\"className\":\"completed\",\"id\":51,\"status\":\"COMPLETED\",\"is_savepoint\":true,\"trigger_timestamp\":1653212748176,\"latest_ack_timestamp\":1653212748233,\"checkpointed_size\":53670,\"state_size\":53670,\"end_to_end_duration\":57,\"alignment_buffered\":0,\"processed_data\":483,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"SAVEPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/savepoints/savepoint-000000-e8ea2482ce4f\",\"discarded\":false},\"failed\":null,\"restored\":{\"id\":27,\"restore_timestamp\":1653212683022,\"is_savepoint\":true,\"external_path\":\"file:/flink-data/savepoints/savepoint-000000-5930e5326ca7\"}},\"history\":[{\"className\":\"completed\",\"id\":96,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212837604,\"latest_ack_timestamp\":1653212837621,\"checkpointed_size\":28437,\"state_size\":28437,\"end_to_end_duration\":17,\"alignment_buffered\":0,\"processed_data\":560,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-96\",\"discarded\":false},{\"className\":\"completed\",\"id\":95,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212835603,\"latest_ack_timestamp\":1653212835622,\"checkpointed_size\":28473,\"state_size\":28473,\"end_to_end_duration\":19,\"alignment_buffered\":0,\"processed_data\":42,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-95\",\"discarded\":true},{\"className\":\"completed\",\"id\":94,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212833603,\"latest_ack_timestamp\":1653212833623,\"checkpointed_size\":27969,\"state_size\":27969,\"end_to_end_duration\":20,\"alignment_buffered\":0,\"processed_data\":28,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-94\",\"discarded\":true},{\"className\":\"completed\",\"id\":93,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212831603,\"latest_ack_timestamp\":1653212831621,\"checkpointed_size\":28113,\"state_size\":28113,\"end_to_end_duration\":18,\"alignment_buffered\":0,\"processed_data\":138,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-93\",\"discarded\":true},{\"className\":\"completed\",\"id\":92,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212829603,\"latest_ack_timestamp\":1653212829621,\"checkpointed_size\":28293,\"state_size\":28293,\"end_to_end_duration\":18,\"alignment_buffered\":0,\"processed_data\":196,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-92\",\"discarded\":true},{\"className\":\"completed\",\"id\":91,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212827603,\"latest_ack_timestamp\":1653212827629,\"checkpointed_size\":27969,\"state_size\":27969,\"end_to_end_duration\":26,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-91\",\"discarded\":true},{\"className\":\"completed\",\"id\":90,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212825603,\"latest_ack_timestamp\":1653212825641,\"checkpointed_size\":27735,\"state_size\":27735,\"end_to_end_duration\":38,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-90\",\"discarded\":true},{\"className\":\"completed\",\"id\":89,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212823603,\"latest_ack_timestamp\":1653212823618,\"checkpointed_size\":28545,\"state_size\":28545,\"end_to_end_duration\":15,\"alignment_buffered\":0,\"processed_data\":364,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-89\",\"discarded\":true},{\"className\":\"completed\",\"id\":88,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212821603,\"latest_ack_timestamp\":1653212821619,\"checkpointed_size\":28275,\"state_size\":28275,\"end_to_end_duration\":16,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-88\",\"discarded\":true},{\"className\":\"completed\",\"id\":87,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212819604,\"latest_ack_timestamp\":1653212819622,\"checkpointed_size\":28518,\"state_size\":28518,\"end_to_end_duration\":18,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-87\",\"discarded\":true}]}";
        String responseWithoutHistory =
                "{\"counts\":{\"restored\":1,\"total\":79,\"in_progress\":0,\"completed\":69,\"failed\":10},\"summary\":{\"checkpointed_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"state_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"end_to_end_duration\":{\"min\":14,\"max\":117,\"avg\":24,\"p50\":22,\"p90\":32,\"p95\":40.5,\"p99\":117,\"p999\":117},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0},\"processed_data\":{\"min\":0,\"max\":1274,\"avg\":280,\"p50\":112,\"p90\":840,\"p95\":1071,\"p99\":1274,\"p999\":1274},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0}},\"latest\":{\"completed\":null,\"savepoint\":null,\"failed\":null,\"restored\":{\"id\":27,\"restore_timestamp\":1653212683022,\"is_savepoint\":true,\"external_path\":\"file:/flink-data/savepoints/savepoint-000000-5930e5326ca7\"}},\"history\":[]}";
        String responseWithoutHistoryInternal =
                "{\"counts\":{\"restored\":1,\"total\":79,\"in_progress\":0,\"completed\":69,\"failed\":10},\"summary\":{\"checkpointed_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"state_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"end_to_end_duration\":{\"min\":14,\"max\":117,\"avg\":24,\"p50\":22,\"p90\":32,\"p95\":40.5,\"p99\":117,\"p999\":117},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0},\"processed_data\":{\"min\":0,\"max\":1274,\"avg\":280,\"p50\":112,\"p90\":840,\"p95\":1071,\"p99\":1274,\"p999\":1274},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0}},\"latest\":{\"completed\":null,\"savepoint\":null,\"failed\":null,\"restored\":{\"id\":27,\"restore_timestamp\":1653212683022,\"is_savepoint\":true,\"external_path\":\"<checkpoint-not-externally-addressable>\"}},\"history\":[]}";

        responseContainer.add(
                objectMapper.readValue(responseWithHistory, CheckpointHistoryWrapper.class));
        var checkpointOpt = flinkService.getLastCheckpoint(new JobID(), new Configuration());
        assertEquals(
                "file:/flink-data/checkpoints/00000000000000000000000000000000/chk-96",
                checkpointOpt.get().getLocation());

        responseContainer.set(
                0, objectMapper.readValue(responseWithoutHistory, CheckpointHistoryWrapper.class));
        checkpointOpt = flinkService.getLastCheckpoint(new JobID(), new Configuration());
        assertEquals(
                "file:/flink-data/savepoints/savepoint-000000-5930e5326ca7",
                checkpointOpt.get().getLocation());

        responseContainer.set(
                0,
                objectMapper.readValue(
                        responseWithoutHistoryInternal, CheckpointHistoryWrapper.class));
        try {
            flinkService.getLastCheckpoint(new JobID(), new Configuration());
            fail();
        } catch (RecoveryFailureException dpe) {

        }
    }

    @Test
    public void fetchSavepointInfoTest() throws Exception {
        var triggerId = new TriggerId();
        var jobId = new JobID();
        var response = new AtomicReference<AsynchronousOperationResult<SavepointInfo>>();
        var flinkService =
                getTestingService(
                        (h, p, r) -> {
                            if (p instanceof SavepointStatusMessageParameters) {
                                var params = (SavepointStatusMessageParameters) p;
                                assertEquals(jobId, params.jobIdPathParameter.getValue());
                                assertEquals(triggerId, params.triggerIdPathParameter.getValue());
                                if (response.get() == null) {
                                    return CompletableFuture.failedFuture(new Exception("fail"));
                                }
                                return CompletableFuture.completedFuture(response.get());
                            }
                            fail("unknown request");
                            return null;
                        });

        response.set(AsynchronousOperationResult.completed(new SavepointInfo("l", null)));
        assertEquals(
                SavepointFetchResult.completed("l"),
                flinkService.fetchSavepointInfo(
                        triggerId.toString(), jobId.toString(), configuration));

        response.set(AsynchronousOperationResult.inProgress());
        assertEquals(
                SavepointFetchResult.pending(),
                flinkService.fetchSavepointInfo(
                        triggerId.toString(), jobId.toString(), configuration));

        response.set(
                AsynchronousOperationResult.completed(
                        new SavepointInfo(
                                null, new SerializedThrowable(new Exception("testErr")))));
        assertTrue(
                flinkService
                        .fetchSavepointInfo(triggerId.toString(), jobId.toString(), configuration)
                        .getError()
                        .contains("testErr"));

        response.set(null);
        assertTrue(
                flinkService
                        .fetchSavepointInfo(triggerId.toString(), jobId.toString(), configuration)
                        .getError()
                        .contains("fail"));
    }

    @Test
    public void fetchCheckpointInfoTest() throws Exception {
        var triggerId = new TriggerId();
        var jobId = new JobID();
        var response = new AtomicReference<AsynchronousOperationResult<CheckpointInfo>>();
        var flinkService =
                getTestingService(
                        (h, p, r) -> {
                            if (p instanceof CheckpointStatusMessageParameters) {
                                var params = (CheckpointStatusMessageParameters) p;
                                assertEquals(jobId, params.jobIdPathParameter.getValue());
                                assertEquals(triggerId, params.triggerIdPathParameter.getValue());
                                if (response.get() == null) {
                                    return CompletableFuture.failedFuture(new Exception("fail"));
                                }
                                return CompletableFuture.completedFuture(response.get());
                            }
                            fail("unknown request");
                            return null;
                        });

        response.set(AsynchronousOperationResult.completed(new CheckpointInfo(123L, null)));
        assertEquals(
                CheckpointFetchResult.completed(),
                flinkService.fetchCheckpointInfo(
                        triggerId.toString(), jobId.toString(), configuration));

        response.set(AsynchronousOperationResult.inProgress());
        assertEquals(
                CheckpointFetchResult.pending(),
                flinkService.fetchCheckpointInfo(
                        triggerId.toString(), jobId.toString(), configuration));

        response.set(
                AsynchronousOperationResult.completed(
                        new CheckpointInfo(
                                null, new SerializedThrowable(new Exception("testErr")))));
        assertTrue(
                flinkService
                        .fetchCheckpointInfo(triggerId.toString(), jobId.toString(), configuration)
                        .getError()
                        .contains("testErr"));

        response.set(null);
        assertTrue(
                flinkService
                        .fetchCheckpointInfo(triggerId.toString(), jobId.toString(), configuration)
                        .getError()
                        .contains("fail"));
    }

    @Test
    public void removeOperatorConfigTest() {
        var key = "kubernetes.operator.meyKey";
        var deployConfig = Configuration.fromMap(Map.of("kubernetes.operator.meyKey", "v"));
        var newConf = AbstractFlinkService.removeOperatorConfigs(deployConfig);
        assertFalse(newConf.containsKey(key));
    }

    @Test
    public void getMetricsTest() throws Exception {
        var jobId = new JobID();
        var metricNames = List.of("m1", "m2");
        var flinkService =
                getTestingService(
                        (h, p, r) -> {
                            if (p instanceof JobMetricsMessageParameters) {
                                var jmmp = ((JobMetricsMessageParameters) p);
                                assertEquals(jobId, jmmp.jobPathParameter.getValue());
                                var output =
                                        jmmp.metricsFilterParameter.getValue().stream()
                                                .map(s -> new Metric(s, s))
                                                .collect(Collectors.toList());
                                return CompletableFuture.completedFuture(
                                        new MetricCollectionResponseBody(output));
                            }
                            fail("unknown request");
                            return null;
                        });
        assertEquals(
                Map.of("m1", "m1", "m2", "m2"),
                flinkService.getMetrics(configuration, jobId.toHexString(), metricNames));
    }

    @Test
    public void getClusterInfoTest() throws Exception {
        var config = new CustomDashboardConfiguration();
        var testVersion = "testVersion";
        var testRevision = "testRevision";
        config.setFlinkVersion(testVersion);
        config.setFlinkRevision(testRevision);

        var tmInfo =
                new TaskManagerInfo(
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
        var tmsInfo = new TaskManagersInfo(List.of(tmInfo));

        var flinkService =
                getTestingService(
                        (h, p, r) -> {
                            if (h instanceof CustomDashboardConfigurationHeaders) {
                                return CompletableFuture.completedFuture(config);
                            } else if (h instanceof TaskManagersHeaders) {
                                return CompletableFuture.completedFuture(tmsInfo);
                            }
                            fail("unknown request");
                            return null;
                        });

        var conf = new Configuration();
        conf.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1000));
        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1000));

        assertEquals(
                Map.of(
                        DashboardConfiguration.FIELD_NAME_FLINK_VERSION,
                        testVersion,
                        DashboardConfiguration.FIELD_NAME_FLINK_REVISION,
                        testRevision,
                        AbstractFlinkService.FIELD_NAME_TOTAL_CPU,
                        "2.0",
                        AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY,
                        "" + MemorySize.ofMebiBytes(1000).getBytes() * 2),
                flinkService.getClusterInfo(conf));
    }

    @Test
    public void effectiveStatusTest() {
        JobDetails allRunning =
                getJobDetails(
                        org.apache.flink.api.common.JobStatus.RUNNING,
                        Tuple2.of(ExecutionState.RUNNING, 4));
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                AbstractFlinkService.getEffectiveStatus(allRunning));

        JobDetails allRunningOrFinished =
                getJobDetails(
                        org.apache.flink.api.common.JobStatus.RUNNING,
                        Tuple2.of(ExecutionState.RUNNING, 2),
                        Tuple2.of(ExecutionState.FINISHED, 2));
        assertEquals(
                org.apache.flink.api.common.JobStatus.RUNNING,
                AbstractFlinkService.getEffectiveStatus(allRunningOrFinished));

        JobDetails allRunningOrScheduled =
                getJobDetails(
                        org.apache.flink.api.common.JobStatus.RUNNING,
                        Tuple2.of(ExecutionState.RUNNING, 2),
                        Tuple2.of(ExecutionState.SCHEDULED, 2));
        assertEquals(
                org.apache.flink.api.common.JobStatus.CREATED,
                AbstractFlinkService.getEffectiveStatus(allRunningOrScheduled));

        JobDetails allFinished =
                getJobDetails(
                        org.apache.flink.api.common.JobStatus.FINISHED,
                        Tuple2.of(ExecutionState.FINISHED, 4));
        assertEquals(
                org.apache.flink.api.common.JobStatus.FINISHED,
                AbstractFlinkService.getEffectiveStatus(allFinished));
    }

    private JobDetails getJobDetails(
            org.apache.flink.api.common.JobStatus status,
            Tuple2<ExecutionState, Integer>... tasksPerState) {
        int[] countPerState = new int[ExecutionState.values().length];
        for (var taskPerState : tasksPerState) {
            countPerState[taskPerState.f0.ordinal()] = taskPerState.f1;
        }
        int numTasks = Arrays.stream(countPerState).sum();
        return new JobDetails(
                new JobID(),
                "test-job",
                System.currentTimeMillis(),
                -1,
                0,
                status,
                System.currentTimeMillis(),
                countPerState,
                numTasks);
    }

    @Test
    public void isJobManagerReadyTest() throws Exception {
        AtomicReference<String> url = new AtomicReference<>();
        var clusterClient =
                new TestingClusterClient<String>(configuration) {
                    @Override
                    public String getWebInterfaceURL() {
                        return url.get();
                    }
                };
        var flinkService = new TestingService(clusterClient);

        assertThrows(
                FlinkRuntimeException.class,
                () -> flinkService.isJobManagerPortReady(configuration));

        int port = 6868;
        url.set("http://127.0.0.1:" + port);

        assertFalse(flinkService.isJobManagerPortReady(configuration));
        try (var socket = new ServerSocket(port)) {
            assertTrue(flinkService.isJobManagerPortReady(configuration));
        }
    }

    class TestingService extends AbstractFlinkService {

        RestClusterClient<String> clusterClient;
        RestClient restClient;
        List<ObjectMeta> deleted = new ArrayList<>();

        Map<Tuple2<String, String>, PodList> jmPods = new HashMap<>();
        Map<Tuple2<String, String>, PodList> tmPods = new HashMap<>();

        TestingService(RestClusterClient<String> clusterClient) {
            this(clusterClient, null);
        }

        TestingService(RestClusterClient<String> clusterClient, RestClient restClient) {
            super(
                    client,
                    AbstractFlinkServiceTest.this.artifactManager,
                    AbstractFlinkServiceTest.this.executorService,
                    AbstractFlinkServiceTest.this.operatorConfig);
            this.clusterClient = clusterClient;
            this.restClient = restClient;
        }

        @Override
        public RestClusterClient<String> getClusterClient(Configuration config) {
            return clusterClient;
        }

        @Override
        protected RestClient getRestClient(Configuration conf) throws ConfigurationException {
            return restClient;
        }

        @Override
        protected PodList getJmPodList(String namespace, String clusterId) {
            return jmPods.getOrDefault(Tuple2.of(namespace, clusterId), new PodList());
        }

        @Override
        protected PodList getTmPodList(String namespace, String clusterId) {
            return tmPods.getOrDefault(Tuple2.of(namespace, clusterId), new PodList());
        }

        @Override
        protected void deployApplicationCluster(JobSpec jobSpec, Configuration conf) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deploySessionCluster(Configuration conf) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancelJob(
                FlinkDeployment deployment, UpgradeMode upgradeMode, Configuration conf) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScalingResult scale(FlinkResourceContext<?> resourceContext, Configuration conf) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean scalingCompleted(FlinkResourceContext<?> resourceContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void deleteClusterInternal(
                ObjectMeta meta,
                Configuration conf,
                boolean deleteHaData,
                DeletionPropagation deletionPropagation) {
            deleted.add(meta);
        }
    }
}
