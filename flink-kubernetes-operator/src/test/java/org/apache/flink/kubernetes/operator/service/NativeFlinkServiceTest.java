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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingClusterClient;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentContext;
import org.apache.flink.kubernetes.operator.exception.RecoveryFailureException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.webmonitor.handlers.JarRunRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadResponseBody;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_PORT;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_FORMAT_TYPE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** @link FlinkService unit tests */
@EnableKubernetesMockClient(crud = true)
public class NativeFlinkServiceTest {
    KubernetesClient client;
    private final Configuration configuration = new Configuration();
    private final FlinkConfigManager configManager = new FlinkConfigManager(configuration);

    private final EventCollector eventCollector = new EventCollector();

    private EventRecorder eventRecorder;
    private FlinkOperatorConfiguration operatorConfig;
    private ExecutorService executorService;

    @BeforeEach
    public void setup() {
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TestUtils.TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);
        configuration.set(FLINK_VERSION, FlinkVersion.v1_15);
        eventRecorder = new EventRecorder(client, eventCollector);
        operatorConfig = FlinkOperatorConfiguration.fromConfiguration(configuration);
        executorService = Executors.newDirectExecutorService();
    }

    @Test
    public void testCancelJobWithStatelessUpgradeMode() throws Exception {
        final TestingClusterClient<String> testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        final CompletableFuture<JobID> cancelFuture = new CompletableFuture<>();
        testingClusterClient.setCancelFunction(
                jobID -> {
                    cancelFuture.complete(jobID);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        final FlinkService flinkService = createFlinkService(testingClusterClient);

        JobID jobID = JobID.generate();
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        JobStatus jobStatus = deployment.getStatus().getJobStatus();
        jobStatus.setJobId(jobID.toHexString());
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        deployment.getStatus().getJobStatus().setState("RUNNING");
        flinkService.cancelJob(
                deployment, UpgradeMode.STATELESS, configManager.getObserveConfig(deployment));
        assertTrue(cancelFuture.isDone());
        assertEquals(jobID, cancelFuture.get());
        assertNull(jobStatus.getSavepointInfo().getLastSavepoint());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void testCancelJobWithSavepointUpgradeMode(FlinkVersion flinkVersion) throws Exception {
        final TestingClusterClient<String> testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        final CompletableFuture<Tuple3<JobID, Boolean, String>> stopWithSavepointFuture =
                new CompletableFuture<>();
        final String savepointPath = "file:///path/of/svp-1";
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointPath);
        testingClusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDir) -> {
                    stopWithSavepointFuture.complete(
                            new Tuple3<>(jobID, advanceToEndOfEventTime, savepointDir));
                    return CompletableFuture.completedFuture(savepointPath);
                });

        final FlinkService flinkService = createFlinkService(testingClusterClient);

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

        deployment.getSpec().setFlinkVersion(flinkVersion);
        flinkService.cancelJob(
                deployment, UpgradeMode.SAVEPOINT, configManager.getObserveConfig(deployment));
        assertTrue(stopWithSavepointFuture.isDone());
        assertEquals(jobID, stopWithSavepointFuture.get().f0);
        assertFalse(stopWithSavepointFuture.get().f1);
        assertEquals(savepointPath, stopWithSavepointFuture.get().f2);
        assertEquals(savepointPath, jobStatus.getSavepointInfo().getLastSavepoint().getLocation());

        assertEquals(jobStatus.getState(), org.apache.flink.api.common.JobStatus.FINISHED.name());
        if (flinkVersion.isNewerVersionThan(FlinkVersion.v1_14)) {
            assertEquals(
                    deployment.getStatus().getJobManagerDeploymentStatus(),
                    JobManagerDeploymentStatus.READY);
        } else {
            assertEquals(
                    deployment.getStatus().getJobManagerDeploymentStatus(),
                    JobManagerDeploymentStatus.MISSING);
        }
    }

    @Test
    public void testCancelJobWithLastStateUpgradeMode() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        final TestingClusterClient<String> testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        final FlinkService flinkService = createFlinkService(testingClusterClient);

        client.resource(createTestingDeployment()).create();

        assertNotNull(
                client.apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .withName(TestUtils.TEST_DEPLOYMENT_NAME)
                        .get());

        JobID jobID = JobID.generate();
        JobStatus jobStatus = deployment.getStatus().getJobStatus();
        jobStatus.setJobId(jobID.toHexString());

        flinkService.cancelJob(
                deployment, UpgradeMode.LAST_STATE, configManager.getObserveConfig(deployment));
        assertNull(jobStatus.getSavepointInfo().getLastSavepoint());
        assertNull(
                client.apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .withName(TestUtils.TEST_DEPLOYMENT_NAME)
                        .get());
    }

    @Test
    public void testTriggerSavepoint() throws Exception {
        final TestingClusterClient<String> testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        final CompletableFuture<Tuple3<JobID, String, Boolean>> triggerSavepointFuture =
                new CompletableFuture<>();
        final String savepointPath = "file:///path/of/svp";
        configuration.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointPath);
        testingClusterClient.setRequestProcessor(
                (headers, parameters, requestBody) -> {
                    triggerSavepointFuture.complete(
                            new Tuple3<>(
                                    ((SavepointTriggerMessageParameters) parameters)
                                            .jobID.getValue(),
                                    ((SavepointTriggerRequestBody) requestBody)
                                            .getTargetDirectory()
                                            .get(),
                                    ((SavepointTriggerRequestBody) requestBody).isCancelJob()));
                    return CompletableFuture.completedFuture(new TriggerResponse(new TriggerId()));
                });

        final FlinkService flinkService = createFlinkService(testingClusterClient);

        final JobID jobID = JobID.generate();
        final FlinkDeployment flinkDeployment = TestUtils.buildApplicationCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(flinkDeployment, new Configuration());
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobId(jobID.toString());
        flinkDeployment.getStatus().setJobStatus(jobStatus);
        flinkService.triggerSavepoint(
                flinkDeployment.getStatus().getJobStatus().getJobId(),
                SavepointTriggerType.MANUAL,
                flinkDeployment.getStatus().getJobStatus().getSavepointInfo(),
                configuration);
        assertTrue(triggerSavepointFuture.isDone());
        assertEquals(jobID, triggerSavepointFuture.get().f0);
        assertEquals(savepointPath, triggerSavepointFuture.get().f1);
        assertFalse(triggerSavepointFuture.get().f2);
    }

    @Test
    public void testGetLastCheckpoint() throws Exception {
        ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
        var testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);

        String responseWithHistory =
                "{\"counts\":{\"restored\":1,\"total\":79,\"in_progress\":0,\"completed\":69,\"failed\":10},\"summary\":{\"checkpointed_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"state_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"end_to_end_duration\":{\"min\":14,\"max\":117,\"avg\":24,\"p50\":22,\"p90\":32,\"p95\":40.5,\"p99\":117,\"p999\":117},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0},\"processed_data\":{\"min\":0,\"max\":1274,\"avg\":280,\"p50\":112,\"p90\":840,\"p95\":1071,\"p99\":1274,\"p999\":1274},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0}},\"latest\":{\"completed\":{\"className\":\"completed\",\"id\":96,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212837604,\"latest_ack_timestamp\":1653212837621,\"checkpointed_size\":28437,\"state_size\":28437,\"end_to_end_duration\":17,\"alignment_buffered\":0,\"processed_data\":560,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-96\",\"discarded\":false},\"savepoint\":{\"className\":\"completed\",\"id\":51,\"status\":\"COMPLETED\",\"is_savepoint\":true,\"trigger_timestamp\":1653212748176,\"latest_ack_timestamp\":1653212748233,\"checkpointed_size\":53670,\"state_size\":53670,\"end_to_end_duration\":57,\"alignment_buffered\":0,\"processed_data\":483,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"SAVEPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/savepoints/savepoint-000000-e8ea2482ce4f\",\"discarded\":false},\"failed\":null,\"restored\":{\"id\":27,\"restore_timestamp\":1653212683022,\"is_savepoint\":true,\"external_path\":\"file:/flink-data/savepoints/savepoint-000000-5930e5326ca7\"}},\"history\":[{\"className\":\"completed\",\"id\":96,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212837604,\"latest_ack_timestamp\":1653212837621,\"checkpointed_size\":28437,\"state_size\":28437,\"end_to_end_duration\":17,\"alignment_buffered\":0,\"processed_data\":560,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-96\",\"discarded\":false},{\"className\":\"completed\",\"id\":95,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212835603,\"latest_ack_timestamp\":1653212835622,\"checkpointed_size\":28473,\"state_size\":28473,\"end_to_end_duration\":19,\"alignment_buffered\":0,\"processed_data\":42,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-95\",\"discarded\":true},{\"className\":\"completed\",\"id\":94,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212833603,\"latest_ack_timestamp\":1653212833623,\"checkpointed_size\":27969,\"state_size\":27969,\"end_to_end_duration\":20,\"alignment_buffered\":0,\"processed_data\":28,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-94\",\"discarded\":true},{\"className\":\"completed\",\"id\":93,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212831603,\"latest_ack_timestamp\":1653212831621,\"checkpointed_size\":28113,\"state_size\":28113,\"end_to_end_duration\":18,\"alignment_buffered\":0,\"processed_data\":138,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-93\",\"discarded\":true},{\"className\":\"completed\",\"id\":92,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212829603,\"latest_ack_timestamp\":1653212829621,\"checkpointed_size\":28293,\"state_size\":28293,\"end_to_end_duration\":18,\"alignment_buffered\":0,\"processed_data\":196,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-92\",\"discarded\":true},{\"className\":\"completed\",\"id\":91,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212827603,\"latest_ack_timestamp\":1653212827629,\"checkpointed_size\":27969,\"state_size\":27969,\"end_to_end_duration\":26,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-91\",\"discarded\":true},{\"className\":\"completed\",\"id\":90,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212825603,\"latest_ack_timestamp\":1653212825641,\"checkpointed_size\":27735,\"state_size\":27735,\"end_to_end_duration\":38,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-90\",\"discarded\":true},{\"className\":\"completed\",\"id\":89,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212823603,\"latest_ack_timestamp\":1653212823618,\"checkpointed_size\":28545,\"state_size\":28545,\"end_to_end_duration\":15,\"alignment_buffered\":0,\"processed_data\":364,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-89\",\"discarded\":true},{\"className\":\"completed\",\"id\":88,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212821603,\"latest_ack_timestamp\":1653212821619,\"checkpointed_size\":28275,\"state_size\":28275,\"end_to_end_duration\":16,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-88\",\"discarded\":true},{\"className\":\"completed\",\"id\":87,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1653212819604,\"latest_ack_timestamp\":1653212819622,\"checkpointed_size\":28518,\"state_size\":28518,\"end_to_end_duration\":18,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-87\",\"discarded\":true}]}";
        String responseWithoutHistory =
                "{\"counts\":{\"restored\":1,\"total\":79,\"in_progress\":0,\"completed\":69,\"failed\":10},\"summary\":{\"checkpointed_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"state_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"end_to_end_duration\":{\"min\":14,\"max\":117,\"avg\":24,\"p50\":22,\"p90\":32,\"p95\":40.5,\"p99\":117,\"p999\":117},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0},\"processed_data\":{\"min\":0,\"max\":1274,\"avg\":280,\"p50\":112,\"p90\":840,\"p95\":1071,\"p99\":1274,\"p999\":1274},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0}},\"latest\":{\"completed\":null,\"savepoint\":null,\"failed\":null,\"restored\":{\"id\":27,\"restore_timestamp\":1653212683022,\"is_savepoint\":true,\"external_path\":\"file:/flink-data/savepoints/savepoint-000000-5930e5326ca7\"}},\"history\":[]}";
        String responseWithoutHistoryInternal =
                "{\"counts\":{\"restored\":1,\"total\":79,\"in_progress\":0,\"completed\":69,\"failed\":10},\"summary\":{\"checkpointed_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"state_size\":{\"min\":23928,\"max\":53670,\"avg\":28551,\"p50\":28239,\"p90\":28563,\"p95\":28635,\"p99\":53670,\"p999\":53670},\"end_to_end_duration\":{\"min\":14,\"max\":117,\"avg\":24,\"p50\":22,\"p90\":32,\"p95\":40.5,\"p99\":117,\"p999\":117},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0},\"processed_data\":{\"min\":0,\"max\":1274,\"avg\":280,\"p50\":112,\"p90\":840,\"p95\":1071,\"p99\":1274,\"p999\":1274},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0}},\"latest\":{\"completed\":null,\"savepoint\":null,\"failed\":null,\"restored\":{\"id\":27,\"restore_timestamp\":1653212683022,\"is_savepoint\":true,\"external_path\":\"<checkpoint-not-externally-addressable>\"}},\"history\":[]}";

        var responseContainer = new ArrayList<CheckpointHistoryWrapper>();

        testingClusterClient.setRequestProcessor(
                (headers, parameters, requestBody) -> {
                    if (headers instanceof CustomCheckpointingStatisticsHeaders) {
                        return CompletableFuture.completedFuture(responseContainer.get(0));
                    }
                    fail("unknown request");
                    return null;
                });

        var flinkService = createFlinkService(testingClusterClient);

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
    public void testGetLastSavepointRestCompatibility() throws JsonProcessingException {
        ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
        String flink14Response =
                "{\"counts\":{\"restored\":0,\"total\":2,\"in_progress\":0,\"completed\":2,\"failed\":0},\"summary\":{\"state_size\":{\"min\":8646,\"max\":25626,\"avg\":17136},\"end_to_end_duration\":{\"min\":95,\"max\":420,\"avg\":257},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0},\"processed_data\":{\"min\":0,\"max\":70,\"avg\":35},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0}},\"latest\":{\"completed\":{\"@class\":\"completed\",\"id\":1,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652347653972,\"latest_ack_timestamp\":1652347654392,\"state_size\":8646,\"end_to_end_duration\":420,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-cp/9f096f515d5d66dbda0d854b5d5a7af2/chk-1\",\"discarded\":true},\"savepoint\":{\"@class\":\"completed\",\"id\":2,\"status\":\"COMPLETED\",\"is_savepoint\":true,\"trigger_timestamp\":1652347655184,\"latest_ack_timestamp\":1652347655279,\"state_size\":25626,\"end_to_end_duration\":95,\"alignment_buffered\":0,\"processed_data\":70,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"SYNC_SAVEPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-sp/savepoint-9f096f-cebc9a861a41\",\"discarded\":false},\"failed\":null,\"restored\":null},\"history\":[{\"@class\":\"completed\",\"id\":2,\"status\":\"COMPLETED\",\"is_savepoint\":true,\"trigger_timestamp\":1652347655184,\"latest_ack_timestamp\":1652347655279,\"state_size\":25626,\"end_to_end_duration\":95,\"alignment_buffered\":0,\"processed_data\":70,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"SYNC_SAVEPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-sp/savepoint-9f096f-cebc9a861a41\",\"discarded\":false},{\"@class\":\"completed\",\"id\":1,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652347653972,\"latest_ack_timestamp\":1652347654392,\"state_size\":8646,\"end_to_end_duration\":420,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-cp/9f096f515d5d66dbda0d854b5d5a7af2/chk-1\",\"discarded\":true}]}";
        String flink15Response =
                "{\"counts\":{\"restored\":0,\"total\":12,\"in_progress\":0,\"completed\":3,\"failed\":9},\"summary\":{\"checkpointed_size\":{\"min\":4308,\"max\":16053,\"avg\":11856,\"p50\":15207,\"p90\":16053,\"p95\":16053,\"p99\":16053,\"p999\":16053},\"state_size\":{\"min\":4308,\"max\":16053,\"avg\":11856,\"p50\":15207,\"p90\":16053,\"p95\":16053,\"p99\":16053,\"p999\":16053},\"end_to_end_duration\":{\"min\":31,\"max\":117,\"avg\":61,\"p50\":36,\"p90\":117,\"p95\":117,\"p99\":117,\"p999\":117},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0},\"processed_data\":{\"min\":0,\"max\":1232,\"avg\":448,\"p50\":112,\"p90\":1232,\"p95\":1232,\"p99\":1232,\"p999\":1232},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0}},\"latest\":{\"completed\":{\"className\":\"completed\",\"id\":3,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348962111,\"latest_ack_timestamp\":1652348962142,\"checkpointed_size\":15207,\"state_size\":15207,\"end_to_end_duration\":31,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-3\",\"discarded\":true},\"savepoint\":null,\"failed\":null,\"restored\":null},\"history\":[{\"className\":\"completed\",\"id\":3,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348962111,\"latest_ack_timestamp\":1652348962142,\"checkpointed_size\":15207,\"state_size\":15207,\"end_to_end_duration\":31,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-3\",\"discarded\":true},{\"className\":\"completed\",\"id\":2,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348960109,\"latest_ack_timestamp\":1652348960145,\"checkpointed_size\":16053,\"state_size\":16053,\"end_to_end_duration\":36,\"alignment_buffered\":0,\"processed_data\":1232,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-2\",\"discarded\":true},{\"className\":\"completed\",\"id\":1,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348958109,\"latest_ack_timestamp\":1652348958226,\"checkpointed_size\":4308,\"state_size\":4308,\"end_to_end_duration\":117,\"alignment_buffered\":0,\"processed_data\":112,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-1\",\"discarded\":true}]}";

        objectMapper.readValue(flink14Response, CheckpointHistoryWrapper.class);
        objectMapper.readValue(flink15Response, CheckpointHistoryWrapper.class);
    }

    @Test
    public void testGetInProgressCheckpointsFromResponseWithoutHistoryDetails()
            throws JsonProcessingException {
        ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
        String response =
                "{\"counts\":{\"restored\":0,\"total\":2,\"in_progress\":0,\"completed\":2,\"failed\":0}}";
        var checkpointHistoryWrapper =
                objectMapper.readValue(response, CheckpointHistoryWrapper.class);
        Optional<CheckpointHistoryWrapper.PendingCheckpointInfo> optionalPendingCheckpointInfo =
                assertDoesNotThrow(checkpointHistoryWrapper::getInProgressCheckpoint);
        assertTrue(optionalPendingCheckpointInfo.isEmpty());
    }

    @Test
    public void testGetInProgressCheckpointsWithoutHistory() {
        CheckpointHistoryWrapper checkpointHistoryWrapper = new CheckpointHistoryWrapper();
        Optional<CheckpointHistoryWrapper.PendingCheckpointInfo> optionalPendingCheckpointInfo =
                assertDoesNotThrow(checkpointHistoryWrapper::getInProgressCheckpoint);
        assertTrue(optionalPendingCheckpointInfo.isEmpty());
    }

    @Test
    public void testClusterInfoRestCompatibility() throws JsonProcessingException {
        ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

        String flink13Response =
                "{\"refresh-interval\":3000,\"timezone-name\":\"Coordinated Universal Time\",\"timezone-offset\":0,\"flink-version\":\"1.13.6\",\"flink-revision\":\"b2ca390 @ 2022-02-03T14:54:22+01:00\",\"features\":{\"web-submit\":false}}";
        String flink14Response =
                "{\"refresh-interval\":3000,\"timezone-name\":\"Coordinated Universal Time\",\"timezone-offset\":0,\"flink-version\":\"1.14.4\",\"flink-revision\":\"895c609 @ 2022-02-25T11:57:14+01:00\",\"features\":{\"web-submit\":false,\"web-cancel\":false}}";

        var dashboardConfiguration =
                objectMapper.readValue(flink13Response, CustomDashboardConfiguration.class);
        dashboardConfiguration =
                objectMapper.readValue(flink14Response, CustomDashboardConfiguration.class);
    }

    @Test
    public void testRemoveOperatorConfig() {
        Configuration deployConfig = createOperatorConfig();
        Configuration newConf = AbstractFlinkService.removeOperatorConfigs(deployConfig);
        assertFalse(newConf.containsKey(OPERATOR_HEALTH_PROBE_PORT.key()));
    }

    @Test
    public void testSubmitApplicationClusterConfigRemoval() throws Exception {
        var testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        Configuration deployConfig = createOperatorConfig();
        final FlinkDeployment deployment = TestUtils.buildApplicationCluster();

        var flinkService = (NativeFlinkService) createFlinkService(testingClusterClient);
        var testingService = new TestingNativeFlinkService(flinkService);
        testingService.submitApplicationCluster(deployment.getSpec().getJob(), deployConfig, false);
        assertFalse(
                testingService.getRuntimeConfig().containsKey(OPERATOR_HEALTH_PROBE_PORT.key()));
    }

    @Test
    public void testSubmitSessionClusterConfigRemoval() throws Exception {
        var testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        Configuration deployConfig = createOperatorConfig();
        var flinkService = (NativeFlinkService) createFlinkService(testingClusterClient);
        var testingService = new TestingNativeFlinkService(flinkService);
        testingService.submitSessionCluster(deployConfig);
        assertFalse(
                testingService.getRuntimeConfig().containsKey(OPERATOR_HEALTH_PROBE_PORT.key()));
    }

    @Test
    public void testEffectiveStatus() {

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

    @Test
    public void testNativeSavepointFormat() throws Exception {
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

        final FlinkService flinkService = createFlinkService(testingClusterClient);

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
                SavepointTriggerType.MANUAL,
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
                        .set(OPERATOR_SAVEPOINT_FORMAT_TYPE, SavepointFormatType.NATIVE));
        assertTrue(stopWithSavepointFuture.isDone());
        assertEquals(jobID, stopWithSavepointFuture.get().f0);
        assertEquals(SavepointFormatType.NATIVE, stopWithSavepointFuture.get().f1);
        assertEquals(savepointPath, stopWithSavepointFuture.get().f2);
    }

    @Test
    public void testDeletionPropagation() {
        var propagation = new ArrayList<DeletionPropagation>();
        NativeFlinkService flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder) {
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

        flinkService =
                new NativeFlinkService(
                        client,
                        null,
                        executorService,
                        FlinkOperatorConfiguration.fromConfiguration(configuration),
                        eventRecorder) {
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
    public void testSendConfigOnRunJar() throws Exception {
        var jarRuns = new ArrayList<JarRunRequestBody>();
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder) {
                    @Override
                    public RestClusterClient<String> getClusterClient(Configuration conf)
                            throws Exception {
                        var client = new TestingClusterClient<String>(conf);
                        client.setRequestProcessor(
                                (h, p, b) -> {
                                    jarRuns.add((JarRunRequestBody) b);
                                    return CompletableFuture.completedFuture(null);
                                });
                        return client;
                    }

                    @Override
                    protected JarUploadResponseBody uploadJar(
                            ObjectMeta objectMeta, FlinkSessionJobSpec spec, Configuration conf) {
                        return new JarUploadResponseBody("test");
                    }

                    @Override
                    protected void deleteJar(Configuration conf, String jarId) {}
                };

        var session = TestUtils.buildSessionCluster();
        session.getSpec().setFlinkVersion(FlinkVersion.v1_17);
        session.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(session.getSpec(), session);

        var job = TestUtils.buildSessionJob();
        var deployConf = configManager.getSessionJobConfig(session, job.getSpec());
        flinkService.submitJobToSessionCluster(job.getMetadata(), job.getSpec(), deployConf, null);

        // Make sure that deploy conf was passed to jar run
        assertEquals(deployConf.toMap(), jarRuns.get(0).getFlinkConfiguration().toMap());

        session.getSpec().setFlinkVersion(FlinkVersion.v1_16);
        session.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(session.getSpec(), session);

        deployConf = configManager.getSessionJobConfig(session, job.getSpec());
        flinkService.submitJobToSessionCluster(job.getMetadata(), job.getSpec(), deployConf, null);

        assertTrue(jarRuns.get(1).getFlinkConfiguration().toMap().isEmpty());
    }

    @Test
    public void testScaling() throws Exception {
        var v1 = new JobVertexID();
        var v2 = new JobVertexID();

        var current = new ArrayList<Map<JobVertexID, JobVertexResourceRequirements>>();
        current.add(null);
        var updated = new ArrayList<Map<JobVertexID, JobVertexResourceRequirements>>();
        updated.add(null);
        var service =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder) {
                    @Override
                    protected Map<JobVertexID, JobVertexResourceRequirements> getVertexResources(
                            RestClusterClient<String> client,
                            AbstractFlinkResource<?, ?> resource) {
                        return current.get(0);
                    }

                    @Override
                    protected void updateVertexResources(
                            RestClusterClient<String> client,
                            AbstractFlinkResource<?, ?> resource,
                            Map<JobVertexID, JobVertexResourceRequirements> newReqs) {
                        updated.set(0, newReqs);
                    }
                };

        var flinkDep = TestUtils.buildApplicationCluster();
        var spec = flinkDep.getSpec();
        spec.setFlinkVersion(FlinkVersion.v1_18);

        var appConfig = Configuration.fromMap(spec.getFlinkConfiguration());
        appConfig.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        spec.setFlinkConfiguration(appConfig.toMap());
        var reconStatus = flinkDep.getStatus().getReconciliationStatus();
        reconStatus.serializeAndSetLastReconciledSpec(spec, flinkDep);

        appConfig.set(PipelineOptions.PARALLELISM_OVERRIDES, Map.of(v1.toHexString(), "4"));
        spec.setFlinkConfiguration(appConfig.toMap());

        flinkDep.getStatus().getJobStatus().setState("RUNNING");

        current.set(
                0,
                Map.of(
                        v1,
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(1, 1)),
                        v2,
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(2, 2))));
        assertTrue(
                service.scale(
                        new FlinkDeploymentContext(
                                flinkDep,
                                TestUtils.createEmptyContext(),
                                null,
                                configManager,
                                ctx -> service)));
        assertEquals(
                Map.of(
                        v1,
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(4, 4)),
                        v2,
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(2, 2))),
                updated.get(0));

        // Baseline
        testScaleConditionDep(flinkDep, service, d -> {}, true);
        testScaleConditionLastSpec(flinkDep, service, d -> {}, true);

        // Do not scale if config disabled
        testScaleConditionDep(
                flinkDep,
                service,
                d ->
                        d.getSpec()
                                .getFlinkConfiguration()
                                .put(
                                        KubernetesOperatorConfigOptions
                                                .JOB_UPGRADE_INPLACE_SCALING_ENABLED
                                                .key(),
                                        "false"),
                false);

        // Do not scale without adaptive scheduler deployed
        testScaleConditionLastSpec(
                flinkDep,
                service,
                ls ->
                        ls.getFlinkConfiguration()
                                .put(
                                        JobManagerOptions.SCHEDULER.key(),
                                        JobManagerOptions.SchedulerType.Default.name()),
                false);

        // Do not scale without adaptive scheduler deployed
        testScaleConditionLastSpec(
                flinkDep, service, ls -> ls.setFlinkVersion(FlinkVersion.v1_17), false);

        testScaleConditionLastSpec(
                flinkDep, service, ls -> ls.setFlinkVersion(FlinkVersion.v1_18), true);

        // Make sure we only try to rescale non-terminal
        testScaleConditionDep(
                flinkDep, service, d -> d.getStatus().getJobStatus().setState("FAILED"), false);

        testScaleConditionDep(
                flinkDep,
                service,
                d -> d.getStatus().getJobStatus().setState("RECONCILING"),
                false);

        testScaleConditionDep(
                flinkDep, service, d -> d.getStatus().getJobStatus().setState("RUNNING"), true);

        testScaleConditionDep(flinkDep, service, d -> d.getSpec().setJob(null), false);

        // Do not scale if parallelism overrides were removed from an active vertex
        testScaleConditionLastSpec(
                flinkDep,
                service,
                s ->
                        s.getFlinkConfiguration()
                                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), v2 + ":3"),
                false);

        // Scale if parallelism overrides were removed only from a non-active vertex
        testScaleConditionLastSpec(
                flinkDep,
                service,
                s ->
                        s.getFlinkConfiguration()
                                .put(
                                        PipelineOptions.PARALLELISM_OVERRIDES.key(),
                                        v1 + ":1," + new JobVertexID() + ":5"),
                true);

        // Do not scale if parallelism overrides were completely removed out
        var flinkDep2 = ReconciliationUtils.clone(flinkDep);
        flinkDep2
                .getSpec()
                .getFlinkConfiguration()
                .remove(PipelineOptions.PARALLELISM_OVERRIDES.key());
        testScaleConditionLastSpec(
                flinkDep2,
                service,
                s ->
                        s.getFlinkConfiguration()
                                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), v2 + ":3"),
                false);

        // Do not scale if overrides never set
        testScaleConditionDep(
                flinkDep2,
                service,
                d ->
                        flinkDep.getSpec()
                                .getFlinkConfiguration()
                                .remove(PipelineOptions.PARALLELISM_OVERRIDES.key()),
                false);
    }

    private void testScaleConditionDep(
            FlinkDeployment dep,
            NativeFlinkService service,
            Consumer<FlinkDeployment> f,
            boolean shouldScale)
            throws Exception {
        var depCopy = ReconciliationUtils.clone(dep);
        f.accept(depCopy);
        assertEquals(
                shouldScale,
                service.scale(
                        new FlinkDeploymentContext(
                                depCopy,
                                TestUtils.createEmptyContext(),
                                null,
                                configManager,
                                c -> service)));
    }

    private void testScaleConditionLastSpec(
            FlinkDeployment dep,
            NativeFlinkService service,
            Consumer<FlinkDeploymentSpec> f,
            boolean shouldScale)
            throws Exception {
        testScaleConditionDep(
                dep,
                service,
                (FlinkDeployment fd) -> {
                    var reconStatus = fd.getStatus().getReconciliationStatus();
                    var lastReconciledSpec = reconStatus.deserializeLastReconciledSpec();
                    f.accept(lastReconciledSpec);
                    reconStatus.serializeAndSetLastReconciledSpec(lastReconciledSpec, fd);
                },
                shouldScale);
    }

    @Test
    public void testScalingCompleted() throws Exception {
        var v1 = new JobVertexID();
        var v2 = new JobVertexID();

        var testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        var service = (NativeFlinkService) createFlinkService(testingClusterClient);

        var flinkDep = TestUtils.buildApplicationCluster();
        var spec = flinkDep.getSpec();
        spec.setFlinkVersion(FlinkVersion.v1_18);

        var appConfig = Configuration.fromMap(spec.getFlinkConfiguration());
        appConfig.set(
                PipelineOptions.PARALLELISM_OVERRIDES,
                Map.of(v1.toHexString(), "4", v2.toHexString(), "1"));
        var reconStatus = flinkDep.getStatus().getReconciliationStatus();
        spec.setFlinkConfiguration(appConfig.toMap());
        reconStatus.serializeAndSetLastReconciledSpec(spec, flinkDep);
        var jobStatus = flinkDep.getStatus().getJobStatus();
        jobStatus.setJobId(new JobID().toHexString());
        var ctx =
                new FlinkDeploymentContext(
                        flinkDep,
                        TestUtils.createEmptyContext(),
                        null,
                        configManager,
                        c -> service);

        var currentJobDetails = new ArrayList<JobDetailsInfo>();
        currentJobDetails.add(null);

        testingClusterClient.setRequestProcessor(
                (headers, parameters, requestBody) -> {
                    if (headers instanceof JobDetailsHeaders) {
                        return CompletableFuture.completedFuture(currentJobDetails.get(0));
                    }
                    return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
                });

        currentJobDetails.set(0, createJobDetailsFor(List.of()));
        assertFalse(service.scalingCompleted(ctx));

        var ioMetricsInfo = new IOMetricsInfo(0, false, 0, false, 0, false, 0, false, 0L, 0L, 0.);
        var vertexInfos =
                List.of(
                        new JobDetailsInfo.JobVertexDetailsInfo(
                                v1,
                                "",
                                900,
                                1,
                                ExecutionState.RUNNING,
                                0,
                                0,
                                0,
                                Map.of(),
                                ioMetricsInfo),
                        new JobDetailsInfo.JobVertexDetailsInfo(
                                v2,
                                "",
                                900,
                                1,
                                ExecutionState.RUNNING,
                                0,
                                0,
                                0,
                                Map.of(),
                                ioMetricsInfo));
        currentJobDetails.set(0, createJobDetailsFor(vertexInfos));
        assertFalse(service.scalingCompleted(ctx));

        vertexInfos =
                List.of(
                        new JobDetailsInfo.JobVertexDetailsInfo(
                                v1,
                                "",
                                900,
                                4,
                                ExecutionState.RUNNING,
                                0,
                                0,
                                0,
                                Map.of(),
                                ioMetricsInfo),
                        new JobDetailsInfo.JobVertexDetailsInfo(
                                v2,
                                "",
                                900,
                                1,
                                ExecutionState.RUNNING,
                                0,
                                0,
                                0,
                                Map.of(),
                                ioMetricsInfo));
        currentJobDetails.set(0, createJobDetailsFor(vertexInfos));
        assertTrue(service.scalingCompleted(ctx));
    }

    public static JobDetailsInfo createJobDetailsFor(
            List<JobDetailsInfo.JobVertexDetailsInfo> vertexInfos) {
        return new JobDetailsInfo(
                new JobID(),
                "",
                false,
                org.apache.flink.api.common.JobStatus.RUNNING,
                0,
                0,
                0,
                0,
                0,
                Map.of(),
                vertexInfos,
                Map.of(),
                new JobPlanInfo.RawJson(""));
    }

    class TestingNativeFlinkService extends NativeFlinkService {
        private Configuration runtimeConfig;

        public TestingNativeFlinkService(NativeFlinkService nativeFlinkService) {
            super(
                    nativeFlinkService.kubernetesClient,
                    nativeFlinkService.artifactManager,
                    nativeFlinkService.executorService,
                    NativeFlinkServiceTest.this.operatorConfig,
                    eventRecorder);
        }

        @Override
        protected void deployApplicationCluster(JobSpec jobSpec, Configuration conf) {
            this.runtimeConfig = conf;
        }

        @Override
        protected void submitClusterInternal(Configuration conf) throws Exception {
            this.runtimeConfig = conf;
        }

        public Configuration getRuntimeConfig() {
            return runtimeConfig;
        }
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

    private FlinkService createFlinkService(RestClusterClient<String> clusterClient) {
        return new NativeFlinkService(
                client, null, executorService, operatorConfig, eventRecorder) {
            @Override
            public RestClusterClient<String> getClusterClient(Configuration config) {
                return clusterClient;
            }
        };
    }

    private Deployment createTestingDeployment() {
        return new DeploymentBuilder()
                .withNewMetadata()
                .withName(TestUtils.TEST_DEPLOYMENT_NAME)
                .withNamespace(TestUtils.TEST_NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .build();
    }

    private Configuration createOperatorConfig() {
        Map<String, String> configMap = Map.of(OPERATOR_HEALTH_PROBE_PORT.key(), "80");
        Configuration deployConfig = Configuration.fromMap(configMap);

        return deployConfig;
    }
}
