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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingClusterClient;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
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

    @BeforeEach
    public void setup() {
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, TestUtils.TEST_DEPLOYMENT_NAME);
        configuration.set(KubernetesConfigOptions.NAMESPACE, TestUtils.TEST_NAMESPACE);
        configuration.set(FLINK_VERSION, FlinkVersion.v1_15);
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

    @Test
    public void testCancelJobWithSavepointUpgradeMode() throws Exception {
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

        flinkService.cancelJob(
                deployment, UpgradeMode.SAVEPOINT, configManager.getObserveConfig(deployment));
        assertTrue(stopWithSavepointFuture.isDone());
        assertEquals(jobID, stopWithSavepointFuture.get().f0);
        assertFalse(stopWithSavepointFuture.get().f1);
        assertEquals(savepointPath, stopWithSavepointFuture.get().f2);
        assertEquals(savepointPath, jobStatus.getSavepointInfo().getLastSavepoint().getLocation());
    }

    @Test
    public void testCancelJobWithLastStateUpgradeMode() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        final TestingClusterClient<String> testingClusterClient =
                new TestingClusterClient<>(configuration, TestUtils.TEST_DEPLOYMENT_NAME);
        final FlinkService flinkService = createFlinkService(testingClusterClient);

        client.apps()
                .deployments()
                .inNamespace(TestUtils.TEST_NAMESPACE)
                .create(createTestingDeployment());
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
        } catch (DeploymentFailedException dpe) {

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

    private FlinkService createFlinkService(ClusterClient<String> clusterClient) {
        return new NativeFlinkService(client, new FlinkConfigManager(configuration)) {
            @Override
            protected ClusterClient<String> getClusterClient(Configuration config) {
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
}
