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
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link FlinkService unit tests */
@EnableKubernetesMockClient(crud = true)
public class FlinkServiceTest {
    KubernetesClient client;
    private final Configuration configuration = new Configuration();

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
        ReconciliationUtils.updateForSpecReconciliationSuccess(deployment, JobState.RUNNING);

        deployment.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        deployment.getStatus().getJobStatus().setState("RUNNING");
        flinkService.cancelJob(deployment, UpgradeMode.STATELESS);
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
        ReconciliationUtils.updateForSpecReconciliationSuccess(deployment, JobState.RUNNING);

        flinkService.cancelJob(deployment, UpgradeMode.SAVEPOINT);
        assertTrue(stopWithSavepointFuture.isDone());
        assertEquals(jobID, stopWithSavepointFuture.get().f0);
        assertFalse(stopWithSavepointFuture.get().f1);
        assertEquals(savepointPath, stopWithSavepointFuture.get().f2);
        assertEquals(savepointPath, jobStatus.getSavepointInfo().getLastSavepoint().getLocation());
    }

    @Test
    public void testCancelJobWithLastStateUpgradeMode() throws Exception {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        ReconciliationUtils.updateForSpecReconciliationSuccess(deployment, JobState.RUNNING);
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

        flinkService.cancelJob(deployment, UpgradeMode.LAST_STATE);
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
        testingClusterClient.setTriggerSavepointFunction(
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
        ReconciliationUtils.updateForSpecReconciliationSuccess(flinkDeployment, JobState.RUNNING);
        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobId(jobID.toString());
        flinkDeployment.getStatus().setJobStatus(jobStatus);
        flinkService.triggerSavepoint(
                flinkDeployment.getStatus().getJobStatus().getJobId(),
                flinkDeployment.getStatus().getJobStatus().getSavepointInfo(),
                configuration);
        assertTrue(triggerSavepointFuture.isDone());
        assertEquals(jobID, triggerSavepointFuture.get().f0);
        assertEquals(savepointPath, triggerSavepointFuture.get().f1);
        assertFalse(triggerSavepointFuture.get().f2);
    }

    @Test
    public void testGetLastSavepointRestCompatibility() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String flink14Response =
                "{\"counts\":{\"restored\":0,\"total\":2,\"in_progress\":0,\"completed\":2,\"failed\":0},\"summary\":{\"state_size\":{\"min\":8646,\"max\":25626,\"avg\":17136},\"end_to_end_duration\":{\"min\":95,\"max\":420,\"avg\":257},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0},\"processed_data\":{\"min\":0,\"max\":70,\"avg\":35},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0}},\"latest\":{\"completed\":{\"@class\":\"completed\",\"id\":1,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652347653972,\"latest_ack_timestamp\":1652347654392,\"state_size\":8646,\"end_to_end_duration\":420,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-cp/9f096f515d5d66dbda0d854b5d5a7af2/chk-1\",\"discarded\":true},\"savepoint\":{\"@class\":\"completed\",\"id\":2,\"status\":\"COMPLETED\",\"is_savepoint\":true,\"trigger_timestamp\":1652347655184,\"latest_ack_timestamp\":1652347655279,\"state_size\":25626,\"end_to_end_duration\":95,\"alignment_buffered\":0,\"processed_data\":70,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"SYNC_SAVEPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-sp/savepoint-9f096f-cebc9a861a41\",\"discarded\":false},\"failed\":null,\"restored\":null},\"history\":[{\"@class\":\"completed\",\"id\":2,\"status\":\"COMPLETED\",\"is_savepoint\":true,\"trigger_timestamp\":1652347655184,\"latest_ack_timestamp\":1652347655279,\"state_size\":25626,\"end_to_end_duration\":95,\"alignment_buffered\":0,\"processed_data\":70,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"SYNC_SAVEPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-sp/savepoint-9f096f-cebc9a861a41\",\"discarded\":false},{\"@class\":\"completed\",\"id\":1,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652347653972,\"latest_ack_timestamp\":1652347654392,\"state_size\":8646,\"end_to_end_duration\":420,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/opt/flink/volume/flink-cp/9f096f515d5d66dbda0d854b5d5a7af2/chk-1\",\"discarded\":true}]}";
        String flink15Response =
                "{\"counts\":{\"restored\":0,\"total\":12,\"in_progress\":0,\"completed\":3,\"failed\":9},\"summary\":{\"checkpointed_size\":{\"min\":4308,\"max\":16053,\"avg\":11856,\"p50\":15207,\"p90\":16053,\"p95\":16053,\"p99\":16053,\"p999\":16053},\"state_size\":{\"min\":4308,\"max\":16053,\"avg\":11856,\"p50\":15207,\"p90\":16053,\"p95\":16053,\"p99\":16053,\"p999\":16053},\"end_to_end_duration\":{\"min\":31,\"max\":117,\"avg\":61,\"p50\":36,\"p90\":117,\"p95\":117,\"p99\":117,\"p999\":117},\"alignment_buffered\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0},\"processed_data\":{\"min\":0,\"max\":1232,\"avg\":448,\"p50\":112,\"p90\":1232,\"p95\":1232,\"p99\":1232,\"p999\":1232},\"persisted_data\":{\"min\":0,\"max\":0,\"avg\":0,\"p50\":0,\"p90\":0,\"p95\":0,\"p99\":0,\"p999\":0}},\"latest\":{\"completed\":{\"className\":\"completed\",\"id\":3,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348962111,\"latest_ack_timestamp\":1652348962142,\"checkpointed_size\":15207,\"state_size\":15207,\"end_to_end_duration\":31,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-3\",\"discarded\":true},\"savepoint\":null,\"failed\":null,\"restored\":null},\"history\":[{\"className\":\"completed\",\"id\":3,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348962111,\"latest_ack_timestamp\":1652348962142,\"checkpointed_size\":15207,\"state_size\":15207,\"end_to_end_duration\":31,\"alignment_buffered\":0,\"processed_data\":0,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-3\",\"discarded\":true},{\"className\":\"completed\",\"id\":2,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348960109,\"latest_ack_timestamp\":1652348960145,\"checkpointed_size\":16053,\"state_size\":16053,\"end_to_end_duration\":36,\"alignment_buffered\":0,\"processed_data\":1232,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-2\",\"discarded\":true},{\"className\":\"completed\",\"id\":1,\"status\":\"COMPLETED\",\"is_savepoint\":false,\"trigger_timestamp\":1652348958109,\"latest_ack_timestamp\":1652348958226,\"checkpointed_size\":4308,\"state_size\":4308,\"end_to_end_duration\":117,\"alignment_buffered\":0,\"processed_data\":112,\"persisted_data\":0,\"num_subtasks\":4,\"num_acknowledged_subtasks\":4,\"checkpoint_type\":\"CHECKPOINT\",\"tasks\":{},\"external_path\":\"file:/flink-data/checkpoints/00000000000000000000000000000000/chk-1\",\"discarded\":true}]}";

        objectMapper.readValue(flink14Response, CheckpointHistoryWrapper.class);
        objectMapper.readValue(flink15Response, CheckpointHistoryWrapper.class);
    }

    private FlinkService createFlinkService(ClusterClient<String> clusterClient) {
        return new FlinkService(client, new FlinkConfigManager(configuration)) {
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
