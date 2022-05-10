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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingClusterClient;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;

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
        configuration.set(
                HighAvailabilityOptions.HA_MODE,
                KubernetesHaServicesFactory.class.getCanonicalName());
        configuration.set(HighAvailabilityOptions.HA_STORAGE_PATH, "file:///path/of/ha");
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
