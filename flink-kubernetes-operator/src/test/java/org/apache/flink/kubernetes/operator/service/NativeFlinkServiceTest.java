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
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingClusterClient;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @link FlinkService unit tests
 */
@EnableKubernetesMockClient(crud = true)
public class NativeFlinkServiceTest {
    KubernetesClient client;
    KubernetesMockServer mockServer;
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
        eventRecorder = new EventRecorder(eventCollector);
        operatorConfig = FlinkOperatorConfiguration.fromConfiguration(configuration);
        executorService = Executors.newDirectExecutorService();
    }

    @Test
    public void testDeleteClusterInternal() {

        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder) {

                    @Override
                    protected Duration deleteDeploymentBlocking(
                            String name,
                            Resource<Deployment> deployment,
                            DeletionPropagation propagation,
                            Duration timeout) {
                        // Ensure deployment is scaled down before deletion
                        assertEquals(0, deployment.get().getSpec().getReplicas());
                        return super.deleteDeploymentBlocking(
                                name, deployment, propagation, timeout);
                    }
                };

        var deployment = TestUtils.buildApplicationCluster();
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());

        var dep =
                new DeploymentBuilder()
                        .withNewMetadata()
                        .withName(TestUtils.TEST_DEPLOYMENT_NAME)
                        .withNamespace(TestUtils.TEST_NAMESPACE)
                        .endMetadata()
                        .withNewSpec()
                        .withReplicas(1)
                        .endSpec()
                        .build();
        client.resource(dep).create();
        assertNotNull(
                client.apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .withName(TestUtils.TEST_DEPLOYMENT_NAME)
                        .get());

        flinkService.deleteClusterInternal(
                deployment.getMetadata().getNamespace(),
                deployment.getMetadata().getName(),
                configManager.getObserveConfig(deployment),
                DeletionPropagation.FOREGROUND);

        assertNull(
                client.apps()
                        .deployments()
                        .inNamespace(TestUtils.TEST_NAMESPACE)
                        .withName(TestUtils.TEST_DEPLOYMENT_NAME)
                        .get());
    }

    @ParameterizedTest
    @MethodSource("org.apache.flink.kubernetes.operator.TestUtils#flinkVersions")
    public void testDeleteOnSavepointBefore1_15(FlinkVersion flinkVersion) throws Exception {
        AtomicBoolean tested = new AtomicBoolean(false);
        var flinkService =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder) {
                    @Override
                    protected void cancelJob(
                            FlinkDeployment deployment,
                            UpgradeMode upgradeMode,
                            Configuration conf,
                            boolean deleteClusterAfterSavepoint) {
                        assertEquals(false, deleteClusterAfterSavepoint);
                        tested.set(true);
                    }
                };

        flinkService.cancelJob(
                TestUtils.buildApplicationCluster(flinkVersion),
                UpgradeMode.SAVEPOINT,
                new Configuration());
        assertTrue(tested.get());
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
    public void testScaling() throws Exception {
        var v1 = new JobVertexID();
        var v2 = new JobVertexID();

        var current = new AtomicReference<Map<JobVertexID, JobVertexResourceRequirements>>();
        var updated = new AtomicReference<Map<JobVertexID, JobVertexResourceRequirements>>();
        var service =
                new NativeFlinkService(
                        client, null, executorService, operatorConfig, eventRecorder) {
                    @Override
                    protected Map<JobVertexID, JobVertexResourceRequirements> getVertexResources(
                            RestClusterClient<String> client,
                            AbstractFlinkResource<?, ?> resource) {
                        return current.get();
                    }

                    @Override
                    protected void updateVertexResources(
                            RestClusterClient<String> client,
                            AbstractFlinkResource<?, ?> resource,
                            Map<JobVertexID, JobVertexResourceRequirements> newReqs) {
                        updated.set(newReqs);
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

        appConfig.set(
                PipelineOptions.PARALLELISM_OVERRIDES,
                Map.of(v1.toHexString(), "4", v2.toHexString(), "1"));
        spec.setFlinkConfiguration(appConfig.toMap());

        flinkDep.getStatus().getJobStatus().setState("RUNNING");

        current.set(
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
                                TestUtils.createEmptyContextWithClient(client),
                                null,
                                configManager,
                                ctx -> service),
                        configManager.getDeployConfig(flinkDep.getMetadata(), flinkDep.getSpec())));
        assertEquals(
                Map.of(
                        v1,
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(1, 4)),
                        v2,
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(1, 1))),
                updated.get());

        // Baseline
        appConfig.set(PipelineOptions.PARALLELISM_OVERRIDES, Map.of(v1.toHexString(), "4"));
        spec.setFlinkConfiguration(appConfig.toMap());
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

        // Do not scale if parallelism overrides were completely removed
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
                        d.getSpec()
                                .getFlinkConfiguration()
                                .remove(PipelineOptions.PARALLELISM_OVERRIDES.key()),
                false);

        // Do not scale if non active vertices are overridden only
        current.set(
                Map.of(
                        v1,
                        new JobVertexResourceRequirements(
                                new JobVertexResourceRequirements.Parallelism(1, 1))));

        // Only override v2 which is not in the current jobgraph
        updated.set(null);
        testScaleConditionDep(
                flinkDep,
                service,
                d ->
                        d.getSpec()
                                .getFlinkConfiguration()
                                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), v2 + ":5"),
                true);
        assertNull(updated.get());

        // Override v2 (not in graph) + v1 with current parallelism
        testScaleConditionDep(
                flinkDep,
                service,
                d ->
                        d.getSpec()
                                .getFlinkConfiguration()
                                .put(
                                        PipelineOptions.PARALLELISM_OVERRIDES.key(),
                                        v2 + ":5," + v1 + ":1"),
                true);
        assertNull(updated.get());

        // Scale if requirements upper/lower bound doesn't match
        current.set(
                Map.of(
                        v1,
                        new JobVertexResourceRequirements(
                                new JobVertexResourceRequirements.Parallelism(1, 2))));
        testScaleConditionDep(
                flinkDep,
                service,
                d ->
                        d.getSpec()
                                .getFlinkConfiguration()
                                .put(
                                        PipelineOptions.PARALLELISM_OVERRIDES.key(),
                                        v2 + ":5," + v1 + ":1"),
                true);
        assertEquals(
                new JobVertexResourceRequirements.Parallelism(1, 1),
                updated.get().get(v1).getParallelism());

        // Test error handling
        current.set(null);
        testScaleConditionDep(flinkDep, service, d -> {}, false);
    }

    private void testScaleConditionDep(
            FlinkDeployment dep,
            NativeFlinkService service,
            Consumer<FlinkDeployment> f,
            boolean scaled)
            throws Exception {
        var depCopy = ReconciliationUtils.clone(dep);
        f.accept(depCopy);
        assertEquals(
                scaled,
                service.scale(
                        new FlinkDeploymentContext(
                                depCopy,
                                TestUtils.createEmptyContextWithClient(client),
                                null,
                                configManager,
                                c -> service),
                        configManager.getDeployConfig(depCopy.getMetadata(), depCopy.getSpec())));
    }

    private void testScaleConditionLastSpec(
            FlinkDeployment dep,
            NativeFlinkService service,
            Consumer<FlinkDeploymentSpec> f,
            boolean scaled)
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
                scaled);
    }

    private JobDetailsInfo.JobVertexDetailsInfo jobVertexDetailsInfo(
            JobVertexID jvi, int parallelism) {
        var ioMetricsInfo = new IOMetricsInfo(0, false, 0, false, 0, false, 0, false, 0L, 0L, 0.);
        return new JobDetailsInfo.JobVertexDetailsInfo(
                jvi,
                "",
                900,
                parallelism,
                ExecutionState.RUNNING,
                0,
                0,
                0,
                Map.of(),
                ioMetricsInfo);
    }

    @Test
    public void resourceRestApiTest() throws Exception {
        var testingClusterClient = new TestingClusterClient<String>(configuration);
        var service = (NativeFlinkService) createFlinkService(testingClusterClient);
        var jobId = new JobID();

        var reqs =
                new JobResourceRequirements(
                        Map.of(
                                new JobVertexID(),
                                new JobVertexResourceRequirements(
                                        new JobVertexResourceRequirements.Parallelism(0, 2))));

        testingClusterClient.setRequestProcessor(
                (h, p, r) -> {
                    if (h instanceof JobResourceRequirementsHeaders) {
                        if (jobId.equals(((JobMessageParameters) p).jobPathParameter.getValue())) {
                            return CompletableFuture.completedFuture(
                                    new JobResourceRequirementsBody(reqs));
                        }
                    } else if (r instanceof JobResourceRequirementsBody) {
                        if (jobId.equals(((JobMessageParameters) p).jobPathParameter.getValue())) {
                            assertEquals(
                                    Optional.of(reqs),
                                    ((JobResourceRequirementsBody) r).asJobResourceRequirements());
                            return CompletableFuture.completedFuture(null);
                        }
                    }
                    fail("unknown request");
                    return null;
                });

        var deployment = TestUtils.buildApplicationCluster();
        deployment.getStatus().getJobStatus().setJobId(jobId.toString());
        assertEquals(
                reqs.getJobVertexParallelisms(),
                service.getVertexResources(testingClusterClient, deployment));
        service.updateVertexResources(
                testingClusterClient, deployment, reqs.getJobVertexParallelisms());
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

    private AbstractFlinkService createFlinkService(RestClusterClient<String> clusterClient) {
        return new NativeFlinkService(
                client, null, executorService, operatorConfig, eventRecorder) {
            @Override
            public RestClusterClient<String> getClusterClient(Configuration config) {
                return clusterClient;
            }
        };
    }

    private Configuration createOperatorConfig() {
        Map<String, String> configMap = Map.of(OPERATOR_HEALTH_PROBE_PORT.key(), "80");
        Configuration deployConfig = Configuration.fromMap(configMap);

        return deployConfig;
    }
}
