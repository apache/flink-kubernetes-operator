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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.Resource;
import org.apache.flink.kubernetes.operator.crd.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.exception.DeploymentFailedException;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.mockwebserver.utils.ResponseProvider;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedDependentResourceContext;
import okhttp3.Headers;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;

/** Testing utilities. */
public class TestUtils {

    public static final String TEST_NAMESPACE = "flink-operator-test";
    public static final String TEST_DEPLOYMENT_NAME = "test-cluster";
    public static final String TEST_SESSION_JOB_NAME = "test-session-job";
    public static final String SERVICE_ACCOUNT = "flink-operator";
    public static final String FLINK_VERSION = "latest";
    public static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    public static final String IMAGE_POLICY = "IfNotPresent";
    public static final String SAMPLE_JAR = "local:///tmp/sample.jar";

    private static final String TEST_PLUGINS = "test-plugins";
    private static final String PlUGINS_JAR = TEST_PLUGINS + "-test-jar.jar";

    public static FlinkDeployment buildSessionCluster() {
        return buildSessionCluster(FlinkVersion.v1_15);
    }

    public static FlinkDeployment buildSessionCluster(FlinkVersion version) {
        FlinkDeployment deployment = new FlinkDeployment();
        deployment.setStatus(new FlinkDeploymentStatus());
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(TEST_DEPLOYMENT_NAME)
                        .withNamespace(TEST_NAMESPACE)
                        .withCreationTimestamp(Instant.now().toString())
                        .build());
        deployment.setSpec(getTestFlinkDeploymentSpec(version));
        return deployment;
    }

    public static FlinkDeployment buildApplicationCluster(FlinkVersion version) {
        FlinkDeployment deployment = buildSessionCluster(version);
        deployment
                .getSpec()
                .setJob(
                        JobSpec.builder()
                                .jarURI(SAMPLE_JAR)
                                .parallelism(1)
                                .upgradeMode(UpgradeMode.STATELESS)
                                .state(JobState.RUNNING)
                                .build());
        deployment.setStatus(deployment.initStatus());
        return deployment;
    }

    public static FlinkDeployment buildApplicationCluster() {
        return buildApplicationCluster(FlinkVersion.v1_15);
    }

    public static FlinkSessionJob buildSessionJob() {
        FlinkSessionJob sessionJob = new FlinkSessionJob();
        sessionJob.setStatus(new FlinkSessionJobStatus());
        sessionJob.setMetadata(
                new ObjectMetaBuilder()
                        .withName(TEST_SESSION_JOB_NAME)
                        .withNamespace(TEST_NAMESPACE)
                        .withCreationTimestamp(Instant.now().toString())
                        .withUid(UUID.randomUUID().toString())
                        .withGeneration(1L)
                        .build());
        sessionJob.setSpec(
                FlinkSessionJobSpec.builder()
                        .deploymentName(TEST_DEPLOYMENT_NAME)
                        .job(
                                JobSpec.builder()
                                        .jarURI(SAMPLE_JAR)
                                        .parallelism(1)
                                        .upgradeMode(UpgradeMode.STATELESS)
                                        .state(JobState.RUNNING)
                                        .build())
                        .build());
        return sessionJob;
    }

    public static FlinkDeploymentSpec getTestFlinkDeploymentSpec(FlinkVersion version) {
        Map<String, String> conf = new HashMap<>();
        conf.put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "2");
        conf.put(
                HighAvailabilityOptions.HA_MODE.key(),
                KubernetesHaServicesFactory.class.getCanonicalName());
        conf.put(HighAvailabilityOptions.HA_STORAGE_PATH.key(), "test");
        conf.put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), "test-savepoint-dir");
        conf.put(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(), "test-checkpoint-dir");

        return FlinkDeploymentSpec.builder()
                .image(IMAGE)
                .imagePullPolicy(IMAGE_POLICY)
                .serviceAccount(SERVICE_ACCOUNT)
                .flinkVersion(version)
                .flinkConfiguration(conf)
                .jobManager(new JobManagerSpec(new Resource(1.0, "2048m"), 1, null))
                .taskManager(new TaskManagerSpec(new Resource(1.0, "2048m"), null, null))
                .build();
    }

    public static Pod getTestPod(String hostname, String apiVersion, List<Container> containers) {
        final PodSpec podSpec = new PodSpec();
        podSpec.setHostname(hostname);
        podSpec.setContainers(containers);
        final Pod pod = new Pod();
        pod.setApiVersion(apiVersion);
        pod.setSpec(podSpec);
        return pod;
    }

    public static PodList createFailedPodList(String crashLoopMessage) {
        ContainerStatus cs =
                new ContainerStatusBuilder()
                        .withNewState()
                        .withNewWaiting()
                        .withReason(DeploymentFailedException.REASON_CRASH_LOOP_BACKOFF)
                        .withMessage(crashLoopMessage)
                        .endWaiting()
                        .endState()
                        .build();

        Pod pod = TestUtils.getTestPod("host", "apiVersion", Collections.emptyList());
        pod.setStatus(
                new PodStatusBuilder()
                        .withContainerStatuses(Collections.singletonList(cs))
                        .build());
        return new PodListBuilder().withItems(pod).build();
    }

    public static Deployment createDeployment(boolean ready) {
        DeploymentStatus status = new DeploymentStatus();
        status.setAvailableReplicas(ready ? 1 : 0);
        status.setReplicas(1);
        DeploymentSpec spec = new DeploymentSpec();
        spec.setReplicas(1);
        Deployment deployment = new Deployment();
        deployment.setMetadata(new ObjectMeta());
        deployment.setSpec(spec);
        deployment.setStatus(status);
        return deployment;
    }

    public static Context createContextWithDeployment(@Nullable Deployment deployment) {
        return new Context() {
            @Override
            public Optional<RetryInfo> getRetryInfo() {
                return Optional.empty();
            }

            @Override
            public Optional getSecondaryResource(Class expectedType, String eventSourceName) {
                return Optional.ofNullable(deployment);
            }

            @Override
            public Set getSecondaryResources(Class expectedType) {
                return null;
            }

            @Override
            public ControllerConfiguration getControllerConfiguration() {
                return null;
            }

            @Override
            public ManagedDependentResourceContext managedDependentResourceContext() {
                return null;
            }
        };
    }

    public static Context createEmptyContext() {
        return createContextWithDeployment(null);
    }

    public static Context createContextWithReadyJobManagerDeployment() {
        return createContextWithDeployment(createDeployment(true));
    }

    public static Context createContextWithInProgressDeployment() {
        return createContextWithDeployment(createDeployment(false));
    }

    public static Context createContextWithReadyFlinkDeployment() {
        return createContextWithReadyFlinkDeployment(new HashMap<>());
    }

    public static Context createContextWithReadyFlinkDeployment(
            Map<String, String> flinkDepConfig) {
        return new Context() {
            @Override
            public Optional<RetryInfo> getRetryInfo() {
                return Optional.empty();
            }

            @Override
            public Optional getSecondaryResource(Class expectedType, String eventSourceName) {
                var session = buildSessionCluster();
                session.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
                session.getSpec().getFlinkConfiguration().putAll(flinkDepConfig);
                session.getStatus()
                        .getReconciliationStatus()
                        .serializeAndSetLastReconciledSpec(session.getSpec(), session);
                return Optional.of(session);
            }

            @Override
            public Set getSecondaryResources(Class expectedType) {
                return null;
            }

            @Override
            public ControllerConfiguration getControllerConfiguration() {
                return null;
            }

            @Override
            public ManagedDependentResourceContext managedDependentResourceContext() {
                return null;
            }
        };
    }

    public static Context createContextWithNotReadyFlinkDeployment() {
        return new Context() {
            @Override
            public Optional<RetryInfo> getRetryInfo() {
                return Optional.empty();
            }

            @Override
            public Optional getSecondaryResource(Class expectedType, String eventSourceName) {
                var session = buildSessionCluster();
                session.getStatus()
                        .setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
                return Optional.of(session);
            }

            @Override
            public Set getSecondaryResources(Class expectedType) {
                return null;
            }

            @Override
            public ControllerConfiguration getControllerConfiguration() {
                return null;
            }

            @Override
            public ManagedDependentResourceContext managedDependentResourceContext() {
                return null;
            }
        };
    }

    public static final String DEPLOYMENT_ERROR = "test deployment error message";

    public static Context createContextWithFailedJobManagerDeployment() {
        return new Context() {
            @Override
            public Optional<RetryInfo> getRetryInfo() {
                return Optional.empty();
            }

            @Override
            public Optional getSecondaryResource(Class expectedType, String eventSourceName) {
                DeploymentStatus status = new DeploymentStatus();
                status.setAvailableReplicas(0);
                status.setReplicas(1);
                List<DeploymentCondition> conditions =
                        Collections.singletonList(
                                new DeploymentCondition(
                                        null,
                                        null,
                                        DEPLOYMENT_ERROR,
                                        "FailedCreate",
                                        "status",
                                        "ReplicaFailure"));
                status.setConditions(conditions);
                DeploymentSpec spec = new DeploymentSpec();
                spec.setReplicas(1);
                Deployment deployment = new Deployment();
                deployment.setSpec(spec);
                deployment.setStatus(status);
                return Optional.of(deployment);
            }

            @Override
            public Set getSecondaryResources(Class expectedType) {
                return null;
            }

            @Override
            public ControllerConfiguration getControllerConfiguration() {
                return null;
            }

            @Override
            public ManagedDependentResourceContext managedDependentResourceContext() {
                return null;
            }
        };
    }

    public static String getTestPluginsRootDir(Path temporaryFolder) throws IOException {
        File testValidatorFolder = new File(temporaryFolder.toFile(), TEST_PLUGINS);
        assertTrue(testValidatorFolder.mkdirs());
        File testValidatorJar = new File("target", PlUGINS_JAR);
        assertTrue(testValidatorJar.exists());
        Files.copy(
                testValidatorJar.toPath(), Paths.get(testValidatorFolder.toString(), PlUGINS_JAR));

        return temporaryFolder.toAbsolutePath().toString();
    }

    // This code is taken slightly modified from: http://stackoverflow.com/a/7201825/568695
    // it changes the environment variables of this JVM. Use only for testing purposes!
    @SuppressWarnings("unchecked")
    public static void setEnv(Map<String, String> newEnv) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> clazz = env.getClass();
            Field field = clazz.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> map = (Map<String, String>) field.get(env);
            map.clear();
            map.putAll(newEnv);
            // only for Windows
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            try {
                Field theCaseInsensitiveEnvironmentField =
                        processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
                theCaseInsensitiveEnvironmentField.setAccessible(true);
                Map<String, String> ciEnv =
                        (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
                ciEnv.clear();
                ciEnv.putAll(newEnv);
            } catch (NoSuchFieldException ignored) {
            }

        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }
    }

    public static <T extends AbstractFlinkResource<?, ?>> MetricManager<T> createTestMetricManager(
            Configuration conf) {
        return createTestMetricManager(
                TestingMetricRegistry.builder()
                        .setDelimiter(".".charAt(0))
                        .setRegisterConsumer((metric, name, group) -> {})
                        .build(),
                conf);
    }

    public static <T extends AbstractFlinkResource<?, ?>> MetricManager<T> createTestMetricManager(
            MetricRegistry metricRegistry, Configuration conf) {

        var confManager = new FlinkConfigManager(conf);
        return new MetricManager<>(createTestMetricGroup(metricRegistry, conf), confManager);
    }

    public static KubernetesOperatorMetricGroup createTestMetricGroup(
            MetricRegistry metricRegistry, Configuration conf) {
        return KubernetesOperatorMetricGroup.create(
                metricRegistry, conf, TEST_NAMESPACE, "testopname", "testhost");
    }

    /** Testing ResponseProvider. */
    public static class ValidatingResponseProvider<T> implements ResponseProvider<Object> {

        private final AtomicBoolean validated = new AtomicBoolean(false);

        private final Consumer<RecordedRequest> validator;
        private final T returnValue;

        public ValidatingResponseProvider(T returnValue, Consumer<RecordedRequest> validator) {
            this.validator = validator;
            this.returnValue = returnValue;
        }

        public void assertValidated() {
            Assertions.assertTrue(validated.get());
        }

        @Override
        public int getStatusCode(RecordedRequest recordedRequest) {
            return HttpURLConnection.HTTP_CREATED;
        }

        @Override
        public Headers getHeaders() {
            return new Headers.Builder().build();
        }

        @Override
        public void setHeaders(Headers headers) {}

        @Override
        public Object getBody(RecordedRequest recordedRequest) {
            validator.accept(recordedRequest);
            validated.set(true);
            return returnValue;
        }
    }
}
