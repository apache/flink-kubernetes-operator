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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
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
import org.apache.flink.kubernetes.operator.informer.InformerManager;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.observer.deployment.ObserverFactory;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ReconcilerFactory;
import org.apache.flink.kubernetes.operator.utils.StatusHelper;
import org.apache.flink.kubernetes.operator.utils.ValidatorUtils;
import org.apache.flink.metrics.testutils.MetricListener;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.mockwebserver.utils.ResponseProvider;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import okhttp3.Headers;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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
                .taskManager(new TaskManagerSpec(new Resource(1.0, "2048m"), null))
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

    public static Context createEmptyContext() {
        return new Context() {
            @Override
            public Optional<RetryInfo> getRetryInfo() {
                return Optional.empty();
            }

            @Override
            public <T> Optional<T> getSecondaryResource(Class<T> aClass, String s) {
                return Optional.empty();
            }
        };
    }

    public static Deployment createDeployment(boolean ready) {
        DeploymentStatus status = new DeploymentStatus();
        status.setAvailableReplicas(ready ? 1 : 0);
        status.setReplicas(1);
        DeploymentSpec spec = new DeploymentSpec();
        spec.setReplicas(1);
        Deployment deployment = new Deployment();
        deployment.setSpec(spec);
        deployment.setStatus(status);
        return deployment;
    }

    public static Context createContextWithReadyJobManagerDeployment() {
        return new Context() {
            @Override
            public Optional<RetryInfo> getRetryInfo() {
                return Optional.empty();
            }

            @Override
            public <T> Optional<T> getSecondaryResource(
                    Class<T> expectedType, String eventSourceName) {
                return Optional.of((T) createDeployment(true));
            }
        };
    }

    public static Context createContextWithInProgressDeployment() {
        return new Context() {
            @Override
            public Optional<RetryInfo> getRetryInfo() {
                return Optional.empty();
            }

            @Override
            public <T> Optional<T> getSecondaryResource(
                    Class<T> expectedType, String eventSourceName) {
                return Optional.of((T) createDeployment(false));
            }
        };
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
            public <T> Optional<T> getSecondaryResource(
                    Class<T> expectedType, String eventSourceName) {
                var session = buildSessionCluster();
                session.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
                session.getSpec().getFlinkConfiguration().putAll(flinkDepConfig);
                session.getStatus()
                        .getReconciliationStatus()
                        .serializeAndSetLastReconciledSpec(session.getSpec());
                return Optional.of((T) session);
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
            public <T> Optional<T> getSecondaryResource(
                    Class<T> expectedType, String eventSourceName) {
                var session = buildSessionCluster();
                session.getStatus()
                        .setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
                return Optional.of((T) session);
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
            public <T> Optional<T> getSecondaryResource(
                    Class<T> expectedType, String eventSourceName) {
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
                return Optional.of((T) deployment);
            }
        };
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

    public static FlinkDeploymentController createTestController(
            FlinkConfigManager configManager,
            KubernetesClient kubernetesClient,
            TestingFlinkService flinkService) {

        var statusHelper = new StatusHelper<FlinkDeploymentStatus>(kubernetesClient);
        return new FlinkDeploymentController(
                configManager,
                kubernetesClient,
                ValidatorUtils.discoverValidators(configManager),
                new ReconcilerFactory(
                        kubernetesClient,
                        flinkService,
                        configManager,
                        new InformerManager(new HashSet<>(), kubernetesClient)),
                new ObserverFactory(kubernetesClient, flinkService, configManager, statusHelper),
                new MetricManager<>(new MetricListener().getMetricGroup()),
                statusHelper);
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
