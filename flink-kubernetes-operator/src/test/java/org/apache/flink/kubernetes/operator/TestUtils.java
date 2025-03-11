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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.health.CanaryResourceManager;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.mockwebserver.utils.ResponseProvider;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.IndexedResourceCache;
import io.javaoperatorsdk.operator.api.reconciler.ResourceDiscriminator;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedDependentResourceContext;
import io.javaoperatorsdk.operator.processing.event.EventSourceRetriever;
import okhttp3.Headers;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.provider.Arguments;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Testing utilities. */
public class TestUtils extends BaseTestUtils {
    public static final int MAX_RECONCILE_TIMES = 3;

    private static final String TEST_PLUGINS = "test-plugins";
    private static final String PlUGINS_JAR = TEST_PLUGINS + "-test-jar.jar";

    public static PodList createFailedPodList(String crashLoopMessage, String reason) {
        ContainerStatus cs =
                new ContainerStatusBuilder()
                        .withNewState()
                        .withNewWaiting()
                        .withReason(reason)
                        .withMessage(crashLoopMessage)
                        .endWaiting()
                        .endState()
                        .build();

        Pod pod = getTestPod("host", "apiVersion", Collections.emptyList());
        pod.setStatus(
                new PodStatusBuilder()
                        .withContainerStatuses(Collections.singletonList(cs))
                        .build());
        return new PodListBuilder().withItems(pod).build();
    }

    public static Deployment createDeployment(boolean ready) {
        String nowTs = Instant.now().toString();
        var status = new DeploymentStatus();
        status.setAvailableReplicas(ready ? 1 : 0);
        status.setReplicas(1);
        var availableCondition = new DeploymentCondition();
        availableCondition.setType("Available");
        availableCondition.setStatus(ready ? "True" : "False");
        availableCondition.setLastTransitionTime(nowTs);
        status.setConditions(List.of(availableCondition));

        DeploymentSpec spec = new DeploymentSpec();
        spec.setReplicas(1);

        var meta = new ObjectMeta();
        meta.setCreationTimestamp(nowTs);

        Deployment deployment = new Deployment();
        deployment.setMetadata(meta);
        deployment.setSpec(spec);
        deployment.setStatus(status);

        return deployment;
    }

    public static Map<String, String> generateTestOwnerReferenceMap(AbstractFlinkResource owner) {
        return Map.of(
                "apiVersion", owner.getApiVersion(),
                "kind", owner.getKind(),
                "name", owner.getMetadata().getName(),
                "uid", owner.getMetadata().getUid(),
                "blockOwnerDeletion", "true",
                "controller", "false");
    }

    public static <T extends HasMetadata> Context<T> createContextWithDeployment(
            @Nullable Deployment deployment, KubernetesClient client) {
        return new TestingContext<>() {
            @Override
            public Optional<T> getSecondaryResource(Class expectedType, String eventSourceName) {
                return (Optional<T>) Optional.ofNullable(deployment);
            }

            @Override
            public KubernetesClient getClient() {
                return client;
            }
        };
    }

    public static <T extends HasMetadata> Context<T> createEmptyContext() {
        return createContextWithDeployment(null, null);
    }

    public static <T extends HasMetadata> Context<T> createEmptyContextWithClient(
            KubernetesClient client) {
        return createContextWithDeployment(null, client);
    }

    public static <T extends HasMetadata> Context<T> createContextWithReadyJobManagerDeployment(
            KubernetesClient client) {
        return createContextWithDeployment(createDeployment(true), client);
    }

    public static <T extends HasMetadata> Context<T> createContextWithInProgressDeployment(
            KubernetesClient client) {
        return createContextWithDeployment(createDeployment(false), client);
    }

    public static <T extends HasMetadata> Context<T> createContextWithReadyFlinkDeployment() {
        return createContextWithReadyFlinkDeployment(new HashMap<>(), null);
    }

    public static <T extends HasMetadata> Context<T> createContextWithReadyFlinkDeployment(
            KubernetesClient client) {
        return createContextWithReadyFlinkDeployment(new HashMap<>(), client);
    }

    public static <T extends HasMetadata> Context<T> createContextWithReadyFlinkDeployment(
            Map<String, String> flinkDepConfig, KubernetesClient client) {
        return createContextWithReadyFlinkDeployment(flinkDepConfig, client, FlinkVersion.v1_18);
    }

    public static <T extends HasMetadata> Context<T> createContextWithReadyFlinkDeployment(
            Map<String, String> flinkDepConfig, KubernetesClient client, FlinkVersion version) {
        return new TestingContext<>() {
            @Override
            public Optional<T> getSecondaryResource(Class expectedType, String eventSourceName) {
                var session = buildSessionCluster(version);
                session.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
                session.getSpec().getFlinkConfiguration().putAll(flinkDepConfig);
                session.getStatus()
                        .getReconciliationStatus()
                        .serializeAndSetLastReconciledSpec(session.getSpec(), session);
                return (Optional<T>) Optional.of(session);
            }

            @Override
            public KubernetesClient getClient() {
                return client;
            }
        };
    }

    public static <T extends HasMetadata> Context<T> createContextWithNotReadyFlinkDeployment() {
        return new TestingContext<>() {
            @Override
            public Optional<T> getSecondaryResource(Class expectedType, String eventSourceName) {
                var session = buildSessionCluster();
                session.getStatus()
                        .setJobManagerDeploymentStatus(JobManagerDeploymentStatus.MISSING);
                return (Optional<T>) Optional.of(session);
            }
        };
    }

    public static final String DEPLOYMENT_ERROR = "test deployment error message";

    public static <T extends HasMetadata> Context<T> createContextWithFailedJobManagerDeployment(
            KubernetesClient client) {
        return new TestingContext<>() {
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
            public KubernetesClient getClient() {
                return client;
            }
        };
    }

    public static Context<FlinkStateSnapshot> createSnapshotContext(
            KubernetesClient client, AbstractFlinkResource<?, ?> secondaryResource) {
        return new TestingContext<>() {
            @Override
            public Optional<AbstractFlinkResource<?, ?>> getSecondaryResource(
                    Class expectedType, String eventSourceName) {
                return Optional.ofNullable(secondaryResource);
            }

            @Override
            public KubernetesClient getClient() {
                return client;
            }
        };
    }

    public static <CR extends AbstractFlinkResource<?, ?>>
            List<FlinkStateSnapshot> getFlinkStateSnapshotsForResource(
                    KubernetesClient kubernetesClient, CR resource) {
        return kubernetesClient
                .resources(FlinkStateSnapshot.class)
                .inAnyNamespace()
                .list()
                .getItems()
                .stream()
                .filter(
                        s ->
                                s.getSpec()
                                        .getJobReference()
                                        .equals(JobReference.fromFlinkResource(resource)))
                .collect(Collectors.toList());
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

    public static KubernetesOperatorMetricGroup createTestMetricGroup(Configuration conf) {
        return createTestMetricGroup(createTestMetricRegistry(), conf);
    }

    public static KubernetesOperatorMetricGroup createTestMetricGroup(
            MetricRegistry metricRegistry, Configuration conf) {
        return KubernetesOperatorMetricGroup.create(
                metricRegistry, conf, TEST_NAMESPACE, "testopname", "testhost");
    }

    public static TestingMetricRegistry createTestMetricRegistry() {

        return TestingMetricRegistry.builder()
                .setDelimiter(".".charAt(0))
                .setRegisterConsumer((metric, name, group) -> {})
                .build();
    }

    public static Stream<Arguments> flinkVersionsAndUpgradeModes() {
        List<Arguments> args = new ArrayList<>();
        for (FlinkVersion version : Set.of(FlinkVersion.v1_16, FlinkVersion.v1_20)) {
            for (UpgradeMode upgradeMode : UpgradeMode.values()) {
                args.add(arguments(version, upgradeMode));
            }
        }
        return args.stream();
    }

    public static Stream<Arguments> flinkVersions() {
        return Stream.of(arguments(FlinkVersion.v1_16), arguments(FlinkVersion.v1_20));
    }

    public static FlinkDeployment createCanaryDeployment() {
        var cr = new FlinkDeployment();
        cr.setSpec(cr.initSpec());
        var meta = new ObjectMeta();
        meta.setGeneration(0L);
        meta.setLabels(Map.of(CanaryResourceManager.CANARY_LABEL, "true"));
        meta.setName("canary");
        meta.setNamespace("default");
        cr.setMetadata(meta);
        return cr;
    }

    public static FlinkSessionJob createCanaryJob() {
        var cr = new FlinkSessionJob();
        cr.setSpec(cr.initSpec());
        var meta = new ObjectMeta();
        meta.setGeneration(0L);
        meta.setLabels(Map.of(CanaryResourceManager.CANARY_LABEL, "true"));
        meta.setName("canary");
        meta.setNamespace("default");
        cr.setMetadata(meta);
        return cr;
    }

    public static void reconcileSpec(FlinkDeployment deployment) {
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
    }

    /**
     * Sets up an active cron trigger by ensuring that the latest successful snapshot happened
     * earlier than the scheduled trigger.
     */
    public static Instant setupCronTrigger(SnapshotType snapshotType, FlinkDeployment deployment) {

        Calendar calendar = Calendar.getInstance();
        calendar.set(2022, Calendar.JUNE, 5, 11, 0);

        String cronOptionKey;
        switch (snapshotType) {
            case SAVEPOINT:
                cronOptionKey = KubernetesOperatorConfigOptions.PERIODIC_SAVEPOINT_INTERVAL.key();
                break;
            case CHECKPOINT:
                cronOptionKey = KubernetesOperatorConfigOptions.PERIODIC_CHECKPOINT_INTERVAL.key();
                break;
            default:
                throw new IllegalArgumentException("Unsupported snapshot type: " + snapshotType);
        }

        deployment.getSpec().getFlinkConfiguration().put(cronOptionKey, "0 0 12 5 6 ? 2022");
        reconcileSpec(deployment);
        return calendar.toInstant();
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

    /**
     * Base context implementation for tests.
     *
     * @param <T> Resource type
     */
    public static class TestingContext<T extends HasMetadata> implements Context<T> {
        @Override
        public Optional<RetryInfo> getRetryInfo() {
            return Optional.empty();
        }

        @Override
        public <T1> Set<T1> getSecondaryResources(Class<T1> aClass) {
            // TODO: improve this, even if we only support FlinkDeployment as a secondary resource
            if (aClass.getSimpleName().equals(FlinkDeployment.class.getSimpleName())) {
                KubernetesClient client = getClient();
                var hasMetadata =
                        new HashSet<>(
                                client.resources(FlinkDeployment.class)
                                        .inAnyNamespace()
                                        .list()
                                        .getItems());
                return (Set<T1>) hasMetadata;
            } else {
                return null;
            }
        }

        @Override
        public <T1> Optional<T1> getSecondaryResource(Class<T1> aClass, String s) {
            return Optional.empty();
        }

        @Override
        public <R> Optional<R> getSecondaryResource(
                Class<R> aClass, ResourceDiscriminator<R, T> resourceDiscriminator) {
            return Optional.empty();
        }

        @Override
        public ControllerConfiguration<T> getControllerConfiguration() {
            return null;
        }

        @Override
        public ManagedDependentResourceContext managedDependentResourceContext() {
            return null;
        }

        @Override
        public EventSourceRetriever<T> eventSourceRetriever() {
            return null;
        }

        @Override
        public KubernetesClient getClient() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public ExecutorService getWorkflowExecutorService() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public IndexedResourceCache<T> getPrimaryCache() {
            return null;
        }
    }
}
