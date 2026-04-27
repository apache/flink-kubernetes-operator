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

package org.apache.flink.kubernetes.operator.admission.mutator;

import org.apache.flink.kubernetes.operator.admission.informer.InformerManager;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.mutator.DefaultFlinkMutator;
import org.apache.flink.kubernetes.operator.mutator.FlinkResourceMutator;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.webhook.admission.Operation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

import static io.javaoperatorsdk.operator.api.reconciler.Constants.DEFAULT_NAMESPACES_SET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link FlinkMutator}. */
@EnableKubernetesMockClient(crud = true)
class FlinkMutatorTest {

    private KubernetesClient kubernetesClient;
    private InformerManager informerManager;
    private FlinkMutator mutator;

    @BeforeEach
    void setup() {
        informerManager = new InformerManager(kubernetesClient);
        informerManager.setNamespaces(DEFAULT_NAMESPACES_SET);
        mutator = new FlinkMutator(Set.of(new DefaultFlinkMutator()), informerManager);
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void deploymentNoMutationReturnsSameInstance(Operation operation) {
        var deployment = createDeployment();

        var result = mutator.mutate(deployment, operation);

        assertSame(
                deployment,
                result,
                "Should return the original resource when no mutation is applied");
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void sessionJobWithoutLabelReturnsMutatedInstance(Operation operation) {
        var sessionJob = createSessionJob();

        var result = mutator.mutate(sessionJob, operation);

        assertNotSame(sessionJob, result, "Should return a mutated instance when label is added");
        assertInstanceOf(FlinkSessionJob.class, result);
        assertEquals(
                "test-deployment",
                result.getMetadata().getLabels().get(CrdConstants.LABEL_TARGET_SESSION));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void sessionJobWithCorrectLabelReturnsSameInstance(Operation operation) {
        var sessionJob = createSessionJob();
        var labels = new HashMap<String, String>();
        labels.put(CrdConstants.LABEL_TARGET_SESSION, "test-deployment");
        sessionJob.getMetadata().setLabels(labels);

        var result = mutator.mutate(sessionJob, operation);

        assertSame(
                sessionJob,
                result,
                "Should return the original resource when the label is already correct");
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void sessionJobWithWrongLabelReturnsMutatedInstance(Operation operation) {
        var sessionJob = createSessionJob();
        var labels = new HashMap<String, String>();
        labels.put(CrdConstants.LABEL_TARGET_SESSION, "wrong-session");
        sessionJob.getMetadata().setLabels(labels);

        var result = mutator.mutate(sessionJob, operation);

        assertNotSame(sessionJob, result);
        assertInstanceOf(FlinkSessionJob.class, result);
        assertEquals(
                "test-deployment",
                result.getMetadata().getLabels().get(CrdConstants.LABEL_TARGET_SESSION));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void stateSnapshotNoMutationReturnsSameInstance(Operation operation) {
        var snapshot = createStateSnapshot();

        var result = mutator.mutate(snapshot, operation);

        assertSame(
                snapshot,
                result,
                "Should return the original resource when no mutation is applied");
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"},
            mode = EnumSource.Mode.EXCLUDE)
    void nonMutatingOperationsReturnOriginalResource(Operation operation) {
        var deployment = createDeployment();
        var sessionJob = createSessionJob();
        var snapshot = createStateSnapshot();

        assertSame(deployment, mutator.mutate(deployment, operation));
        assertSame(sessionJob, mutator.mutate(sessionJob, operation));
        assertSame(snapshot, mutator.mutate(snapshot, operation));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void deploymentMutatorExceptionIsWrappedInRuntimeException(Operation operation) {
        var failingMutator =
                new FlinkMutator(
                        Set.of(
                                new DefaultFlinkMutator() {
                                    @Override
                                    public FlinkDeployment mutateDeployment(
                                            FlinkDeployment deployment) {
                                        throw new IllegalStateException("deployment error");
                                    }
                                }),
                        informerManager);
        var deployment = createDeployment();

        var exception =
                assertThrows(
                        RuntimeException.class, () -> failingMutator.mutate(deployment, operation));
        assertInstanceOf(IllegalStateException.class, exception.getCause());
        assertEquals("deployment error", exception.getCause().getMessage());
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void sessionJobMutatorExceptionIsWrappedInRuntimeException(Operation operation) {
        var failingMutator =
                new FlinkMutator(
                        Set.of(
                                new DefaultFlinkMutator() {
                                    @Override
                                    public FlinkSessionJob mutateSessionJob(
                                            FlinkSessionJob sessionJob,
                                            Optional<FlinkDeployment> session) {
                                        throw new IllegalStateException("session job error");
                                    }
                                }),
                        informerManager);
        var sessionJob = createSessionJob();

        var exception =
                assertThrows(
                        RuntimeException.class, () -> failingMutator.mutate(sessionJob, operation));
        assertInstanceOf(IllegalStateException.class, exception.getCause());
        assertEquals("session job error", exception.getCause().getMessage());
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void stateSnapshotMutatorExceptionIsWrappedInRuntimeException(Operation operation) {
        var failingMutator =
                new FlinkMutator(
                        Set.of(
                                new DefaultFlinkMutator() {
                                    @Override
                                    public FlinkStateSnapshot mutateStateSnapshot(
                                            FlinkStateSnapshot stateSnapshot) {
                                        throw new IllegalStateException("snapshot error");
                                    }
                                }),
                        informerManager);
        var snapshot = createStateSnapshot();

        var exception =
                assertThrows(
                        RuntimeException.class, () -> failingMutator.mutate(snapshot, operation));
        assertInstanceOf(IllegalStateException.class, exception.getCause());
        assertEquals("snapshot error", exception.getCause().getMessage());
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void customLabelMutatorOnDeploymentReturnsMutatedInstance(Operation operation) {
        var labelMutator = createMutatorWith(new LabelInjectingMutator());
        var deployment = createDeployment();

        var result = labelMutator.mutate(deployment, operation);

        assertNotSame(deployment, result);
        assertInstanceOf(FlinkDeployment.class, result);
        assertEquals("injected", result.getMetadata().getLabels().get("custom-env"));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void customLabelMutatorOnDeploymentWithMatchingLabelReturnsSameInstance(Operation operation) {
        var labelMutator = createMutatorWith(new LabelInjectingMutator());
        var deployment = createDeployment();
        var labels = new HashMap<String, String>();
        labels.put("custom-env", "injected");
        deployment.getMetadata().setLabels(labels);

        var result = labelMutator.mutate(deployment, operation);

        assertSame(deployment, result);
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void customLabelMutatorOnSnapshotReturnsMutatedInstance(Operation operation) {
        var labelMutator = createMutatorWith(new LabelInjectingMutator());
        var snapshot = createStateSnapshot();

        var result = labelMutator.mutate(snapshot, operation);

        assertNotSame(snapshot, result);
        assertInstanceOf(FlinkStateSnapshot.class, result);
        assertEquals("injected", result.getMetadata().getLabels().get("custom-env"));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void customLabelMutatorOnSnapshotWithMatchingLabelReturnsSameInstance(Operation operation) {
        var labelMutator = createMutatorWith(new LabelInjectingMutator());
        var snapshot = createStateSnapshot();
        var labels = new HashMap<String, String>();
        labels.put("custom-env", "injected");
        snapshot.getMetadata().setLabels(labels);

        var result = labelMutator.mutate(snapshot, operation);

        assertSame(snapshot, result);
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void customSpecMutatorOnDeploymentReturnsMutatedInstance(Operation operation) {
        var specMutator = createMutatorWith(new SpecModifyingMutator());
        var deployment = createDeployment();

        var result = specMutator.mutate(deployment, operation);

        assertNotSame(deployment, result);
        assertInstanceOf(FlinkDeployment.class, result);
        assertEquals("mutated:latest", ((FlinkDeployment) result).getSpec().getImage());
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void customSpecMutatorOnSessionJobReturnsMutatedInstance(Operation operation) {
        var specMutator = createMutatorWith(new SpecModifyingMutator());
        var sessionJob = createSessionJob();

        var result = specMutator.mutate(sessionJob, operation);

        assertNotSame(sessionJob, result);
        assertInstanceOf(FlinkSessionJob.class, result);
        assertEquals(
                "mutated-deployment", ((FlinkSessionJob) result).getSpec().getDeploymentName());
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE"})
    void customSpecMutatorOnSnapshotReturnsMutatedInstance(Operation operation) {
        var specMutator = createMutatorWith(new SpecModifyingMutator());
        var snapshot = createStateSnapshot();

        var result = specMutator.mutate(snapshot, operation);

        assertNotSame(snapshot, result);
        assertInstanceOf(FlinkStateSnapshot.class, result);
        assertEquals(5, ((FlinkStateSnapshot) result).getSpec().getBackoffLimit());
    }

    private FlinkMutator createMutatorWith(FlinkResourceMutator resourceMutator) {
        return new FlinkMutator(Set.of(resourceMutator), informerManager);
    }

    private FlinkDeployment createDeployment() {
        var deployment = new FlinkDeployment();
        var meta = new ObjectMeta();
        meta.setName("test-deployment");
        meta.setNamespace("default");
        deployment.setMetadata(meta);
        deployment.setSpec(new FlinkDeploymentSpec());
        return deployment;
    }

    private FlinkSessionJob createSessionJob() {
        var sessionJob = new FlinkSessionJob();
        var meta = new ObjectMeta();
        meta.setName("test-job");
        meta.setNamespace("default");
        sessionJob.setMetadata(meta);
        sessionJob.setSpec(
                FlinkSessionJobSpec.builder()
                        .job(JobSpec.builder().jarURI("http://test-job.jar").build())
                        .deploymentName("test-deployment")
                        .build());
        return sessionJob;
    }

    private FlinkStateSnapshot createStateSnapshot() {
        var snapshot = new FlinkStateSnapshot();
        var meta = new ObjectMeta();
        meta.setName("test-snapshot");
        meta.setNamespace("default");
        snapshot.setMetadata(meta);
        snapshot.setSpec(new FlinkStateSnapshotSpec());
        return snapshot;
    }

    /**
     * A custom mutator that injects a {@code custom-env=injected} label on FlinkDeployment and
     * FlinkStateSnapshot if not already present. Mirrors the pattern of DefaultFlinkMutator's
     * target-session label on FlinkSessionJob.
     */
    private static class LabelInjectingMutator implements FlinkResourceMutator {
        @Override
        public FlinkDeployment mutateDeployment(FlinkDeployment deployment) {
            addLabelIfMissing(deployment.getMetadata());
            return deployment;
        }

        @Override
        public FlinkSessionJob mutateSessionJob(
                FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {
            return sessionJob;
        }

        @Override
        public FlinkStateSnapshot mutateStateSnapshot(FlinkStateSnapshot stateSnapshot) {
            addLabelIfMissing(stateSnapshot.getMetadata());
            return stateSnapshot;
        }

        private void addLabelIfMissing(ObjectMeta meta) {
            var labels = meta.getLabels();
            if (labels == null) {
                labels = new HashMap<>();
            }
            if (!"injected".equals(labels.get("custom-env"))) {
                labels.put("custom-env", "injected");
                meta.setLabels(labels);
            }
        }
    }

    /**
     * A custom mutator that modifies the spec of every CRD to verify that spec-level changes are
     * always detected by the before/after tree comparison.
     */
    private static class SpecModifyingMutator implements FlinkResourceMutator {
        @Override
        public FlinkDeployment mutateDeployment(FlinkDeployment deployment) {
            deployment.getSpec().setImage("mutated:latest");
            return deployment;
        }

        @Override
        public FlinkSessionJob mutateSessionJob(
                FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {
            sessionJob.getSpec().setDeploymentName("mutated-deployment");
            return sessionJob;
        }

        @Override
        public FlinkStateSnapshot mutateStateSnapshot(FlinkStateSnapshot stateSnapshot) {
            stateSnapshot.getSpec().setBackoffLimit(5);
            return stateSnapshot;
        }
    }
}
