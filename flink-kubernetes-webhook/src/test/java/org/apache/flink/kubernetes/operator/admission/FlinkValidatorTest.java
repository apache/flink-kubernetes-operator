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

package org.apache.flink.kubernetes.operator.admission;

import org.apache.flink.kubernetes.operator.admission.informer.InformerManager;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobKind;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.webhook.admission.NotAllowedException;
import io.javaoperatorsdk.webhook.admission.Operation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

import static io.javaoperatorsdk.operator.api.reconciler.Constants.DEFAULT_NAMESPACES_SET;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkValidator}. */
@EnableKubernetesMockClient(crud = true)
class FlinkValidatorTest {

    private KubernetesClient kubernetesClient;
    private InformerManager informerManager;

    @BeforeEach
    void setup() {
        informerManager = new InformerManager(kubernetesClient);
        informerManager.setNamespaces(DEFAULT_NAMESPACES_SET);
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void canaryResourceSkipsValidation(Operation operation) {
        var validator = createValidator(failingValidator("should not be called"));
        var deployment = createDeployment();
        var labels = new HashMap<String, String>();
        labels.put("flink.apache.org/canary", "true");
        deployment.getMetadata().setLabels(labels);

        assertDoesNotThrow(() -> validator.validate(deployment, null, operation));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void deploymentPassesValidation(Operation operation) {
        var validator = createValidator(passingValidator());
        var deployment = createDeployment();

        assertDoesNotThrow(() -> validator.validate(deployment, null, operation));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void deploymentFailsValidation(Operation operation) {
        var validator = createValidator(failingValidator("deployment is invalid"));
        var deployment = createDeployment();

        var exception =
                assertThrows(
                        NotAllowedException.class,
                        () -> validator.validate(deployment, null, operation));
        assertEquals("deployment is invalid", exception.getMessage());
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void sessionJobPassesValidation(Operation operation) {
        var validator = createValidator(passingValidator());
        var sessionJob = createSessionJob();

        assertDoesNotThrow(() -> validator.validate(sessionJob, null, operation));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void sessionJobFailsValidation(Operation operation) {
        var validator = createValidator(failingValidator("session job is invalid"));
        var sessionJob = createSessionJob();

        var exception =
                assertThrows(
                        NotAllowedException.class,
                        () -> validator.validate(sessionJob, null, operation));
        assertEquals("session job is invalid", exception.getMessage());
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void stateSnapshotPassesValidation(Operation operation) {
        var validator = createValidator(passingValidator());
        var snapshot = createStateSnapshot();

        assertDoesNotThrow(() -> validator.validate(snapshot, null, operation));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void stateSnapshotFailsValidation(Operation operation) {
        var validator = createValidator(failingValidator("snapshot is invalid"));
        var snapshot = createStateSnapshot();

        var exception =
                assertThrows(
                        NotAllowedException.class,
                        () -> validator.validate(snapshot, null, operation));
        assertEquals("snapshot is invalid", exception.getMessage());
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void unexpectedResourceKindThrowsNotAllowedException(Operation operation) {
        var validator = createValidator(passingValidator());
        var pod = new io.fabric8.kubernetes.api.model.Pod();
        pod.setMetadata(new ObjectMeta());
        pod.getMetadata().setName("test-pod");
        pod.getMetadata().setNamespace("default");

        var exception =
                assertThrows(
                        NotAllowedException.class, () -> validator.validate(pod, null, operation));
        assertEquals("Unexpected resource: Pod", exception.getMessage());
    }

    @Test
    void firstValidatorErrorStopsValidation() {
        FlinkResourceValidator first = failingValidator("first error");
        FlinkResourceValidator second = failingValidator("second error");
        var validator = new FlinkValidator(Set.of(first, second), informerManager);
        var deployment = createDeployment();

        var exception =
                assertThrows(
                        NotAllowedException.class,
                        () -> validator.validate(deployment, null, Operation.CREATE));
        // With Set ordering, we can't guarantee which fires first, but one of them must
        assertTrue(
                ("first error".equals(exception.getMessage())
                                && !"second error".equals(exception.getMessage()))
                        || (!"first error".equals(exception.getMessage())
                                && "second error".equals(exception.getMessage())));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void stateSnapshotWithFlinkDeploymentJobRefPassesValidation(Operation operation) {
        var validator = createValidator(passingValidator());
        var snapshot = createStateSnapshotWithJobRef(JobKind.FLINK_DEPLOYMENT, "test-deployment");

        assertDoesNotThrow(() -> validator.validate(snapshot, null, operation));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void stateSnapshotWithFlinkSessionJobJobRefPassesValidation(Operation operation) {
        var validator = createValidator(passingValidator());
        var snapshot = createStateSnapshotWithJobRef(JobKind.FLINK_SESSION_JOB, "test-session-job");

        assertDoesNotThrow(() -> validator.validate(snapshot, null, operation));
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void stateSnapshotWithFlinkDeploymentJobRefThrowsWhenNamespaceMissing(Operation operation) {
        var validator = createValidator(passingValidator());
        var snapshot =
                createStateSnapshotWithJobRefNoNamespace(
                        JobKind.FLINK_DEPLOYMENT, "test-deployment");

        var exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> validator.validate(snapshot, null, operation));
        assertEquals("Cannot determine namespace for snapshot", exception.getMessage());
    }

    @ParameterizedTest
    @EnumSource(
            value = Operation.class,
            names = {"CREATE", "UPDATE", "DELETE", "CONNECT"})
    void stateSnapshotWithFlinkSessionJobJobRefThrowsWhenNamespaceMissing(Operation operation) {
        var validator = createValidator(passingValidator());
        var snapshot =
                createStateSnapshotWithJobRefNoNamespace(
                        JobKind.FLINK_SESSION_JOB, "test-session-job");

        var exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> validator.validate(snapshot, null, operation));
        assertEquals("Cannot determine namespace for snapshot", exception.getMessage());
    }

    private FlinkValidator createValidator(FlinkResourceValidator resourceValidator) {
        return new FlinkValidator(Set.of(resourceValidator), informerManager);
    }

    private FlinkResourceValidator passingValidator() {
        return new FlinkResourceValidator() {
            @Override
            public Optional<String> validateDeployment(FlinkDeployment deployment) {
                return Optional.empty();
            }

            @Override
            public Optional<String> validateSessionJob(
                    FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {
                return Optional.empty();
            }

            @Override
            public Optional<String> validateStateSnapshot(
                    FlinkStateSnapshot savepoint, Optional<AbstractFlinkResource<?, ?>> target) {
                return Optional.empty();
            }
        };
    }

    private FlinkResourceValidator failingValidator(String errorMessage) {
        return new FlinkResourceValidator() {
            @Override
            public Optional<String> validateDeployment(FlinkDeployment deployment) {
                return Optional.of(errorMessage);
            }

            @Override
            public Optional<String> validateSessionJob(
                    FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {
                return Optional.of(errorMessage);
            }

            @Override
            public Optional<String> validateStateSnapshot(
                    FlinkStateSnapshot savepoint, Optional<AbstractFlinkResource<?, ?>> target) {
                return Optional.of(errorMessage);
            }
        };
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

    private FlinkStateSnapshot createStateSnapshotWithJobRef(JobKind kind, String name) {
        var snapshot = new FlinkStateSnapshot();
        var meta = new ObjectMeta();
        meta.setName("test-snapshot");
        meta.setNamespace("default");
        snapshot.setMetadata(meta);
        var spec = new FlinkStateSnapshotSpec();
        spec.setJobReference(JobReference.builder().kind(kind).name(name).build());
        snapshot.setSpec(spec);
        return snapshot;
    }

    private FlinkStateSnapshot createStateSnapshotWithJobRefNoNamespace(JobKind kind, String name) {
        var snapshot = new FlinkStateSnapshot();
        var meta = new ObjectMeta();
        meta.setName("test-snapshot");
        snapshot.setMetadata(meta);
        var spec = new FlinkStateSnapshotSpec();
        spec.setJobReference(JobReference.builder().kind(kind).name(name).build());
        snapshot.setSpec(spec);
        return snapshot;
    }
}
