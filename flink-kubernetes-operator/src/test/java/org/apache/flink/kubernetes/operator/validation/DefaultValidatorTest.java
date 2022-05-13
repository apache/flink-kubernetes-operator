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

package org.apache.flink.kubernetes.operator.validation;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentReconciliationStatus;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Test deployment validation logic. */
public class DefaultValidatorTest {

    private final DefaultValidator validator =
            new DefaultValidator(new FlinkConfigManager(new Configuration()));

    @Test
    public void testValidationWithoutDefaultConfig() {
        testSuccess(dep -> {});

        // Test job validation
        testError(dep -> dep.getSpec().getJob().setJarURI(null), "Jar URI must be defined");
        testError(
                dep -> dep.getSpec().getJob().setState(JobState.SUSPENDED),
                "Job must start in running state");

        testError(
                dep -> dep.getSpec().getJob().setParallelism(0),
                "Job parallelism must be larger than 0");
        testError(
                dep -> dep.getSpec().getJob().setParallelism(-1),
                "Job parallelism must be larger than 0");
        testError(
                dep -> {
                    dep.getSpec().setFlinkConfiguration(new HashMap<>());
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
                },
                "Job could not be upgraded with last-state while Kubernetes HA disabled");

        testError(
                dep -> {
                    dep.getSpec()
                            .getFlinkConfiguration()
                            .remove(CheckpointingOptions.SAVEPOINT_DIRECTORY.key());
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
                },
                String.format(
                        "Job could not be upgraded with savepoint while config key[%s] is not set",
                        CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));

        testError(
                dep -> {
                    dep.getSpec()
                            .getFlinkConfiguration()
                            .remove(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key());
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
                },
                "Checkpoint directory");

        testError(
                dep -> {
                    dep.getSpec()
                            .getFlinkConfiguration()
                            .remove(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key());
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
                },
                "Checkpoint directory");

        testSuccess(
                dep -> {
                    dep.getSpec()
                            .getFlinkConfiguration()
                            .remove(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key());
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
                });

        testError(
                dep -> {
                    dep.getSpec().setFlinkConfiguration(new HashMap<>());
                    dep.getSpec()
                            .getJob()
                            .setSavepointTriggerNonce(ThreadLocalRandom.current().nextLong());
                },
                String.format(
                        "Savepoint could not be manually triggered for the running job while config key[%s] is not set",
                        CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));

        // Test conf validation
        testSuccess(
                dep ->
                        dep.getSpec()
                                .setFlinkConfiguration(
                                        Collections.singletonMap("random", "config")));
        testError(
                dep ->
                        dep.getSpec()
                                .setFlinkConfiguration(
                                        Collections.singletonMap(
                                                KubernetesConfigOptions.NAMESPACE.key(), "myns")),
                "Forbidden Flink config key");

        // Test log config validation
        testSuccess(
                dep ->
                        dep.getSpec()
                                .setLogConfiguration(
                                        Map.of(
                                                Constants.CONFIG_FILE_LOG4J_NAME,
                                                "rootLogger.level = INFO")));

        testError(
                dep -> dep.getSpec().setIngress(new IngressSpec()),
                "Ingress template must be defined");

        testError(
                dep ->
                        dep.getSpec()
                                .setIngress(
                                        IngressSpec.builder().template("example.com:port").build()),
                "Unable to process the Ingress template(example.com:port). Error: Error at index 0 in: \"port\"");
        testSuccess(
                dep ->
                        dep.getSpec()
                                .setIngress(
                                        IngressSpec.builder()
                                                .template("example.com/{{namespace}}/{{name}}")
                                                .build()));

        testError(
                dep -> dep.getSpec().setLogConfiguration(Map.of("random", "config")),
                "Invalid log config key");

        testError(
                dep -> {
                    dep.getSpec().setFlinkConfiguration(new HashMap<>());
                    dep.getSpec().getJobManager().setReplicas(2);
                },
                "Kubernetes High availability should be enabled when starting standby JobManagers.");
        testError(
                dep -> dep.getSpec().getJobManager().setReplicas(0),
                "JobManager replicas should not be configured less than one.");

        // Test resource validation
        testSuccess(dep -> dep.getSpec().getTaskManager().getResource().setMemory("1G"));
        testSuccess(dep -> dep.getSpec().getTaskManager().getResource().setMemory("100"));

        testError(
                dep -> dep.getSpec().getTaskManager().getResource().setMemory("invalid"),
                "TaskManager resource memory parse error");
        testError(
                dep -> dep.getSpec().getJobManager().getResource().setMemory("invalid"),
                "JobManager resource memory parse error");

        testError(
                dep -> dep.getSpec().getTaskManager().getResource().setMemory(null),
                "TaskManager resource memory must be defined");
        testError(
                dep -> dep.getSpec().getJobManager().getResource().setMemory(null),
                "JobManager resource memory must be defined");

        // Test savepoint restore validation
        testSuccess(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());
                    dep.getStatus()
                            .getJobStatus()
                            .getSavepointInfo()
                            .setLastSavepoint(Savepoint.of("sp"));

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    FlinkDeploymentSpec spec = ReconciliationUtils.clone(dep.getSpec());
                    spec.getJob().setState(JobState.SUSPENDED);

                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(spec);

                    dep.getSpec()
                            .getFlinkConfiguration()
                            .put(
                                    CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                                    "file:///flink-data/savepoints");
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
                });

        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    FlinkDeploymentSpec spec = ReconciliationUtils.clone(dep.getSpec());
                    spec.getJob().setState(JobState.SUSPENDED);
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(spec);

                    dep.getSpec()
                            .getFlinkConfiguration()
                            .put(
                                    CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                                    "file:///flink-data/savepoints");
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
                },
                "Cannot perform savepoint restore without a valid savepoint");

        // Test cluster type validation
        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(
                                    ReconciliationUtils.clone(dep.getSpec()));
                    dep.getSpec().setJob(null);
                },
                "Cannot switch from job to session cluster");

        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    FlinkDeploymentSpec spec = ReconciliationUtils.clone(dep.getSpec());
                    spec.setJob(null);
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(spec);
                },
                "Cannot switch from session to job cluster");

        // Test upgrade mode change validation
        testError(
                dep -> {
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
                    dep.getSpec()
                            .getFlinkConfiguration()
                            .remove(CheckpointingOptions.SAVEPOINT_DIRECTORY.key());
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    FlinkDeploymentSpec spec = ReconciliationUtils.clone(dep.getSpec());
                    spec.getJob().setUpgradeMode(UpgradeMode.STATELESS);
                    spec.getFlinkConfiguration().remove(HighAvailabilityOptions.HA_MODE.key());

                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(spec);
                    dep.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
                },
                String.format(
                        "Job could not be upgraded to last-state while config key[%s] is not set",
                        CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));

        testError(dep -> dep.getSpec().setFlinkVersion(null), "Flink Version must be defined.");

        testSuccess(dep -> dep.getSpec().setFlinkVersion(FlinkVersion.v1_15));

        testError(
                dep -> dep.getSpec().setServiceAccount(null),
                "spec.serviceAccount must be defined. If you use helm, its value should be the same with the name of jobServiceAccount.");

        testSuccess(dep -> dep.getSpec().setServiceAccount("flink"));
    }

    @Test
    public void testValidationWithDefaultConfig() {
        final Configuration defaultFlinkConf = new Configuration();
        defaultFlinkConf.set(
                HighAvailabilityOptions.HA_MODE,
                KubernetesHaServicesFactory.class.getCanonicalName());
        defaultFlinkConf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "cpdir");
        final DefaultValidator validatorWithDefaultConfig =
                new DefaultValidator(new FlinkConfigManager(defaultFlinkConf));
        testSuccess(
                dep -> {
                    dep.getSpec().setFlinkConfiguration(new HashMap<>());
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
                },
                validatorWithDefaultConfig);
    }

    private void testSuccess(Consumer<FlinkDeployment> deploymentModifier) {
        testSuccess(deploymentModifier, validator);
    }

    private void testSuccess(
            Consumer<FlinkDeployment> deploymentModifier, DefaultValidator validator) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deploymentModifier.accept(deployment);
        validator.validateDeployment(deployment).ifPresent(Assertions::fail);
    }

    private void testError(Consumer<FlinkDeployment> deploymentModifier, String expectedErr) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deploymentModifier.accept(deployment);
        Optional<String> error = validator.validateDeployment(deployment);
        if (error.isPresent()) {
            assertTrue(error.get().startsWith(expectedErr), error.get());
        } else {
            fail("Did not get expected error: " + expectedErr);
        }
    }

    @Test
    public void testSessionJobWithSession() {
        testSessionJobValidateWithModifier(job -> {}, session -> {}, null);

        testSessionJobValidate(TestUtils.buildSessionJob(), Optional.empty(), null);
        testSessionJobValidateWithModifier(
                sessionJob -> sessionJob.getSpec().setDeploymentName("not-match"),
                deployment -> {},
                "The session job's cluster id is not match with the session cluster");

        testSessionJobValidateWithModifier(
                job -> {},
                deployment -> deployment.getSpec().setJob(new JobSpec()),
                "Can not submit session job to application cluster");

        testSessionJobValidateWithModifier(
                sessionJob -> sessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE),
                flinkDeployment -> {},
                "The LAST_STATE upgrade mode is not supported in session job now.");

        testSessionJobValidateWithModifier(
                sessionJob -> {
                    sessionJob.getSpec().getJob().setState(JobState.SUSPENDED);
                },
                flinkDeployment -> {},
                "Job must start in running state");

        testSessionJobValidateWithModifier(
                sessionJob -> {
                    sessionJob
                            .getSpec()
                            .setFlinkConfiguration(
                                    Map.of(
                                            KubernetesOperatorConfigOptions.JAR_ARTIFACT_HTTP_HEADER
                                                    .key(),
                                            "headerKey1:headerValue1,headerKey2:headerValue2"));
                },
                flinkDeployment -> {},
                null);

        testSessionJobValidateWithModifier(
                sessionJob -> {
                    sessionJob
                            .getSpec()
                            .setFlinkConfiguration(
                                    Map.of(
                                            KubernetesOperatorConfigOptions
                                                    .OPERATOR_RECONCILER_RESCHEDULE_INTERVAL
                                                    .key(),
                                            "60"));
                },
                flinkDeployment -> {},
                "Invalid session job flinkConfiguration key: kubernetes.operator.reconciler.reschedule.interval."
                        + " Allowed keys are [kubernetes.operator.user.artifacts.http.header]");
    }

    private void testSessionJobValidateWithModifier(
            Consumer<FlinkSessionJob> sessionJobModifier,
            Consumer<FlinkDeployment> sessionModifier,
            @Nullable String expectedErr) {
        FlinkDeployment session = TestUtils.buildSessionCluster();
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        sessionModifier.accept(session);
        sessionJobModifier.accept(sessionJob);
        testSessionJobValidate(sessionJob, Optional.of(session), expectedErr);
    }

    private void testSessionJobValidate(
            FlinkSessionJob sessionJob,
            Optional<FlinkDeployment> session,
            @Nullable String expectedErr) {
        Optional<String> error = validator.validateSessionJob(sessionJob, session);
        if (expectedErr == null) {
            error.ifPresent(Assertions::fail);
        } else {
            if (error.isPresent()) {
                assertTrue(error.get().startsWith(expectedErr), error.get());
            } else {
                fail("Did not get expected error: " + expectedErr);
            }
        }
    }
}
