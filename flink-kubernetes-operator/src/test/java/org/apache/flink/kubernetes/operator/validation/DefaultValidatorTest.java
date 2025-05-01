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

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.CheckpointSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobKind;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.SavepointSpec;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentReconciliationStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.utils.Constants;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static org.apache.flink.configuration.JobManagerOptions.JVM_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.TOTAL_FLINK_MEMORY;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Test deployment validation logic. */
public class DefaultValidatorTest {

    private final DefaultValidator validator =
            new DefaultValidator(new FlinkConfigManager(new Configuration()));

    @Test
    public void testValidationWithoutDefaultConfig() {
        testSuccess(dep -> {});

        // Test meta.name
        testSuccess(dep -> dep.getMetadata().setName("session-cluster"));
        testError(
                dep -> dep.getMetadata().setName("session-cluster-1.13"),
                "The FlinkDeployment name: session-cluster-1.13 is invalid, must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123'), and the length must be no more than 45 characters.");

        testSuccess(dep -> dep.getSpec().getJob().setState(JobState.SUSPENDED));

        testSuccess(dep -> dep.getSpec().getJob().setState(JobState.RUNNING));

        testError(
                dep -> dep.getSpec().getJob().setParallelism(0),
                "Job parallelism must be larger than 0");

        testError(
                dep -> {
                    var tmSpec = new TaskManagerSpec();
                    tmSpec.setReplicas(0);
                    dep.getSpec().setTaskManager(tmSpec);
                },
                "TaskManager replicas must be larger than 0");

        testSuccess(
                dep -> {
                    dep.getSpec().getTaskManager().setReplicas(1);
                    dep.getSpec().getJob().setParallelism(0);
                });

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

        testError(
                dep ->
                        dep.getSpec()
                                .setFlinkConfiguration(
                                        Map.of(
                                                KubernetesOperatorConfigOptions
                                                        .PERIODIC_SAVEPOINT_INTERVAL
                                                        .key(),
                                                "1m")),
                String.format(
                        "Periodic savepoints cannot be enabled when config key[%s] is not set",
                        CheckpointingOptions.SAVEPOINT_DIRECTORY.key()));

        testError(
                dep ->
                        dep.getSpec()
                                .setFlinkConfiguration(
                                        Map.of(
                                                KubernetesOperatorConfigOptions
                                                        .OPERATOR_JOB_UPGRADE_LAST_STATE_CHECKPOINT_MAX_AGE
                                                        .key(),
                                                "1m")),
                String.format(
                        "In order to use max-checkpoint age functionality config key[%s] must be set to allow triggering savepoint upgrades.",
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

        testError(
                dep ->
                        dep.getSpec()
                                .setFlinkConfiguration(
                                        Collections.singletonMap(
                                                HighAvailabilityOptions.HA_CLUSTER_ID.key(),
                                                "my-cluster-id")),
                "Forbidden Flink config key");

        testError(
                dep ->
                        dep.getSpec()
                                .setFlinkConfiguration(
                                        Map.of(
                                                KubernetesOperatorConfigOptions
                                                                .OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED
                                                                .key(),
                                                        "true",
                                                KubernetesOperatorConfigOptions
                                                                .OPERATOR_JM_DEPLOYMENT_RECOVERY_ENABLED
                                                                .key(),
                                                        "false")),
                "Deployment recovery ("
                        + KubernetesOperatorConfigOptions.OPERATOR_JM_DEPLOYMENT_RECOVERY_ENABLED
                                .key()
                        + ") must be enabled");

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
                "High availability should be enabled when starting standby JobManagers.");

        testError(
                dep ->
                        dep.getSpec()
                                .setFlinkConfiguration(
                                        Map.of(
                                                KubernetesOperatorConfigOptions
                                                        .DEPLOYMENT_ROLLBACK_ENABLED
                                                        .key(),
                                                "true")),
                "HA must be enabled for rollback support.");

        testError(
                dep -> dep.getSpec().getJobManager().setReplicas(0),
                "JobManager replicas should not be configured less than one.");

        // Test resource validation
        testSuccess(dep -> dep.getSpec().getTaskManager().getResource().setMemory("1G"));
        testSuccess(dep -> dep.getSpec().getTaskManager().getResource().setMemory("100"));

        // Test resource validation with k8s specification
        testSuccess(dep -> dep.getSpec().getJobManager().getResource().setMemory("1Gi"));
        testSuccess(dep -> dep.getSpec().getTaskManager().getResource().setMemory("1Gi"));

        testError(
                dep -> dep.getSpec().getTaskManager().getResource().setMemory("invalid"),
                "TaskManager resource memory parse error");
        testError(
                dep -> dep.getSpec().getJobManager().getResource().setMemory("invalid"),
                "JobManager resource memory parse error");
        testError(
                dep -> dep.getSpec().getTaskManager().getResource().setMemory(null),
                "TaskManager resource memory must be defined using `spec.taskManager.resource.memory`");
        testError(
                dep -> dep.getSpec().getJobManager().getResource().setMemory(null),
                "JobManager resource memory must be defined using `spec.jobManager.resource.memory`");
        testError(
                dep -> {
                    dep.getSpec().getTaskManager().getResource().setMemory(null);
                    dep.getSpec().setFlinkConfiguration(Map.of(TASK_HEAP_MEMORY.key(), "1024m"));
                },
                "TaskManager resource memory must be defined using `spec.taskManager.resource.memory`");

        testSuccess(
                dep -> {
                    dep.getSpec().getJobManager().getResource().setMemory(null);
                    dep.getSpec().setFlinkConfiguration(Map.of(JVM_HEAP_MEMORY.key(), "2048m"));
                });
        testSuccess(
                dep -> {
                    dep.getSpec().getTaskManager().getResource().setMemory(null);
                    dep.getSpec().setFlinkConfiguration(Map.of(TOTAL_FLINK_MEMORY.key(), "2048m"));
                });
        testSuccess(
                dep -> {
                    dep.getSpec().getTaskManager().getResource().setMemory(null);
                    dep.getSpec()
                            .setFlinkConfiguration(
                                    Map.of(
                                            TASK_HEAP_MEMORY.key(),
                                            "1024m",
                                            MANAGED_MEMORY_SIZE.key(),
                                            "1024m"));
                });

        // Test savepoint restore validation
        testSuccess(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());
                    dep.getStatus()
                            .getJobStatus()
                            .getSavepointInfo()
                            .setLastSavepoint(Savepoint.of("sp", SnapshotTriggerType.UPGRADE));

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    FlinkDeploymentSpec spec = ReconciliationUtils.clone(dep.getSpec());
                    spec.getJob().setState(JobState.SUSPENDED);

                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(spec, dep);

                    dep.getSpec()
                            .getFlinkConfiguration()
                            .put(
                                    CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                                    "file:///flink-data/savepoints");
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
                });

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
                                    ReconciliationUtils.clone(dep.getSpec()), dep);
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
                            .serializeAndSetLastReconciledSpec(spec, dep);
                },
                "Cannot switch from session to job cluster");

        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    dep.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);
                    FlinkDeploymentSpec spec = ReconciliationUtils.clone(dep.getSpec());

                    spec.setMode(KubernetesDeploymentMode.NATIVE);
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(spec, dep);
                },
                "Cannot switch from native kubernetes to standalone kubernetes cluster");

        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    dep.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);
                    FlinkDeploymentSpec spec = ReconciliationUtils.clone(dep.getSpec());

                    spec.setMode(null);
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(spec, dep);
                },
                "Cannot switch from native kubernetes to standalone kubernetes cluster");

        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    dep.getSpec().setMode(null);
                    FlinkDeploymentSpec spec = ReconciliationUtils.clone(dep.getSpec());

                    spec.setMode(KubernetesDeploymentMode.STANDALONE);
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(spec, dep);
                },
                "Cannot switch from standalone kubernetes to native kubernetes cluster");

        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus()
                            .setReconciliationStatus(new FlinkDeploymentReconciliationStatus());
                    dep.getSpec().setMode(KubernetesDeploymentMode.NATIVE);
                    FlinkDeploymentSpec spec = ReconciliationUtils.clone(dep.getSpec());

                    spec.setMode(KubernetesDeploymentMode.STANDALONE);
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(spec, dep);
                },
                "Cannot switch from standalone kubernetes to native kubernetes cluster");

        testError(dep -> dep.getSpec().setFlinkVersion(null), "Flink Version must be defined.");

        testError(
                dep -> dep.getSpec().setFlinkVersion(FlinkVersion.v1_14),
                "Flink version "
                        + FlinkVersion.v1_14
                        + " is not supported by this operator version");

        testSuccess(dep -> dep.getSpec().setFlinkVersion(FlinkVersion.v1_15));

        testError(
                dep -> dep.getSpec().setServiceAccount(null),
                "spec.serviceAccount must be defined. If you use helm, its value should be the same with the name of jobServiceAccount.");

        testSuccess(dep -> dep.getSpec().setServiceAccount("flink"));

        testSuccess(
                dep -> {
                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
                    dep.getSpec()
                            .getFlinkConfiguration()
                            .put(
                                    HighAvailabilityOptions.HA_MODE.key(),
                                    // Hardcoded config value should be removed when upgrading Flink
                                    // dependency to 1.16
                                    "kubernetes");
                });

        testError(
                dep -> {
                    dep.getSpec().getJobManager().getResource().setEphemeralStorage("abc");
                },
                "JobManager resource ephemeral storage parse error: Character a is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.");

        testError(
                dep -> {
                    dep.getSpec().getTaskManager().getResource().setEphemeralStorage("abc");
                },
                "TaskManager resource ephemeral storage parse error: Character a is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.");
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

    @Test
    public void testSavepointRedeployValidation() {
        testSuccess(
                dep -> {
                    var job = dep.getSpec().getJob();
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(dep.getSpec(), dep);
                    job.setSavepointRedeployNonce(1L);
                    job.setInitialSavepointPath("s");
                });

        testSuccess(
                dep -> {
                    var job = dep.getSpec().getJob();
                    job.setSavepointRedeployNonce(1L);
                    job.setInitialSavepointPath("s");
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(dep.getSpec(), dep);

                    job.setSavepointRedeployNonce(null);
                    job.setInitialSavepointPath("s");
                });

        testSuccess(
                dep -> {
                    var job = dep.getSpec().getJob();
                    job.setSavepointRedeployNonce(1L);
                    job.setInitialSavepointPath("s");
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(dep.getSpec(), dep);

                    job.setSavepointRedeployNonce(null);
                    job.setInitialSavepointPath(null);
                });

        testSuccess(
                dep -> {
                    var job = dep.getSpec().getJob();
                    job.setSavepointRedeployNonce(1L);
                    job.setInitialSavepointPath("s");
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(dep.getSpec(), dep);

                    job.setSavepointRedeployNonce(2L);
                    job.setInitialSavepointPath("s");
                });

        testError(
                dep -> {
                    var job = dep.getSpec().getJob();
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(dep.getSpec(), dep);
                    job.setSavepointRedeployNonce(1L);
                    job.setInitialSavepointPath(null);
                },
                "InitialSavepointPath must not be empty for savepoint redeployment");

        testError(
                dep -> {
                    var job = dep.getSpec().getJob();
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(dep.getSpec(), dep);
                    job.setSavepointRedeployNonce(1L);
                    job.setInitialSavepointPath(" ");
                },
                "InitialSavepointPath must not be empty for savepoint redeploymen");

        testError(
                dep -> {
                    var job = dep.getSpec().getJob();
                    job.setSavepointRedeployNonce(1L);
                    job.setInitialSavepointPath("s");
                    dep.getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(dep.getSpec(), dep);

                    job.setSavepointRedeployNonce(2L);
                    job.setInitialSavepointPath(null);
                },
                "InitialSavepointPath must not be empty for savepoint redeploymen");
    }

    @ParameterizedTest
    @EnumSource(UpgradeMode.class)
    public void testFlinkVersionChangeValidation(UpgradeMode toUpgradeMode) {
        var lastStateVersionChange =
                createFlinkVersionChange(UpgradeMode.LAST_STATE, toUpgradeMode, JobState.SUSPENDED);
        if (toUpgradeMode == UpgradeMode.STATELESS) {
            testSuccess(lastStateVersionChange);
        } else {
            testError(
                    lastStateVersionChange,
                    "Changing flinkVersion after last-state suspend is not allowed.");
        }

        // Make sure validation always succeeds for running jobs
        for (UpgradeMode fromUpgradeMode : UpgradeMode.values()) {
            testSuccess(createFlinkVersionChange(fromUpgradeMode, toUpgradeMode, JobState.RUNNING));
        }

        // We should allow changing version after savepoint/stateless suspend
        testSuccess(
                createFlinkVersionChange(UpgradeMode.SAVEPOINT, toUpgradeMode, JobState.SUSPENDED));
        testSuccess(
                createFlinkVersionChange(UpgradeMode.STATELESS, toUpgradeMode, JobState.SUSPENDED));
    }

    @NotNull
    private Consumer<FlinkDeployment> createFlinkVersionChange(
            UpgradeMode fromUpgrade, UpgradeMode toUpgrade, JobState fromState) {
        return dep -> {
            var spec = dep.getSpec();
            spec.setFlinkVersion(FlinkVersion.v1_20);
            spec.getJob().setUpgradeMode(toUpgrade);

            var suspendSpec = ReconciliationUtils.clone(spec);

            // Stopped with LAST_STATE mode with different Flink Version
            suspendSpec.getJob().setUpgradeMode(fromUpgrade);
            suspendSpec.getJob().setState(fromState);
            suspendSpec.setFlinkVersion(FlinkVersion.v1_18);

            dep.getStatus()
                    .getReconciliationStatus()
                    .serializeAndSetLastReconciledSpec(suspendSpec, dep);
        };
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
                null);

        testSessionJobValidateWithModifier(
                sessionJob ->
                        sessionJob
                                .getSpec()
                                .setFlinkConfiguration(
                                        Map.of(
                                                KubernetesOperatorConfigOptions
                                                        .JAR_ARTIFACT_HTTP_HEADER
                                                        .key(),
                                                "headerKey1:headerValue1,headerKey2:headerValue2")),
                flinkDeployment -> {},
                null);

        testSessionJobValidateWithModifier(
                sessionJob ->
                        sessionJob
                                .getSpec()
                                .setFlinkConfiguration(
                                        Map.of(
                                                KubernetesOperatorConfigOptions
                                                        .PERIODIC_SAVEPOINT_INTERVAL
                                                        .key(),
                                                "1m")),
                flinkDeployment -> {},
                null);

        testSessionJobValidateWithModifier(
                sessionJob -> {
                    sessionJob
                            .getStatus()
                            .getReconciliationStatus()
                            .serializeAndSetLastReconciledSpec(sessionJob.getSpec(), sessionJob);
                    sessionJob.getSpec().setDeploymentName("new-deployment-name");
                },
                flinkDeployment -> {},
                "The deploymentName can't be changed");

        testSessionJobValidateWithModifier(
                sessionJob -> {
                    sessionJob.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
                    sessionJob
                            .getSpec()
                            .setFlinkConfiguration(
                                    Map.of(
                                            CheckpointingOptions.SAVEPOINT_DIRECTORY.key(),
                                                    "test-savepoint-dir",
                                            CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
                                                    "test-checkpoint-dir"));
                },
                flinkDeployment -> {
                    flinkDeployment.getSpec().setFlinkConfiguration(Map.of());
                },
                null);
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

    @Test
    public void testAutoScalerDeployment() {
        testAutoScalerConfiguration(flinkConf -> {}).ifPresent(Assertions::fail);
    }

    @Test
    public void testAutoScalerDeploymentWithInvalidNegativeScaleDownFactor() {
        var result =
                testAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(
                                        AutoScalerOptions.MAX_SCALE_DOWN_FACTOR.key(), "-0.1"));
        assertErrorContains(
                result, getFormattedErrorMessage(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.0d));
    }

    @Test
    public void testAutoScalerDeploymentWithInvalidNegativeScaleUpFactor() {
        var result =
                testAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.MAX_SCALE_UP_FACTOR.key(), "-0.1"));
        assertErrorContains(
                result, getFormattedErrorMessage(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.0d));
    }

    @Test
    public void testAutoScalerDeploymentWithInvalidNegativeUtilization() {
        var result =
                testAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "-0.6"));
        assertErrorContains(
                result, getFormattedErrorMessage(AutoScalerOptions.UTILIZATION_TARGET, 0.0d, 1.0d));
    }

    @Test
    public void testAutoScalerDeploymentWithInvalidNegativeUtilizationBoundary() {
        var resultMaxUtilization =
                testAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "-0.6"));
        assertErrorContains(
                resultMaxUtilization,
                getFormattedErrorMessage(
                        AutoScalerOptions.UTILIZATION_MAX,
                        AutoScalerOptions.UTILIZATION_TARGET.defaultValue(),
                        1.0));

        var resultMinUtilization =
                testAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "-0.6"));
        assertErrorContains(
                resultMinUtilization,
                getFormattedErrorMessage(
                        AutoScalerOptions.UTILIZATION_MIN,
                        0.0d,
                        AutoScalerOptions.UTILIZATION_TARGET.defaultValue()));
    }

    @Test
    public void testAutoScalerDeploymentWithInvalidExcludedPeriods() {
        var result =
                testAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(
                                        AutoScalerOptions.EXCLUDED_PERIODS.key(), "12:00-10:00"));
        assertTrue(result.isPresent());
    }

    @Test
    public void testAutoScalerDeploymentWithInvalidScalingCoefficientMin() {
        var result =
                testAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(
                                        AutoScalerOptions.OBSERVED_SCALABILITY_COEFFICIENT_MIN
                                                .key(),
                                        "1.2"));
        assertErrorContains(
                result,
                getFormattedErrorMessage(
                        AutoScalerOptions.OBSERVED_SCALABILITY_COEFFICIENT_MIN, 0.01d, 1d));
    }

    @Test
    public void testNonEnabledAutoScalerDeploymentJob() {
        var result =
                testAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.remove(AutoScalerOptions.AUTOSCALER_ENABLED.key());
                            flinkConf.put(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.MAX_SCALE_UP_FACTOR.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "-1.6");
                        });
        assertErrorNotContains(result);
    }

    @Test
    public void testDisabledEnabledAutoScalerDeploymentJob() {
        var result =
                testAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.AUTOSCALER_ENABLED.key(), "false");
                            flinkConf.put(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.MAX_SCALE_UP_FACTOR.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "-1.6");
                        });
        assertErrorNotContains(result);
    }

    @Test
    public void testValidateSessionJob() {
        testSessionJobAutoScalerConfiguration(flinkConf -> {}).ifPresent(Assertions::fail);
    }

    @Test
    public void testValidateSessionJobWithInvalidNegativeScaleDownFactor() {
        var result =
                testSessionJobAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(
                                        AutoScalerOptions.MAX_SCALE_DOWN_FACTOR.key(), "-0.1"));
        assertErrorContains(
                result, getFormattedErrorMessage(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.0d));
    }

    @Test
    public void testValidateSessionJobWithInvalidNegativeScaleUpFactor() {
        var result =
                testSessionJobAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.MAX_SCALE_UP_FACTOR.key(), "-0.1"));
        assertErrorContains(
                result, getFormattedErrorMessage(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.0d));
    }

    @Test
    public void testValidateSessionJobWithInvalidNegativeUtilization() {
        var result =
                testSessionJobAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "-0.6"));
        assertErrorContains(
                result, getFormattedErrorMessage(AutoScalerOptions.UTILIZATION_TARGET, 0.0d, 1.0));
    }

    @Test
    public void testValidateSessionJobWithInvalidNegativeUtilizationBoundary() {
        var resultMaxUtilization =
                testSessionJobAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "-0.6"));
        assertErrorContains(
                resultMaxUtilization,
                getFormattedErrorMessage(
                        AutoScalerOptions.UTILIZATION_MAX,
                        AutoScalerOptions.UTILIZATION_TARGET.defaultValue(),
                        1.0d));

        var resultMinUtilization =
                testSessionJobAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "-0.6"));
        assertErrorContains(
                resultMinUtilization,
                getFormattedErrorMessage(
                        AutoScalerOptions.UTILIZATION_MIN,
                        0.0d,
                        AutoScalerOptions.UTILIZATION_TARGET.defaultValue()));
    }

    @Test
    public void testValidateSessionJobWithInvalidUtilizationBoundary() {
        var resultMaxUtilization =
                testSessionJobAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "-0.6"));
        assertErrorContains(
                resultMaxUtilization,
                getFormattedErrorMessage(
                        AutoScalerOptions.UTILIZATION_MAX,
                        AutoScalerOptions.UTILIZATION_TARGET.defaultValue(),
                        1.0d));

        var resultMinUtilization =
                testSessionJobAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "-0.6"));
        assertErrorContains(
                resultMinUtilization,
                getFormattedErrorMessage(
                        AutoScalerOptions.UTILIZATION_MIN,
                        0.0,
                        AutoScalerOptions.UTILIZATION_TARGET.defaultValue()));
    }

    @Test
    public void testValidateSessionJobWithInvalidExcludedPeriods() {
        var result =
                testSessionJobAutoScalerConfiguration(
                        flinkConf ->
                                flinkConf.put(
                                        AutoScalerOptions.EXCLUDED_PERIODS.key(), "12:00-10:00"));
        assertTrue(result.isPresent());
    }

    @Test
    public void testNonEnabledAutoScalerSessionJob() {
        var result =
                testSessionJobAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.AUTOSCALER_ENABLED.key(), "false");
                            flinkConf.put(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.MAX_SCALE_UP_FACTOR.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "-1.6");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "-1.6");
                        });
        assertErrorNotContains(result);
    }

    @Test
    public void testAutoScalerUtilizationConfiguration() {
        var deploymentResult =
                testAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "0.3");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "0.5");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "0.4");
                        });
        assertErrorContains(
                deploymentResult,
                getFormattedErrorMessage(AutoScalerOptions.UTILIZATION_MAX, 0.5, 1.0));

        deploymentResult =
                testAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "0.8");
                        });
        assertErrorContains(
                deploymentResult,
                getFormattedErrorMessage(
                        AutoScalerOptions.UTILIZATION_MIN,
                        0.0,
                        AutoScalerOptions.UTILIZATION_TARGET.defaultValue()));

        deploymentResult =
                testAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "1.5");
                        });

        assertErrorContains(
                deploymentResult,
                getFormattedErrorMessage(AutoScalerOptions.UTILIZATION_TARGET, 0.0, 1.0));

        deploymentResult =
                testAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "0.2");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "0.5");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "0.6");
                        });
        assertErrorNotContains(deploymentResult);

        var sessionResult =
                testSessionJobAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "0.3");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "0.5");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "0.4");
                        });
        assertErrorContains(
                sessionResult,
                getFormattedErrorMessage(AutoScalerOptions.UTILIZATION_MAX, 0.5, 1.0));

        sessionResult =
                testSessionJobAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "0.6");
                        });
        assertErrorContains(
                sessionResult,
                getFormattedErrorMessage(
                        AutoScalerOptions.UTILIZATION_MAX,
                        AutoScalerOptions.UTILIZATION_TARGET.defaultValue(),
                        1.0));

        sessionResult =
                testSessionJobAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "1.5");
                        });

        assertErrorContains(
                sessionResult,
                getFormattedErrorMessage(AutoScalerOptions.UTILIZATION_TARGET, 0.0, 1.0));

        sessionResult =
                testSessionJobAutoScalerConfiguration(
                        flinkConf -> {
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "0.2");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "0.5");
                            flinkConf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "0.6");
                        });
        assertErrorNotContains(sessionResult);
    }

    private Optional<String> testSessionJobAutoScalerConfiguration(
            Consumer<Map<String, String>> flinkConfigurationModifier) {
        var sessionCluster = TestUtils.buildSessionCluster();
        var sessionJob = TestUtils.buildSessionJob();
        var flinkConfiguration = getDefaultTestAutoScalerFlinkConfigurationMap();
        flinkConfigurationModifier.accept(flinkConfiguration);
        sessionCluster.getSpec().setFlinkConfiguration(flinkConfiguration);
        return validator.validateSessionJob(sessionJob, Optional.of(sessionCluster));
    }

    public Optional<String> testAutoScalerConfiguration(
            Consumer<Map<String, String>> flinkConfigurationModifier) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        var flinkConfiguration = getDefaultTestAutoScalerFlinkConfigurationMap();
        flinkConfigurationModifier.accept(flinkConfiguration);
        deployment.getSpec().setFlinkConfiguration(flinkConfiguration);
        return validator.validateDeployment(deployment);
    }

    private Map<String, String> getDefaultTestAutoScalerFlinkConfigurationMap() {
        Map<String, String> conf = new HashMap<>();
        conf.put(AutoScalerOptions.AUTOSCALER_ENABLED.key(), "true");
        conf.put(AutoScalerOptions.MAX_SCALE_UP_FACTOR.key(), "100000.0");
        conf.put(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR.key(), "0.6");
        conf.put(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED.key(), "0.1");
        conf.put(AutoScalerOptions.UTILIZATION_TARGET.key(), "0.7");
        conf.put(AutoScalerOptions.UTILIZATION_MAX.key(), "1.0");
        conf.put(AutoScalerOptions.UTILIZATION_MIN.key(), "0.3");
        return conf;
    }

    private static String getFormattedErrorMessage(
            ConfigOption<Double> configValue, Double min, Double max) {
        return String.format(
                "The AutoScalerOption %s is invalid, it should be a value within the range [%s, %s]",
                configValue.key(),
                min != null ? min.toString() : "-Infinity",
                max != null ? max.toString() : "+Infinity");
    }

    private static String getFormattedNumberOrderErrorMessage(
            ConfigOption<Double> configValueLeft, ConfigOption<Double> configValueRight) {
        return String.format(
                "The AutoScalerOption %s or %s is invalid, %s must be less than or equal to the value of "
                        + "%s",
                configValueLeft.key(),
                configValueRight.key(),
                configValueLeft.key(),
                configValueRight.key());
    }

    private static String getFormattedErrorMessage(ConfigOption<Double> configValue, Double min) {
        return getFormattedErrorMessage(configValue, min, null);
    }

    private static void assertErrorContains(Optional<String> result, String error) {
        if (result.isEmpty()) {
            Assertions.fail("Invalid Configuration not caught in the tests");
        } else {
            Assertions.assertEquals(error, result.get());
        }
    }

    private static void assertErrorNotContains(Optional<String> result) {
        if (result.isPresent()) {
            Assertions.fail("Invalid Configuration not caught in the tests");
        }
    }

    @Test
    public void testFlinkStateSnapshotValidator() {
        testStateSnapshotValidateWithModifier(snapshot -> {}, null);

        testStateSnapshotValidateWithModifier(
                snapshot -> {
                    snapshot.getSpec().setCheckpoint(CheckpointSpec.builder().build());
                    snapshot.getSpec().setSavepoint(SavepointSpec.builder().build());
                },
                "Exactly one of checkpoint or savepoint configurations has to be set.");

        testStateSnapshotValidateWithModifier(
                snapshot -> snapshot.getSpec().setJobReference(null),
                "Job reference must be supplied for this snapshot");

        testStateSnapshotValidateWithModifier(
                snapshot -> {
                    snapshot.getSpec().setJobReference(null);
                    snapshot.getSpec().getSavepoint().setAlreadyExists(true);
                },
                null);

        var refName = "does-not-exist";
        var snapshot =
                TestUtils.buildFlinkStateSnapshotSavepoint(
                        false,
                        JobReference.builder()
                                .kind(JobKind.FLINK_DEPLOYMENT)
                                .name(refName)
                                .build());
        testStateSnapshotValidate(
                snapshot,
                Optional.empty(),
                String.format("Target for snapshot test/%s was not found", refName));
    }

    private void testStateSnapshotValidateWithModifier(
            Consumer<FlinkStateSnapshot> snapshotModifier, @Nullable String expectedErr) {
        var flinkDeployment = TestUtils.buildApplicationCluster();
        var snapshot =
                TestUtils.buildFlinkStateSnapshotSavepoint(
                        false, JobReference.fromFlinkResource(flinkDeployment));

        snapshotModifier.accept(snapshot);
        testStateSnapshotValidate(snapshot, Optional.of(flinkDeployment), expectedErr);
    }

    private void testStateSnapshotValidate(
            FlinkStateSnapshot flinkStateSnapshot,
            Optional<AbstractFlinkResource<?, ?>> secondaryResource,
            @Nullable String expectedErr) {
        Optional<String> error =
                validator.validateStateSnapshot(flinkStateSnapshot, secondaryResource);
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
