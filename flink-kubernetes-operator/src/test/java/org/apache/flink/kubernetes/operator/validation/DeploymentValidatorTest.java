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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test deployment validation logic. */
public class DeploymentValidatorTest {

    private final DefaultDeploymentValidator validator = new DefaultDeploymentValidator();

    @Test
    public void testValidation() {
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
                dep -> dep.getSpec().getJobManager().setReplicas(2),
                "High availability should be enabled when starting standby JobManagers.");
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
                    dep.getStatus().getJobStatus().setSavepointLocation("sp");

                    dep.getStatus().setReconciliationStatus(new ReconciliationStatus());
                    dep.getStatus()
                            .getReconciliationStatus()
                            .setLastReconciledSpec(TestUtils.clone(dep.getSpec()));
                    dep.getStatus()
                            .getReconciliationStatus()
                            .getLastReconciledSpec()
                            .getJob()
                            .setState(JobState.SUSPENDED);

                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
                });

        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus().setReconciliationStatus(new ReconciliationStatus());
                    dep.getStatus()
                            .getReconciliationStatus()
                            .setLastReconciledSpec(TestUtils.clone(dep.getSpec()));
                    dep.getStatus()
                            .getReconciliationStatus()
                            .getLastReconciledSpec()
                            .getJob()
                            .setState(JobState.SUSPENDED);

                    dep.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
                },
                "Cannot perform savepoint restore without a valid savepoint");

        // Test cluster type validation
        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus().setReconciliationStatus(new ReconciliationStatus());
                    dep.getStatus()
                            .getReconciliationStatus()
                            .setLastReconciledSpec(TestUtils.clone(dep.getSpec()));
                    dep.getSpec().setJob(null);
                },
                "Cannot switch from job to session cluster");

        testError(
                dep -> {
                    dep.setStatus(new FlinkDeploymentStatus());
                    dep.getStatus().setJobStatus(new JobStatus());

                    dep.getStatus().setReconciliationStatus(new ReconciliationStatus());
                    dep.getStatus()
                            .getReconciliationStatus()
                            .setLastReconciledSpec(TestUtils.clone(dep.getSpec()));
                    dep.getStatus().getReconciliationStatus().getLastReconciledSpec().setJob(null);
                },
                "Cannot switch from session to job cluster");
    }

    private void testSuccess(Consumer<FlinkDeployment> deploymentModifier) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deploymentModifier.accept(deployment);
        validator.validate(deployment).ifPresent(Assert::fail);
    }

    private void testError(Consumer<FlinkDeployment> deploymentModifier, String expectedErr) {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        deploymentModifier.accept(deployment);
        Optional<String> error = validator.validate(deployment);
        if (error.isPresent()) {
            assertTrue(error.get(), error.get().startsWith(expectedErr));
        } else {
            fail("Did not get expected error: " + expectedErr);
        }
    }
}
