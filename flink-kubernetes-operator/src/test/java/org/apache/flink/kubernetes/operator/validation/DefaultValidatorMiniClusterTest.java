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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultValidator#validateMiniCluster}. */
class DefaultValidatorMiniClusterTest {

    private final DefaultValidator validator =
            new DefaultValidator(new FlinkConfigManager(new Configuration()));

    @Test
    void testValidMiniCluster() {
        testSuccess(mc -> {});
    }

    @Test
    void testName() {
        testSuccess(mc -> mc.getMetadata().setName("my-minicluster-1"));
        testError(
                mc -> mc.getMetadata().setName("my.minicluster"),
                "The FlinkMiniCluster name: my.minicluster is invalid");
    }

    @Test
    void testFlinkVersionRequired() {
        testError(mc -> mc.getSpec().setFlinkVersion(null), "Flink Version must be defined.");
    }

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    void testMinimumFlinkVersion(FlinkVersion version) {
        if (!FlinkVersion.isSupported(version)) {
            testError(mc -> mc.getSpec().setFlinkVersion(version), "Flink version " + version);
        } else if (version.isEqualOrNewer(DefaultValidator.MINICLUSTER_MIN_VERSION)) {
            testSuccess(mc -> mc.getSpec().setFlinkVersion(version));
        } else {
            testError(
                    mc -> mc.getSpec().setFlinkVersion(version),
                    "FlinkMiniCluster requires Flink version "
                            + DefaultValidator.MINICLUSTER_MIN_VERSION
                            + " or newer, but "
                            + version
                            + " was requested");
        }
    }

    @Test
    void testJobRequired() {
        testError(mc -> mc.getSpec().setJob(null), "The job spec should not be empty");
    }

    @Test
    void testJarUriOptional() {
        // As for FlinkDeployment, the job jar can be provided in usrlib instead.
        testSuccess(mc -> mc.getSpec().getJob().setJarURI(null));
    }

    @Test
    void testJobSpec() {
        testError(
                mc -> mc.getSpec().getJob().setParallelism(0),
                "Job parallelism must be larger than 0");
        testError(
                mc -> {
                    mc.getSpec()
                            .getFlinkConfiguration()
                            .remove(CheckpointingOptions.SAVEPOINT_DIRECTORY.key());
                    mc.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
                },
                "Job could not be upgraded with savepoint while config key");
    }

    @Test
    void testParallelismLimitedByTaskSlots() {
        // Without a configured slot count, the slots are sized from the job parallelism.
        testSuccess(mc -> mc.getSpec().getJob().setParallelism(4));
        testError(
                mc -> {
                    mc.getSpec()
                            .getFlinkConfiguration()
                            .put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "1");
                    mc.getSpec().getJob().setParallelism(2);
                },
                "Job parallelism (2) must not exceed the MiniCluster task slots ("
                        + TaskManagerOptions.NUM_TASK_SLOTS.key()
                        + "=1)");
        testError(
                mc -> {
                    mc.getSpec()
                            .getFlinkConfiguration()
                            .put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "2");
                    mc.getSpec().getJob().setParallelism(3);
                },
                "Job parallelism (3) must not exceed the MiniCluster task slots ("
                        + TaskManagerOptions.NUM_TASK_SLOTS.key()
                        + "=2)");
        testSuccess(
                mc -> {
                    mc.getSpec()
                            .getFlinkConfiguration()
                            .put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "4");
                    mc.getSpec().getJob().setParallelism(4);
                });
        testSuccess(
                mc -> {
                    mc.getSpec()
                            .getFlinkConfiguration()
                            .put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "4");
                    mc.getSpec().getJob().setParallelism(2);
                });
        testError(
                mc ->
                        mc.getSpec()
                                .getFlinkConfiguration()
                                .put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "many"),
                "Invalid value for " + TaskManagerOptions.NUM_TASK_SLOTS.key());
    }

    @Test
    void testParallelismLimitedByOperatorDefaultTaskSlots() {
        var conf = new Configuration();
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        var validator = new DefaultValidator(new FlinkConfigManager(conf));
        var miniCluster = TestUtils.buildMiniCluster();
        miniCluster.getSpec().getJob().setParallelism(3);
        assertThat(validator.validateMiniCluster(miniCluster))
                .contains(
                        "Job parallelism (3) must not exceed the MiniCluster task slots ("
                                + TaskManagerOptions.NUM_TASK_SLOTS.key()
                                + "=2)");
    }

    @Test
    void testAutoscalerNotSupported() {
        testError(
                mc ->
                        mc.getSpec()
                                .getFlinkConfiguration()
                                .put(AutoScalerOptions.AUTOSCALER_ENABLED.key(), "true"),
                "The autoscaler is not supported for FlinkMiniCluster resources, "
                        + AutoScalerOptions.AUTOSCALER_ENABLED.key()
                        + " must not be enabled");
        testError(
                mc ->
                        mc.getSpec()
                                .getFlinkConfiguration()
                                .put("kubernetes.operator.job.autoscaler.enabled", "true"),
                "The autoscaler is not supported for FlinkMiniCluster resources");
        testError(
                mc ->
                        mc.getSpec()
                                .getFlinkConfiguration()
                                .put(AutoScalerOptions.AUTOSCALER_ENABLED.key(), "sometimes"),
                "Invalid value for " + AutoScalerOptions.AUTOSCALER_ENABLED.key());
        testSuccess(
                mc ->
                        mc.getSpec()
                                .getFlinkConfiguration()
                                .put(AutoScalerOptions.AUTOSCALER_ENABLED.key(), "false"));
        testSuccess(mc -> mc.getSpec().getJob().setAutoscalerResetNonce(1L));
    }

    @Test
    void testAutoscalerEnabledByOperatorDefault() {
        var conf = new Configuration();
        conf.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        var validator = new DefaultValidator(new FlinkConfigManager(conf));
        var miniCluster = TestUtils.buildMiniCluster();
        assertThat(validator.validateMiniCluster(miniCluster)).isEmpty();
    }

    @Test
    void testMemoryRequired() {
        testError(
                mc -> mc.getSpec().setResources(null),
                "MiniCluster memory must be defined using spec.resources.requests.memory or spec.resources.limits.memory");
        testError(
                mc ->
                        mc.getSpec()
                                .setResources(
                                        new ResourceRequirementsBuilder()
                                                .addToRequests("cpu", new Quantity("1"))
                                                .build()),
                "MiniCluster memory must be defined");
        testSuccess(
                mc ->
                        mc.getSpec()
                                .setResources(
                                        new ResourceRequirementsBuilder()
                                                .addToLimits("memory", new Quantity("1Gi"))
                                                .build()));
    }

    @Test
    void testResourceRequirements() {
        testError(
                mc ->
                        mc.getSpec()
                                .setResources(
                                        new ResourceRequirementsBuilder()
                                                .addToRequests("memory", new Quantity("2Gi"))
                                                .addToLimits("memory", new Quantity("1Gi"))
                                                .build()),
                "MiniCluster resource requirements memory request (2Gi) must not exceed its limit (1Gi)");
    }

    @Test
    void testForbiddenConfig() {
        testError(
                mc ->
                        mc.getSpec()
                                .getFlinkConfiguration()
                                .put(KubernetesConfigOptions.CLUSTER_ID.key(), "other"),
                "Forbidden Flink config key: " + KubernetesConfigOptions.CLUSTER_ID.key());
        testError(
                mc ->
                        mc.getSpec()
                                .getFlinkConfiguration()
                                .put(HighAvailabilityOptions.HA_CLUSTER_ID.key(), "other"),
                "Forbidden Flink config key: " + HighAvailabilityOptions.HA_CLUSTER_ID.key());
    }

    @Test
    void testIngress() {
        testError(mc -> mc.getSpec().setIngress(new IngressSpec()), "Ingress template must");
        testSuccess(
                mc ->
                        mc.getSpec()
                                .setIngress(
                                        IngressSpec.builder()
                                                .template("{{name}}.{{namespace}}.example.com")
                                                .build()));
    }

    @Test
    void testLogConfig() {
        testSuccess(mc -> mc.getSpec().setLogConfiguration(Map.of("log4j-console.properties", "")));
        testError(
                mc -> mc.getSpec().setLogConfiguration(Map.of("other.properties", "")),
                "Invalid log config key: other.properties");
    }

    @Test
    void testServiceAccountRequired() {
        testError(mc -> mc.getSpec().setServiceAccount(null), "spec.serviceAccount must be");
    }

    @Test
    void testSavepointRedeployRequiresInitialSavepointPath() {
        testError(
                mc -> {
                    markDeployed(mc);
                    mc.getSpec().getJob().setSavepointRedeployNonce(1L);
                },
                "InitialSavepointPath must not be empty for savepoint redeployment");
        testSuccess(
                mc -> {
                    markDeployed(mc);
                    mc.getSpec().getJob().setSavepointRedeployNonce(1L);
                    mc.getSpec().getJob().setInitialSavepointPath("file:///savepoint");
                });
    }

    @Test
    void testFlinkVersionChangeAfterLastStateSuspend() {
        // v2_3 is below the MiniCluster minimum, but v2_4 is currently the only version that
        // FlinkMiniCluster supports, so there is no supported pair of versions to change between.
        testError(
                mc -> {
                    mc.getSpec().setFlinkVersion(FlinkVersion.v2_3);
                    mc.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
                    mc.getSpec().getJob().setState(JobState.SUSPENDED);
                    markDeployed(mc);
                    mc.getSpec().setFlinkVersion(FlinkVersion.v2_4);
                    mc.getSpec().getJob().setState(JobState.RUNNING);
                },
                "Changing flinkVersion after last-state suspend is not allowed.");
    }

    private static void markDeployed(FlinkMiniCluster miniCluster) {
        var reconciliationStatus = miniCluster.getStatus().getReconciliationStatus();
        reconciliationStatus.setState(ReconciliationState.DEPLOYED);
        reconciliationStatus.setLastReconciledSpec(
                SpecUtils.writeSpecWithMeta(miniCluster.getSpec(), miniCluster));
    }

    private void testSuccess(Consumer<FlinkMiniCluster> modifier) {
        var miniCluster = TestUtils.buildMiniCluster();
        modifier.accept(miniCluster);
        assertThat(validator.validateMiniCluster(miniCluster)).isEmpty();
    }

    private void testError(Consumer<FlinkMiniCluster> modifier, String expectedErrorPrefix) {
        var miniCluster = TestUtils.buildMiniCluster();
        modifier.accept(miniCluster);
        Optional<String> error = validator.validateMiniCluster(miniCluster);
        assertThat(error).hasValueSatisfying(e -> assertThat(e).startsWith(expectedErrorPrefix));
    }
}
