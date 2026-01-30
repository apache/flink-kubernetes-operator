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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionMode;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.TRANSITION_STAGE;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.ABORT_GRACE_PERIOD;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentConfigOptions.RECONCILIATION_RESCHEDULING_INTERVAL;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.SAMPLE_JAR;

/** Shared utilities for Blue/Green testing. */
public class BlueGreenTestUtils {

    public static final String SERVICE_ACCOUNT = "flink-operator";
    public static final String FLINK_VERSION = "latest";
    public static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    public static final String IMAGE_POLICY = "IfNotPresent";
    private static final int DEFAULT_DELETION_DELAY_VALUE = 500;

    public static FlinkBlueGreenDeployment buildAdvancedDeployment(
            String name,
            String namespace,
            FlinkVersion version,
            String initialSavepointPath,
            UpgradeMode upgradeMode) {
        var deployment = new FlinkBlueGreenDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withCreationTimestamp(Instant.now().toString())
                        .withUid(UUID.randomUUID().toString())
                        .withResourceVersion("1")
                        .build());
        var bgDeploymentSpec = getTestFlinkDeploymentSpec(version);

        bgDeploymentSpec
                .getTemplate()
                .getSpec()
                .setJob(
                        JobSpec.builder()
                                .jarURI(SAMPLE_JAR)
                                .parallelism(1)
                                .upgradeMode(upgradeMode)
                                .state(JobState.RUNNING)
                                .initialSavepointPath(initialSavepointPath)
                                .build());

        bgDeploymentSpec.getTemplate().setTransitionMode(TransitionMode.ADVANCED);
        deployment.setSpec(bgDeploymentSpec);
        return deployment;
    }

    public static ConfigMap getConfigMapFromSecondaryResources(
            Context<FlinkBlueGreenDeployment> context, String deploymentName) {
        var configMaps = context.getSecondaryResources(ConfigMap.class);
        return configMaps.stream()
                .filter(cm -> cm.getMetadata().getName().equals(deploymentName + "-configmap"))
                .findFirst()
                .orElseThrow(
                        () ->
                                new RuntimeException(
                                        "ConfigMap not found for deployment: " + deploymentName));
    }

    public static TransitionStage getCurrentConfigMapStage(
            Context<FlinkBlueGreenDeployment> context, String deploymentName) {
        ConfigMap configMap = getConfigMapFromSecondaryResources(context, deploymentName);
        String stage = configMap.getData().get(TRANSITION_STAGE.getLabel());
        return TransitionStage.valueOf(stage);
    }

    public static void assertConfigMapProtocol(ConfigMap configMap) {
        Map<String, String> data = configMap.getData();
        if (!data.containsKey("IS_FIRST_DEPLOYMENT")
                || !data.containsKey("ACTIVE_DEPLOYMENT_TYPE")
                || !data.containsKey("TRANSITION_STAGE")) {
            throw new AssertionError("ConfigMap missing required protocol fields");
        }
    }

    public static boolean isValidTransitionStage(String stage) {
        try {
            TransitionStage.valueOf(stage);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public static FlinkBlueGreenDeploymentSpec getTestFlinkDeploymentSpec(FlinkVersion version) {
        Map<String, String> conf = new HashMap<>();
        conf.put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "2");
        conf.put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), "test-savepoint-dir");
        conf.put(CheckpointingOptions.INCREMENTAL_CHECKPOINTS.key(), "true");
        conf.put(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(), "10");
        conf.put(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
                "file:///test/test-checkpoint-dir");

        var flinkDeploymentSpec =
                FlinkDeploymentSpec.builder()
                        .image(IMAGE)
                        .imagePullPolicy(IMAGE_POLICY)
                        .serviceAccount(SERVICE_ACCOUNT)
                        .flinkVersion(version)
                        .flinkConfiguration(new ConfigObjectNode())
                        .jobManager(new JobManagerSpec(new Resource(1.0, "2048m", "2G"), 1, null))
                        .taskManager(
                                new TaskManagerSpec(new Resource(1.0, "2048m", "2G"), null, null))
                        .build();

        flinkDeploymentSpec.setFlinkConfiguration(conf);

        Map<String, String> configuration = new HashMap<>();
        configuration.put(ABORT_GRACE_PERIOD.key(), "1");
        configuration.put(RECONCILIATION_RESCHEDULING_INTERVAL.key(), "500");
        configuration.put(
                DEPLOYMENT_DELETION_DELAY.key(), String.valueOf(DEFAULT_DELETION_DELAY_VALUE));

        var flinkDeploymentTemplateSpec =
                FlinkDeploymentTemplateSpec.builder().spec(flinkDeploymentSpec).build();

        return new FlinkBlueGreenDeploymentSpec(configuration, flinkDeploymentTemplateSpec);
    }
}
