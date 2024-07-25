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

package org.apache.flink.kubernetes.operator.api.utils;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.CheckpointSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobReference;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.SavepointSpec;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CheckpointType;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Base Testing utilities. */
public class BaseTestUtils {

    public static final String TEST_NAMESPACE = "flink-operator-test";
    public static final String TEST_DEPLOYMENT_NAME = "test-cluster";
    public static final String TEST_SESSION_JOB_NAME = "test-session-job";
    public static final String SERVICE_ACCOUNT = "flink-operator";
    public static final String FLINK_VERSION = "latest";
    public static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    public static final String IMAGE_POLICY = "IfNotPresent";
    public static final String SAMPLE_JAR = "local:///tmp/sample.jar";

    public static FlinkDeployment buildSessionCluster() {
        return buildSessionCluster(FlinkVersion.v1_17);
    }

    public static FlinkDeployment buildSessionCluster(FlinkVersion version) {
        return buildSessionCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, version);
    }

    public static FlinkDeployment buildSessionCluster(
            String name, String namespace, FlinkVersion version) {
        FlinkDeployment deployment = new FlinkDeployment();
        deployment.setStatus(new FlinkDeploymentStatus());
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withCreationTimestamp(Instant.now().toString())
                        .withUid(UUID.randomUUID().toString())
                        .withResourceVersion("1")
                        .build());
        deployment.setSpec(getTestFlinkDeploymentSpec(version));
        return deployment;
    }

    public static FlinkDeployment buildApplicationCluster(JobState state) {
        return buildApplicationCluster(FlinkVersion.v1_17, state);
    }

    public static FlinkDeployment buildApplicationCluster() {
        return buildApplicationCluster(FlinkVersion.v1_17, JobState.RUNNING);
    }

    public static FlinkDeployment buildApplicationCluster(String name, String namespace) {
        return buildApplicationCluster(name, namespace, FlinkVersion.v1_17, JobState.RUNNING);
    }

    public static FlinkDeployment buildApplicationCluster(FlinkVersion version) {
        return buildApplicationCluster(
                TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, version, JobState.RUNNING);
    }

    public static FlinkDeployment buildApplicationCluster(FlinkVersion version, JobState state) {
        return buildApplicationCluster(TEST_DEPLOYMENT_NAME, TEST_NAMESPACE, version, state);
    }

    public static FlinkDeployment buildApplicationCluster(
            String name, String namespace, FlinkVersion version, JobState state) {
        FlinkDeployment deployment = buildSessionCluster(name, namespace, version);
        deployment
                .getSpec()
                .setJob(
                        JobSpec.builder()
                                .jarURI(SAMPLE_JAR)
                                .parallelism(1)
                                .upgradeMode(UpgradeMode.STATELESS)
                                .state(state)
                                .build());
        deployment.setStatus(deployment.initStatus());
        return deployment;
    }

    public static FlinkSessionJob buildSessionJob(String name, String namespace) {
        return buildSessionJob(name, namespace, JobState.RUNNING);
    }

    public static FlinkSessionJob buildSessionJob(
            String name, String namespace, JobState jobState) {
        FlinkSessionJob sessionJob = new FlinkSessionJob();
        sessionJob.setStatus(new FlinkSessionJobStatus());
        sessionJob.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withCreationTimestamp(Instant.now().toString())
                        .withUid(UUID.randomUUID().toString())
                        .withGeneration(1L)
                        .withResourceVersion("1")
                        .build());

        Map<String, String> conf = new HashMap<>();
        conf.put("kubernetes.operator.user.artifacts.http.header", "header");
        sessionJob.setSpec(
                FlinkSessionJobSpec.builder()
                        .deploymentName(TEST_DEPLOYMENT_NAME)
                        .job(
                                JobSpec.builder()
                                        .jarURI(SAMPLE_JAR)
                                        .parallelism(1)
                                        .upgradeMode(UpgradeMode.STATELESS)
                                        .state(jobState)
                                        .build())
                        .flinkConfiguration(conf)
                        .build());
        return sessionJob;
    }

    public static FlinkSessionJob buildSessionJob() {
        return buildSessionJob(JobState.RUNNING);
    }

    public static FlinkSessionJob buildSessionJob(JobState state) {
        return buildSessionJob(TEST_SESSION_JOB_NAME, TEST_NAMESPACE, state);
    }

    public static FlinkDeploymentSpec getTestFlinkDeploymentSpec(FlinkVersion version) {
        Map<String, String> conf = new HashMap<>();
        conf.put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "2");
        conf.put(
                HighAvailabilityOptions.HA_MODE.key(),
                KubernetesHaServicesFactory.class.getCanonicalName());
        conf.put(HighAvailabilityOptions.HA_STORAGE_PATH.key(), "test");
        conf.put(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), "test-savepoint-dir");
        conf.put(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
                "file:///test/test-checkpoint-dir");

        return FlinkDeploymentSpec.builder()
                .image(IMAGE)
                .imagePullPolicy(IMAGE_POLICY)
                .serviceAccount(SERVICE_ACCOUNT)
                .flinkVersion(version)
                .flinkConfiguration(conf)
                .jobManager(new JobManagerSpec(new Resource(1.0, "2048m", "2G"), 1, null))
                .taskManager(new TaskManagerSpec(new Resource(1.0, "2048m", "2G"), null, null))
                .build();
    }

    public static PodTemplateSpec getTestPodTemplate(String hostname, List<Container> containers) {
        final PodSpec podSpec = new PodSpec();
        podSpec.setHostname(hostname);
        podSpec.setContainers(containers);
        var pod = new PodTemplateSpec();
        pod.setSpec(podSpec);
        return pod;
    }

    public static Pod getTestPod(String hostname, String apiVersion, List<Container> containers) {
        var pod = new Pod();
        var podTemplate = getTestPodTemplate(hostname, containers);
        pod.setApiVersion(apiVersion);
        pod.setSpec(podTemplate.getSpec());
        pod.setMetadata(podTemplate.getMetadata());
        return pod;
    }

    public static FlinkStateSnapshot buildFlinkStateSnapshotSavepoint(
            boolean alreadyExists, JobReference jobReference) {
        return buildFlinkStateSnapshotSavepoint(
                "test-savepoint", "test", "test-path", alreadyExists, jobReference);
    }

    public static FlinkStateSnapshot buildFlinkStateSnapshotSavepoint(
            String name,
            String namespace,
            String path,
            boolean alreadyExists,
            JobReference jobReference) {
        var savepointSpec = SavepointSpec.builder().path(path).alreadyExists(alreadyExists).build();
        var spec =
                FlinkStateSnapshotSpec.builder()
                        .jobReference(jobReference)
                        .savepoint(savepointSpec)
                        .build();
        var snapshot = new FlinkStateSnapshot();
        snapshot.setSpec(spec);

        snapshot.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withCreationTimestamp(Instant.now().toString())
                        .withUid(UUID.randomUUID().toString())
                        .withGeneration(1L)
                        .withResourceVersion("1")
                        .build());

        return snapshot;
    }

    public static FlinkStateSnapshot buildFlinkStateSnapshotCheckpoint(
            String name,
            String namespace,
            CheckpointType checkpointType,
            JobReference jobReference) {
        var spec =
                FlinkStateSnapshotSpec.builder()
                        .jobReference(jobReference)
                        .checkpoint(new CheckpointSpec())
                        .build();
        var snapshot = new FlinkStateSnapshot();
        snapshot.setSpec(spec);

        snapshot.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withCreationTimestamp(Instant.now().toString())
                        .withUid(UUID.randomUUID().toString())
                        .withGeneration(1L)
                        .withResourceVersion("1")
                        .build());

        return snapshot;
    }
}
