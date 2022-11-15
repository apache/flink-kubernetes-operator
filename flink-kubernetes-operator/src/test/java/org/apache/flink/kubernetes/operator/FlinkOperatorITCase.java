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

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.exception.StatusConflictException;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Flink Operator integration test. */
public class FlinkOperatorITCase {

    private static final String TEST_NAMESPACE = "flink-operator-test";
    private static final String SERVICE_ACCOUNT = "flink-operator";
    private static final String CLUSTER_ROLE_BINDING = "flink-operator-role-binding";
    private static final String FLINK_VERSION = "1.15";
    private static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    private static final Logger LOG = LoggerFactory.getLogger(FlinkOperatorITCase.class);
    public static final String SESSION_NAME = "test-session-cluster";
    private static KubernetesClient client;

    @BeforeEach
    public void setup() {
        client = new KubernetesClientBuilder().build();
        LOG.info("Cleaning up namespace {}", TEST_NAMESPACE);
        client.namespaces().withName(TEST_NAMESPACE).delete();
        await().atMost(1, MINUTES)
                .until(() -> client.namespaces().withName(TEST_NAMESPACE).get() == null);

        LOG.info("Recreating namespace {}", TEST_NAMESPACE);
        Namespace testNs =
                new NamespaceBuilder()
                        .withMetadata(new ObjectMetaBuilder().withName(TEST_NAMESPACE).build())
                        .build();
        client.resource(testNs).create();
        rbacSetup();
    }

    @AfterEach
    public void cleanup() {
        LOG.info("Cleaning up namespace {}", TEST_NAMESPACE);
        client.namespaces().withName(TEST_NAMESPACE).delete();
        client.close();
    }

    @Test
    public void test() {
        FlinkDeployment flinkDeployment = buildSessionCluster();
        LOG.info("Deploying {}", flinkDeployment.getMetadata().getName());
        var v1 = client.resource(flinkDeployment).createOrReplace();

        await().atMost(1, MINUTES)
                .untilAsserted(
                        () ->
                                assertThat(
                                        client.apps()
                                                .deployments()
                                                .inNamespace(TEST_NAMESPACE)
                                                .withName(SESSION_NAME)
                                                .isReady(),
                                        is(true)));

        // Test status recorder locking logic
        var statusRecorder =
                new StatusRecorder<FlinkDeployment, FlinkDeploymentStatus>(
                        client, new MetricManager<>(), (a, b) -> {});
        try {
            v1.getStatus().setError("e2");
            // Should throw error as status was modified externally
            statusRecorder.patchAndCacheStatus(v1);
            fail();
        } catch (StatusConflictException expected) {
        }
    }

    private static FlinkDeployment buildSessionCluster() {
        FlinkDeployment deployment = new FlinkDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(SESSION_NAME)
                        .withNamespace(TEST_NAMESPACE)
                        .build());
        FlinkDeploymentSpec spec = new FlinkDeploymentSpec();
        spec.setImage(IMAGE);
        spec.setFlinkVersion(FlinkVersion.v1_15);
        spec.setServiceAccount(SERVICE_ACCOUNT);
        Resource resource = new Resource();
        resource.setMemory("2048m");
        resource.setCpu(1.0);
        JobManagerSpec jm = new JobManagerSpec();
        jm.setResource(resource);
        jm.setReplicas(1);
        spec.setJobManager(jm);
        TaskManagerSpec tm = new TaskManagerSpec();
        tm.setResource(resource);
        spec.setTaskManager(tm);
        deployment.setSpec(spec);
        return deployment;
    }

    private static void rbacSetup() {
        LOG.info("Creating service account {}", SERVICE_ACCOUNT);
        ServiceAccount serviceAccount =
                new ServiceAccountBuilder()
                        .withNewMetadata()
                        .withName(SERVICE_ACCOUNT)
                        .withNamespace(TEST_NAMESPACE)
                        .endMetadata()
                        .build();

        client.resource(serviceAccount).createOrReplace();

        ClusterRoleBinding current =
                client.rbac().clusterRoleBindings().withName(CLUSTER_ROLE_BINDING).get();
        boolean exists =
                current.getSubjects().stream()
                        .anyMatch(
                                s ->
                                        SERVICE_ACCOUNT.equals(s.getName())
                                                && TEST_NAMESPACE.equals(s.getNamespace()));
        if (!exists) {
            LOG.info("Patching crb {}", CLUSTER_ROLE_BINDING);
            client.rbac()
                    .clusterRoleBindings()
                    .withName(CLUSTER_ROLE_BINDING)
                    .edit(
                            crb ->
                                    new ClusterRoleBindingBuilder(crb)
                                            .addNewSubject()
                                            .withKind("ServiceAccount")
                                            .withName(SERVICE_ACCOUNT)
                                            .withNamespace(TEST_NAMESPACE)
                                            .endSubject()
                                            .build());
        }
    }
}
