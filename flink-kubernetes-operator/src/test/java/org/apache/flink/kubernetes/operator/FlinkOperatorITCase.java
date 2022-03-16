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

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.crd.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.Resource;
import org.apache.flink.kubernetes.operator.crd.spec.TaskManagerSpec;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Flink Operator integration test. */
public class FlinkOperatorITCase {

    private static final String TEST_NAMESPACE = "flink-operator-test";
    private static final String SERVICE_ACCOUNT = "flink-operator";
    private static final String CLUSTER_ROLE_BINDING = "flink-operator-cluster-role-binding";
    private static final String FLINK_VERSION = "1.14.3";
    private static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    private static final Logger LOG = LoggerFactory.getLogger(FlinkOperatorITCase.class);
    private KubernetesClient client;

    @BeforeEach
    public void setup() {
        client = new DefaultKubernetesClient();
        LOG.info("Cleaning up namespace {}", TEST_NAMESPACE);
        client.namespaces().withName(TEST_NAMESPACE).delete();
        await().atMost(1, MINUTES)
                .until(() -> client.namespaces().withName(TEST_NAMESPACE).get() == null);

        LOG.info("Recreating namespace {}", TEST_NAMESPACE);
        Namespace testNs =
                new NamespaceBuilder()
                        .withMetadata(new ObjectMetaBuilder().withName(TEST_NAMESPACE).build())
                        .build();
        client.namespaces().create(testNs);
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
        client.resource(flinkDeployment).createOrReplace();

        await().atMost(1, MINUTES)
                .untilAsserted(
                        () -> {
                            assertThat(
                                    client.apps()
                                            .deployments()
                                            .inNamespace(TEST_NAMESPACE)
                                            .withName(flinkDeployment.getMetadata().getName())
                                            .isReady(),
                                    is(true));
                        });
    }

    private static FlinkDeployment buildSessionCluster() {
        FlinkDeployment deployment = new FlinkDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName("test-session-cluster")
                        .withNamespace(TEST_NAMESPACE)
                        .build());
        FlinkDeploymentSpec spec = new FlinkDeploymentSpec();
        spec.setImage(IMAGE);
        spec.setFlinkVersion(FlinkVersion.v1_14);
        spec.setServiceAccount(SERVICE_ACCOUNT);
        Resource resource = new Resource();
        resource.setMemory("2048m");
        resource.setCpu(1);
        JobManagerSpec jm = new JobManagerSpec();
        jm.setResource(resource);
        spec.setJobManager(jm);
        TaskManagerSpec tm = new TaskManagerSpec();
        tm.setResource(resource);
        spec.setTaskManager(tm);
        deployment.setSpec(spec);
        return deployment;
    }

    private void rbacSetup() {
        LOG.info("Creating service account {}", SERVICE_ACCOUNT);
        ServiceAccount serviceAccount =
                new ServiceAccountBuilder()
                        .withNewMetadata()
                        .withName(SERVICE_ACCOUNT)
                        .withNamespace(TEST_NAMESPACE)
                        .endMetadata()
                        .build();
        client.serviceAccounts().inNamespace(TEST_NAMESPACE).createOrReplace(serviceAccount);

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
