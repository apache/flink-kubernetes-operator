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

package org.apache.flink.kubernetes.operator.kubeclient.decorators;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.ParametersTestBase;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPort;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVar;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.LocalObjectReference;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Pod;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Quantity;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @link InitStandaloneTaskManagerDecorator unit tests
 */
public class InitStandaloneTaskManagerDecoratorTest extends ParametersTestBase {

    private Pod resultPod;
    private Container resultMainContainer;

    @BeforeEach
    public void setup() {
        setupFlinkConfig();

        ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();
        StandaloneKubernetesTaskManagerParameters kubernetesTaskManagerParameters =
                new StandaloneKubernetesTaskManagerParameters(flinkConfig, clusterSpecification);
        InitStandaloneTaskManagerDecorator decorator =
                new InitStandaloneTaskManagerDecorator(kubernetesTaskManagerParameters);

        final FlinkPod resultFlinkPod = decorator.decorateFlinkPod(createPodTemplate());
        resultPod = resultFlinkPod.getPodWithoutMainContainer();
        resultMainContainer = resultFlinkPod.getMainContainer();
    }

    @Test
    public void testOverrideApiVersion() {
        assertEquals(Constants.API_VERSION, resultPod.getApiVersion());
    }

    @Test
    public void testMainContainerName() {
        assertEquals(Constants.MAIN_CONTAINER_NAME, resultMainContainer.getName());
    }

    @Test
    public void testOverrideMainContainerImage() {
        assertEquals(TestUtils.IMAGE, resultMainContainer.getImage());
    }

    @Test
    public void testOverrideMainContainerImagePullPolicy() {
        assertEquals(TestUtils.IMAGE_POLICY, resultMainContainer.getImagePullPolicy());
    }

    @Test
    public void testOverrideMainContainerResourceRequirements() {
        final ResourceRequirements resourceRequirements = resultMainContainer.getResources();

        final Map<String, Quantity> requests = resourceRequirements.getRequests();
        assertEquals(Double.toString(TestUtils.TASK_MANAGER_CPU), requests.get("cpu").getAmount());
        assertEquals(
                String.valueOf(TestUtils.TASK_MANAGER_MEMORY_MB),
                requests.get("memory").getAmount());

        final Map<String, Quantity> limits = resourceRequirements.getLimits();
        assertEquals(Double.toString(TestUtils.TASK_MANAGER_CPU), limits.get("cpu").getAmount());
        assertEquals(
                String.valueOf(TestUtils.TASK_MANAGER_MEMORY_MB), limits.get("memory").getAmount());
    }

    @Test
    public void testMergeMainContainerPorts() {
        final List<ContainerPort> expectedContainerPorts =
                Arrays.asList(
                        new ContainerPortBuilder()
                                .withName(Constants.TASK_MANAGER_RPC_PORT_NAME)
                                .withContainerPort(Constants.TASK_MANAGER_RPC_PORT)
                                .build(),
                        new ContainerPortBuilder()
                                .withName(TEMPLATE_PORT_NAME)
                                .withContainerPort(TEMPLATE_PORT)
                                .build());

        assertThat(
                resultMainContainer.getPorts(),
                containsInAnyOrder(expectedContainerPorts.toArray()));
    }

    @Test
    public void testMergePodLabels() {
        final Map<String, String> expectedLabels =
                new HashMap<>(
                        StandaloneKubernetesUtils.getTaskManagerSelectors(TestUtils.CLUSTER_ID));
        expectedLabels.putAll(userLabels);
        expectedLabels.putAll(templateLabels);

        assertEquals(expectedLabels, resultPod.getMetadata().getLabels());
    }

    @Test
    public void testMergePodAnnotations() {
        final Map<String, String> expectedAnnotations = new HashMap<>(userAnnotations);
        expectedAnnotations.putAll(templateAnnotations);
        assertEquals(expectedAnnotations, resultPod.getMetadata().getAnnotations());
    }

    @Test
    public void testOverridePodServiceAccountName() {
        assertEquals(TestUtils.SERVICE_ACCOUNT, resultPod.getSpec().getServiceAccountName());
    }

    @Test
    public void testMergeImagePullSecrets() {
        final List<String> resultSecrets =
                resultPod.getSpec().getImagePullSecrets().stream()
                        .map(LocalObjectReference::getName)
                        .collect(Collectors.toList());
        final List<String> expectedImagePullSecrets = new ArrayList<>(userImagePullSecrets);
        expectedImagePullSecrets.addAll(templateImagePullSecrets);

        assertThat(resultSecrets, containsInAnyOrder(expectedImagePullSecrets.toArray()));
    }

    @Test
    public void testMergeNodeSelector() {
        final Map<String, String> expectedNodeSelectors = new HashMap<>(userNodeSelectors);
        expectedNodeSelectors.putAll(templateNodeSelector);
        assertEquals(expectedNodeSelectors, resultPod.getSpec().getNodeSelector());
    }

    @Test
    public void testEnvs() {
        final List<EnvVar> envVars = resultMainContainer.getEnv();

        final Map<String, String> envs = new HashMap<>();
        envVars.forEach(env -> envs.put(env.getName(), env.getValue()));

        assertEquals(templateEnvs, envs);
    }
}
