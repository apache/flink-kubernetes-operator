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

package org.apache.flink.kubernetes.operator.kubeclient.factory;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesOwnerReference;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.ParametersTestBase;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPort;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ObjectMeta;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.OwnerReference;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodSpec;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Quantity;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.Deployment;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @link StandaloneKubernetesJobManagerFactory unit tests
 */
public class StandaloneKubernetesTaskManagerFactoryTest extends ParametersTestBase {

    private Deployment deployment;

    @BeforeEach
    public void setup() {
        setupFlinkConfig();
        FlinkPod podTemplate = createPodTemplate();
        StandaloneKubernetesTaskManagerParameters tmParameters =
                new StandaloneKubernetesTaskManagerParameters(
                        flinkConfig, TestUtils.createClusterSpecification());
        deployment =
                StandaloneKubernetesTaskManagerFactory.buildKubernetesTaskManagerDeployment(
                        podTemplate, tmParameters);
    }

    @Test
    public void testDeploymentMetadata() {
        assertEquals(
                StandaloneKubernetesUtils.getTaskManagerDeploymentName(TestUtils.CLUSTER_ID),
                deployment.getMetadata().getName());

        final Map<String, String> expectedLabels =
                new HashMap<>(
                        StandaloneKubernetesUtils.getTaskManagerSelectors(TestUtils.CLUSTER_ID));
        expectedLabels.putAll(userLabels);
        assertEquals(expectedLabels, deployment.getMetadata().getLabels());

        assertEquals(userAnnotations, deployment.getMetadata().getAnnotations());

        final List<OwnerReference> expectedOwnerReferences =
                List.of(
                        KubernetesOwnerReference.fromMap(flinkDeploymentOwnerReference)
                                .getInternalResource());
        assertEquals(expectedOwnerReferences, deployment.getMetadata().getOwnerReferences());
    }

    @Test
    public void testDeploymentSpec() {
        assertEquals(1, deployment.getSpec().getReplicas());
        assertEquals(
                StandaloneKubernetesUtils.getTaskManagerSelectors(TestUtils.CLUSTER_ID),
                deployment.getSpec().getSelector().getMatchLabels());
    }

    @Test
    public void testTemplateMetadata() {
        final ObjectMeta podMetadata = deployment.getSpec().getTemplate().getMetadata();

        final Map<String, String> expectedLabels =
                new HashMap<>(
                        StandaloneKubernetesUtils.getTaskManagerSelectors(TestUtils.CLUSTER_ID));
        expectedLabels.putAll(userLabels);
        expectedLabels.putAll(templateLabels);
        assertEquals(expectedLabels, podMetadata.getLabels());

        final Map<String, String> expectedAnnotations = new HashMap<>(userAnnotations);
        expectedAnnotations.putAll(templateAnnotations);
        assertEquals(expectedAnnotations, podMetadata.getAnnotations());
    }

    @Test
    public void testTemplateSpec() {
        final PodSpec podSpec = deployment.getSpec().getTemplate().getSpec();

        assertEquals(1, podSpec.getContainers().size());
        assertEquals(TestUtils.SERVICE_ACCOUNT, podSpec.getServiceAccountName());
        // Config and secret volumes
        assertEquals(2, podSpec.getVolumes().size());

        final Container mainContainer = podSpec.getContainers().get(0);
        assertEquals(Constants.MAIN_CONTAINER_NAME, mainContainer.getName());
        assertEquals(TestUtils.IMAGE, mainContainer.getImage());
        assertEquals(TestUtils.IMAGE_POLICY, mainContainer.getImagePullPolicy());

        final Map<String, String> envs = new HashMap<>();
        mainContainer.getEnv().forEach(env -> envs.put(env.getName(), env.getValue()));

        assertEquals(templateEnvs, envs);

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

        assertThat(mainContainer.getPorts(), containsInAnyOrder(expectedContainerPorts.toArray()));

        final ResourceRequirements resourceRequirements = mainContainer.getResources();

        final Map<String, Quantity> requests = resourceRequirements.getRequests();
        assertEquals(Double.toString(TestUtils.TASK_MANAGER_CPU), requests.get("cpu").getAmount());
        assertEquals(
                String.valueOf(TestUtils.TASK_MANAGER_MEMORY_MB),
                requests.get("memory").getAmount());

        final Map<String, Quantity> limits = resourceRequirements.getLimits();
        assertEquals(Double.toString(TestUtils.TASK_MANAGER_CPU), limits.get("cpu").getAmount());
        assertEquals(
                String.valueOf(TestUtils.TASK_MANAGER_MEMORY_MB), limits.get("memory").getAmount());

        assertEquals(2, mainContainer.getVolumeMounts().size());
    }
}
