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

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesOwnerReference;
import org.apache.flink.kubernetes.kubeclient.services.HeadlessClusterIPService;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.ParametersTestBase;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesJobManagerParameters;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ConfigMap;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPort;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.HasMetadata;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ObjectMeta;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.OwnerReference;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodSpec;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Quantity;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Service;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE;
import static org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils.CLUSTER_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @link StandaloneKubernetesJobManagerFactory unit tests
 */
public class StandaloneKubernetesJobManagerFactoryTest extends ParametersTestBase {

    KubernetesJobManagerSpecification jmSpec;

    @BeforeEach
    public void setup() throws Exception {
        setupFlinkConfig();
        flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, "/missing/dir");
        flinkConfig.set(PipelineOptions.CLASSPATHS, Collections.singletonList("/path"));
        FlinkPod podTemplate = createPodTemplate();
        StandaloneKubernetesJobManagerParameters tmParameters =
                new StandaloneKubernetesJobManagerParameters(
                        flinkConfig, TestUtils.createClusterSpecification());
        jmSpec =
                StandaloneKubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        podTemplate, tmParameters);
    }

    @Test
    public void testDeploymentMetadata() {
        ObjectMeta deploymentMetadata = jmSpec.getDeployment().getMetadata();
        assertEquals(
                StandaloneKubernetesUtils.getJobManagerDeploymentName(CLUSTER_ID),
                deploymentMetadata.getName());

        final Map<String, String> expectedLabels =
                new HashMap<>(StandaloneKubernetesUtils.getJobManagerSelectors(CLUSTER_ID));
        expectedLabels.putAll(userLabels);
        assertEquals(expectedLabels, deploymentMetadata.getLabels());

        assertEquals(userAnnotations, deploymentMetadata.getAnnotations());

        final List<OwnerReference> expectedOwnerReferences =
                List.of(
                        KubernetesOwnerReference.fromMap(flinkDeploymentOwnerReference)
                                .getInternalResource());
        assertEquals(expectedOwnerReferences, deploymentMetadata.getOwnerReferences());
    }

    @Test
    public void testDeploymentSpec() {
        DeploymentSpec deploymentSpec = jmSpec.getDeployment().getSpec();

        assertEquals(1, deploymentSpec.getReplicas());
        assertEquals(
                StandaloneKubernetesUtils.getJobManagerSelectors(CLUSTER_ID),
                deploymentSpec.getSelector().getMatchLabels());
    }

    @Test
    public void testTemplateMetadata() {
        final ObjectMeta podMetadata = jmSpec.getDeployment().getSpec().getTemplate().getMetadata();

        final Map<String, String> expectedLabels =
                new HashMap<>(StandaloneKubernetesUtils.getJobManagerSelectors(CLUSTER_ID));
        expectedLabels.putAll(userLabels);
        expectedLabels.putAll(templateLabels);
        assertEquals(expectedLabels, podMetadata.getLabels());

        final Map<String, String> expectedAnnotations = new HashMap<>(userAnnotations);
        expectedAnnotations.putAll(templateAnnotations);
        assertEquals(expectedAnnotations, podMetadata.getAnnotations());
    }

    @Test
    public void testTemplateSpec() {
        final PodSpec podSpec = jmSpec.getDeployment().getSpec().getTemplate().getSpec();

        assertEquals(1, podSpec.getContainers().size());
        assertEquals(TestUtils.SERVICE_ACCOUNT, podSpec.getServiceAccountName());
        // Config and secret volumes
        assertEquals(4, podSpec.getVolumes().size());

        final Container mainContainer = podSpec.getContainers().get(0);
        assertEquals(Constants.MAIN_CONTAINER_NAME, mainContainer.getName());
        assertEquals(TestUtils.IMAGE, mainContainer.getImage());
        assertEquals(TestUtils.IMAGE_POLICY, mainContainer.getImagePullPolicy());

        final Map<String, String> envs = new HashMap<>();
        mainContainer.getEnv().forEach(env -> envs.put(env.getName(), env.getValue()));

        Map<String, String> expectedEnvs = new HashMap<>(templateEnvs);
        expectedEnvs.put(Constants.ENV_FLINK_POD_IP_ADDRESS, null);
        assertEquals(expectedEnvs, envs);

        final List<ContainerPort> expectedContainerPorts =
                Arrays.asList(
                        new ContainerPortBuilder()
                                .withName(Constants.JOB_MANAGER_RPC_PORT_NAME)
                                .withContainerPort(6123)
                                .build(),
                        new ContainerPortBuilder()
                                .withName(Constants.BLOB_SERVER_PORT_NAME)
                                .withContainerPort(Constants.BLOB_SERVER_PORT)
                                .build(),
                        new ContainerPortBuilder()
                                .withName(Constants.REST_PORT_NAME)
                                .withContainerPort(Constants.REST_PORT)
                                .build(),
                        new ContainerPortBuilder()
                                .withName(TEMPLATE_PORT_NAME)
                                .withContainerPort(TEMPLATE_PORT)
                                .build());

        assertThat(mainContainer.getPorts(), containsInAnyOrder(expectedContainerPorts.toArray()));

        final ResourceRequirements resourceRequirements = mainContainer.getResources();

        final Map<String, Quantity> requests = resourceRequirements.getRequests();
        assertEquals(Double.toString(TestUtils.JOB_MANAGER_CPU), requests.get("cpu").getAmount());
        assertEquals(
                String.valueOf(TestUtils.JOB_MANAGER_MEMORY_MB),
                requests.get("memory").getAmount());

        final Map<String, Quantity> limits = resourceRequirements.getLimits();
        assertEquals(Double.toString(TestUtils.JOB_MANAGER_CPU), limits.get("cpu").getAmount());
        assertEquals(
                String.valueOf(TestUtils.JOB_MANAGER_MEMORY_MB), limits.get("memory").getAmount());

        assertEquals(4, mainContainer.getVolumeMounts().size());
    }

    @Test
    public void testAdditionalResourcesSize() {
        final List<HasMetadata> resultAdditionalResources = this.jmSpec.getAccompanyingResources();
        assertEquals(3, resultAdditionalResources.size());

        final List<HasMetadata> resultServices =
                resultAdditionalResources.stream()
                        .filter(x -> x instanceof Service)
                        .collect(Collectors.toList());
        assertEquals(2, resultServices.size());

        final List<HasMetadata> resultConfigMaps =
                resultAdditionalResources.stream()
                        .filter(x -> x instanceof ConfigMap)
                        .collect(Collectors.toList());
        assertEquals(1, resultConfigMaps.size());
    }

    @Test
    public void testServices() throws IOException {
        final List<Service> resultServices =
                this.jmSpec.getAccompanyingResources().stream()
                        .filter(x -> x instanceof Service)
                        .map(x -> (Service) x)
                        .collect(Collectors.toList());

        assertEquals(2, resultServices.size());

        final List<Service> internalServiceCandidates =
                resultServices.stream()
                        .filter(
                                x ->
                                        x.getMetadata()
                                                .getName()
                                                .equals(
                                                        InternalServiceDecorator
                                                                .getInternalServiceName(
                                                                        CLUSTER_ID)))
                        .collect(Collectors.toList());
        assertEquals(1, internalServiceCandidates.size());

        final List<Service> restServiceCandidates =
                resultServices.stream()
                        .filter(
                                x ->
                                        x.getMetadata()
                                                .getName()
                                                .equals(
                                                        ExternalServiceDecorator
                                                                .getExternalServiceName(
                                                                        CLUSTER_ID)))
                        .collect(Collectors.toList());
        assertEquals(1, restServiceCandidates.size());

        final Service resultInternalService = internalServiceCandidates.get(0);
        assertEquals(2, resultInternalService.getMetadata().getLabels().size());

        assertNull(resultInternalService.getSpec().getType());
        assertEquals(
                HeadlessClusterIPService.HEADLESS_CLUSTER_IP,
                resultInternalService.getSpec().getClusterIP());
        assertEquals(2, resultInternalService.getSpec().getPorts().size());
        assertEquals(3, resultInternalService.getSpec().getSelector().size());

        final Service resultRestService = restServiceCandidates.get(0);
        assertEquals(2, resultRestService.getMetadata().getLabels().size());

        assertEquals(
                REST_SERVICE_EXPOSED_TYPE.defaultValue().toString(),
                resultRestService.getSpec().getType());
        assertEquals(1, resultRestService.getSpec().getPorts().size());
        assertEquals(3, resultRestService.getSpec().getSelector().size());
    }

    @Test
    public void testFlinkConfConfigMap() throws IOException {
        final ConfigMap resultConfigMap =
                (ConfigMap)
                        jmSpec.getAccompanyingResources().stream()
                                .filter(
                                        x ->
                                                x instanceof ConfigMap
                                                        && x.getMetadata()
                                                                .getName()
                                                                .equals(
                                                                        FlinkConfMountDecorator
                                                                                .getFlinkConfConfigMapName(
                                                                                        CLUSTER_ID)))
                                .collect(Collectors.toList())
                                .get(0);

        assertEquals(2, resultConfigMap.getMetadata().getLabels().size());

        final Map<String, String> resultData = resultConfigMap.getData();
        assertEquals(1, resultData.size());
    }
}
