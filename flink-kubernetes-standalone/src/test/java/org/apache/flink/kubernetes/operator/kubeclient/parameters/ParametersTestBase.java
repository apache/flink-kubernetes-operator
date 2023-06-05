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

package org.apache.flink.kubernetes.operator.kubeclient.parameters;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVar;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVarBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.LocalObjectReference;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Base class for Kubernetes tests. */
public class ParametersTestBase {

    protected Configuration flinkConfig;

    protected ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();

    protected final Map<String, String> userLabels =
            TestUtils.generateTestStringStringMap("label", "value", 2);

    protected final Map<String, String> userAnnotations =
            TestUtils.generateTestStringStringMap("annotation", "value", 2);

    protected final Map<String, String> userNodeSelectors =
            TestUtils.generateTestStringStringMap("selector", "val", 2);

    protected final List<String> userImagePullSecrets = Arrays.asList("s1", "s2", "s3");

    protected static final String TEMPLATE_PORT_NAME = "user-port";
    protected static final int TEMPLATE_PORT = 9458;

    protected final Map<String, String> templateEnvs =
            TestUtils.generateTestStringStringMap("TEMPLATE_KEY", "VAL", 2);

    protected final Map<String, String> templateLabels =
            TestUtils.generateTestStringStringMap("template-label", "value", 2);

    protected final Map<String, String> templateAnnotations =
            TestUtils.generateTestStringStringMap("template-annotation", "value", 2);

    protected final Map<String, String> templateNodeSelector =
            TestUtils.generateTestStringStringMap("template-node-selector", "value", 2);

    protected final List<String> templateImagePullSecrets = Arrays.asList("ts1", "ts2", "ts3");

    protected final Map<String, String> flinkDeploymentOwnerReference =
            TestUtils.generateTestOwnerReferenceMap("FlinkDeployment");

    private static final String SECRETS = "ssl-cert:/etc/ssl";

    protected FlinkPod createPodTemplate() {
        List<EnvVar> envVars = new ArrayList<>();
        templateEnvs.forEach(
                (k, v) -> envVars.add(new EnvVarBuilder().withName(k).withValue(v).build()));

        Container mainContainer =
                new ContainerBuilder()
                        .withImagePullPolicy("templatePullPolicy")
                        .withImage("templateImage")
                        .withResources(
                                KubernetesUtils.getResourceRequirements(
                                        new ResourceRequirements(),
                                        1234,
                                        1,
                                        102,
                                        1,
                                        Collections.emptyMap(),
                                        Collections.emptyMap()))
                        .withPorts(
                                new ContainerPortBuilder()
                                        .withName(TEMPLATE_PORT_NAME)
                                        .withContainerPort(TEMPLATE_PORT)
                                        .build())
                        .withEnv(envVars)
                        .build();

        return new FlinkPod.Builder()
                .withMainContainer(mainContainer)
                .withPod(
                        new PodBuilder()
                                .withApiVersion("templateAPIVersion")
                                .editOrNewSpec()
                                .withServiceAccountName("templateServiceAccountName")
                                .withServiceAccount("templateServiceAccount")
                                .endSpec()
                                .editOrNewMetadata()
                                .addToLabels(templateLabels)
                                .addToAnnotations(templateAnnotations)
                                .endMetadata()
                                .editOrNewSpec()
                                .addToImagePullSecrets(getImagePullSecrets())
                                .addToNodeSelector(templateNodeSelector)
                                .endSpec()
                                .build())
                .build();
    }

    private LocalObjectReference[] getImagePullSecrets() {
        return templateImagePullSecrets.stream()
                .map(String::trim)
                .filter(secret -> !secret.isEmpty())
                .map(LocalObjectReference::new)
                .toArray(LocalObjectReference[]::new);
    }

    protected void setupFlinkConfig() {
        flinkConfig = TestUtils.createTestFlinkConfig();
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS, userAnnotations);
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS, userAnnotations);
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_LABELS, userLabels);
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_LABELS, userLabels);
        flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR, userNodeSelectors);
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_NODE_SELECTOR, userNodeSelectors);
        flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS, userImagePullSecrets);
        flinkConfig.setString(KubernetesConfigOptions.KUBERNETES_SECRETS.key(), SECRETS);
        flinkConfig.set(
                KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE,
                List.of(flinkDeploymentOwnerReference));
    }
}
