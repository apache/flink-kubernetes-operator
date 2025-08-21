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

package org.apache.flink.kubernetes.operator.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubeapitest.junit.EnableKubeAPIServer;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonDeletingOperation;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@EnableKubeAPIServer
class FlinkConfigurationYamlSupportTest {

    private static final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
    public static final String OLD_CRD_PATH =
            "src/test/resources/pre-flink-configuration-yaml-crd.yml";
    public static final String NEW_CRD_PATH =
            "../helm/flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml";

    static KubernetesClient client;

    @Test
    void upgradeCRDToYamlFlinkConfiguration() {
        applyCRD(OLD_CRD_PATH);

        applyResource("src/test/resources/test-deployment-key-value-configuration.yaml");

        var deployment =
                client.resources(FlinkDeployment.class)
                        .inNamespace("default")
                        .withName("basic-example")
                        .get();
        assertThat(deployment.getSpec().getFlinkConfiguration()).hasSize(5);

        applyCRD(NEW_CRD_PATH);

        applyResource("src/test/resources/test-deployment-yaml-configuration.yaml");

        deployment =
                client.resources(FlinkDeployment.class)
                        .inNamespace("default")
                        .withName("basic-example")
                        .get();
        assertThat(deployment.getSpec().getFlinkConfiguration()).hasSize(3);
        assertThat(deployment.getSpec().getFlinkConfiguration().asFlatMap()).hasSize(5);
    }

    private GenericKubernetesResource applyResource(String path) {
        try {
            GenericKubernetesResource d =
                    objectMapper.readValue(new File(path), GenericKubernetesResource.class);

            return client.resource(d).createOr(NonDeletingOperation::update);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CustomResourceDefinition applyCRD(String path) {
        try {
            var crd = objectMapper.readValue(new File(path), CustomResourceDefinition.class);

            var res = client.resource(crd).createOr(NonDeletingOperation::update);
            Thread.sleep(1000);
            return res;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
