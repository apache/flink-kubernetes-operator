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

import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubeapitest.junit.EnableKubeAPIServer;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NonDeletingOperation;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnableKubeAPIServer
class FlinkConfigurationYamlSupportTest {

    private static final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
    public static final String OLD_CRD_PATH =
            "src/test/resources/pre-flink-configuration-yaml-crd.yml";
    public static final String NEW_CRD_PATH =
            "../helm/flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml";
    public static final String SNAPSHOT_CRD_PATH =
            "../helm/flink-kubernetes-operator/crds/flinkstatesnapshots.flink.apache.org-v1.yml";

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

    /**
     * Verifies that the validation constraints declared via CRD-generator annotations are enforced
     * by the API server at admission time, rather than only by {@code
     * DefaultValidator} during reconciliation. Covers {@code @Min(1)} on the JobManager/TaskManager
     * replicas, {@code @Required} on {@code flinkVersion} and the ingress template, and
     * {@code @Min(-1)} on the snapshot {@code backoffLimit}.
     */
    @Test
    void crdRejectsInvalidSpecValues() {
        applyCRD(NEW_CRD_PATH);

        // A fully specified deployment satisfies all the schema constraints.
        client.resource(validDeployment("valid-dep")).create();

        // @Min(1) on JobManagerSpec.replicas
        var jmReplicas = validDeployment("invalid-jm-replicas");
        jmReplicas.getSpec().getJobManager().setReplicas(0);
        assertRejected(jmReplicas);

        // @Min(1) on TaskManagerSpec.replicas
        var tmReplicas = validDeployment("invalid-tm-replicas");
        tmReplicas.getSpec().getTaskManager().setReplicas(0);
        assertRejected(tmReplicas);

        // @Required on FlinkDeploymentSpec.flinkVersion
        var noVersion = validDeployment("invalid-no-version");
        noVersion.getSpec().setFlinkVersion(null);
        assertRejected(noVersion);

        // @Required on IngressSpec.template (an ingress without a template)
        var noTemplate = validDeployment("invalid-ingress");
        noTemplate.getSpec().setIngress(new IngressSpec());
        assertRejected(noTemplate);

        // @Min(-1) on FlinkStateSnapshotSpec.backoffLimit (-1 is the unlimited-retries sentinel)
        applyCRD(SNAPSHOT_CRD_PATH);
        client.resource(validSnapshot("valid-snapshot")).create();
        var badBackoff = validSnapshot("invalid-backoff");
        badBackoff.getSpec().setBackoffLimit(-2);
        assertRejected(badBackoff);
    }

    private FlinkDeployment validDeployment(String name) {
        var deployment = BaseTestUtils.buildApplicationCluster();
        deployment.getMetadata().setName(name);
        deployment.getMetadata().setNamespace("default");
        return deployment;
    }

    private FlinkStateSnapshot validSnapshot(String name) {
        return BaseTestUtils.buildFlinkStateSnapshotSavepoint(
                name, "default", "test-path", true, null);
    }

    private void assertRejected(HasMetadata resource) {
        assertThatThrownBy(() -> client.resource(resource).create())
                .isInstanceOf(KubernetesClientException.class);
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
