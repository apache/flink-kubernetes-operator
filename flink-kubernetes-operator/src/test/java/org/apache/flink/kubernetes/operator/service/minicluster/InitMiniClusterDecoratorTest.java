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

package org.apache.flink.kubernetes.operator.service.minicluster;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.InitJobManagerDecorator;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodBuilder;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InitMiniClusterDecorator}. */
class InitMiniClusterDecoratorTest {

    /**
     * Checks that the MiniCluster pod is decorated in the same way as a FlinkDeployment's
     * JobManager pod, apart from the differences described in {@link InitMiniClusterDecorator}.
     *
     * <p>This fails if a change to {@link InitJobManagerDecorator} (e.g. from a Flink version
     * upgrade) changes how the options set here are applied, so that the MiniCluster decorator can
     * be updated to match.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSameAsJobManagerExceptIntendedDifferences(boolean hostNetwork) {
        var parameters = new MiniClusterParameters(config(hostNetwork));

        var expected = new InitJobManagerDecorator(parameters).decorateFlinkPod(podTemplate());
        var actual = new InitMiniClusterDecorator(parameters).decorateFlinkPod(podTemplate());

        // 1) The resources in the pod template are kept, rather than set from the JobManager
        // memory and CPU options
        var expectedMain =
                new ContainerBuilder(expected.getMainContainer())
                        .withResources(podTemplate().getMainContainer().getResources())
                        // 2) The JobManager RPC and blob server ports aren't declared
                        .withPorts(
                                expected.getMainContainer().getPorts().stream()
                                        .filter(
                                                p ->
                                                        !p.getName()
                                                                        .equals(
                                                                                Constants
                                                                                        .JOB_MANAGER_RPC_PORT_NAME)
                                                                && !p.getName()
                                                                        .equals(
                                                                                Constants
                                                                                        .BLOB_SERVER_PORT_NAME))
                                        .collect(Collectors.toList()))
                        .build();

        // 3) The restart policy is set to 'Always'
        var expectedPod =
                new PodBuilder(expected.getPodWithoutMainContainer())
                        .editSpec()
                        .withRestartPolicy(InitMiniClusterDecorator.RESTART_POLICY_ALWAYS)
                        .endSpec()
                        .build();

        assertThat(actual.getPodWithoutMainContainer()).isEqualTo(expectedPod);
        assertThat(actual.getMainContainer()).isEqualTo(expectedMain);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDefaultTerminationGracePeriod(boolean templateSetsGracePeriod) {
        var podTemplate = podTemplate();
        if (!templateSetsGracePeriod) {
            podTemplate
                    .getPodWithoutMainContainer()
                    .getSpec()
                    .setTerminationGracePeriodSeconds(null);
        }

        var decorated =
                new InitMiniClusterDecorator(new MiniClusterParameters(config(false)))
                        .decorateFlinkPod(podTemplate);

        assertThat(
                        decorated
                                .getPodWithoutMainContainer()
                                .getSpec()
                                .getTerminationGracePeriodSeconds())
                .isEqualTo(
                        templateSetsGracePeriod
                                ? 10L
                                : InitMiniClusterDecorator.TERMINATION_GRACE_PERIOD_SECONDS);
    }

    /** Sets every option that the JobManager decorator applies to the pod. */
    private static Configuration config(boolean hostNetwork) {
        var conf = new Configuration();
        conf.set(KubernetesConfigOptions.CLUSTER_ID, "test-minicluster");
        conf.set(KubernetesConfigOptions.NAMESPACE, "test-namespace");
        conf.set(KubernetesConfigOptions.CONTAINER_IMAGE, "flink:from-config");
        conf.set(
                KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                KubernetesConfigOptions.ImagePullPolicy.Always);
        conf.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS, List.of("registry-secret"));
        conf.set(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, "from-config");
        conf.set(KubernetesConfigOptions.KUBERNETES_HOSTNETWORK_ENABLED, hostNetwork);
        conf.set(KubernetesConfigOptions.FLINK_LOG_DIR, "/flink-logs");
        conf.set(KubernetesConfigOptions.JOB_MANAGER_LABELS, Map.of("team", "data"));
        conf.set(
                KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS,
                Map.of("example.com/key", "value"));
        conf.set(KubernetesConfigOptions.JOB_MANAGER_NODE_SELECTOR, Map.of("disk", "ssd"));
        conf.set(
                KubernetesConfigOptions.JOB_MANAGER_TOLERATIONS,
                List.of(
                        Map.of(
                                "key", "dedicated",
                                "operator", "Equal",
                                "value", "flink",
                                "effect", "NoSchedule")));
        conf.setString("containerized.master.env.AWS_REGION", "eu-west-1");
        // As in the MiniCluster pod factory, the REST bind port is fixed. The JobManager decorator
        // declares the blob server port, so it needs to be fixed for the comparison.
        conf.set(RestOptions.BIND_PORT, String.valueOf(Constants.REST_PORT));
        conf.set(BlobServerOptions.PORT, String.valueOf(Constants.BLOB_SERVER_PORT));
        return conf;
    }

    private static FlinkPod podTemplate() {
        var pod =
                new PodBuilder()
                        .withNewMetadata()
                        .addToLabels("from", "template")
                        .addToAnnotations("template.example.com/key", "template-value")
                        .endMetadata()
                        .withNewSpec()
                        .withTerminationGracePeriodSeconds(10L)
                        .addToNodeSelector("zone", "a")
                        .addNewContainer()
                        .withName("sidecar")
                        .withImage("busybox")
                        .endContainer()
                        .addNewVolume()
                        .withName("state")
                        .withNewEmptyDir()
                        .endEmptyDir()
                        .endVolume()
                        .endSpec()
                        .build();
        var mainContainer =
                new ContainerBuilder()
                        .withName(Constants.MAIN_CONTAINER_NAME)
                        .withImage("flink:from-template")
                        .withNewResources()
                        .addToRequests(
                                "memory",
                                new org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api
                                        .model.Quantity("1Gi"))
                        .endResources()
                        .addNewEnv()
                        .withName("USER_ENV")
                        .withValue("value")
                        .endEnv()
                        .addNewPort()
                        .withName("metrics")
                        .withContainerPort(9249)
                        .endPort()
                        .addNewVolumeMount()
                        .withName("state")
                        .withMountPath("/flink-data")
                        .endVolumeMount()
                        .build();
        return new FlinkPod(pod, mainContainer);
    }
}
