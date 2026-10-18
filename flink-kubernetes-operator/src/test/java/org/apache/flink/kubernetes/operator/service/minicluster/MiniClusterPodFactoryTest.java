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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.ArtifactFetchOptions;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ConfigMap;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVar;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.HasMetadata;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodSpec;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Service;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Volume;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.VolumeMount;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/** Tests for {@link MiniClusterPodFactory}. */
class MiniClusterPodFactoryTest {

    private static final String NAME = TestUtils.TEST_MINI_CLUSTER_NAME;

    @TempDir Path tempDir;

    private FlinkConfigManager configManager;
    private FlinkMiniCluster miniCluster;

    @BeforeEach
    void setup() {
        // Point the operator's own conf dir at an empty directory, so the tests don't depend on
        // log configuration files in the working directory
        var operatorConfig = new Configuration();
        operatorConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, tempDir.toString());
        configManager = new FlinkConfigManager(operatorConfig);
        miniCluster = TestUtils.buildMiniCluster();
    }

    @Test
    void testNamesAndLabels() throws IOException {
        var resources = build();
        var commonLabels =
                Map.of(
                        Constants.LABEL_TYPE_KEY,
                        MiniClusterPodFactory.LABEL_TYPE_MINICLUSTER,
                        Constants.LABEL_APP_KEY,
                        NAME);
        var selector = MiniClusterPodFactory.getSelectorLabels(NAME);
        assertThat(selector)
                .containsAllEntriesOf(commonLabels)
                .containsEntry(
                        Constants.LABEL_COMPONENT_KEY,
                        MiniClusterPodFactory.LABEL_COMPONENT_MINICLUSTER)
                .hasSize(3);

        var deployment = resources.getDeployment();
        assertThat(deployment.getMetadata().getName()).isEqualTo(NAME);
        assertThat(deployment.getMetadata().getLabels()).isEqualTo(selector);
        assertThat(deployment.getSpec().getSelector().getMatchLabels()).isEqualTo(selector);
        assertThat(deployment.getSpec().getTemplate().getMetadata().getLabels())
                .containsAllEntriesOf(selector);

        // As for a FlinkDeployment, the accompanying resources have the common labels
        var service = restService(resources);
        assertThat(service.getMetadata().getName()).isEqualTo(NAME + "-rest");
        assertThat(service.getMetadata().getLabels()).isEqualTo(commonLabels);
        assertThat(service.getSpec().getSelector()).isEqualTo(selector);
        assertThat(service.getSpec().getType()).isEqualTo("ClusterIP");
        assertThat(service.getSpec().getPorts())
                .singleElement()
                .satisfies(
                        port -> {
                            assertThat(port.getName()).isEqualTo(Constants.REST_PORT_NAME);
                            assertThat(port.getPort()).isEqualTo(RestOptions.PORT.defaultValue());
                        });

        var configMap = configMap(resources);
        assertThat(configMap.getMetadata().getName()).isEqualTo("flink-config-" + NAME);
        assertThat(configMap.getMetadata().getLabels()).isEqualTo(commonLabels);
    }

    @Test
    void testOnlyRestServiceAndConfigMapAccompanyDeployment() throws IOException {
        // JobManager and TaskManager are in the same process, so there is no internal Service
        assertThat(build().getAccompanyingResources())
                .extracting(HasMetadata::getKind)
                .containsExactlyInAnyOrder("Service", "ConfigMap");
    }

    @Test
    void testOwnerReferenceFromConfig() throws IOException {
        // As for a FlinkDeployment, the reconciler sets the owner of the Deployment in the config,
        // and the Kubernetes client makes the Deployment the owner of the accompanying resources
        var conf = deployConfig();
        conf.set(
                KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE,
                List.of(
                        Map.of(
                                "apiVersion", "flink.apache.org/v1beta1",
                                "kind", "FlinkMiniCluster",
                                "name", NAME,
                                "uid", miniCluster.getMetadata().getUid(),
                                "blockOwnerDeletion", "true",
                                "controller", "false")));

        var resources = MiniClusterPodFactory.build(miniCluster.getSpec(), conf);

        assertThat(resources.getDeployment().getMetadata().getOwnerReferences())
                .singleElement()
                .satisfies(
                        ref -> {
                            assertThat(ref.getKind()).isEqualTo("FlinkMiniCluster");
                            assertThat(ref.getName()).isEqualTo(NAME);
                            assertThat(ref.getUid()).isEqualTo(miniCluster.getMetadata().getUid());
                        });
        assertThat(resources.getAccompanyingResources())
                .allSatisfy(r -> assertThat(r.getMetadata().getOwnerReferences()).isEmpty());
    }

    @Test
    void testDeploymentRecordsGeneration() throws IOException {
        miniCluster.getMetadata().setGeneration(7L);
        var deployment = build().getDeployment();
        assertThat(deployment.getMetadata().getAnnotations())
                .containsEntry(FlinkUtils.CR_GENERATION_LABEL, "7");
        assertThat(deployment.getSpec().getTemplate().getMetadata().getAnnotations())
                .containsEntry(FlinkUtils.CR_GENERATION_LABEL, "7");
    }

    @Test
    void testJobManagerLabelsAndAnnotations() throws IOException {
        var flinkConfiguration = miniCluster.getSpec().getFlinkConfiguration();
        flinkConfiguration.put(
                KubernetesConfigOptions.JOB_MANAGER_LABELS.key(),
                "team:data," + Constants.LABEL_COMPONENT_KEY + ":not-the-selector");
        flinkConfiguration.put(
                KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS.key(), "example.com/key:value");
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewMetadata()
                                .addToLabels("from", "template")
                                .addToAnnotations("template.example.com/key", "template-value")
                                .endMetadata()
                                .build());
        var selector = MiniClusterPodFactory.getSelectorLabels(NAME);

        var resources = build();

        var deployment = resources.getDeployment();
        assertThat(deployment.getMetadata().getLabels())
                .containsAllEntriesOf(selector)
                .containsEntry("team", "data");
        assertThat(deployment.getMetadata().getAnnotations())
                .containsEntry("example.com/key", "value");
        assertThat(deployment.getSpec().getSelector().getMatchLabels()).isEqualTo(selector);

        var podMeta = deployment.getSpec().getTemplate().getMetadata();
        assertThat(podMeta.getLabels())
                .containsAllEntriesOf(selector)
                .containsEntry("team", "data")
                .containsEntry("from", "template");
        assertThat(podMeta.getAnnotations())
                .containsEntry("example.com/key", "value")
                .containsEntry("template.example.com/key", "template-value");

        // As for a FlinkDeployment, the JobManager labels are not put on the Service or ConfigMap
        assertThat(restService(resources).getMetadata().getLabels()).doesNotContainKey("team");
        assertThat(configMap(resources).getMetadata().getLabels()).doesNotContainKey("team");
    }

    @Test
    void testJobManagerPodOptions() throws IOException {
        var flinkConfiguration = miniCluster.getSpec().getFlinkConfiguration();
        flinkConfiguration.put(KubernetesConfigOptions.JOB_MANAGER_NODE_SELECTOR.key(), "disk:ssd");
        flinkConfiguration.put(
                KubernetesConfigOptions.JOB_MANAGER_TOLERATIONS.key(),
                "key:dedicated,operator:Equal,value:flink,effect:NoSchedule");
        flinkConfiguration.put(
                KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS.key(), "registry-secret");
        flinkConfiguration.put("containerized.master.env.AWS_REGION", "eu-west-1");
        flinkConfiguration.put(
                KubernetesConfigOptions.KUBERNETES_ENV_SECRET_KEY_REF.key(),
                "env:SECRET_ENV,secret:env-secret,key:password");
        flinkConfiguration.put(
                KubernetesConfigOptions.KUBERNETES_SECRETS.key(), "mounted-secret:/opt/secrets");

        var podSpec = podSpec(build());

        assertThat(podSpec.getNodeSelector()).containsEntry("disk", "ssd");
        assertThat(podSpec.getTolerations())
                .singleElement()
                .satisfies(
                        t -> {
                            assertThat(t.getKey()).isEqualTo("dedicated");
                            assertThat(t.getValue()).isEqualTo("flink");
                            assertThat(t.getEffect()).isEqualTo("NoSchedule");
                        });
        assertThat(podSpec.getImagePullSecrets())
                .extracting(s -> s.getName())
                .containsExactly("registry-secret");
        assertThat(podSpec.getVolumes())
                .filteredOn(v -> v.getSecret() != null)
                .singleElement()
                .satisfies(
                        v -> assertThat(v.getSecret().getSecretName()).isEqualTo("mounted-secret"));

        var main = mainContainer(podSpec);
        assertThat(main.getEnv())
                .anySatisfy(
                        e -> {
                            assertThat(e.getName()).isEqualTo("AWS_REGION");
                            assertThat(e.getValue()).isEqualTo("eu-west-1");
                        })
                .anySatisfy(
                        e -> {
                            assertThat(e.getName()).isEqualTo("SECRET_ENV");
                            assertThat(e.getValueFrom().getSecretKeyRef().getName())
                                    .isEqualTo("env-secret");
                        });
        assertThat(main.getVolumeMounts())
                .extracting(VolumeMount::getMountPath)
                .contains("/opt/secrets");
    }

    @Test
    void testDeploymentRunsOnePodAtATime() throws IOException {
        var deploymentSpec = build().getDeployment().getSpec();
        assertThat(deploymentSpec.getReplicas()).isEqualTo(1);
        assertThat(deploymentSpec.getStrategy().getType()).isEqualTo("Recreate");
    }

    @Test
    void testMainContainer() throws IOException {
        miniCluster
                .getSpec()
                .setResources(
                        new ResourceRequirementsBuilder()
                                .addToRequests("memory", new Quantity("1Gi"))
                                .addToLimits("memory", new Quantity("2Gi"))
                                .build());
        var podSpec = podSpec(build());

        assertThat(podSpec.getServiceAccountName()).isEqualTo(TestUtils.SERVICE_ACCOUNT);
        assertThat(podSpec.getTerminationGracePeriodSeconds())
                .isEqualTo(InitMiniClusterDecorator.TERMINATION_GRACE_PERIOD_SECONDS);
        assertThat(podSpec.getVolumes())
                .extracting(Volume::getName)
                .containsExactlyInAnyOrder(
                        Constants.FLINK_CONF_VOLUME, Constants.USER_ARTIFACTS_VOLUME);
        assertThat(podSpec.getVolumes())
                .filteredOn(v -> v.getName().equals(Constants.FLINK_CONF_VOLUME))
                .singleElement()
                .satisfies(
                        v ->
                                assertThat(v.getConfigMap().getName())
                                        .isEqualTo("flink-config-" + NAME));

        var container = mainContainer(podSpec);
        assertThat(container.getName()).isEqualTo(Constants.MAIN_CONTAINER_NAME);
        assertThat(container.getImage()).isEqualTo(TestUtils.IMAGE);
        assertThat(container.getImagePullPolicy()).isEqualTo(TestUtils.IMAGE_POLICY);
        assertThat(container.getResources().getRequests().get("memory").toString())
                .isEqualTo("1Gi");
        assertThat(container.getResources().getLimits().get("memory").toString()).isEqualTo("2Gi");
        assertThat(container.getCommand()).containsExactly("/docker-entrypoint.sh");
        assertThat(container.getArgs()).first().isEqualTo("/opt/flink/bin/minicluster.sh");
        assertThat(container.getPorts())
                .singleElement()
                .satisfies(
                        p -> {
                            assertThat(p.getName()).isEqualTo(Constants.REST_PORT_NAME);
                            assertThat(p.getContainerPort())
                                    .isEqualTo(RestOptions.PORT.defaultValue());
                        });
        assertThat(container.getEnv())
                .anySatisfy(
                        e -> {
                            assertThat(e.getName()).isEqualTo("FLINK_CONF_DIR");
                            assertThat(e.getValue()).isEqualTo(tempDir.toString());
                        });
        assertThat(container.getVolumeMounts())
                .extracting(VolumeMount::getName, VolumeMount::getMountPath)
                .containsExactlyInAnyOrder(
                        tuple(Constants.FLINK_CONF_VOLUME, tempDir.toString()),
                        tuple(
                                Constants.USER_ARTIFACTS_VOLUME,
                                ArtifactFetchOptions.BASE_DIR.defaultValue()));
    }

    @Test
    void testResourcesAreMergedWithPodTemplate() throws IOException {
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewSpec()
                                .addNewContainer()
                                .withName(Constants.MAIN_CONTAINER_NAME)
                                .withNewResources()
                                .addToRequests("memory", new Quantity("512Mi"))
                                .addToLimits("ephemeral-storage", new Quantity("10Gi"))
                                .endResources()
                                .endContainer()
                                .endSpec()
                                .build());
        miniCluster
                .getSpec()
                .setResources(
                        new ResourceRequirementsBuilder()
                                .addToRequests("memory", new Quantity("1Gi"))
                                .addToLimits("memory", new Quantity("2Gi"))
                                .build());

        var resources = mainContainer(podSpec(build())).getResources();

        assertThat(resources.getRequests())
                .containsOnlyKeys("memory")
                .extractingByKey("memory")
                .hasToString("1Gi");
        assertThat(resources.getLimits()).containsOnlyKeys("memory", "ephemeral-storage");
        assertThat(resources.getLimits().get("memory")).hasToString("2Gi");
        assertThat(resources.getLimits().get("ephemeral-storage")).hasToString("10Gi");
    }

    @Test
    void testResourcesAreAppliedToLastMainContainer() throws IOException {
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewSpec()
                                .addNewContainer()
                                .withName(Constants.MAIN_CONTAINER_NAME)
                                .withWorkingDir("/first")
                                .endContainer()
                                .addNewContainer()
                                .withName(Constants.MAIN_CONTAINER_NAME)
                                .withWorkingDir("/last")
                                .endContainer()
                                .endSpec()
                                .build());
        miniCluster
                .getSpec()
                .setResources(
                        new ResourceRequirementsBuilder()
                                .addToRequests("memory", new Quantity("1Gi"))
                                .addToLimits("memory", new Quantity("2Gi"))
                                .build());

        var podSpec = podSpec(build());

        // Only the last main container is kept, as Flink keeps it for a FlinkDeployment
        assertThat(podSpec.getContainers())
                .filteredOn(c -> c.getName().equals(Constants.MAIN_CONTAINER_NAME))
                .hasSize(1);
        var mainContainer = mainContainer(podSpec);
        assertThat(mainContainer.getWorkingDir()).isEqualTo("/last");
        assertThat(mainContainer.getResources().getRequests().get("memory")).hasToString("1Gi");
        assertThat(mainContainer.getResources().getLimits().get("memory")).hasToString("2Gi");
    }

    @Test
    void testPodTemplateRequestIsReplacedBySpecLimit() throws IOException {
        // A pod template request kept alongside the spec limit would exceed it
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewSpec()
                                .addNewContainer()
                                .withName(Constants.MAIN_CONTAINER_NAME)
                                .withNewResources()
                                .addToRequests("memory", new Quantity("4Gi"))
                                .addToRequests("cpu", new Quantity("1"))
                                .endResources()
                                .endContainer()
                                .endSpec()
                                .build());
        miniCluster
                .getSpec()
                .setResources(
                        new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("2Gi"))
                                .build());

        var resources = mainContainer(podSpec(build())).getResources();

        assertThat(resources.getRequests())
                .containsOnlyKeys("cpu")
                .extractingByKey("cpu")
                .hasToString("1");
        assertThat(resources.getLimits())
                .containsOnlyKeys("memory")
                .extractingByKey("memory")
                .hasToString("2Gi");
    }

    @Test
    void testPodTemplateLimitIsReplacedBySpecRequest() throws IOException {
        // A pod template limit kept alongside the spec request would be below it
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewSpec()
                                .addNewContainer()
                                .withName(Constants.MAIN_CONTAINER_NAME)
                                .withNewResources()
                                .addToLimits("memory", new Quantity("1Gi"))
                                .addToLimits("cpu", new Quantity("2"))
                                .endResources()
                                .endContainer()
                                .endSpec()
                                .build());
        miniCluster
                .getSpec()
                .setResources(
                        new ResourceRequirementsBuilder()
                                .addToRequests("memory", new Quantity("2Gi"))
                                .build());

        var resources = mainContainer(podSpec(build())).getResources();

        assertThat(resources.getRequests())
                .containsOnlyKeys("memory")
                .extractingByKey("memory")
                .hasToString("2Gi");
        assertThat(resources.getLimits())
                .containsOnlyKeys("cpu")
                .extractingByKey("cpu")
                .hasToString("2");
    }

    @Test
    void testStartupProbe() throws IOException {
        var probe = mainContainer(podSpec(build())).getStartupProbe();

        assertThat(probe.getHttpGet().getPort().getStrVal()).isEqualTo(Constants.REST_PORT_NAME);
        assertThat(probe.getFailureThreshold()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testStartupProbeDisabled() throws IOException {
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(
                        KubernetesOperatorConfigOptions.OPERATOR_JM_STARTUP_PROBE_ENABLED.key(),
                        "false");

        assertThat(mainContainer(podSpec(build())).getStartupProbe()).isNull();
    }

    @Test
    void testStartupProbeFromPodTemplateIsKept() throws IOException {
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewSpec()
                                .addNewContainer()
                                .withName(Constants.MAIN_CONTAINER_NAME)
                                .withNewStartupProbe()
                                .withNewExec()
                                .withCommand("true")
                                .endExec()
                                .endStartupProbe()
                                .endContainer()
                                .endSpec()
                                .build());

        var probe = mainContainer(podSpec(build())).getStartupProbe();

        assertThat(probe.getExec().getCommand()).containsExactly("true");
        assertThat(probe.getHttpGet()).isNull();
    }

    @Test
    void testRestartPolicyFromPodTemplateIsOverwritten() throws IOException {
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewSpec()
                                .withRestartPolicy("Never")
                                .endSpec()
                                .build());

        assertThat(podSpec(build()).getRestartPolicy())
                .isEqualTo(InitMiniClusterDecorator.RESTART_POLICY_ALWAYS);
    }

    @Test
    void testUserArtifactsDirectoryFromConfig() throws IOException {
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(ArtifactFetchOptions.BASE_DIR.key(), "/tmp/artifacts");

        assertThat(mainContainer(podSpec(build())).getVolumeMounts())
                .filteredOn(m -> m.getName().equals(Constants.USER_ARTIFACTS_VOLUME))
                .singleElement()
                .extracting(VolumeMount::getMountPath)
                .isEqualTo("/tmp/artifacts");
    }

    @Test
    void testBuildsFromGivenSpec() throws IOException {
        var stableSpec = ReconciliationUtils.clone(miniCluster.getSpec());
        stableSpec.setImage("flink:stable");
        miniCluster.getSpec().setImage("flink:latest-change");

        var resources =
                MiniClusterPodFactory.build(
                        stableSpec,
                        configManager.getDeployConfig(miniCluster.getMetadata(), stableSpec));

        assertThat(mainContainer(podSpec(resources)).getImage()).isEqualTo("flink:stable");
    }

    @Test
    void testImageFromPodTemplateWithoutSpecImage() throws IOException {
        miniCluster.getSpec().setImage(null);
        miniCluster.getSpec().setPodTemplate(podTemplateWithMainImage("flink:from-template"));

        assertThat(mainContainer(podSpec(build())).getImage()).isEqualTo("flink:from-template");
    }

    @Test
    void testConfiguredImageOverridesPodTemplate() throws IOException {
        miniCluster.getSpec().setImage(null);
        miniCluster.getSpec().setPodTemplate(podTemplateWithMainImage("flink:from-template"));
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesConfigOptions.CONTAINER_IMAGE.key(), "flink:from-config");

        assertThat(mainContainer(podSpec(build())).getImage()).isEqualTo("flink:from-config");
    }

    @Test
    void testImagePullPolicyFromSpecOverridesConfigAndPodTemplate() throws IOException {
        miniCluster.getSpec().setImagePullPolicy("Never");
        miniCluster.getSpec().setPodTemplate(podTemplateWithMainImagePullPolicy("Always"));
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY.key(), "Always");

        assertThat(mainContainer(podSpec(build())).getImagePullPolicy()).isEqualTo("Never");
    }

    @Test
    void testImagePullPolicyFromPodTemplateWithoutSpecOrConfig() throws IOException {
        miniCluster.getSpec().setImagePullPolicy(null);
        miniCluster.getSpec().setPodTemplate(podTemplateWithMainImagePullPolicy("Never"));

        assertThat(mainContainer(podSpec(build())).getImagePullPolicy()).isEqualTo("Never");
    }

    @Test
    void testConfiguredImagePullPolicyOverridesPodTemplate() throws IOException {
        var operatorConfig = new Configuration();
        operatorConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, tempDir.toString());
        operatorConfig.set(
                KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                KubernetesConfigOptions.ImagePullPolicy.Always);
        configManager = new FlinkConfigManager(operatorConfig);
        miniCluster.getSpec().setImagePullPolicy(null);
        miniCluster.getSpec().setPodTemplate(podTemplateWithMainImagePullPolicy("Never"));

        assertThat(mainContainer(podSpec(build())).getImagePullPolicy()).isEqualTo("Always");
    }

    @Test
    void testDefaultImagePullPolicyWithoutAnyPolicy() throws IOException {
        miniCluster.getSpec().setImagePullPolicy(null);

        assertThat(mainContainer(podSpec(build())).getImagePullPolicy())
                .isEqualTo(
                        KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY.defaultValue().name());
    }

    @Test
    void testDefaultImageWithoutAnyImage() throws IOException {
        miniCluster.getSpec().setImage(null);

        assertThat(mainContainer(podSpec(build())).getImage())
                .isEqualTo(KubernetesConfigOptions.CONTAINER_IMAGE.defaultValue());
    }

    @Test
    void testServiceAccountFromPodTemplateWithoutSpecServiceAccount() throws IOException {
        miniCluster.getSpec().setServiceAccount(null);
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewSpec()
                                .withServiceAccountName("from-template")
                                .endSpec()
                                .build());

        assertThat(podSpec(build()).getServiceAccountName()).isEqualTo("from-template");
    }

    @Test
    void testEntryPathAndRestPortFromConfig() throws IOException {
        var conf = deployConfig();
        conf.set(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, "/custom-entrypoint.sh");
        conf.set(RestOptions.PORT, 9091);
        var resources = MiniClusterPodFactory.build(miniCluster.getSpec(), conf);

        var container = mainContainer(podSpec(resources));
        assertThat(container.getCommand()).containsExactly("/custom-entrypoint.sh");
        assertThat(container.getPorts().get(0).getContainerPort()).isEqualTo(9091);
        assertThat(restService(resources).getSpec().getPorts().get(0).getPort()).isEqualTo(9091);
    }

    @Test
    void testServiceTargetsRestBindPort() throws IOException {
        var conf = deployConfig();
        conf.set(RestOptions.PORT, 9091);
        conf.set(RestOptions.BIND_PORT, "9092");
        var resources = MiniClusterPodFactory.build(miniCluster.getSpec(), conf);

        assertThat(mainContainer(podSpec(resources)).getPorts().get(0).getContainerPort())
                .isEqualTo(9092);
        var servicePort = restService(resources).getSpec().getPorts().get(0);
        assertThat(servicePort.getPort()).isEqualTo(9091);
        assertThat(servicePort.getTargetPort().getIntVal()).isEqualTo(9092);
    }

    @Test
    void testRestServiceOptionsFromConfig() throws IOException {
        var flinkConfiguration = miniCluster.getSpec().getFlinkConfiguration();
        flinkConfiguration.put(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE.key(), "LoadBalancer");
        flinkConfiguration.put(
                KubernetesConfigOptions.REST_SERVICE_ANNOTATIONS.key(), "example.com/key:value");

        var service = restService(build());

        assertThat(service.getSpec().getType()).isEqualTo("LoadBalancer");
        assertThat(service.getMetadata().getAnnotations())
                .containsExactlyEntriesOf(Map.of("example.com/key", "value"));
        assertThat(service.getSpec().getSelector())
                .isEqualTo(MiniClusterPodFactory.getSelectorLabels(NAME));
    }

    @Test
    void testHeadlessRestService() throws IOException {
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE.key(), "Headless_ClusterIP");

        var service = restService(build());

        assertThat(service.getSpec().getType()).isEqualTo("ClusterIP");
        assertThat(service.getSpec().getClusterIP()).isEqualTo("None");
    }

    @Test
    void testRandomRestBindPortIsFixed() throws IOException {
        // The Service can't target a random port, so the REST endpoint listens on a fixed one
        var conf = deployConfig();
        conf.set(RestOptions.BIND_PORT, "0");
        var resources = MiniClusterPodFactory.build(miniCluster.getSpec(), conf);

        assertThat(mainContainer(podSpec(resources)).getPorts().get(0).getContainerPort())
                .isEqualTo(Constants.REST_PORT);
        assertThat(restService(resources).getSpec().getPorts().get(0).getTargetPort())
                .extracting(port -> port.getIntVal())
                .isEqualTo(Constants.REST_PORT);
        assertThat(loadConfigFile(resources).get(RestOptions.BIND_PORT))
                .isEqualTo(String.valueOf(Constants.REST_PORT));
        assertThat(conf.get(RestOptions.BIND_PORT)).isEqualTo("0");
    }

    @Test
    void testRestBindPortRangeIsRejected() {
        var conf = deployConfig();
        conf.set(RestOptions.BIND_PORT, "50100-50200");

        assertThatThrownBy(() -> MiniClusterPodFactory.build(miniCluster.getSpec(), conf))
                .hasMessageContaining("rest.bind-port should be specified to a fixed port");
    }

    @Test
    void testUserRestPortUsesRestBindPort() throws IOException {
        var userMain =
                new ContainerBuilder()
                        .withName(Constants.MAIN_CONTAINER_NAME)
                        .addNewPort()
                        .withName("metrics")
                        .withContainerPort(9249)
                        .endPort()
                        .addNewPort()
                        .withName(Constants.REST_PORT_NAME)
                        .withContainerPort(9000)
                        .withHostPort(30081)
                        .endPort()
                        .build();
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewSpec()
                                .withContainers(userMain)
                                .endSpec()
                                .build());

        var ports = mainContainer(podSpec(build())).getPorts();

        assertThat(ports)
                .extracting(p -> p.getName(), p -> p.getContainerPort(), p -> p.getHostPort())
                .containsExactly(
                        tuple("metrics", 9249, null),
                        tuple(Constants.REST_PORT_NAME, RestOptions.PORT.defaultValue(), 30081));
    }

    @Test
    void testLauncherArgs() {
        var job = JobSpec.builder().jarURI("local:///opt/flink/usrlib/job.jar").build();
        var conf = new Configuration();

        assertThat(CmdMiniClusterDecorator.getLauncherArgs(job, conf))
                .containsExactly(
                        "/opt/flink/bin/minicluster.sh",
                        "--jars",
                        "local:///opt/flink/usrlib/job.jar");

        var jobId = new JobID().toHexString();
        conf.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId);
        job.setEntryClass("com.example.Job");
        job.setArgs(new String[] {"--jars", "has space", "has\"quote"});

        assertThat(CmdMiniClusterDecorator.getLauncherArgs(job, conf))
                .containsExactly(
                        "/opt/flink/bin/minicluster.sh",
                        "--jars",
                        "local:///opt/flink/usrlib/job.jar",
                        "--job-classname",
                        "com.example.Job",
                        "--job-id",
                        jobId,
                        "--",
                        "--jars",
                        "has space",
                        "has\"quote");

        job.setArgs(new String[0]);
        assertThat(CmdMiniClusterDecorator.getLauncherArgs(job, conf)).doesNotContain("--");
    }

    @Test
    void testLauncherArgsWithoutJarURI() {
        // With no jarURI, the launcher runs the entry class from the classpath or usrlib
        var job = JobSpec.builder().entryClass("com.example.Job").build();

        assertThat(CmdMiniClusterDecorator.getLauncherArgs(job, new Configuration()))
                .containsExactly(
                        "/opt/flink/bin/minicluster.sh", "--job-classname", "com.example.Job");
    }

    @Test
    void testPodTemplateIsMergedWithMainContainer() throws IOException {
        var sidecar = new ContainerBuilder().withName("sidecar").withImage("busybox").build();
        var userMain =
                new ContainerBuilder()
                        .withName(Constants.MAIN_CONTAINER_NAME)
                        .withImage("ignored")
                        .addNewEnv()
                        .withName("USER_ENV")
                        .withValue("value")
                        .endEnv()
                        .addNewVolumeMount()
                        .withName("state")
                        .withMountPath("/flink-data")
                        .endVolumeMount()
                        .build();
        miniCluster
                .getSpec()
                .setPodTemplate(
                        new PodTemplateSpecBuilder()
                                .withNewMetadata()
                                .addToLabels("user-label", "value")
                                .endMetadata()
                                .withNewSpec()
                                .withTerminationGracePeriodSeconds(10L)
                                .withContainers(userMain, sidecar)
                                .addNewVolume()
                                .withName("state")
                                .withNewEmptyDir()
                                .endEmptyDir()
                                .endVolume()
                                .endSpec()
                                .build());

        var podTemplate = build().getDeployment().getSpec().getTemplate();
        var containers = podTemplate.getSpec().getContainers();

        assertThat(podTemplate.getMetadata().getLabels()).containsEntry("user-label", "value");
        assertThat(podTemplate.getSpec().getTerminationGracePeriodSeconds()).isEqualTo(10L);
        assertThat(podTemplate.getSpec().getVolumes())
                .extracting(Volume::getName)
                .contains("state", Constants.FLINK_CONF_VOLUME);

        // As for a FlinkDeployment, the main container is added after any other containers
        assertThat(containers)
                .extracting(Container::getName)
                .containsExactly("sidecar", Constants.MAIN_CONTAINER_NAME);
        assertThat(containers.get(0).getImage()).isEqualTo("busybox");

        var main = containers.get(1);
        assertThat(main.getImage()).isEqualTo(TestUtils.IMAGE);
        assertThat(main.getEnv())
                .extracting(EnvVar::getName)
                .contains("USER_ENV", "FLINK_CONF_DIR");
        assertThat(main.getVolumeMounts())
                .extracting(VolumeMount::getName)
                .contains("state", Constants.FLINK_CONF_VOLUME);
    }

    @Test
    void testUserPodTemplateIsNotModified() throws IOException {
        var podTemplate =
                new PodTemplateSpecBuilder()
                        .withNewSpec()
                        .addNewContainer()
                        .withName(Constants.MAIN_CONTAINER_NAME)
                        .endContainer()
                        .endSpec()
                        .build();
        miniCluster.getSpec().setPodTemplate(podTemplate);
        miniCluster
                .getSpec()
                .setResources(
                        new ResourceRequirementsBuilder()
                                .addToRequests("memory", new Quantity("1Gi"))
                                .build());
        var original = new PodTemplateSpecBuilder(podTemplate).build();

        build();

        assertThat(miniCluster.getSpec().getPodTemplate()).isEqualTo(original);
    }

    @Test
    void testConfigFileIsReadableByFlink() throws IOException {
        miniCluster
                .getSpec()
                .getFlinkConfiguration()
                .put("env.java.opts.all", "-Dname=\"quoted value\" -Dother='single quoted'");
        miniCluster.getSpec().getJob().setArgs(new String[] {"--input", "/data/in"});

        var conf = loadConfigFile(build());

        assertThat(conf.get(CoreOptions.FLINK_JVM_OPTIONS))
                .isEqualTo("-Dname=\"quoted value\" -Dother='single quoted'");
        assertThat(conf.get(ApplicationConfiguration.APPLICATION_ARGS))
                .containsExactly("--input", "/data/in");
        assertThat(conf.get(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH)).isFalse();
        assertThat(conf.get(KubernetesConfigOptions.CLUSTER_ID)).isEqualTo(NAME);
        assertThat(conf.get(CoreOptions.DEFAULT_PARALLELISM)).isEqualTo(1);
    }

    @Test
    void testConfigFileIsWrittenInTargetDialect() throws IOException {
        // Values that the legacy flat dialect cannot represent in a standard YAML config file
        miniCluster.getSpec().getJob().setArgs(new String[] {"a;b", "c,d"});
        var conf = deployConfig();
        conf.set(PipelineOptions.CLASSPATHS, List.of());

        var written = loadConfigFile(MiniClusterPodFactory.build(miniCluster.getSpec(), conf));

        assertThat(written.get(ApplicationConfiguration.APPLICATION_ARGS))
                .containsExactly("a;b", "c,d");
        assertThat(written.get(PipelineOptions.CLASSPATHS)).isEmpty();
    }

    @Test
    void testConfigFileNameFollowsFlinkVersion() throws IOException {
        assertThat(configMap(build()).getData()).containsKey("config.yaml");

        miniCluster.getSpec().setFlinkVersion(FlinkVersion.v1_20);
        assertThat(configMap(build()).getData())
                .containsKey("flink-conf.yaml")
                .doesNotContainKey("config.yaml");
    }

    @Test
    void testAddressesAndOperatorOptionsAreNotWritten() throws IOException {
        var conf = deployConfig();
        conf.set(RestOptions.BIND_ADDRESS, "localhost");
        conf.set(JobManagerOptions.BIND_HOST, "localhost");
        conf.set(TaskManagerOptions.BIND_HOST, "localhost");
        conf.set(TaskManagerOptions.HOST, "localhost");
        conf.set(KubernetesConfigOptions.KUBE_CONFIG_FILE, "/kubeconfig");

        var written = loadConfigFile(MiniClusterPodFactory.build(miniCluster.getSpec(), conf));

        assertThat(written.contains(RestOptions.BIND_ADDRESS)).isFalse();
        assertThat(written.contains(JobManagerOptions.BIND_HOST)).isFalse();
        assertThat(written.contains(TaskManagerOptions.BIND_HOST)).isFalse();
        assertThat(written.contains(TaskManagerOptions.HOST)).isFalse();
        assertThat(written.contains(KubernetesConfigOptions.KUBE_CONFIG_FILE)).isFalse();
    }

    @Test
    void testLogConfiguration() throws IOException {
        Files.writeString(tempDir.resolve("log4j-console.properties"), "operator-log4j");
        Files.writeString(tempDir.resolve("logback-console.xml"), "operator-logback");

        assertThat(configMap(build()).getData())
                .containsEntry("log4j-console.properties", "operator-log4j")
                .containsEntry("logback-console.xml", "operator-logback");

        miniCluster
                .getSpec()
                .setLogConfiguration(Map.of("log4j-console.properties", "custom-log4j"));
        assertThat(configMap(build()).getData())
                .containsEntry("log4j-console.properties", "custom-log4j")
                .doesNotContainKey("logback-console.xml");
    }

    @Test
    void testNoLogConfigurationAvailable() throws IOException {
        assertThat(configMap(build()).getData()).containsOnlyKeys("config.yaml");
    }

    private Configuration deployConfig() {
        return configManager.getDeployConfig(miniCluster.getMetadata(), miniCluster.getSpec());
    }

    private KubernetesJobManagerSpecification build() throws IOException {
        return MiniClusterPodFactory.build(miniCluster.getSpec(), deployConfig());
    }

    private static PodSpec podSpec(KubernetesJobManagerSpecification resources) {
        return resources.getDeployment().getSpec().getTemplate().getSpec();
    }

    private static Container mainContainer(PodSpec podSpec) {
        return podSpec.getContainers().stream()
                .filter(c -> c.getName().equals(Constants.MAIN_CONTAINER_NAME))
                .findFirst()
                .orElseThrow();
    }

    private static Service restService(KubernetesJobManagerSpecification resources) {
        return resources.getAccompanyingResources().stream()
                .filter(Service.class::isInstance)
                .map(Service.class::cast)
                .findFirst()
                .orElseThrow();
    }

    private static ConfigMap configMap(KubernetesJobManagerSpecification resources) {
        return resources.getAccompanyingResources().stream()
                .filter(ConfigMap.class::isInstance)
                .map(ConfigMap.class::cast)
                .findFirst()
                .orElseThrow();
    }

    private static PodTemplateSpec podTemplateWithMainImage(String image) {
        return new PodTemplateSpecBuilder()
                .withNewSpec()
                .addNewContainer()
                .withName(Constants.MAIN_CONTAINER_NAME)
                .withImage(image)
                .endContainer()
                .endSpec()
                .build();
    }

    private static PodTemplateSpec podTemplateWithMainImagePullPolicy(String imagePullPolicy) {
        return new PodTemplateSpecBuilder()
                .withNewSpec()
                .addNewContainer()
                .withName(Constants.MAIN_CONTAINER_NAME)
                .withImagePullPolicy(imagePullPolicy)
                .endContainer()
                .endSpec()
                .build();
    }

    private Configuration loadConfigFile(KubernetesJobManagerSpecification resources)
            throws IOException {
        var confDir = Files.createTempDirectory(tempDir, "pod-conf");
        Files.writeString(
                confDir.resolve("config.yaml"), configMap(resources).getData().get("config.yaml"));
        return GlobalConfiguration.loadConfiguration(confDir.toString());
    }
}
