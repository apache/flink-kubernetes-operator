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

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.configuration.DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.IMAGE;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.IMAGE_POLICY;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.SAMPLE_JAR;
import static org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils.SERVICE_ACCOUNT;
import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.DEFAULT_CHECKPOINTING_INTERVAL;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** FlinkConfigBuilderTest. */
public class FlinkConfigBuilderTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private static FlinkDeployment flinkDeployment;
    private static final String CUSTOM_LOG_CONFIG = "rootLogger.level = INFO";

    @BeforeEach
    public void prepareFlinkDeployment() {
        flinkDeployment = TestUtils.buildApplicationCluster();
        flinkDeployment.getSpec().setIngress(IngressSpec.builder().template("test.com").build());
        flinkDeployment.getSpec().getJobManager().setReplicas(2);
        flinkDeployment.getSpec().getJob().setParallelism(2);
        flinkDeployment
                .getSpec()
                .setLogConfiguration(Map.of(Constants.CONFIG_FILE_LOG4J_NAME, CUSTOM_LOG_CONFIG));
    }

    @Test
    public void testApplyImage() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration()).applyImage().build();
        assertEquals(IMAGE, configuration.get(KubernetesConfigOptions.CONTAINER_IMAGE));
    }

    @Test
    public void testApplyImagePolicy() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyImagePullPolicy()
                        .build();
        assertEquals(
                IMAGE_POLICY,
                configuration.get(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY).toString());
    }

    @Test
    public void testApplyFlinkConfiguration() {
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyFlinkConfiguration()
                        .build();
        assertEquals(2, (int) configuration.get(TaskManagerOptions.NUM_TASK_SLOTS));
        assertEquals(
                KubernetesConfigOptions.ServiceExposedType.ClusterIP,
                configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE));
        assertEquals(false, configuration.get(WebOptions.CANCEL_ENABLE));

        FlinkDeployment deployment = ReconciliationUtils.clone(flinkDeployment);
        deployment
                .getSpec()
                .setFlinkConfiguration(
                        Map.of(
                                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE.key(),
                                KubernetesConfigOptions.ServiceExposedType.LoadBalancer.name()));

        configuration =
                new FlinkConfigBuilder(deployment, new Configuration())
                        .applyFlinkConfiguration()
                        .build();
        assertEquals(
                KubernetesConfigOptions.ServiceExposedType.LoadBalancer,
                configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE));

        deployment = TestUtils.buildSessionCluster();
        configuration =
                new FlinkConfigBuilder(deployment, new Configuration())
                        .applyFlinkConfiguration()
                        .build();
        assertEquals(false, configuration.get(WebOptions.CANCEL_ENABLE));
    }

    @Test
    public void testApplyLogConfiguration() throws IOException {
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyLogConfiguration()
                        .build();

        File log4jFile =
                new File(
                        configuration.get(DeploymentOptionsInternal.CONF_DIR),
                        CONFIG_FILE_LOG4J_NAME);
        Assertions.assertTrue(log4jFile.exists() && log4jFile.isFile() && log4jFile.canRead());
        assertEquals(CUSTOM_LOG_CONFIG, Files.readString(log4jFile.toPath()));
    }

    @Test
    public void testApplyCommonPodTemplate() throws Exception {
        flinkDeployment.getSpec().getJobManager().getResource().setEphemeralStorage(null);
        flinkDeployment.getSpec().getTaskManager().getResource().setEphemeralStorage(null);

        var container0 = new Container();
        container0.setName("c0");
        var container1 = new Container();
        container1.setName("c1");

        var mainContainer = new Container();
        mainContainer.setName(Constants.MAIN_CONTAINER_NAME);
        mainContainer.setImage("test");

        flinkDeployment
                .getSpec()
                .setPodTemplate(TestUtils.getTestPodTemplate("", List.of(container0)));

        var inConfig = new Configuration();
        inConfig.set(KubernetesOperatorConfigOptions.OPERATOR_JM_STARTUP_PROBE_ENABLED, false);

        var configuration =
                new FlinkConfigBuilder(flinkDeployment, inConfig.clone())
                        .applyPodTemplate()
                        .build();

        Assertions.assertEquals(
                configuration.getString(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE),
                configuration.getString(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE));

        Assertions.assertEquals(
                List.of(container0), getJmPod(configuration).getSpec().getContainers());

        flinkDeployment.getSpec().getJobManager().getResource().setEphemeralStorage("2G");
        flinkDeployment.getSpec().getTaskManager().getResource().setEphemeralStorage("3G");

        configuration =
                new FlinkConfigBuilder(flinkDeployment, inConfig.clone())
                        .applyPodTemplate()
                        .build();

        Assertions.assertNotEquals(
                configuration.getString(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE),
                configuration.getString(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE));
        assertMainContainerEphemeralStorage(
                getJmPod(configuration).getSpec().getContainers().get(1), "2G");
        Assertions.assertEquals(
                container0, getJmPod(configuration).getSpec().getContainers().get(0));
        assertMainContainerEphemeralStorage(
                getTmPod(configuration).getSpec().getContainers().get(1), "3G");
        Assertions.assertEquals(
                container0, getTmPod(configuration).getSpec().getContainers().get(0));

        flinkDeployment.getSpec().getJobManager().getResource().setEphemeralStorage(null);
        flinkDeployment.getSpec().getTaskManager().getResource().setEphemeralStorage(null);

        // Test with startup probe:
        inConfig.set(KubernetesOperatorConfigOptions.OPERATOR_JM_STARTUP_PROBE_ENABLED, true);

        configuration =
                new FlinkConfigBuilder(flinkDeployment, inConfig.clone())
                        .applyPodTemplate()
                        .build();

        var jmPod = getJmPod(configuration);
        var tmPod = getTmPod(configuration);

        Assertions.assertEquals(2, jmPod.getSpec().getContainers().size());
        Assertions.assertEquals(
                Constants.MAIN_CONTAINER_NAME, jmPod.getSpec().getContainers().get(1).getName());
        Assertions.assertNotNull(
                jmPod.getSpec().getContainers().get(1).getStartupProbe().getHttpGet());

        Assertions.assertEquals(List.of(container0), tmPod.getSpec().getContainers());

        // With completely empty podtemplates still generate startup probe
        flinkDeployment.getSpec().setPodTemplate(null);
        configuration =
                new FlinkConfigBuilder(flinkDeployment, inConfig.clone())
                        .applyPodTemplate()
                        .build();
        Assertions.assertNull(
                configuration.getString(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE));
        jmPod = getJmPod(configuration);
        Assertions.assertEquals(1, jmPod.getSpec().getContainers().size());
        Assertions.assertEquals(
                Constants.MAIN_CONTAINER_NAME, jmPod.getSpec().getContainers().get(0).getName());
        Assertions.assertNotNull(
                jmPod.getSpec().getContainers().get(0).getStartupProbe().getHttpGet());

        // Override independently
        flinkDeployment
                .getSpec()
                .getJobManager()
                .setPodTemplate(
                        TestUtils.getTestPodTemplate("", List.of(mainContainer, container0)));
        flinkDeployment
                .getSpec()
                .getTaskManager()
                .setPodTemplate(TestUtils.getTestPodTemplate("", List.of(container1)));

        configuration =
                new FlinkConfigBuilder(flinkDeployment, inConfig.clone())
                        .applyPodTemplate()
                        .build();

        jmPod = getJmPod(configuration);
        tmPod = getTmPod(configuration);

        Assertions.assertEquals(2, jmPod.getSpec().getContainers().size());
        Assertions.assertEquals(
                Constants.MAIN_CONTAINER_NAME, jmPod.getSpec().getContainers().get(0).getName());
        Assertions.assertNotNull(
                jmPod.getSpec().getContainers().get(0).getStartupProbe().getHttpGet());
        Assertions.assertEquals("test", jmPod.getSpec().getContainers().get(0).getImage());
        Assertions.assertEquals(container0, jmPod.getSpec().getContainers().get(1));

        Assertions.assertEquals(List.of(container1), tmPod.getSpec().getContainers());

        // Override common
        var common = TestUtils.getTestPodTemplate("", Collections.emptyList());
        common.getSpec().setDnsPolicy("test");
        flinkDeployment.getSpec().setPodTemplate(common);

        configuration =
                new FlinkConfigBuilder(flinkDeployment, inConfig.clone())
                        .applyPodTemplate()
                        .build();

        jmPod = getJmPod(configuration);
        tmPod = getTmPod(configuration);

        Assertions.assertEquals(2, jmPod.getSpec().getContainers().size());
        Assertions.assertEquals(
                Constants.MAIN_CONTAINER_NAME, jmPod.getSpec().getContainers().get(0).getName());
        Assertions.assertNotNull(
                jmPod.getSpec().getContainers().get(0).getStartupProbe().getHttpGet());
        Assertions.assertEquals("test", jmPod.getSpec().getContainers().get(0).getImage());
        Assertions.assertEquals(container0, jmPod.getSpec().getContainers().get(1));
        Assertions.assertEquals("test", jmPod.getSpec().getDnsPolicy());

        Assertions.assertEquals(List.of(container1), tmPod.getSpec().getContainers());
        Assertions.assertEquals("test", tmPod.getSpec().getDnsPolicy());

        inConfig.set(KubernetesOperatorConfigOptions.OPERATOR_JM_STARTUP_PROBE_ENABLED, false);
        flinkDeployment.getSpec().setPodTemplate(null);
        flinkDeployment.getSpec().setTaskManager(null);
        flinkDeployment.getSpec().setJobManager(null);
        configuration =
                new FlinkConfigBuilder(flinkDeployment, inConfig.clone())
                        .applyPodTemplate()
                        .build();
        Assertions.assertFalse(
                configuration.contains(KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE));
        Assertions.assertFalse(
                configuration.contains(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE));
        Assertions.assertFalse(
                configuration.contains(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE));

        flinkDeployment.getSpec().setTaskManager(new TaskManagerSpec());
        flinkDeployment.getSpec().setJobManager(new JobManagerSpec());
        configuration =
                new FlinkConfigBuilder(flinkDeployment, inConfig.clone())
                        .applyPodTemplate()
                        .build();
        Assertions.assertFalse(
                configuration.contains(KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE));
        Assertions.assertFalse(
                configuration.contains(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE));
        Assertions.assertFalse(
                configuration.contains(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE));
    }

    private PodTemplateSpec getJmPod(Configuration configuration) throws IOException {
        return OBJECT_MAPPER.readValue(
                new File(configuration.getString(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE)),
                PodTemplateSpec.class);
    }

    private PodTemplateSpec getTmPod(Configuration configuration) throws IOException {
        return OBJECT_MAPPER.readValue(
                new File(
                        configuration.getString(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE)),
                PodTemplateSpec.class);
    }

    @Test
    public void testApplyServiceAccount() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyServiceAccount()
                        .build();
        assertEquals(
                SERVICE_ACCOUNT,
                configuration.get(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT));
    }

    @Test
    public void testDeprecatedConfigKeys() throws Exception {
        FlinkDeployment deploymentClone = ReconciliationUtils.clone(flinkDeployment);

        // We must use deprecated configs for 1.16 and before
        deploymentClone.getSpec().setFlinkVersion(FlinkVersion.v1_16);
        deploymentClone.getSpec().setPodTemplate(new PodTemplateSpec());
        Configuration configuration =
                new FlinkConfigBuilder(deploymentClone, new Configuration())
                        .applyJobManagerSpec()
                        .applyTaskManagerSpec()
                        .applyPodTemplate()
                        .build();

        var confMap = configuration.toMap();

        assertEquals("1.0", confMap.get("kubernetes.jobmanager.cpu"));
        assertEquals("1.0", confMap.get("kubernetes.taskmanager.cpu"));
        // Set new config all the time to simplify reading side
        assertEquals(Double.valueOf(1), configuration.get(KubernetesConfigOptions.JOB_MANAGER_CPU));
        assertEquals(
                Double.valueOf(1), configuration.get(KubernetesConfigOptions.TASK_MANAGER_CPU));
    }

    @Test
    public void testApplyJobManagerSpec() throws Exception {
        FlinkDeployment deploymentClone = ReconciliationUtils.clone(flinkDeployment);
        Configuration configuration =
                new FlinkConfigBuilder(deploymentClone, new Configuration())
                        .applyJobManagerSpec()
                        .build();

        assertEquals(
                MemorySize.parse("2 gb"),
                configuration.get(JobManagerOptions.TOTAL_PROCESS_MEMORY));
        assertEquals(Double.valueOf(1), configuration.get(KubernetesConfigOptions.JOB_MANAGER_CPU));
        assertEquals(
                Integer.valueOf(2),
                configuration.get(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS));
    }

    @Test
    public void testJmEphemeralStorage() throws Exception {
        flinkDeployment.getSpec().getJobManager().setPodTemplate(createTestPodWithContainers());
        var configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyPodTemplate()
                        .build();

        assertMainContainerEphemeralStorage(
                getJmPod(configuration).getSpec().getContainers().get(0), "2G");

        flinkDeployment
                .getSpec()
                .getJobManager()
                .setPodTemplate(TestUtils.getTestPodTemplate("pod1 hostname", new ArrayList<>()));
        configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyPodTemplate()
                        .build();

        var jmPod = getJmPod(configuration);
        assertMainContainerEphemeralStorage(jmPod.getSpec().getContainers().get(0), "2G");
    }

    @Test
    public void testTaskManagerSpec() {
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();

        assertEquals(
                MemorySize.parse("2 gb"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
        assertEquals(
                Double.valueOf(1), configuration.get(KubernetesConfigOptions.TASK_MANAGER_CPU));
    }

    @Test
    public void testApplyJobManagerSpecWithBiByteMemorySetting() {
        var resource = new Resource(1.0, "1Gi", "20Gi");
        flinkDeployment.getSpec().getJobManager().setResource(resource);
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyJobManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("1 gb"),
                configuration.get(JobManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith2048MiSetting() {
        var resource = new Resource(1.0, "2048Mi", "20Gi");
        flinkDeployment.getSpec().getTaskManager().setResource(resource);
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("2147483648 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith2gSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("2g");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("2147483648 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith2giSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("2gi");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("2147483648 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith2gbSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("2gb");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("2 gb"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith2gibSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("2gib");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("2 gb"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith2GSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("2G");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("2 gb"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith2GiSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("2Gi");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("2147483648 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith2_gSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("2 g");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("2 gb"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith2_GiSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("2 Gi");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("2147483648 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith512mSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("512m");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("512 mb"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith512miSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("512mi");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("536870912 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith1024mSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("1024m");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("1 gb"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith1024miSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("1024mi");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("1073741824 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith1000000bSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("1000000b");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("1000000 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith1000000_bSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("1000000 b");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("1000000 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith1e6Setting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("1e6");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("1000000 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTaskManagerSpecWith1e6_bSetting() {
        flinkDeployment.getSpec().getTaskManager().getResource().setMemory("1e6 b");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyTaskManagerSpec()
                        .build();
        assertEquals(
                MemorySize.parse("1000000 b"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
    }

    @Test
    public void testTmEphemeralStorage() throws Exception {
        FlinkDeployment deploymentClone = ReconciliationUtils.clone(flinkDeployment);
        deploymentClone.getSpec().setPodTemplate(null);
        Configuration configuration =
                new FlinkConfigBuilder(deploymentClone, new Configuration())
                        .applyPodTemplate()
                        .build();

        assertMainContainerEphemeralStorage(
                getTmPod(configuration).getSpec().getContainers().get(0), "2G");

        deploymentClone = ReconciliationUtils.clone(flinkDeployment);
        deploymentClone.getSpec().setPodTemplate(null);
        deploymentClone.getSpec().getTaskManager().setPodTemplate(createTestPodWithContainers());
        configuration =
                new FlinkConfigBuilder(deploymentClone, new Configuration())
                        .applyPodTemplate()
                        .build();

        assertMainContainerEphemeralStorage(
                getTmPod(configuration).getSpec().getContainers().get(0), "2G");

        flinkDeployment
                .getSpec()
                .getTaskManager()
                .setPodTemplate(TestUtils.getTestPodTemplate("pod2 hostname", new ArrayList<>()));
        configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyPodTemplate()
                        .build();

        var tmPod = getTmPod(configuration);
        assertMainContainerEphemeralStorage(tmPod.getSpec().getContainers().get(0), "2G");
    }

    @Test
    public void testApplyJobOrSessionSpec() throws Exception {
        FlinkDeployment deploymentClone = ReconciliationUtils.clone(flinkDeployment);
        deploymentClone.getSpec().getJob().setAllowNonRestoredState(true);
        deploymentClone.getSpec().getJob().setArgs(new String[] {"--test", "123"});
        var configuration =
                new FlinkConfigBuilder(deploymentClone, new Configuration())
                        .applyJobOrSessionSpec()
                        .build();
        Assertions.assertTrue(
                configuration.getBoolean(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE));
        assertEquals(
                KubernetesDeploymentTarget.APPLICATION.getName(),
                configuration.get(DeploymentOptions.TARGET));
        assertEquals(SAMPLE_JAR, configuration.get(PipelineOptions.JARS).get(0));
        assertEquals(Integer.valueOf(2), configuration.get(CoreOptions.DEFAULT_PARALLELISM));
        assertEquals(
                List.of("--test", "123"),
                configuration.get(ApplicationConfiguration.APPLICATION_ARGS));

        var dep = ReconciliationUtils.clone(deploymentClone);
        dep.getSpec().setTaskManager(new TaskManagerSpec());
        dep.getSpec().getTaskManager().setReplicas(3);
        dep.getSpec().getFlinkConfiguration().put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "4");
        configuration =
                new FlinkConfigBuilder(dep, new Configuration())
                        .applyFlinkConfiguration()
                        .applyJobOrSessionSpec()
                        .build();

        assertEquals(12, configuration.get(CoreOptions.DEFAULT_PARALLELISM));
        assertEquals(
                true, configuration.get(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR));
        Assertions.assertFalse(configuration.getBoolean(SHUTDOWN_ON_APPLICATION_FINISH));
        assertEquals(
                flinkDeployment.getMetadata().getName(), configuration.get(PipelineOptions.NAME));

        dep = ReconciliationUtils.clone(deploymentClone);
        dep.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        configuration =
                new FlinkConfigBuilder(dep, new Configuration())
                        .applyFlinkConfiguration()
                        .applyJobOrSessionSpec()
                        .build();
        assertEquals(
                DEFAULT_CHECKPOINTING_INTERVAL,
                configuration.get(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL));
    }

    @Test
    public void testApplyJobOrSessionSpecWithNoJar() throws Exception {
        FlinkDeployment deploymentClone = ReconciliationUtils.clone(flinkDeployment);
        deploymentClone.getSpec().getJob().setJarURI(null);

        var configuration =
                new FlinkConfigBuilder(deploymentClone, new Configuration())
                        .applyJobOrSessionSpec()
                        .build();

        assertNull(configuration.get(PipelineOptions.JARS));
    }

    @Test
    public void testAllowNonRestoredStateInSpecOverrideInFlinkConf() throws URISyntaxException {
        flinkDeployment.getSpec().getJob().setAllowNonRestoredState(false);
        flinkDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key(), "true");
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyJobOrSessionSpec()
                        .build();
        Assertions.assertFalse(
                configuration.getBoolean(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE));

        flinkDeployment.getSpec().getJob().setAllowNonRestoredState(true);
        flinkDeployment
                .getSpec()
                .getFlinkConfiguration()
                .put(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key(), "false");
        configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyJobOrSessionSpec()
                        .build();
        Assertions.assertTrue(
                configuration.getBoolean(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE));
    }

    @Test
    public void testApplyStandaloneApplicationSpec() throws URISyntaxException, IOException {
        FlinkDeployment dep = ReconciliationUtils.clone(flinkDeployment);
        final String entryClass = "entry.class";
        final String jarUri = "local:///flink/opt/StateMachine.jar";
        final String correctedJarUri = "file:///flink/opt/StateMachine.jar";
        dep.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);
        dep.getSpec().getJob().setEntryClass(entryClass);
        dep.getSpec().getJob().setJarURI(jarUri);
        dep.getSpec().setTaskManager(new TaskManagerSpec());
        dep.getSpec().getTaskManager().setReplicas(3);
        dep.getSpec().getFlinkConfiguration().put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "2");

        Configuration configuration =
                new FlinkConfigBuilder(dep, new Configuration())
                        .applyFlinkConfiguration()
                        .applyTaskManagerSpec()
                        .applyJobOrSessionSpec()
                        .build();

        assertEquals("remote", configuration.getString(DeploymentOptions.TARGET));
        assertEquals(
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION,
                configuration.get(StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE));
        assertEquals(6, configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM));
        assertEquals(
                entryClass,
                configuration.getString(ApplicationConfiguration.APPLICATION_MAIN_CLASS));
        assertEquals(
                3,
                configuration.get(
                        StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS));
        List<String> classpaths =
                ConfigUtils.decodeListFromConfig(
                        configuration, PipelineOptions.CLASSPATHS, String::toString);
        assertThat(classpaths, containsInAnyOrder(correctedJarUri));

        dep.getSpec().getTaskManager().setReplicas(null);
        dep.getSpec().getJob().setParallelism(10);

        configuration =
                new FlinkConfigBuilder(dep, new Configuration())
                        .applyFlinkConfiguration()
                        .applyTaskManagerSpec()
                        .applyJobOrSessionSpec()
                        .build();
        assertEquals(
                5,
                configuration.get(
                        StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS));

        dep.getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "vertex1:10,vertex2:20");
        configuration =
                new FlinkConfigBuilder(dep, new Configuration())
                        .applyFlinkConfiguration()
                        .applyTaskManagerSpec()
                        .applyJobOrSessionSpec()
                        .build();
        assertEquals(
                10,
                configuration.get(
                        StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS));
    }

    @Test
    public void testApplyStandaloneSessionSpec() throws URISyntaxException, IOException {
        FlinkDeployment dep = ReconciliationUtils.clone(flinkDeployment);
        dep.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);
        dep.getSpec().setJob(null);
        dep.getSpec().setTaskManager(new TaskManagerSpec());
        dep.getSpec().getTaskManager().setReplicas(5);
        dep.getSpec().getFlinkConfiguration().put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "2");

        Configuration configuration =
                new FlinkConfigBuilder(dep, new Configuration())
                        .applyFlinkConfiguration()
                        .applyTaskManagerSpec()
                        .applyJobOrSessionSpec()
                        .build();

        assertEquals("remote", configuration.getString(DeploymentOptions.TARGET));
        assertEquals(
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.SESSION,
                configuration.get(StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE));
        assertEquals(
                5,
                configuration.get(
                        StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS));
    }

    @Test
    public void testBuildFrom() throws Exception {
        final Configuration configuration =
                FlinkConfigBuilder.buildFrom(
                        flinkDeployment.getMetadata().getNamespace(),
                        flinkDeployment.getMetadata().getName(),
                        flinkDeployment.getSpec(),
                        new Configuration());
        final String namespace = flinkDeployment.getMetadata().getNamespace();
        final String clusterId = flinkDeployment.getMetadata().getName();
        // Most configs have been tested by previous unit tests, thus we only verify the namespace
        // and clusterId here.
        assertEquals(namespace, configuration.get(KubernetesConfigOptions.NAMESPACE));
        assertEquals(clusterId, configuration.get(KubernetesConfigOptions.CLUSTER_ID));
    }

    @Test
    public void testApplyResourceToPodTemplate() {
        Resource resource = flinkDeployment.getSpec().getTaskManager().getResource();

        var pod = FlinkConfigBuilder.applyResourceToPodTemplate(null, resource);
        assertEquals(Constants.MAIN_CONTAINER_NAME, pod.getSpec().getContainers().get(0).getName());
        assertMainContainerEphemeralStorage(pod.getSpec().getContainers().get(0), "2G");

        var podWithMetadata = new PodTemplateSpec();
        ObjectMeta metaData = new ObjectMeta();
        podWithMetadata.setMetadata(metaData);
        pod = FlinkConfigBuilder.applyResourceToPodTemplate(podWithMetadata, resource);
        assertEquals(metaData, pod.getMetadata());
        assertEquals(Constants.MAIN_CONTAINER_NAME, pod.getSpec().getContainers().get(0).getName());
        assertMainContainerEphemeralStorage(pod.getSpec().getContainers().get(0), "2G");
    }

    private void assertMainContainerEphemeralStorage(
            Container container, String expectedEphemeralStorage) {
        assertEquals(
                expectedEphemeralStorage,
                container
                        .getResources()
                        .getLimits()
                        .get(CrdConstants.EPHEMERAL_STORAGE)
                        .toString());
        assertEquals(
                expectedEphemeralStorage,
                container
                        .getResources()
                        .getRequests()
                        .get(CrdConstants.EPHEMERAL_STORAGE)
                        .toString());
    }

    private PodTemplateSpec createTestPodWithContainers() {
        Container mainContainer = new Container();
        mainContainer.setName(Constants.MAIN_CONTAINER_NAME);
        Container sideCarContainer = new Container();
        sideCarContainer.setName("sidecar");
        var pod =
                TestUtils.getTestPodTemplate("hostname", List.of(mainContainer, sideCarContainer));
        return pod;
    }

    private static Stream<KubernetesConfigOptions.ServiceExposedType> serviceExposedTypes() {
        return Stream.of(
                null,
                KubernetesConfigOptions.ServiceExposedType.ClusterIP,
                KubernetesConfigOptions.ServiceExposedType.LoadBalancer,
                KubernetesConfigOptions.ServiceExposedType.Headless_ClusterIP,
                KubernetesConfigOptions.ServiceExposedType.NodePort);
    }
}
