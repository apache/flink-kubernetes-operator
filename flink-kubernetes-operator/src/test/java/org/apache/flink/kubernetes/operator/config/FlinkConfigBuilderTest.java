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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.crd.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.crd.spec.TaskManagerSpec;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.TestUtils.IMAGE;
import static org.apache.flink.kubernetes.operator.TestUtils.IMAGE_POLICY;
import static org.apache.flink.kubernetes.operator.TestUtils.SAMPLE_JAR;
import static org.apache.flink.kubernetes.operator.TestUtils.SERVICE_ACCOUNT;
import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.DEFAULT_CHECKPOINTING_INTERVAL;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** FlinkConfigBuilderTest. */
public class FlinkConfigBuilderTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private static FlinkDeployment flinkDeployment;
    private static final String CUSTOM_LOG_CONFIG = "rootLogger.level = INFO";

    @BeforeAll
    public static void prepareFlinkDeployment() {
        flinkDeployment = TestUtils.buildApplicationCluster();
        final Container container0 = new Container();
        container0.setName("container0");
        final Pod pod0 =
                TestUtils.getTestPod(
                        "pod0 hostname", "pod0 api version", Arrays.asList(container0));

        flinkDeployment.getSpec().setPodTemplate(pod0);
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
        Assertions.assertEquals(IMAGE, configuration.get(KubernetesConfigOptions.CONTAINER_IMAGE));
    }

    @Test
    public void testApplyImagePolicy() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyImagePullPolicy()
                        .build();
        Assertions.assertEquals(
                IMAGE_POLICY,
                configuration.get(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY).toString());
    }

    @Test
    public void testApplyFlinkConfiguration() {
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyFlinkConfiguration()
                        .build();
        Assertions.assertEquals(2, (int) configuration.get(TaskManagerOptions.NUM_TASK_SLOTS));
        Assertions.assertEquals(
                KubernetesConfigOptions.ServiceExposedType.ClusterIP,
                configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE));
        Assertions.assertEquals(false, configuration.get(WebOptions.CANCEL_ENABLE));

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
        Assertions.assertEquals(
                KubernetesConfigOptions.ServiceExposedType.LoadBalancer,
                configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE));

        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        configuration =
                new FlinkConfigBuilder(
                                deployment,
                                new Configuration()
                                        .set(
                                                HighAvailabilityOptions.HA_MODE,
                                                KubernetesHaServicesFactory.class
                                                        .getCanonicalName()))
                        .applyFlinkConfiguration()
                        .build();
        Assertions.assertEquals(
                DEFAULT_CHECKPOINTING_INTERVAL,
                configuration.get(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL));

        deployment = TestUtils.buildSessionCluster();
        configuration =
                new FlinkConfigBuilder(deployment, new Configuration())
                        .applyFlinkConfiguration()
                        .build();
        Assertions.assertEquals(false, configuration.get(WebOptions.CANCEL_ENABLE));
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
        Assertions.assertEquals(CUSTOM_LOG_CONFIG, Files.readString(log4jFile.toPath()));
    }

    @Test
    public void testApplyCommonPodTemplate() throws Exception {
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyCommonPodTemplate()
                        .build();
        final Pod jmPod =
                OBJECT_MAPPER.readValue(
                        new File(
                                configuration.getString(
                                        KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE)),
                        Pod.class);
        final Pod tmPod =
                OBJECT_MAPPER.readValue(
                        new File(
                                configuration.getString(
                                        KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE)),
                        Pod.class);
        Assertions.assertEquals("container0", jmPod.getSpec().getContainers().get(0).getName());
        Assertions.assertEquals("container0", tmPod.getSpec().getContainers().get(0).getName());
    }

    @Test
    public void testApplyIngressDomain() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyIngressDomain()
                        .build();
        Assertions.assertEquals(
                KubernetesConfigOptions.ServiceExposedType.ClusterIP,
                configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE));
    }

    @Test
    public void testApplyServiceAccount() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyServiceAccount()
                        .build();
        Assertions.assertEquals(
                SERVICE_ACCOUNT,
                configuration.get(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT));
    }

    @Test
    public void testApplyJobManagerSpec() throws Exception {
        Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyJobManagerSpec()
                        .build();

        Assertions.assertNull(
                configuration.getString(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE));
        Assertions.assertEquals(
                MemorySize.parse("2048m"),
                configuration.get(JobManagerOptions.TOTAL_PROCESS_MEMORY));
        Assertions.assertEquals(
                Double.valueOf(1), configuration.get(KubernetesConfigOptions.JOB_MANAGER_CPU));
        Assertions.assertEquals(
                Integer.valueOf(2),
                configuration.get(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS));

        flinkDeployment
                .getSpec()
                .getJobManager()
                .setPodTemplate(
                        TestUtils.getTestPod(
                                "pod1 hostname", "pod1 api version", new ArrayList<>()));
        configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyJobManagerSpec()
                        .build();

        final Pod jmPod =
                OBJECT_MAPPER.readValue(
                        new File(
                                configuration.getString(
                                        KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE)),
                        Pod.class);
        Assertions.assertEquals("pod1 api version", jmPod.getApiVersion());
    }

    @Test
    public void testApplyTaskManagerSpec() throws Exception {
        FlinkDeployment deploymentClone = ReconciliationUtils.clone(flinkDeployment);
        deploymentClone.getSpec().setPodTemplate(null);

        Configuration configuration =
                new FlinkConfigBuilder(deploymentClone, new Configuration())
                        .applyTaskManagerSpec()
                        .build();

        Assertions.assertNull(
                configuration.getString(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE));
        Assertions.assertEquals(
                MemorySize.parse("2048m"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
        Assertions.assertEquals(
                Double.valueOf(1), configuration.get(KubernetesConfigOptions.TASK_MANAGER_CPU));

        deploymentClone
                .getSpec()
                .getTaskManager()
                .setPodTemplate(
                        TestUtils.getTestPod(
                                "pod2 hostname", "pod2 api version", new ArrayList<>()));
        configuration =
                new FlinkConfigBuilder(deploymentClone, new Configuration())
                        .applyTaskManagerSpec()
                        .build();

        final Pod tmPod =
                OBJECT_MAPPER.readValue(
                        new File(
                                configuration.getString(
                                        KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE)),
                        Pod.class);
        Assertions.assertEquals("pod2 api version", tmPod.getApiVersion());
    }

    @Test
    public void testApplyJobOrSessionSpec() throws Exception {
        flinkDeployment.getSpec().getJob().setAllowNonRestoredState(true);
        var configuration =
                new FlinkConfigBuilder(flinkDeployment, new Configuration())
                        .applyJobOrSessionSpec()
                        .build();
        Assertions.assertTrue(
                configuration.getBoolean(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE));
        Assertions.assertEquals(
                KubernetesDeploymentTarget.APPLICATION.getName(),
                configuration.get(DeploymentOptions.TARGET));
        Assertions.assertEquals(SAMPLE_JAR, configuration.get(PipelineOptions.JARS).get(0));
        Assertions.assertEquals(
                Integer.valueOf(2), configuration.get(CoreOptions.DEFAULT_PARALLELISM));

        var dep = ReconciliationUtils.clone(flinkDeployment);
        dep.getSpec().setTaskManager(new TaskManagerSpec());
        dep.getSpec().getTaskManager().setReplicas(3);
        dep.getSpec().getFlinkConfiguration().put(TaskManagerOptions.NUM_TASK_SLOTS.key(), "4");
        configuration =
                new FlinkConfigBuilder(dep, new Configuration())
                        .applyFlinkConfiguration()
                        .applyJobOrSessionSpec()
                        .build();

        Assertions.assertEquals(12, configuration.get(CoreOptions.DEFAULT_PARALLELISM));
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

        Assertions.assertEquals("remote", configuration.getString(DeploymentOptions.TARGET));
        Assertions.assertEquals(
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION,
                configuration.get(StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE));
        Assertions.assertEquals(6, configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM));
        Assertions.assertEquals(
                entryClass,
                configuration.getString(ApplicationConfiguration.APPLICATION_MAIN_CLASS));
        Assertions.assertEquals(
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
        Assertions.assertEquals(
                5,
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

        Assertions.assertEquals("remote", configuration.getString(DeploymentOptions.TARGET));
        Assertions.assertEquals(
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.SESSION,
                configuration.get(StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE));
        Assertions.assertEquals(
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
        Assertions.assertEquals(namespace, configuration.get(KubernetesConfigOptions.NAMESPACE));
        Assertions.assertEquals(clusterId, configuration.get(KubernetesConfigOptions.CLUSTER_ID));
    }
}
