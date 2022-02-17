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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.flink.kubernetes.operator.TestUtils.IMAGE;
import static org.apache.flink.kubernetes.operator.TestUtils.IMAGE_POLICY;
import static org.apache.flink.kubernetes.operator.TestUtils.SAMPLE_JAR;
import static org.apache.flink.kubernetes.operator.TestUtils.SERVICE_ACCOUNT;

/** FlinkConfigBuilderTest. */
public class FlinkConfigBuilderTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private static FlinkDeployment flinkDeployment;

    @BeforeAll
    public static void prepareFlinkDeployment() {
        flinkDeployment = TestUtils.buildApplicationCluster();
        final Container container0 = new Container();
        container0.setName("container0");
        final Pod pod0 =
                TestUtils.getTestPod(
                        "pod0 hostname", "pod0 api version", Arrays.asList(container0));
        final Pod pod1 =
                TestUtils.getTestPod("pod1 hostname", "pod1 api version", new ArrayList<>());
        final Pod pod2 =
                TestUtils.getTestPod("pod2 hostname", "pod2 api version", new ArrayList<>());

        flinkDeployment.getSpec().setPodTemplate(pod0);
        flinkDeployment.getSpec().setIngressDomain("test.com");
        flinkDeployment.getSpec().getJobManager().setPodTemplate(pod1);
        flinkDeployment.getSpec().getTaskManager().setPodTemplate(pod2);
        flinkDeployment.getSpec().getJob().setParallelism(2);
    }

    @Test
    public void testApplyImage() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment).applyImage().build();
        Assert.assertEquals(IMAGE, configuration.get(KubernetesConfigOptions.CONTAINER_IMAGE));
    }

    @Test
    public void testApplyImagePolicy() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment).applyImagePullPolicy().build();
        Assert.assertEquals(
                IMAGE_POLICY,
                configuration.get(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY).toString());
    }

    @Test
    public void testApplyFlinkConfiguration() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment).applyFlinkConfiguration().build();
        Assert.assertEquals(
                SERVICE_ACCOUNT,
                configuration.get(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT));
    }

    @Test
    public void testApplyCommonPodTemplate() throws Exception {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment).applyCommonPodTemplate().build();
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
        Assert.assertEquals("container0", jmPod.getSpec().getContainers().get(0).getName());
        Assert.assertEquals("container0", tmPod.getSpec().getContainers().get(0).getName());
    }

    @Test
    public void testApplyIngressDomain() {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment).applyIngressDomain().build();
        Assert.assertEquals(
                KubernetesConfigOptions.ServiceExposedType.ClusterIP,
                configuration.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE));
    }

    @Test
    public void testApplyJobManagerSpec() throws Exception {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment).applyJobManagerSpec().build();
        final Pod jmPod =
                OBJECT_MAPPER.readValue(
                        new File(
                                configuration.getString(
                                        KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE)),
                        Pod.class);
        Assert.assertEquals(
                MemorySize.parse("2048m"),
                configuration.get(JobManagerOptions.TOTAL_PROCESS_MEMORY));
        Assert.assertEquals(
                Double.valueOf(1), configuration.get(KubernetesConfigOptions.JOB_MANAGER_CPU));
        Assert.assertEquals("pod1 api version", jmPod.getApiVersion());
    }

    @Test
    public void testApplyTaskManagerSpec() throws Exception {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment).applyTaskManagerSpec().build();
        final Pod tmPod =
                OBJECT_MAPPER.readValue(
                        new File(
                                configuration.getString(
                                        KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE)),
                        Pod.class);
        Assert.assertEquals(
                MemorySize.parse("2048m"),
                configuration.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY));
        Assert.assertEquals(
                Double.valueOf(1), configuration.get(KubernetesConfigOptions.TASK_MANAGER_CPU));
        Assert.assertEquals(
                Integer.valueOf(2), configuration.get(TaskManagerOptions.NUM_TASK_SLOTS));
        Assert.assertEquals("pod2 api version", tmPod.getApiVersion());
    }

    @Test
    public void testApplyJobOrSessionSpec() throws Exception {
        final Configuration configuration =
                new FlinkConfigBuilder(flinkDeployment).applyJobOrSessionSpec().build();
        Assert.assertEquals(
                KubernetesDeploymentTarget.APPLICATION.getName(),
                configuration.get(DeploymentOptions.TARGET));
        Assert.assertEquals(SAMPLE_JAR, configuration.get(PipelineOptions.JARS).get(0));
        Assert.assertEquals(Integer.valueOf(2), configuration.get(CoreOptions.DEFAULT_PARALLELISM));
    }

    @Test
    public void testBuildFrom() throws Exception {
        final Configuration configuration = FlinkConfigBuilder.buildFrom(flinkDeployment);
        final String namespace = flinkDeployment.getMetadata().getNamespace();
        final String clusterId = flinkDeployment.getMetadata().getName();
        // Most configs have been tested by previous unit tests, thus we only verify the namespace
        // and clusterId here.
        Assert.assertEquals(namespace, configuration.get(KubernetesConfigOptions.NAMESPACE));
        Assert.assertEquals(clusterId, configuration.get(KubernetesConfigOptions.CLUSTER_ID));
    }
}
