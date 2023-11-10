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

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @link StandaloneKubernetesTaskManagerParameters unit tests
 */
public class StandaloneKubernetesTaskManagerParametersTest extends ParametersTestBase {

    private StandaloneKubernetesTaskManagerParameters kubernetesTaskManagerParameters;

    @BeforeEach
    public void setup() {
        setupFlinkConfig();
        kubernetesTaskManagerParameters =
                new StandaloneKubernetesTaskManagerParameters(flinkConfig, clusterSpecification);
    }

    @Test
    public void testGetLabels() {
        Map<String, String> expectedLabels = new HashMap<>();
        expectedLabels.putAll(userLabels);
        expectedLabels.putAll(
                StandaloneKubernetesUtils.getTaskManagerSelectors(TestUtils.CLUSTER_ID));
        assertEquals(expectedLabels, kubernetesTaskManagerParameters.getLabels());
    }

    @Test
    public void testGetSelectors() {
        Map<String, String> expectedSelectors =
                StandaloneKubernetesUtils.getTaskManagerSelectors(TestUtils.CLUSTER_ID);

        assertEquals(expectedSelectors, kubernetesTaskManagerParameters.getSelectors());
    }

    @Test
    public void testGetNodeSelector() {
        assertEquals(userNodeSelectors, kubernetesTaskManagerParameters.getNodeSelector());
    }

    @Test
    public void testGetEnvironments() {
        assertTrue(kubernetesTaskManagerParameters.getEnvironments().isEmpty());
    }

    @Test
    public void testGetAnnotations() {
        assertEquals(userAnnotations, kubernetesTaskManagerParameters.getAnnotations());
    }

    @Test
    public void testGetReplicas() {
        int tmReplicas = 11;
        flinkConfig.set(
                StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS,
                tmReplicas);
        assertEquals(tmReplicas, kubernetesTaskManagerParameters.getReplicas());
    }

    @Test
    public void testInvalidReplicas() {
        flinkConfig.set(
                StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS, -1);
        kubernetesTaskManagerParameters =
                new StandaloneKubernetesTaskManagerParameters(flinkConfig, clusterSpecification);

        assertThrows(
                IllegalArgumentException.class,
                () -> kubernetesTaskManagerParameters.getReplicas());
    }

    @Test
    public void testGetServiceAccount() {
        assertEquals(
                TestUtils.SERVICE_ACCOUNT, kubernetesTaskManagerParameters.getServiceAccount());
    }

    @Test
    public void testGetTaskManagerMemoryMB() {
        assertEquals(
                TestUtils.TASK_MANAGER_MEMORY_MB,
                kubernetesTaskManagerParameters.getTaskManagerMemoryMB());
    }

    @Test
    public void testGetTaskManagerCPU() {
        assertEquals(
                TestUtils.TASK_MANAGER_CPU,
                kubernetesTaskManagerParameters.getTaskManagerCPU(),
                0.00001);
    }

    @Test
    public void testGetGetRPCPort() {
        assertEquals(Constants.TASK_MANAGER_RPC_PORT, kubernetesTaskManagerParameters.getRPCPort());
    }

    @Test
    public void testInvalidRPCPort() {
        flinkConfig.set(TaskManagerOptions.RPC_PORT, String.valueOf(0));
        assertThrows(
                IllegalArgumentException.class, () -> kubernetesTaskManagerParameters.getRPCPort());
    }

    @Test
    public void testGetPodTemplateFilePath() {
        String templateFilePath = "/tmp/tst.yml";
        flinkConfig.setString(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE, templateFilePath);
        Optional<File> templateFile = kubernetesTaskManagerParameters.getPodTemplateFilePath();
        assertTrue(templateFile.isPresent());
        String filePath = templateFile.get().getAbsolutePath();
        assertEquals(templateFilePath, filePath);
    }

    @Test
    public void testGetNoPodTemplateFilePath() {
        flinkConfig.removeConfig(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE);
        Optional<File> templateFile = kubernetesTaskManagerParameters.getPodTemplateFilePath();
        assertFalse(templateFile.isPresent());
    }
}
