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
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.kubernetes.operator.kubeclient.utils.TestUtils;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @link StandaloneKubernetesJobManagerParameters unit tests
 */
public class StandaloneKubernetesJobManagerParametersTest extends ParametersTestBase {
    private StandaloneKubernetesJobManagerParameters kubernetesJobManagerParameters;

    @BeforeEach
    public void setup() {
        setupFlinkConfig();
        ClusterSpecification clusterSpecification = TestUtils.createClusterSpecification();
        kubernetesJobManagerParameters =
                new StandaloneKubernetesJobManagerParameters(flinkConfig, clusterSpecification);
    }

    @Test
    public void testGetLabels() {
        Map<String, String> expectedLabels = new HashMap<>();
        expectedLabels.putAll(userLabels);
        expectedLabels.putAll(
                StandaloneKubernetesUtils.getJobManagerSelectors(TestUtils.CLUSTER_ID));
        assertEquals(expectedLabels, kubernetesJobManagerParameters.getLabels());
    }

    @Test
    public void testGetSelectors() {
        assertEquals(
                StandaloneKubernetesUtils.getJobManagerSelectors(TestUtils.CLUSTER_ID),
                kubernetesJobManagerParameters.getSelectors());
    }

    @Test
    public void testGetCommonLabels() {
        assertEquals(
                StandaloneKubernetesUtils.getCommonLabels(TestUtils.CLUSTER_ID),
                kubernetesJobManagerParameters.getCommonLabels());
    }

    @Test
    public void testIsInternalServiceEnabled() {
        assertTrue(kubernetesJobManagerParameters.isInternalServiceEnabled());
    }

    @Test
    public void testIsApplicationCluster() {
        flinkConfig.set(
                StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE,
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION);
        assertTrue(kubernetesJobManagerParameters.isApplicationCluster());

        flinkConfig.set(
                StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE,
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.SESSION);
        assertFalse(kubernetesJobManagerParameters.isApplicationCluster());
    }

    @Test
    public void testGetMainClass() {
        String entryClass = "my.main.test.class";
        flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, entryClass);

        flinkConfig.set(
                StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE,
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.SESSION);
        assertNull(kubernetesJobManagerParameters.getMainClass());

        flinkConfig.set(
                StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE,
                StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION);
        assertEquals(entryClass, kubernetesJobManagerParameters.getMainClass());
    }
}
