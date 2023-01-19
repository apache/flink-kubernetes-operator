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

import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** @link StandaloneFlinkService unit tests */
@EnableKubernetesMockClient(crud = true)
public class KubernetesClientUtilsTest {
    KubernetesMockServer mockServer;
    KubernetesClient kubernetesClient;

    @BeforeEach
    public void setup() {
        kubernetesClient = mockServer.createClient().inAnyNamespace();
    }

    @Test
    public void testSubmitNewSpec() {
        var firstVersion = TestUtils.buildApplicationCluster(FlinkVersion.v1_14);
        var secondVersion = TestUtils.buildApplicationCluster(FlinkVersion.v1_15);

        kubernetesClient.resource(firstVersion).create();
        firstVersion = kubernetesClient.resource(firstVersion).fromServer().get();

        // simulate external upgrade
        kubernetesClient.resource(secondVersion).replace();
        secondVersion = kubernetesClient.resource(secondVersion).fromServer().get();

        KubernetesClientUtils.applyToStoredCr(
                kubernetesClient,
                firstVersion,
                cr -> cr.getSpec().setFlinkVersion(FlinkVersion.v1_16));

        // Make sure the spec change wasn't applied
        assertEquals(
                FlinkVersion.v1_15,
                kubernetesClient
                        .resource(secondVersion)
                        .fromServer()
                        .get()
                        .getSpec()
                        .getFlinkVersion());

        // Apply with correct version (generation)
        KubernetesClientUtils.applyToStoredCr(
                kubernetesClient,
                secondVersion,
                cr -> cr.getSpec().setFlinkVersion(FlinkVersion.v1_16));
        assertEquals(
                FlinkVersion.v1_16,
                kubernetesClient
                        .resource(secondVersion)
                        .fromServer()
                        .get()
                        .getSpec()
                        .getFlinkVersion());
    }
}
