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

package org.apache.flink.kubernetes.operator.informer;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.javaoperatorsdk.operator.api.reconciler.Constants.DEFAULT_NAMESPACES_SET;

/** Test for {@link InformerManager}. */
@EnableKubernetesMockClient(crud = true)
public class InformerManagerTest {

    private KubernetesMockServer mockServer;
    private KubernetesClient kubernetesClient;

    @Test
    public void testNamespacedInformerCreated() {
        var informerManager = new InformerManager(DEFAULT_NAMESPACES_SET, kubernetesClient);
        Assertions.assertNotNull(informerManager.getFlinkDepInformer("ns1"));

        informerManager = new InformerManager(Set.of("ns1", "ns2"), kubernetesClient);
        Assertions.assertNotNull(informerManager.getFlinkDepInformer("ns1"));
        Assertions.assertNotNull(informerManager.getFlinkDepInformer("ns2"));

        informerManager.changNameSpaces(Set.of("ns1", "ns2", "ns3"));
        Assertions.assertNotNull(informerManager.getFlinkDepInformer("ns1"));
        Assertions.assertNotNull(informerManager.getFlinkDepInformer("ns2"));
        Assertions.assertNotNull(informerManager.getFlinkDepInformer("ns3"));
    }
}
