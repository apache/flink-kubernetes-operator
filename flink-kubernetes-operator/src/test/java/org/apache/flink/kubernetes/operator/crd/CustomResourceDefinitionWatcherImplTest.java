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

package org.apache.flink.kubernetes.operator.crd;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeploymentList;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CustomResourceDefinitionWatcherImpl}. */
@EnableKubernetesMockClient
public class CustomResourceDefinitionWatcherImplTest {
    private KubernetesClient client;
    private KubernetesMockServer server;

    @Test
    public void testIsCrdInstalled() {
        var crdWatcher = new CustomResourceDefinitionWatcherImpl(client);
        server.expect()
                .get()
                .withPath("/apis/flink.apache.org/v1beta1/namespaces/test/flinkstatesnapshots")
                .andReturn(HTTP_BAD_REQUEST, null)
                .once();
        server.expect()
                .get()
                .withPath("/apis/flink.apache.org/v1beta1/namespaces/test/flinkdeployments")
                .andReturn(HTTP_OK, new FlinkDeploymentList())
                .once();

        assertFalse(crdWatcher.isCrdInstalled(FlinkStateSnapshot.class));
        assertTrue(crdWatcher.isCrdInstalled(FlinkDeployment.class));
    }
}
