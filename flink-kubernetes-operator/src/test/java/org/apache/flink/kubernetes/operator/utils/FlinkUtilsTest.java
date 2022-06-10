/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FlinkUtilsTest. */
@EnableKubernetesMockClient(crud = true)
public class FlinkUtilsTest {

    KubernetesClient kubernetesClient;
    KubernetesMockServer mockServer;

    @Test
    public void testMergePods() throws Exception {

        Container container1 = new Container();
        container1.setName("container1");
        Container container2 = new Container();
        container2.setName("container2");

        Pod pod1 =
                TestUtils.getTestPod(
                        "pod1 hostname", "pod1 api version", Arrays.asList(container2));

        Pod pod2 =
                TestUtils.getTestPod(
                        "pod2 hostname", "pod2 api version", Arrays.asList(container1, container2));

        Pod mergedPod = FlinkUtils.mergePodTemplates(pod1, pod2);

        assertEquals(pod2.getApiVersion(), mergedPod.getApiVersion());
        assertEquals(pod2.getSpec().getContainers(), mergedPod.getSpec().getContainers());
    }

    @Test
    public void testDeleteJobGraphInKubernetesHA() {
        final String name = "ha-configmap";
        final String clusterId = "cluster-id";
        final Map<String, String> data = new HashMap<>();
        data.put(Constants.JOB_GRAPH_STORE_KEY_PREFIX + JobID.generate(), "job-graph-data");
        data.put("leader", "localhost");
        createHAConfigMapWithData(name, clusterId, data);
        assertNotNull(kubernetesClient.configMaps().withName(name).get());
        assertEquals(2, kubernetesClient.configMaps().withName(name).get().getData().size());

        FlinkUtils.deleteJobGraphInKubernetesHA(
                clusterId, kubernetesClient.getNamespace(), kubernetesClient);

        assertEquals(1, kubernetesClient.configMaps().withName(name).get().getData().size());
        assertTrue(
                kubernetesClient.configMaps().withName(name).get().getData().containsKey("leader"));
    }

    @Test
    public void testDeleteJobGraphInKubernetesHAShouldNotUpdateWithEmptyConfigMap() {
        final String name = "empty-ha-configmap";
        final String clusterId = "cluster-id-2";
        mockServer
                .expect()
                .put()
                .withPath("/api/v1/namespaces/test/configmaps/" + name)
                .andReturn(HttpURLConnection.HTTP_INTERNAL_ERROR, new ConfigMapBuilder().build())
                .once();
        createHAConfigMapWithData(name, clusterId, null);
        assertNull(kubernetesClient.configMaps().withName(name).get().getData());
        FlinkUtils.deleteJobGraphInKubernetesHA(
                clusterId, kubernetesClient.getNamespace(), kubernetesClient);
    }

    @Test
    public void testComputeNumTms() {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_PARALLELISM, 2);
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);

        assertEquals(2, FlinkUtils.getNumTaskManagers(conf));

        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        assertEquals(1, FlinkUtils.getNumTaskManagers(conf));

        conf.set(CoreOptions.DEFAULT_PARALLELISM, 7);
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
        assertEquals(4, FlinkUtils.getNumTaskManagers(conf));
    }

    private void createHAConfigMapWithData(
            String configMapName, String clusterId, Map<String, String> data) {
        final ConfigMap kubernetesConfigMap =
                new ConfigMapBuilder()
                        .withNewMetadata()
                        .withName(configMapName)
                        .withLabels(
                                KubernetesUtils.getConfigMapLabels(
                                        clusterId,
                                        Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                        .endMetadata()
                        .withData(data)
                        .build();

        kubernetesClient.configMaps().create(kubernetesConfigMap);
    }
}
