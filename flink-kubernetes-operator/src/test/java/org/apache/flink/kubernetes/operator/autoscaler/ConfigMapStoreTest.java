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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.apache.flink.kubernetes.operator.autoscaler.TestingKubernetesAutoscalerUtils.createContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link ConfigMapStore}. */
@EnableKubernetesMockClient(crud = true)
public class ConfigMapStoreTest {

    KubernetesClient kubernetesClient;

    KubernetesMockServer mockWebServer;

    @Test
    void testCaching() {
        KubernetesJobAutoScalerContext ctx1 = createContext("cr1", kubernetesClient);
        KubernetesJobAutoScalerContext ctx2 = createContext("cr2", kubernetesClient);

        var configMapStore = new ConfigMapStore(kubernetesClient);
        assertEquals(0, mockWebServer.getRequestCount());

        String key1 = "key1";
        String value1 = "value1";
        String key2 = "key2";
        String value2 = "value2";
        String key3 = "key3";
        String value3 = "value3";
        assertThat(configMapStore.getSerializedState(ctx1, key1)).isEmpty();
        assertEquals(1, mockWebServer.getRequestCount());

        // Further gets should not go to K8s
        assertThat(configMapStore.getSerializedState(ctx1, key2)).isEmpty();
        assertThat(configMapStore.getSerializedState(ctx1, key3)).isEmpty();
        assertEquals(1, mockWebServer.getRequestCount());

        assertThat(configMapStore.getConfigMapFromKubernetes(ctx1)).isEmpty();
        assertEquals(2, mockWebServer.getRequestCount());

        configMapStore.putSerializedState(ctx1, key1, value1);
        assertEquals(4, mockWebServer.getRequestCount());

        // The put just update the data to cache, and shouldn't request kubernetes.
        configMapStore.putSerializedState(ctx1, key2, value2);
        assertEquals(4, mockWebServer.getRequestCount());

        assertThat(configMapStore.getConfigMapFromKubernetes(ctx1)).isPresent();
        assertThat(configMapStore.getConfigMapFromKubernetes(ctx2)).isEmpty();

        assertThat(configMapStore.getSerializedState(ctx1, key1)).isPresent();
        assertThat(configMapStore.getSerializedState(ctx2, key1)).isEmpty();

        configMapStore.flush(ctx1);
        configMapStore.putSerializedState(ctx2, key3, value3);
        configMapStore.flush(ctx2);

        configMapStore = new ConfigMapStore(kubernetesClient);
        assertThat(configMapStore.getSerializedState(ctx1, key1)).contains(value1);
        assertThat(configMapStore.getSerializedState(ctx2, key3)).contains(value3);

        // Removing from the cache should not affect the results
        configMapStore.removeInfoFromCache(ctx1.getJobKey());
        assertThat(configMapStore.getSerializedState(ctx1, key1)).contains(value1);
        assertThat(configMapStore.getSerializedState(ctx2, key3)).contains(value3);
    }

    @Test
    void testErrorHandling() {
        KubernetesJobAutoScalerContext ctx = createContext("cr1", kubernetesClient);

        var configMapStore = new ConfigMapStore(kubernetesClient);
        // Test for the invalid flush.
        configMapStore.flush(ctx);

        assertEquals(0, mockWebServer.getRequestCount());

        configMapStore.putSerializedState(ctx, "a", "1");
        Optional<ConfigMap> configMapOpt = configMapStore.getCache().get(ctx.getJobKey());
        assertThat(configMapOpt).isPresent();
        assertEquals(2, mockWebServer.getRequestCount());

        // Modify the autoscaler info in the background
        var cm = ReconciliationUtils.clone(configMapOpt.get());
        cm.getData().put("a", "2");
        kubernetesClient.resource(cm).update();

        // Replace should throw an error due to the modification
        assertThrows(KubernetesClientException.class, () -> configMapStore.flush(ctx));
        assertThat(configMapStore.getCache()).isEmpty();

        // Make sure we can get the new version
        assertThat(configMapStore.getSerializedState(ctx, "a")).contains("2");
    }
}
