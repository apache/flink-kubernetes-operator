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

package org.apache.flink.kubernetes.operator.autoscaler.state;

import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.mock.Whitebox;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.kubernetes.operator.autoscaler.TestingKubernetesAutoscalerUtils.createContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link ConfigMapStore}. */
@EnableKubernetesMockClient(crud = true)
public class ConfigMapStoreTest {

    KubernetesClient kubernetesClient;

    KubernetesMockServer mockWebServer;

    ConfigMapStore configMapStore;

    KubernetesJobAutoScalerContext ctx;

    @BeforeEach
    public void setup() {
        configMapStore = new ConfigMapStore(kubernetesClient);
        ctx = createContext("cr1", kubernetesClient);
    }

    @Test
    void testCaching() {
        KubernetesJobAutoScalerContext ctx1 = createContext("cr1", kubernetesClient);
        KubernetesJobAutoScalerContext ctx2 = createContext("cr2", kubernetesClient);

        assertEquals(0, mockWebServer.getRequestCount());

        String key1 = "key1";
        String value1 = "value1";
        String key2 = "key2";
        String value2 = "value2";
        String key3 = "key3";
        String value3 = "value3";
        assertThat(configMapStore.getSerializedState(ctx1, key1)).isEmpty();
        // Retrieve configMap
        assertEquals(1, mockWebServer.getRequestCount());

        // Further gets should not go to K8s
        assertThat(configMapStore.getSerializedState(ctx1, key2)).isEmpty();
        assertThat(configMapStore.getSerializedState(ctx1, key3)).isEmpty();
        assertEquals(1, mockWebServer.getRequestCount());

        // Manually trigger retrieval from Kubernetes
        assertThat(configMapStore.getConfigMapFromKubernetes(ctx1).getDataReadOnly()).isEmpty();
        assertEquals(2, mockWebServer.getRequestCount());

        // Putting does not go to Kubernetes, unless flushing.
        configMapStore.putSerializedState(ctx1, key1, value1);
        assertEquals(2, mockWebServer.getRequestCount());

        // The put just update the data to cache, and shouldn't request kubernetes.
        configMapStore.putSerializedState(ctx1, key2, value2);
        assertEquals(2, mockWebServer.getRequestCount());

        // Flush!
        configMapStore.flush(ctx1);
        assertEquals(3, mockWebServer.getRequestCount());

        assertThat(configMapStore.getConfigMapFromKubernetes(ctx1).getDataReadOnly()).isNotEmpty();
        assertThat(configMapStore.getConfigMapFromKubernetes(ctx2).getDataReadOnly()).isEmpty();

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

        // Test for the invalid flush.
        configMapStore.flush(ctx);

        assertEquals(0, mockWebServer.getRequestCount());

        configMapStore.putSerializedState(ctx, "a", "1");
        ConfigMap configMap =
                (ConfigMap)
                        Whitebox.getInternalState(
                                configMapStore.getCache().get(ctx.getJobKey()), "configMap");
        assertThat(configMap.getData()).isNotEmpty();
        assertEquals(1, mockWebServer.getRequestCount());

        // Modify the autoscaler info in the background
        var cm = ReconciliationUtils.clone(configMap);
        cm.getData().put("a", "2");
        kubernetesClient.resource(cm).create();

        // Replace should throw an error due to the modification
        assertThrows(KubernetesClientException.class, () -> configMapStore.flush(ctx));
        assertThat(configMapStore.getCache()).isEmpty();

        // Make sure we can get the new version
        assertThat(configMapStore.getSerializedState(ctx, "a")).contains("2");
    }

    @Test
    void testMinimalAmountOfFlushing() {
        KubernetesJobAutoScalerContext ctx = createContext("cr1", kubernetesClient);
        var key = "key";
        var value = "value";

        configMapStore.getSerializedState(ctx, key);
        assertEquals(1, mockWebServer.getRequestCount());

        configMapStore.putSerializedState(ctx, key, value);
        assertEquals(1, mockWebServer.getRequestCount());

        configMapStore.flush(ctx);
        assertEquals(2, mockWebServer.getRequestCount());

        // Get from cache
        assertThat(configMapStore.getSerializedState(ctx, key)).hasValue(value);
        assertEquals(2, mockWebServer.getRequestCount());

        configMapStore.removeSerializedState(ctx, key);
        assertEquals(2, mockWebServer.getRequestCount());

        configMapStore.flush(ctx);
        assertEquals(3, mockWebServer.getRequestCount());

        // Subsequent flushes do not trigger an API call
        configMapStore.flush(ctx);
        assertEquals(3, mockWebServer.getRequestCount());
    }

    @Test
    public void testDiscardAllState() {
        configMapStore.putSerializedState(
                ctx, KubernetesAutoScalerStateStore.COLLECTED_METRICS_KEY, "state1");
        configMapStore.putSerializedState(
                ctx, KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY, "state2");
        configMapStore.putSerializedState(
                ctx, KubernetesAutoScalerStateStore.PARALLELISM_OVERRIDES_KEY, "state3");

        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.COLLECTED_METRICS_KEY))
                .isPresent();
        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY))
                .isPresent();
        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.PARALLELISM_OVERRIDES_KEY))
                .isPresent();

        configMapStore.flush(ctx);

        configMapStore.clearAll(ctx);

        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.COLLECTED_METRICS_KEY))
                .isEmpty();
        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.SCALING_HISTORY_KEY))
                .isEmpty();
        assertThat(
                        configMapStore.getSerializedState(
                                ctx, KubernetesAutoScalerStateStore.PARALLELISM_OVERRIDES_KEY))
                .isEmpty();

        // We haven't flushed the clear operation, ConfigMap in Kubernetes should not be empty
        assertThat(configMapStore.getConfigMapFromKubernetes(ctx).getDataReadOnly()).isNotEmpty();

        configMapStore.flush(ctx);

        assertThat(configMapStore.getConfigMapFromKubernetes(ctx).getDataReadOnly()).isEmpty();
    }
}
