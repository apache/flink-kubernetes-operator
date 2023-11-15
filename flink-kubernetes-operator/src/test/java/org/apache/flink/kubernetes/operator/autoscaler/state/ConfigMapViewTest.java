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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for ConfigMapView. */
@EnableKubernetesMockClient(crud = true)
public class ConfigMapViewTest {

    KubernetesClient kubernetesClient;

    KubernetesMockServer mockWebServer;

    @Test
    void testAllOperations() {
        var cmView =
                new ConfigMapView(createConfigMapSkeleton(), cm -> kubernetesClient.resource(cm));
        assertThat(mockWebServer.getRequestCount()).isEqualTo(1);

        assertThat(cmView.get("test")).isNull();
        cmView.put("test", "value");
        assertThat(cmView.get("test")).isEqualTo("value");

        cmView.clear();
        assertThat(cmView.get("test")).isNull();

        // Add one new key before flushing
        cmView.put("test2", "value2");
        assertThat(cmView.get("test2")).isEqualTo("value2");

        // Not flushed yet
        assertThat(keyExistsInKubernetes("test")).isFalse();
        assertThat(keyExistsInKubernetes("test2")).isFalse();

        var requestCount = mockWebServer.getRequestCount();
        cmView.flush();
        assertThat(mockWebServer.getRequestCount()).isEqualTo(requestCount + 1);

        assertThat(keyExistsInKubernetes("test")).isFalse();
        assertThat(keyExistsInKubernetes("test2")).isTrue();

        cmView.removeKey("test2");
        assertThat(cmView.get("test2")).isNull();

        cmView.flush();
        assertThat(keyExistsInKubernetes("test")).isFalse();
        assertThat(keyExistsInKubernetes("test2")).isFalse();
    }

    @Test
    void testAvoidUnnecessaryFlushes() {
        var cmView =
                new ConfigMapView(createConfigMapSkeleton(), cm -> kubernetesClient.resource(cm));
        assertThat(mockWebServer.getRequestCount()).isEqualTo(1);
        // Flush will create ConfigMap
        cmView.flush();
        // Subsequent flushes do nothing
        cmView.flush();
        assertThat(mockWebServer.getRequestCount()).isEqualTo(2);

        cmView.removeKey("test");
        cmView.clear();
        cmView.flush();
        assertThat(mockWebServer.getRequestCount()).isEqualTo(2);
    }

    private boolean keyExistsInKubernetes(String key) {
        int requestCount = mockWebServer.getRequestCount();
        var cmView2 =
                new ConfigMapView(createConfigMapSkeleton(), cm -> kubernetesClient.resource(cm));
        assertThat(mockWebServer.getRequestCount()).isEqualTo(requestCount + 1);
        return cmView2.get(key) != null;
    }

    private static ConfigMap createConfigMapSkeleton() {
        var configMapSkeleton = new ConfigMap();
        var metadata = new ObjectMeta();
        metadata.setName("configMapName");
        configMapSkeleton.setMetadata(metadata);
        return configMapSkeleton;
    }
}
