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
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void testTakesOverTheConfigMapOfAPreviousOwner() {
        var leftBehind = createConfigMapSkeleton();
        leftBehind.getMetadata().setOwnerReferences(List.of(ownerReference("uid-1")));
        leftBehind.setData(Map.of("previous", "state"));
        kubernetesClient.resource(leftBehind).create();

        var skeleton = createConfigMapSkeleton();
        skeleton.getMetadata().setOwnerReferences(List.of(ownerReference("uid-2")));
        var cmView = new ConfigMapView(skeleton, cm -> kubernetesClient.resource(cm));

        assertThat(cmView.get("previous")).isNull();
        assertThat(cmView.getOwnerUid()).isEqualTo("uid-2");

        cmView.flush();
        var persisted = kubernetesClient.resource(createConfigMapSkeleton()).get();
        assertThat(persisted.getMetadata().getOwnerReferences().get(0).getUid()).isEqualTo("uid-2");
        assertThat(persisted.getData().isEmpty()).isTrue();
    }

    @Test
    void testFlushRecreatesWhenTheConfigMapVanishedBeforeTheUpdate() {
        var stored = createConfigMapSkeleton();
        stored.getMetadata().setResourceVersion("1");
        stored.getMetadata().setUid("cm-uid-1");
        stored.setData(new HashMap<>(Map.of("a", "1")));

        var sent = new AtomicReference<ConfigMap>();
        var updates = new AtomicInteger();
        var creates = new AtomicInteger();
        Function<ConfigMap, Resource<ConfigMap>> apiServer =
                cm -> {
                    sent.set(cm);
                    return mockResource(
                            () -> stored,
                            () -> {
                                updates.incrementAndGet();
                                throw new KubernetesClientException("Not Found", 404, null);
                            },
                            () -> {
                                creates.incrementAndGet();
                                var meta = sent.get().getMetadata();
                                // A real API server refuses to create an object that still carries
                                // the identity of the one it replaces. The mock server does not,
                                // so this is the only place that check is enforced.
                                if (meta.getResourceVersion() != null || meta.getUid() != null) {
                                    throw new KubernetesClientException(
                                            "resourceVersion should not be set on objects to be created",
                                            400,
                                            null);
                                }
                                return new ConfigMapBuilder(sent.get())
                                        .editMetadata()
                                        .withResourceVersion("2")
                                        .withUid("cm-uid-2")
                                        .endMetadata()
                                        .build();
                            });
                };

        var cmView = new ConfigMapView(createConfigMapSkeleton(), apiServer);
        cmView.put("b", "2");

        cmView.flush();
        assertThat(updates.get()).isEqualTo(1);
        assertThat(creates.get()).isEqualTo(1);
        assertThat(cmView.get("a")).isEqualTo("1");
        assertThat(cmView.get("b")).isEqualTo("2");
        assertThat(cmView.getConfigMap().getMetadata().getResourceVersion()).isEqualTo("2");
        assertThat(cmView.getConfigMap().getMetadata().getUid()).isEqualTo("cm-uid-2");

        // perform one additional flush to check the state remains the same
        cmView.flush();
        assertThat(updates.get()).isEqualTo(1);
        assertThat(creates.get()).isEqualTo(1);
        assertThat(cmView.get("a")).isEqualTo("1");
        assertThat(cmView.get("b")).isEqualTo("2");
        assertThat(cmView.getConfigMap().getMetadata().getResourceVersion()).isEqualTo("2");
        assertThat(cmView.getConfigMap().getMetadata().getUid()).isEqualTo("cm-uid-2");
    }

    @Test
    void testFlushDoesNotRecreateOnOtherUpdateFailures() {
        var stored = createConfigMapSkeleton();
        stored.getMetadata().setResourceVersion("1");
        stored.setData(new HashMap<>(Map.of("a", "1")));

        var creates = new AtomicInteger();
        Function<ConfigMap, Resource<ConfigMap>> apiServer =
                cm ->
                        mockResource(
                                () -> stored,
                                () -> {
                                    throw new KubernetesClientException("Conflict", 409, null);
                                },
                                () -> {
                                    creates.incrementAndGet();
                                    return cm;
                                });

        var cmView = new ConfigMapView(createConfigMapSkeleton(), apiServer);
        cmView.put("b", "2");

        var failure = assertThrows(KubernetesClientException.class, cmView::flush);
        assertThat(failure.getCode()).isEqualTo(409);
        assertThat(creates.get()).isEqualTo(0);
        // The pending write is kept for the retry on the next cycle
        assertThat(cmView.get("b")).isEqualTo("2");
    }

    @SuppressWarnings("unchecked")
    private static Resource<ConfigMap> mockResource(
            Supplier<ConfigMap> get, Supplier<ConfigMap> update, Supplier<ConfigMap> create) {
        return (Resource<ConfigMap>)
                Proxy.newProxyInstance(
                        Resource.class.getClassLoader(),
                        new Class<?>[] {Resource.class},
                        (proxy, method, args) -> {
                            if (args != null && args.length > 0) {
                                throw new UnsupportedOperationException(method.getName());
                            }
                            return switch (method.getName()) {
                                case "get" -> get.get();
                                case "update" -> update.get();
                                case "create" -> create.get();
                                case "toString" -> "mockResource";
                                case "hashCode" -> System.identityHashCode(proxy);
                                default -> throw new UnsupportedOperationException(
                                        method.getName());
                            };
                        });
    }

    private static OwnerReference ownerReference(String uid) {
        var ownerReference = new OwnerReference();
        ownerReference.setApiVersion("flink.apache.org/v1beta1");
        ownerReference.setKind("FlinkDeployment");
        ownerReference.setName("cr");
        ownerReference.setUid(uid);
        return ownerReference;
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
