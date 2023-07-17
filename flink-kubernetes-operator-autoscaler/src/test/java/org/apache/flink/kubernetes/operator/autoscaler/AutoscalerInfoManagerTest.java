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

import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link AutoscalerInfoManager}. */
@EnableKubernetesMockClient(crud = true)
public class AutoscalerInfoManagerTest {

    KubernetesClient kubernetesClient;
    KubernetesMockServer mockWebServer;

    @Test
    void testCaching() throws Exception {
        var cr1 = TestUtils.buildApplicationCluster();
        cr1.getMetadata().setName("cr1");
        var cr2 = TestUtils.buildApplicationCluster();
        cr2.getMetadata().setName("cr2");

        var manager = new AutoscalerInfoManager(kubernetesClient);
        assertEquals(0, mockWebServer.getRequestCount());

        assertTrue(manager.getInfo(cr1).isEmpty());
        assertEquals(1, mockWebServer.getRequestCount());

        // Further gets should not go to K8s
        assertTrue(manager.getInfo(cr1).isEmpty());
        assertTrue(manager.getInfo(cr1).isEmpty());
        assertEquals(1, mockWebServer.getRequestCount());

        assertTrue(manager.getInfoFromKubernetes(cr1).isEmpty());
        assertEquals(2, mockWebServer.getRequestCount());

        var info1 = manager.getOrCreateInfo(cr1);
        assertEquals(4, mockWebServer.getRequestCount());

        assertTrue(info1 == manager.getOrCreateInfo(cr1));
        assertEquals(4, mockWebServer.getRequestCount());

        assertFalse(manager.getInfoFromKubernetes(cr1).isEmpty());
        assertTrue(manager.getInfoFromKubernetes(cr2).isEmpty());

        assertFalse(manager.getInfo(cr1).isEmpty());
        assertTrue(manager.getInfo(cr2).isEmpty());

        var info2 = manager.getOrCreateInfo(cr2);
        info1.setCurrentOverrides(Map.of("a", "1"));
        info1.replaceInKubernetes(kubernetesClient);
        info2.setCurrentOverrides(Map.of("b", "1"));
        info2.replaceInKubernetes(kubernetesClient);

        manager = new AutoscalerInfoManager(kubernetesClient);
        assertEquals(Map.of("a", "1"), manager.getInfo(cr1).get().getCurrentOverrides());
        assertEquals(Map.of("b", "1"), manager.getOrCreateInfo(cr2).getCurrentOverrides());

        // Removing from the cache should not affect the results
        manager.removeInfoFromCache(cr1);
        assertEquals(Map.of("a", "1"), manager.getInfo(cr1).get().getCurrentOverrides());
        assertEquals(Map.of("b", "1"), manager.getOrCreateInfo(cr2).getCurrentOverrides());
    }

    @Test
    void testErrorHandling() {
        var cr1 = TestUtils.buildApplicationCluster();
        cr1.getMetadata().setName("cr1");

        var manager = new AutoscalerInfoManager(kubernetesClient);
        assertEquals(0, mockWebServer.getRequestCount());

        var info1 = manager.getOrCreateInfo(cr1);
        assertTrue(info1.isValid());
        assertEquals(2, mockWebServer.getRequestCount());

        // Modify the autoscaler info in the background
        var cm = ReconciliationUtils.clone(info1.getConfigMap());
        cm.getData().put(AutoScalerInfo.PARALLELISM_OVERRIDES_KEY, "a:3");
        kubernetesClient.resource(cm).update();

        info1.setCurrentOverrides(Map.of("a", "1"));

        // Replace should throw an error due to the modification
        assertThrows(
                KubernetesClientException.class, () -> info1.replaceInKubernetes(kubernetesClient));
        assertFalse(info1.isValid());

        // Make sure we can get the new version
        assertEquals(Map.of("a", "3"), manager.getInfo(cr1).get().getCurrentOverrides());
        assertEquals(Map.of("a", "3"), manager.getOrCreateInfo(cr1).getCurrentOverrides());
    }
}
