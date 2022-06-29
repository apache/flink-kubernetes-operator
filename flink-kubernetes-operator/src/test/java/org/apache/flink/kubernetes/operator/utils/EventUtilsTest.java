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

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link EventUtils}. */
@EnableKubernetesMockClient(crud = true)
public class EventUtilsTest {

    private KubernetesMockServer mockServer;
    private KubernetesClient kubernetesClient;

    @Test
    public void testCreateOrReplaceEvent() {
        var flinkApp = TestUtils.buildApplicationCluster();
        var reason = "Cleanup";
        var message = "message";
        var eventName =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator);
        Assertions.assertTrue(
                EventUtils.createOrUpdateEvent(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        e -> {}));
        var event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();
        Assertions.assertNotNull(event);
        Assertions.assertEquals(1, event.getCount());
        Assertions.assertEquals(reason, event.getReason());

        Assertions.assertFalse(
                EventUtils.createOrUpdateEvent(
                        kubernetesClient,
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator,
                        e -> {}));
        event =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(flinkApp.getMetadata().getNamespace())
                        .withName(eventName)
                        .get();

        Assertions.assertEquals(2, event.getCount());
    }

    @Test
    public void testSameResourceNameWithDifferentUidNotShareEvents() {
        var flinkApp = TestUtils.buildApplicationCluster();
        flinkApp.getMetadata().setUid("uid1");
        var reason = "Cleanup";
        var message = "message";
        var name1 =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator);
        flinkApp.getMetadata().setUid("uid2");
        var name2 =
                EventUtils.generateEventName(
                        flinkApp,
                        EventRecorder.Type.Warning,
                        reason,
                        message,
                        EventRecorder.Component.Operator);
        Assertions.assertNotEquals(name1, name2);
    }
}
