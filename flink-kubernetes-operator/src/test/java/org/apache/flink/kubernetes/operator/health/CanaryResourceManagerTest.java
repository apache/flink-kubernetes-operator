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

package org.apache.flink.kubernetes.operator.health;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;

import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CanaryResourceManager}. */
@EnableKubernetesMockClient(crud = true)
public class CanaryResourceManagerTest {

    KubernetesClient client;
    KubernetesMockServer mockServer;

    private CanaryResourceManager<FlinkDeployment> canaryManager;
    private ResourceID canaryId;

    @BeforeAll
    public static void setAutoTryKubeConfig() {
        System.setProperty(Config.KUBERNETES_AUTH_TRYKUBECONFIG_SYSTEM_PROPERTY, "false");
    }

    @BeforeEach
    public void setup() {
        canaryManager = new CanaryResourceManager<>(new FlinkConfigManager(new Configuration()));
        var canary = TestUtils.createCanaryDeployment();
        client.resource(canary).create();
        canaryId = ResourceID.fromResource(canary);
    }

    @Test
    public void testHealthDependsOnReconcilingTheBump() {
        reconcileLatest();
        assertEquals(1, canaryManager.getNumberOfActiveCanaries());
        assertTrue(canaryManager.allCanariesHealthy());

        checkHealth(client);
        assertFalse(canaryManager.allCanariesHealthy());

        reconcileLatest();
        checkHealth(client);
        assertTrue(canaryManager.allCanariesHealthy());
    }

    @Test
    public void testBumpPreservesConcurrentMetadataChanges() {
        reconcileLatest();
        var nonce = latest().getSpec().getRestartNonce();

        client.resource(latest())
                .edit(
                        cr -> {
                            var labels = new HashMap<>(cr.getMetadata().getLabels());
                            labels.put("test", "label");
                            cr.getMetadata().setLabels(labels);
                            return cr;
                        });

        checkHealth(client);

        var canary = latest();
        assertEquals("label", canary.getMetadata().getLabels().get("test"));
        assertEquals(nonce + 1, canary.getSpec().getRestartNonce());
    }

    @Test
    public void testCanaryWithoutSpec() throws InterruptedException {
        var context =
                new ResourceDefinitionContext.Builder()
                        .withGroup("flink.apache.org")
                        .withVersion("v1beta1")
                        .withKind("FlinkDeployment")
                        .withPlural("flinkdeployments")
                        .withNamespaced(true)
                        .build();
        client.genericKubernetesResources(context)
                .inNamespace("default")
                .resource(
                        new GenericKubernetesResourceBuilder()
                                .withApiVersion("flink.apache.org/v1beta1")
                                .withKind("FlinkDeployment")
                                .withNewMetadata()
                                .withName("no-spec")
                                .withNamespace("default")
                                .withLabels(Map.of(CanaryResourceManager.CANARY_LABEL, "true"))
                                .endMetadata()
                                .build())
                .create();
        var noSpec =
                client.resources(FlinkDeployment.class).inNamespace("default").withName("no-spec");

        assertTrue(canaryManager.handleCanaryResourceReconciliation(noSpec.get(), client));

        var lastRequest = mockServer.getLastRequest();
        assertEquals("PUT", lastRequest.getMethod());
        assertEquals(1L, noSpec.get().getSpec().getRestartNonce());
    }

    @Test
    public void testFailedBumpsReportUnhealthyAfterMaxAttempts() {
        reconcileLatest();
        reconcileLatest();
        try (var unreachableClient = unreachableClient()) {
            checkHealth(unreachableClient);
            assertTrue(canaryManager.allCanariesHealthy());

            for (int i = 1; i < CanaryResourceManager.MAX_FAILED_UPDATES; i++) {
                checkHealth(unreachableClient);
                assertTrue(canaryManager.allCanariesHealthy());
            }

            checkHealth(unreachableClient);
            assertFalse(canaryManager.allCanariesHealthy());
            assertEquals(1, canaryManager.getNumberOfActiveCanaries());
        }

        checkHealth(client);
        reconcileLatest();
        checkHealth(client);
        assertTrue(canaryManager.allCanariesHealthy());
    }

    @Test
    public void testLateReconcileRecoversWhileBumpsFail() {
        reconcileLatest();
        try (var unreachableClient = unreachableClient()) {
            checkHealth(unreachableClient);
            assertFalse(canaryManager.allCanariesHealthy());

            reconcileLatest();
            checkHealth(unreachableClient);
            assertTrue(canaryManager.allCanariesHealthy());
        }
    }

    @Test
    public void testFailedFirstBump() {
        try (var unreachableClient = unreachableClient()) {
            assertTrue(
                    canaryManager.handleCanaryResourceReconciliation(latest(), unreachableClient));
            checkHealth(unreachableClient);
            assertTrue(canaryManager.allCanariesHealthy());
        }
    }

    @Test
    public void testDeletedWithoutCleanupStopsHealthChecks() {
        reconcileLatest();

        client.resource(latest()).delete();

        checkHealth(client);
        assertEquals(0, canaryManager.getNumberOfActiveCanaries());
        assertTrue(canaryManager.allCanariesHealthy());
    }

    @Test
    public void testCanaryLabelRemoved() {
        reconcileLatest();
        var nonce = latest().getSpec().getRestartNonce();

        client.resource(latest())
                .edit(
                        cr -> {
                            cr.getMetadata().getLabels().remove(CanaryResourceManager.CANARY_LABEL);
                            return cr;
                        });

        checkHealth(client);
        assertEquals(0, canaryManager.getNumberOfActiveCanaries());
        assertEquals(nonce, latest().getSpec().getRestartNonce());
    }

    @Test
    public void testNamespaceNoLongerWatched() {
        var otherCanary = TestUtils.createCanaryDeployment();
        otherCanary.getMetadata().setNamespace("other");
        client.resource(otherCanary).create();
        reconcileLatest();
        assertTrue(canaryManager.handleCanaryResourceReconciliation(otherCanary, client));
        assertEquals(2, canaryManager.getNumberOfActiveCanaries());

        canaryManager.retainNamespaces(Set.of(Constants.WATCH_ALL_NAMESPACES));
        assertEquals(2, canaryManager.getNumberOfActiveCanaries());

        var nonce = latest().getSpec().getRestartNonce();
        canaryManager.retainNamespaces(Set.of("other"));
        assertEquals(1, canaryManager.getNumberOfActiveCanaries());

        checkHealth(client);
        assertEquals(1, canaryManager.getNumberOfActiveCanaries());
        assertEquals(nonce, latest().getSpec().getRestartNonce());
        assertTrue(canaryManager.allCanariesHealthy());
    }

    @Test
    public void testReconcilingFormerCanaryStopsTracking() {
        reconcileLatest();
        assertEquals(1, canaryManager.getNumberOfActiveCanaries());

        var formerCanary = latest();
        formerCanary.getMetadata().getLabels().remove(CanaryResourceManager.CANARY_LABEL);
        assertFalse(canaryManager.handleCanaryResourceReconciliation(formerCanary, client));
        assertEquals(0, canaryManager.getNumberOfActiveCanaries());
        assertEquals(0, pendingChecks());
    }

    @Test
    public void testRecreateKeepsOneHealthCheck() {
        reconcileLatest();
        assertEquals(1, pendingChecks());

        assertTrue(canaryManager.handleCanaryResourceDeletion(latest()));
        assertEquals(0, pendingChecks());

        client.resource(latest()).delete();
        client.resource(TestUtils.createCanaryDeployment()).create();
        reconcileLatest();
        assertEquals(1, canaryManager.getNumberOfActiveCanaries());
        assertEquals(1, pendingChecks());
    }

    @Test
    public void testRecreateWithoutCleanup() {
        reconcileLatest();
        reconcileLatest();
        var oldUid = latest().getMetadata().getUid();

        client.resource(latest()).delete();
        client.resource(TestUtils.createCanaryDeployment()).create();
        assertNotEquals(oldUid, latest().getMetadata().getUid());

        reconcileLatest();
        assertEquals(1, canaryManager.getNumberOfActiveCanaries());
        assertEquals(1, pendingChecks());

        reconcileLatest();
        checkHealth(client);
        assertTrue(canaryManager.allCanariesHealthy());
    }

    @Test
    public void testOutdatedCheckHasNoEffect() {
        reconcileLatest();
        var oldState = canaryManager.canaryResources.get(canaryId);

        client.resource(latest()).delete();
        client.resource(TestUtils.createCanaryDeployment()).create();
        reconcileLatest();
        var nonce = latest().getSpec().getRestartNonce();

        canaryManager.checkHealth(canaryId, oldState, client);
        assertEquals(nonce, latest().getSpec().getRestartNonce());
        assertEquals(1, pendingChecks());
        assertTrue(canaryManager.allCanariesHealthy());
    }

    @Test
    public void testNamespaceWatchedAgain() {
        reconcileLatest();
        canaryManager.retainNamespaces(Set.of("other"));
        assertEquals(0, pendingChecks());

        reconcileLatest();
        assertEquals(1, canaryManager.getNumberOfActiveCanaries());
        assertEquals(1, pendingChecks());
    }

    @Test
    public void testDeletion() {
        reconcileLatest();
        assertTrue(canaryManager.handleCanaryResourceDeletion(latest()));
        assertEquals(0, canaryManager.getNumberOfActiveCanaries());
        assertEquals(0, pendingChecks());

        checkHealth(client);
        assertEquals(0, canaryManager.getNumberOfActiveCanaries());
    }

    private void reconcileLatest() {
        assertTrue(canaryManager.handleCanaryResourceReconciliation(latest(), client));
    }

    private void checkHealth(KubernetesClient client) {
        canaryManager.checkHealth(canaryId, canaryManager.canaryResources.get(canaryId), client);
    }

    private long pendingChecks() {
        return ((ScheduledThreadPoolExecutor) canaryManager.executorService)
                .getQueue().stream().filter(check -> !((Future<?>) check).isCancelled()).count();
    }

    private static KubernetesClient unreachableClient() {
        return new KubernetesClientBuilder()
                .withConfig(
                        new ConfigBuilder()
                                .withMasterUrl("http://localhost:1")
                                .withRequestRetryBackoffLimit(0)
                                .build())
                .build();
    }

    private FlinkDeployment latest() {
        return client.resources(FlinkDeployment.class)
                .inNamespace(canaryId.getNamespace().orElseThrow())
                .withName(canaryId.getName())
                .get();
    }
}
