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

package org.apache.flink.kubernetes.operator.controller.bluegreen;

import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.bluegreen.GateKubernetesService;
import org.apache.flink.kubernetes.operator.api.bluegreen.TransitionStage;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.ACTIVE_DEPLOYMENT_TYPE;
import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.TRANSITION_STAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Tests for {@link GateKubernetesService#compareAndSet}, the gates' only ConfigMap write. */
@EnableKubernetesMockClient(crud = true)
public class GateKubernetesServiceTest {

    private static final String NAMESPACE = "test";
    private static final String NAME = "bg-configmap";
    private static final String TOGGLE = "watermark-toggle-value";

    private KubernetesClient kubernetesClient;

    @BeforeEach
    public void createTransitionConfigMap() {
        kubernetesClient
                .configMaps()
                .inNamespace(NAMESPACE)
                .resource(
                        new ConfigMapBuilder()
                                .withNewMetadata()
                                .withName(NAME)
                                .endMetadata()
                                .addToData(ACTIVE_DEPLOYMENT_TYPE.getLabel(), "GREEN")
                                .addToData(
                                        TRANSITION_STAGE.getLabel(),
                                        TransitionStage.TRANSITIONING.toString())
                                .build())
                .create();
    }

    @Test
    public void testFirstWriteWins() {
        var gate = new GateKubernetesService(kubernetesClient, NAMESPACE, NAME);

        assertEquals(Optional.of("1000"), toggle(gate, "1000"));
        assertEquals(Optional.of("1000"), toggle(gate, "2000"));
        assertEquals("1000", data().get(TOGGLE));
    }

    @Test
    public void testWriterThatReadBeforeTheWinnerAdoptsIt() {
        ConfigMap readBeforeTheWinner =
                kubernetesClient.configMaps().inNamespace(NAMESPACE).withName(NAME).get();
        toggle(new GateKubernetesService(kubernetesClient, NAMESPACE, NAME), "1000");

        var reads = new AtomicInteger();
        var lateWriter =
                new GateKubernetesService(kubernetesClient, NAMESPACE, NAME) {
                    @Override
                    public ConfigMap parseConfigMap() {
                        return reads.getAndIncrement() == 0
                                ? readBeforeTheWinner
                                : super.parseConfigMap();
                    }
                };

        // Its update carries the old resourceVersion and is rejected, then it reads the winner
        assertEquals(Optional.of("1000"), toggle(lateWriter, "2000"));
        assertEquals(2, reads.get());
        assertEquals("1000", data().get(TOGGLE));
    }

    @Test
    public void testWritesNothingInAnotherTransitionsConfigMap() {
        var gate = new GateKubernetesService(kubernetesClient, NAMESPACE, NAME);

        assertEquals(
                Optional.empty(),
                gate.compareAndSet(
                        BlueGreenDeploymentType.BLUE, TOGGLE, null, Map.of(TOGGLE, "1000")));
        assertFalse(data().containsKey(TOGGLE));
    }

    @Test
    public void testWritesOnlyFromTheExpectedValue() {
        kubernetesClient
                .configMaps()
                .inNamespace(NAMESPACE)
                .withName(NAME)
                .edit(
                        configMap -> {
                            configMap
                                    .getData()
                                    .put(
                                            TRANSITION_STAGE.getLabel(),
                                            TransitionStage.FAILING.toString());
                            return configMap;
                        });
        var gate = new GateKubernetesService(kubernetesClient, NAMESPACE, NAME);

        // A late teardown signal must not undo an abort
        assertEquals(
                Optional.of(TransitionStage.FAILING.toString()),
                gate.compareAndSet(
                        BlueGreenDeploymentType.GREEN,
                        TRANSITION_STAGE.getLabel(),
                        TransitionStage.TRANSITIONING.toString(),
                        Map.of(
                                TRANSITION_STAGE.getLabel(),
                                TransitionStage.CLEAR_TO_TEARDOWN.toString())));
        assertEquals(TransitionStage.FAILING.toString(), data().get(TRANSITION_STAGE.getLabel()));
    }

    private static Optional<String> toggle(GateKubernetesService gate, String value) {
        return gate.compareAndSet(
                BlueGreenDeploymentType.GREEN, TOGGLE, null, Map.of(TOGGLE, value));
    }

    private Map<String, String> data() {
        return kubernetesClient.configMaps().inNamespace(NAMESPACE).withName(NAME).get().getData();
    }
}
