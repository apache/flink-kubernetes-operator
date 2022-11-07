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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@link FlinkSessionJobController} tests. */
@EnableKubernetesMockClient(crud = true)
class FlinkSessionJobControllerTest {
    private KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private TestingFlinkService flinkService = new TestingFlinkService();
    private TestingFlinkSessionJobController testController;

    @BeforeEach
    public void before() {
        flinkService = new TestingFlinkService();
        testController =
                new TestingFlinkSessionJobController(configManager, kubernetesClient, flinkService);

        kubernetesClient.resource(TestUtils.buildSessionJob()).createOrReplace();
    }

    @Test
    public void testSubmitJobButException() {
        FlinkSessionJob sessionJob = TestUtils.buildSessionJob();

        flinkService.setDeployFailure(true);

        try {
            testController.reconcile(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());
        } catch (Exception e) {
            // Ignore
        }

        Assertions.assertEquals(1, testController.events().size());
        var event = testController.events().remove();
        Assertions.assertEquals(EventRecorder.Type.Warning.toString(), event.getType());
        Assertions.assertEquals("SessionJobException", event.getReason());

        testController.cleanup(sessionJob, TestUtils.createContextWithReadyFlinkDeployment());

        flinkService.setDeployFailure(false);
        flinkService.clear();
    }
}
