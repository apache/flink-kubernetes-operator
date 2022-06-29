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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link org.apache.flink.kubernetes.operator.reconciler.deployment.SessionReconciler}.
 */
@EnableKubernetesMockClient(crud = true)
public class SessionReconcilerTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private final EventRecorder eventRecorder = new EventRecorder(null, (r, e) -> {});
    private KubernetesClient kubernetesClient;

    @Test
    public void testStartSession() throws Exception {
        Context context = TestUtils.createEmptyContext();
        var count = new AtomicInteger(0);
        TestingFlinkService flinkService =
                new TestingFlinkService() {
                    @Override
                    public void submitSessionCluster(Configuration conf) {
                        super.submitSessionCluster(conf);
                        count.addAndGet(1);
                    }
                };

        SessionReconciler reconciler =
                new SessionReconciler(
                        kubernetesClient,
                        flinkService,
                        configManager,
                        eventRecorder,
                        new StatusRecorder<>(kubernetesClient, (r, e) -> {}));
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        kubernetesClient.resource(deployment).createOrReplace();
        reconciler.reconcile(deployment, context);
        assertEquals(1, count.get());
    }
}
