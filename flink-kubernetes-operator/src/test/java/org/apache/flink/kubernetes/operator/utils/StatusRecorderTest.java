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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link StatusRecorder}. */
@EnableKubernetesMockClient(crud = true)
public class StatusRecorderTest {

    private KubernetesClient kubernetesClient;
    private KubernetesMockServer mockServer;

    @Test
    public void testPatchOnlyWhenChanged() throws InterruptedException {
        var helper =
                new StatusRecorder<FlinkDeployment, FlinkDeploymentStatus>(
                        new MetricManager<>(), (e, s) -> {});
        var deployment = TestUtils.buildApplicationCluster();
        kubernetesClient.resource(deployment).createOrReplace();
        var lastRequest = mockServer.getLastRequest();

        helper.patchAndCacheStatus(deployment, kubernetesClient);
        assertThat(lastRequest).isNotSameAs(mockServer.getLastRequest());
        lastRequest = mockServer.getLastRequest();
        deployment.getStatus().getReconciliationStatus().setState(ReconciliationState.ROLLING_BACK);
        helper.patchAndCacheStatus(deployment, kubernetesClient);

        // We intentionally compare references
        assertThat(lastRequest).isNotSameAs(mockServer.getLastRequest());
        lastRequest = mockServer.getLastRequest();

        // No update
        helper.patchAndCacheStatus(deployment, kubernetesClient);
        assertThat(lastRequest).isSameAs(mockServer.getLastRequest());
    }

    @Test
    public void testNullLatestResource() {
        var statusRecorder =
                new StatusRecorder<FlinkDeployment, FlinkDeploymentStatus>(
                        new MetricManager<>(), (e, s) -> {});

        var resource = TestUtils.buildApplicationCluster();
        var cause = new KubernetesClientException("dummy");
        assertThatThrownBy(
                        () ->
                                statusRecorder.handleLockingError(
                                        resource, null, kubernetesClient, 0, cause))
                .isInstanceOf(KubernetesClientException.class)
                .hasMessage("Failed to retrieve latest resource")
                .hasCause(cause);
    }
}
