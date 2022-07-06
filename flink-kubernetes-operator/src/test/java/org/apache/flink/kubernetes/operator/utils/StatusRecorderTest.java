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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.metrics.testutils.MetricListener;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.METRIC_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.MetricManager.NS_SCOPE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link StatusRecorder}. */
@EnableKubernetesMockClient(crud = true)
public class StatusRecorderTest {

    private KubernetesClient kubernetesClient;
    private KubernetesMockServer mockServer;

    @Test
    public void testPatchOnlyWhenChanged() throws InterruptedException {
        var helper =
                new StatusRecorder<FlinkDeploymentStatus>(
                        kubernetesClient,
                        new MetricManager<>(new MetricListener().getMetricGroup()),
                        (e, s) -> {});
        var deployment = TestUtils.buildApplicationCluster();
        kubernetesClient.resource(deployment).createOrReplace();
        var lastRequest = mockServer.getLastRequest();

        helper.patchAndCacheStatus(deployment);
        assertTrue(mockServer.getLastRequest() != lastRequest);
        lastRequest = mockServer.getLastRequest();
        deployment.getStatus().getReconciliationStatus().setState(ReconciliationState.ROLLING_BACK);
        helper.patchAndCacheStatus(deployment);

        // We intentionally compare references
        assertTrue(mockServer.getLastRequest() != lastRequest);
        lastRequest = mockServer.getLastRequest();

        // No update
        helper.patchAndCacheStatus(deployment);
        assertTrue(mockServer.getLastRequest() == lastRequest);
    }

    @Test
    public void testFlinkDeploymentMetrics() throws InterruptedException {
        var metricListener = new MetricListener();
        var helper =
                new StatusRecorder<FlinkDeploymentStatus>(
                        kubernetesClient,
                        new MetricManager<>(metricListener.getMetricGroup()),
                        (e, s) -> {});

        var deployment = TestUtils.buildApplicationCluster();
        kubernetesClient.resource(deployment).createOrReplace();

        helper.updateStatusFromCache(deployment);
        assertEquals(1, metricListener.getGauge(totalIdentifier(deployment)).get().getValue());
        assertEquals(1, metricListener.getGauge(perStatusIdentifier(deployment)).get().getValue());

        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            deployment.getStatus().setJobManagerDeploymentStatus(status);
            helper.patchAndCacheStatus(deployment);
            assertEquals(1, metricListener.getGauge(totalIdentifier(deployment)).get().getValue());
            assertEquals(
                    1, metricListener.getGauge(perStatusIdentifier(deployment)).get().getValue());
        }

        helper.removeCachedStatus(deployment);
        assertEquals(0, metricListener.getGauge(totalIdentifier(deployment)).get().getValue());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            assertEquals(
                    0, metricListener.getGauge(perStatusIdentifier(deployment)).get().getValue());
        }
    }

    private String[] totalIdentifier(FlinkDeployment deployment) {
        return new String[] {
            NS_SCOPE_KEY, deployment.getMetadata().getNamespace(), METRIC_GROUP_NAME, "Count"
        };
    }

    private String[] perStatusIdentifier(FlinkDeployment deployment) {
        return new String[] {
            NS_SCOPE_KEY,
            deployment.getMetadata().getNamespace(),
            METRIC_GROUP_NAME,
            deployment.getStatus().getJobManagerDeploymentStatus().name(),
            "Count"
        };
    }
}
