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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.COUNTER_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.FLINK_DEPLOYMENT_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkDeploymentMetrics.JM_DEPLOYMENT_STATUS_GROUP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** @link FlinkDeploymentMetrics tests. */
@EnableKubernetesMockClient(crud = true)
public class FlinkDeploymentMetricsTest {
    private KubernetesClient kubernetesClient;

    @Test
    public void testFlinkDeploymentMetrics() throws InterruptedException {
        var metrics = new HashMap<String, Metric>();
        TestingMetricRegistry registry =
                TestingMetricRegistry.builder()
                        .setDelimiter(".".charAt(0))
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    metrics.put(group.getMetricIdentifier(name), metric);
                                })
                        .build();

        var metricManager =
                (MetricManager) TestUtils.createTestMetricManager(registry, new Configuration());
        var helper =
                new StatusRecorder<FlinkDeploymentStatus>(
                        kubernetesClient, metricManager, (e, s) -> {});

        var deployment = TestUtils.buildApplicationCluster();
        kubernetesClient.resource(deployment).createOrReplace();

        helper.updateStatusFromCache(deployment);
        assertEquals(1, ((Gauge) metrics.get(totalIdentifier(deployment))).getValue());
        assertEquals(1, ((Gauge) metrics.get(perStatusIdentifier(deployment))).getValue());

        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            deployment.getStatus().setJobManagerDeploymentStatus(status);
            helper.patchAndCacheStatus(deployment);
            assertEquals(1, ((Gauge) metrics.get(totalIdentifier(deployment))).getValue());
            assertEquals(1, ((Gauge) metrics.get(perStatusIdentifier(deployment))).getValue());
        }

        helper.removeCachedStatus(deployment);
        assertEquals(0, ((Gauge) metrics.get(totalIdentifier(deployment))).getValue());
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            assertEquals(0, ((Gauge) metrics.get(perStatusIdentifier(deployment))).getValue());
        }
    }

    private String totalIdentifier(FlinkDeployment deployment) {
        String baseScope = "testhost.k8soperator.flink-operator-test.testopname.";
        String[] metricScope =
                new String[] {
                    "namespace",
                    deployment.getMetadata().getNamespace(),
                    FLINK_DEPLOYMENT_GROUP_NAME,
                    COUNTER_NAME
                };
        return baseScope + String.join(".", metricScope);
    }

    private String perStatusIdentifier(FlinkDeployment deployment) {

        String baseScope = "testhost.k8soperator.flink-operator-test.testopname.";
        String[] metricScope =
                new String[] {
                    "namespace",
                    deployment.getMetadata().getNamespace(),
                    FLINK_DEPLOYMENT_GROUP_NAME,
                    JM_DEPLOYMENT_STATUS_GROUP_NAME,
                    deployment.getStatus().getJobManagerDeploymentStatus().name(),
                    COUNTER_NAME
                };

        return baseScope + String.join(".", metricScope);
    }
}
