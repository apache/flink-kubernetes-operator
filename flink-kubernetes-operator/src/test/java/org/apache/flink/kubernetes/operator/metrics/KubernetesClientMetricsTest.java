/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.utils.KubernetesClientUtils;
import org.apache.flink.metrics.testutils.MetricListener;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.COUNTER;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.HISTO;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.HTTP_REQUEST_FAILED_GROUP;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.HTTP_REQUEST_GROUP;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.HTTP_RESPONSE_GROUP;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.KUBE_CLIENT_GROUP;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.METER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link KubernetesClientMetrics} tests. */
@EnableKubernetesMockClient(crud = true)
@TestMethodOrder(OrderAnnotation.class)
public class KubernetesClientMetricsTest {
    private KubernetesMockServer mockServer;
    private final MetricListener listener = new MetricListener();

    private static final String REQUEST_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_REQUEST_GROUP, COUNTER);
    private static final String REQUEST_METER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_REQUEST_GROUP, METER);
    private static final String REQUEST_FAILED_METER_ID =
            String.join(
                    ".", KUBE_CLIENT_GROUP, HTTP_REQUEST_GROUP, HTTP_REQUEST_FAILED_GROUP, METER);
    private static final String REQUEST_POST_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_REQUEST_GROUP, "POST", COUNTER);
    private static final String REQUEST_DELETE_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_REQUEST_GROUP, "DELETE", COUNTER);
    private static final String REQUEST_FAILED_COUNTER_ID =
            String.join(
                    ".", KUBE_CLIENT_GROUP, HTTP_REQUEST_GROUP, HTTP_REQUEST_FAILED_GROUP, COUNTER);
    private static final String RESPONSE_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, COUNTER);
    private static final String RESPONSE_METER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, METER);
    private static final String RESPONSE_200_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "200", COUNTER);
    private static final String RESPONSE_404_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "404", COUNTER);
    private static final String RESPONSE_LATENCY_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, HISTO);

    @Test
    @Order(1)
    public void testMetricsDisabled() {
        var deployment = TestUtils.buildApplicationCluster();
        KubernetesClient noMetricsClient = getKubernetesClient(false);
        noMetricsClient.resource(deployment).get();
        assertFalse(listener.getCounter(REQUEST_COUNTER_ID).isPresent());
        assertFalse(listener.getMeter(REQUEST_METER_ID).isPresent());
        assertFalse(listener.getCounter(REQUEST_FAILED_COUNTER_ID).isPresent());
        assertFalse(listener.getMeter(REQUEST_FAILED_METER_ID).isPresent());
        assertFalse(listener.getCounter(RESPONSE_COUNTER_ID).isPresent());
        assertFalse(listener.getMeter(RESPONSE_METER_ID).isPresent());
        assertFalse(listener.getHistogram(RESPONSE_LATENCY_ID).isPresent());
        assertFalse(listener.getHistogram(RESPONSE_LATENCY_ID).isPresent());
    }

    @Test
    @Order(2)
    public void testMetricsEnabled() {
        KubernetesClient kubernetesClient = getKubernetesClient(true);
        var deployment = TestUtils.buildApplicationCluster();
        assertEquals(0, listener.getCounter(REQUEST_COUNTER_ID).get().getCount());
        assertEquals(0.0, listener.getMeter(REQUEST_METER_ID).get().getRate());
        assertEquals(0, listener.getCounter(REQUEST_FAILED_COUNTER_ID).get().getCount());
        assertEquals(0.0, listener.getMeter(REQUEST_FAILED_METER_ID).get().getRate());
        assertEquals(0, listener.getCounter(RESPONSE_COUNTER_ID).get().getCount());
        assertEquals(0.0, listener.getMeter(RESPONSE_METER_ID).get().getRate());
        assertEquals(0, listener.getHistogram(RESPONSE_LATENCY_ID).get().getStatistics().getMin());
        assertEquals(0, listener.getHistogram(RESPONSE_LATENCY_ID).get().getStatistics().getMax());

        kubernetesClient.resource(deployment).createOrReplace();
        assertEquals(1, listener.getCounter(REQUEST_COUNTER_ID).get().getCount());
        assertEquals(1, listener.getCounter(REQUEST_POST_COUNTER_ID).get().getCount());
        assertEquals(1, listener.getCounter(RESPONSE_COUNTER_ID).get().getCount());
        assertEquals(1, listener.getCounter(RESPONSE_200_COUNTER_ID).get().getCount());
        assertTrue(listener.getHistogram(RESPONSE_LATENCY_ID).get().getStatistics().getMin() > 0);
        assertTrue(listener.getHistogram(RESPONSE_LATENCY_ID).get().getStatistics().getMax() > 0);

        kubernetesClient.resource(deployment).delete();
        assertEquals(1, listener.getCounter(REQUEST_DELETE_COUNTER_ID).get().getCount());

        kubernetesClient.resource(deployment).delete();
        assertEquals(2, listener.getCounter(REQUEST_DELETE_COUNTER_ID).get().getCount());
        assertEquals(1, listener.getCounter(RESPONSE_404_COUNTER_ID).get().getCount());
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .until(
                        () -> {
                            kubernetesClient.resource(deployment).createOrReplace();
                            return listener.getMeter(REQUEST_METER_ID).get().getRate() > 0.1
                                    && listener.getMeter(RESPONSE_METER_ID).get().getRate() > 0.1;
                        });
    }

    @Test
    @Order(3)
    public void testAPIServerIsDown() {
        var deployment = TestUtils.buildApplicationCluster();
        KubernetesClient kubernetesClient = getKubernetesClient(true);
        mockServer.shutdown();
        assertEquals(0, listener.getCounter(REQUEST_FAILED_COUNTER_ID).get().getCount());
        assertEquals(0.0, listener.getMeter(REQUEST_FAILED_METER_ID).get().getRate());
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .until(
                        () -> {
                            assertThrows(
                                    KubernetesClientException.class,
                                    () -> kubernetesClient.resource(deployment).createOrReplace());
                            return listener.getCounter(REQUEST_FAILED_COUNTER_ID).get().getCount()
                                            > 0
                                    && listener.getMeter(REQUEST_FAILED_METER_ID).get().getRate()
                                            > 0.1;
                        });
    }

    private KubernetesClient getKubernetesClient(boolean enableMetrics) {
        var configuration = new Configuration();
        configuration.setBoolean(
                KubernetesOperatorMetricOptions.OPERATOR_KUBERNETES_CLIENT_METRICS_ENABLED,
                enableMetrics);
        var configManager = new FlinkConfigManager(configuration);
        return KubernetesClientUtils.getKubernetesClient(
                configManager.getOperatorConfiguration(),
                listener.getMetricGroup(),
                mockServer.createClient().getConfiguration());
    }
}
