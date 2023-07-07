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
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.utils.KubernetesClientUtils;

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
    private static final String RESPONSE_201_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "201", COUNTER);
    private static final String RESPONSE_201_METER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "201", METER);
    private static final String RESPONSE_2xx_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "2xx", COUNTER);
    private static final String RESPONSE_2xx_METER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "2xx", METER);
    private static final String RESPONSE_404_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "404", COUNTER);
    private static final String RESPONSE_404_METER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "404", METER);
    private static final String RESPONSE_4xx_COUNTER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "4xx", COUNTER);
    private static final String RESPONSE_4xx_METER_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, "4xx", METER);
    private static final String RESPONSE_LATENCY_ID =
            String.join(".", KUBE_CLIENT_GROUP, HTTP_RESPONSE_GROUP, HISTO);

    @Test
    @Order(1)
    public void testMetricsDisabled() {
        var configuration = new Configuration();
        configuration.set(
                KubernetesOperatorMetricOptions.OPERATOR_KUBERNETES_CLIENT_METRICS_ENABLED, false);
        var listener = new TestingMetricListener(configuration);
        var kubernetesClient =
                KubernetesClientUtils.getKubernetesClient(
                        FlinkOperatorConfiguration.fromConfiguration(configuration),
                        listener.getMetricGroup(),
                        mockServer.createClient().getConfiguration());

        var deployment = TestUtils.buildApplicationCluster();
        kubernetesClient.resource(deployment).get();
        assertFalse(listener.getCounter(listener.getMetricId(REQUEST_COUNTER_ID)).isPresent());
        assertFalse(listener.getMeter(listener.getMetricId(REQUEST_METER_ID)).isPresent());
        assertFalse(
                listener.getCounter(listener.getMetricId(REQUEST_FAILED_COUNTER_ID)).isPresent());
        assertFalse(listener.getMeter(listener.getMetricId(REQUEST_FAILED_METER_ID)).isPresent());
        assertFalse(listener.getCounter(listener.getMetricId(RESPONSE_COUNTER_ID)).isPresent());
        assertFalse(listener.getMeter(listener.getMetricId(RESPONSE_METER_ID)).isPresent());
        assertFalse(listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID)).isPresent());
        assertFalse(listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID)).isPresent());
    }

    @Test
    @Order(2)
    public void testMetricsEnabled() {
        var configuration = new Configuration();
        var listener = new TestingMetricListener(configuration);
        var kubernetesClient =
                KubernetesClientUtils.getKubernetesClient(
                        FlinkOperatorConfiguration.fromConfiguration(configuration),
                        listener.getMetricGroup(),
                        mockServer.createClient().getConfiguration());

        var deployment = TestUtils.buildApplicationCluster();
        assertEquals(
                0, listener.getCounter(listener.getMetricId(REQUEST_COUNTER_ID)).get().getCount());
        assertEquals(
                0.0, listener.getMeter(listener.getMetricId(REQUEST_METER_ID)).get().getRate());
        assertEquals(
                0,
                listener.getCounter(listener.getMetricId(REQUEST_FAILED_COUNTER_ID))
                        .get()
                        .getCount());
        assertEquals(
                0.0,
                listener.getMeter(listener.getMetricId(REQUEST_FAILED_METER_ID)).get().getRate());
        assertEquals(
                0, listener.getCounter(listener.getMetricId(RESPONSE_COUNTER_ID)).get().getCount());
        assertEquals(
                0.0, listener.getMeter(listener.getMetricId(RESPONSE_METER_ID)).get().getRate());
        assertEquals(
                0,
                listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID))
                        .get()
                        .getStatistics()
                        .getMin());
        assertEquals(
                0,
                listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID))
                        .get()
                        .getStatistics()
                        .getMax());

        kubernetesClient.resource(deployment).createOrReplace();
        assertEquals(
                1, listener.getCounter(listener.getMetricId(REQUEST_COUNTER_ID)).get().getCount());
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(REQUEST_POST_COUNTER_ID))
                        .get()
                        .getCount());
        assertEquals(
                1, listener.getCounter(listener.getMetricId(RESPONSE_COUNTER_ID)).get().getCount());
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(RESPONSE_201_COUNTER_ID))
                        .get()
                        .getCount());
        assertTrue(
                listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID))
                                .get()
                                .getStatistics()
                                .getMin()
                        > 0);
        assertTrue(
                listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID))
                                .get()
                                .getStatistics()
                                .getMax()
                        > 0);

        kubernetesClient.resource(deployment).delete();
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(REQUEST_DELETE_COUNTER_ID))
                        .get()
                        .getCount());

        kubernetesClient.resource(deployment).delete();
        assertEquals(
                2,
                listener.getCounter(listener.getMetricId(REQUEST_DELETE_COUNTER_ID))
                        .get()
                        .getCount());
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(RESPONSE_404_COUNTER_ID))
                        .get()
                        .getCount());
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .until(
                        () -> {
                            kubernetesClient.resource(deployment).createOrReplace();
                            return listener.getMeter(listener.getMetricId(REQUEST_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01
                                    && listener.getMeter(listener.getMetricId(RESPONSE_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01
                                    && listener.getMeter(
                                                            listener.getMetricId(
                                                                    RESPONSE_201_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01
                                    && listener.getMeter(
                                                            listener.getMetricId(
                                                                    RESPONSE_404_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01;
                        });
    }

    @Test
    @Order(3)
    public void testMetricsHttpResponseCodeGroupsEnabled() {
        var configuration = new Configuration();
        configuration.set(
                KubernetesOperatorMetricOptions
                        .OPERATOR_KUBERNETES_CLIENT_METRICS_HTTP_RESPONSE_CODE_GROUPS_ENABLED,
                true);
        var listener = new TestingMetricListener(configuration);
        var kubernetesClient =
                KubernetesClientUtils.getKubernetesClient(
                        FlinkOperatorConfiguration.fromConfiguration(configuration),
                        listener.getMetricGroup(),
                        mockServer.createClient().getConfiguration());

        var deployment = TestUtils.buildApplicationCluster();
        assertEquals(
                0, listener.getCounter(listener.getMetricId(REQUEST_COUNTER_ID)).get().getCount());
        assertEquals(
                0.0, listener.getMeter(listener.getMetricId(REQUEST_METER_ID)).get().getRate());
        assertEquals(
                0,
                listener.getCounter(listener.getMetricId(REQUEST_FAILED_COUNTER_ID))
                        .get()
                        .getCount());
        assertEquals(
                0.0,
                listener.getMeter(listener.getMetricId(REQUEST_FAILED_METER_ID)).get().getRate());
        assertEquals(
                0, listener.getCounter(listener.getMetricId(RESPONSE_COUNTER_ID)).get().getCount());
        assertEquals(
                0.0, listener.getMeter(listener.getMetricId(RESPONSE_METER_ID)).get().getRate());
        assertEquals(
                0,
                listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID))
                        .get()
                        .getStatistics()
                        .getMin());
        assertEquals(
                0,
                listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID))
                        .get()
                        .getStatistics()
                        .getMax());

        kubernetesClient.resource(deployment).createOrReplace();
        assertEquals(
                1, listener.getCounter(listener.getMetricId(REQUEST_COUNTER_ID)).get().getCount());
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(REQUEST_POST_COUNTER_ID))
                        .get()
                        .getCount());
        assertEquals(
                1, listener.getCounter(listener.getMetricId(RESPONSE_COUNTER_ID)).get().getCount());
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(RESPONSE_201_COUNTER_ID))
                        .get()
                        .getCount());
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(RESPONSE_2xx_COUNTER_ID))
                        .get()
                        .getCount());
        assertTrue(
                listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID))
                                .get()
                                .getStatistics()
                                .getMin()
                        > 0);
        assertTrue(
                listener.getHistogram(listener.getMetricId(RESPONSE_LATENCY_ID))
                                .get()
                                .getStatistics()
                                .getMax()
                        > 0);

        kubernetesClient.resource(deployment).delete();
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(REQUEST_DELETE_COUNTER_ID))
                        .get()
                        .getCount());

        kubernetesClient.resource(deployment).delete();
        assertEquals(
                2,
                listener.getCounter(listener.getMetricId(REQUEST_DELETE_COUNTER_ID))
                        .get()
                        .getCount());
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(RESPONSE_404_COUNTER_ID))
                        .get()
                        .getCount());
        assertEquals(
                1,
                listener.getCounter(listener.getMetricId(RESPONSE_4xx_COUNTER_ID))
                        .get()
                        .getCount());
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .until(
                        () -> {
                            kubernetesClient.resource(deployment).createOrReplace();
                            return listener.getMeter(listener.getMetricId(REQUEST_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01
                                    && listener.getMeter(listener.getMetricId(RESPONSE_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01
                                    && listener.getMeter(
                                                            listener.getMetricId(
                                                                    RESPONSE_201_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01
                                    && listener.getMeter(
                                                            listener.getMetricId(
                                                                    RESPONSE_404_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01
                                    && listener.getMeter(
                                                            listener.getMetricId(
                                                                    RESPONSE_2xx_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01
                                    && listener.getMeter(
                                                            listener.getMetricId(
                                                                    RESPONSE_4xx_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01;
                        });
    }

    @Test
    @Order(3)
    public void testAPIServerIsDown() {
        var configuration = new Configuration();
        var listener = new TestingMetricListener(configuration);
        var kubernetesClient =
                KubernetesClientUtils.getKubernetesClient(
                        FlinkOperatorConfiguration.fromConfiguration(configuration),
                        listener.getMetricGroup(),
                        mockServer.createClient().getConfiguration());

        var deployment = TestUtils.buildApplicationCluster();
        mockServer.shutdown();
        assertEquals(
                0,
                listener.getCounter(listener.getMetricId(REQUEST_FAILED_COUNTER_ID))
                        .get()
                        .getCount());
        assertEquals(
                0.0,
                listener.getMeter(listener.getMetricId(REQUEST_FAILED_METER_ID)).get().getRate());
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .until(
                        () -> {
                            assertThrows(
                                    KubernetesClientException.class,
                                    () -> kubernetesClient.resource(deployment).createOrReplace());
                            return listener.getCounter(
                                                            listener.getMetricId(
                                                                    REQUEST_FAILED_COUNTER_ID))
                                                    .get()
                                                    .getCount()
                                            > 0
                                    && listener.getMeter(
                                                            listener.getMetricId(
                                                                    REQUEST_FAILED_METER_ID))
                                                    .get()
                                                    .getRate()
                                            > 0.01;
                        });
    }
}
