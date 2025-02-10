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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.utils.KubernetesClientUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.COUNTER;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.HISTO;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.HTTP_REQUEST_FAILED_GROUP;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.HTTP_REQUEST_GROUP;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.HTTP_RESPONSE_GROUP;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.KUBE_CLIENT_GROUP;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics.METER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.byLessThan;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** {@link KubernetesClientMetrics} tests. */
@EnableKubernetesMockClient(crud = true)
@TestMethodOrder(OrderAnnotation.class)
class KubernetesClientMetricsTest {
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

    @BeforeAll
    static void beforeAll() {
        Awaitility.ignoreExceptionByDefault(AssertionError.class);
    }

    @Test
    @Order(1)
    void testMetricsDisabled() {
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
    void testMetricsEnabled() {
        var configuration = new Configuration();
        var listener = new TestingMetricListener(configuration);
        var kubernetesClient =
                KubernetesClientUtils.getKubernetesClient(
                        FlinkOperatorConfiguration.fromConfiguration(configuration),
                        listener.getMetricGroup(),
                        mockServer.createClient().getConfiguration());

        var deployment = TestUtils.buildApplicationCluster();
        assertCounterIsZero(listener, REQUEST_COUNTER_ID);
        assertRateIsZero(listener, REQUEST_METER_ID);
        assertCounterIsZero(listener, REQUEST_FAILED_COUNTER_ID);
        assertRateIsZero(listener, REQUEST_FAILED_METER_ID);
        assertCounterIsZero(listener, RESPONSE_COUNTER_ID);
        assertRateIsZero(listener, RESPONSE_METER_ID);
        assertHistogramHasZeroStatistics(listener);

        kubernetesClient.resource(deployment).createOrReplace();
        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () -> {
                            assertCounterHasValue(listener, REQUEST_COUNTER_ID, 1);
                            assertCounterHasValue(listener, REQUEST_POST_COUNTER_ID, 1);
                            assertCounterHasValue(listener, RESPONSE_COUNTER_ID, 1);
                            assertCounterHasValue(listener, RESPONSE_201_COUNTER_ID, 1);

                            assertHistogramHasStatistics(listener);
                            return true;
                        });

        kubernetesClient.resource(deployment).delete();
        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () -> {
                            assertCounterHasValue(listener, REQUEST_DELETE_COUNTER_ID, 1);
                            return true;
                        });

        kubernetesClient.resource(deployment).delete();
        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () -> {
                            assertCounterHasValue(listener, REQUEST_DELETE_COUNTER_ID, 2);
                            assertCounterHasValue(listener, RESPONSE_404_COUNTER_ID, 1);
                            return true;
                        });

        kubernetesClient.resource(deployment).createOrReplace();
        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () -> {
                            assertPositiveRate(listener, REQUEST_METER_ID);
                            assertPositiveRate(listener, RESPONSE_METER_ID);
                            assertPositiveRate(listener, RESPONSE_201_METER_ID);
                            assertPositiveRate(listener, RESPONSE_404_METER_ID);
                            return true;
                        });
    }

    @Test
    void shouldTrackMetricsOnWebsocketRequests() {
        // Given
        var configuration = new Configuration();
        var listener = new TestingMetricListener(configuration);
        var kubernetesClient =
                KubernetesClientUtils.getKubernetesClient(
                        FlinkOperatorConfiguration.fromConfiguration(configuration),
                        listener.getMetricGroup(),
                        mockServer.createClient().getConfiguration());

        var deployment = TestUtils.buildApplicationCluster();
        final Counter responseCounter =
                listener.getCounter(listener.getMetricId(RESPONSE_COUNTER_ID)).orElseThrow();
        final Counter requestCounter =
                listener.getCounter(listener.getMetricId(REQUEST_COUNTER_ID)).orElseThrow();

        AtomicLong watchEventCount = new AtomicLong(0);

        final NamespaceableResource<FlinkDeployment> deploymentResource =
                kubernetesClient.resource(deployment);
        deploymentResource.createOrReplace();

        var initialRequestCount = requestCounter.getCount();
        var initialResponseCount = responseCounter.getCount();
        // When
        try (var ignored =
                deploymentResource.inform(
                        new ResourceEventHandler<>() {
                            @Override
                            public void onAdd(FlinkDeployment obj) {
                                watchEventCount.getAndIncrement();
                            }

                            @Override
                            public void onUpdate(FlinkDeployment oldObj, FlinkDeployment newObj) {
                                watchEventCount.getAndIncrement();
                            }

                            @Override
                            public void onDelete(
                                    FlinkDeployment obj, boolean deletedFinalStateUnknown) {
                                watchEventCount.getAndIncrement();
                            }
                        })) {

            // Then
            await().atMost(20, TimeUnit.SECONDS)
                    .untilAtomic(watchEventCount, Matchers.greaterThanOrEqualTo(1L));
            assertThat(requestCounter)
                    .extracting(Counter::getCount)
                    .asInstanceOf(LONG)
                    .isGreaterThan(
                            initialRequestCount
                                    + 1); // +1 as that is the request to start the watch.

            assertThat(responseCounter)
                    .extracting(Counter::getCount)
                    .asInstanceOf(LONG)
                    .isGreaterThan(initialResponseCount + watchEventCount.get());
        }
    }

    @Test
    @Order(3)
    void testMetricsHttpResponseCodeGroupsEnabled() {
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

        assertCounterIsZero(listener, REQUEST_COUNTER_ID);
        assertRateIsZero(listener, REQUEST_METER_ID);
        assertCounterIsZero(listener, REQUEST_FAILED_COUNTER_ID);
        assertRateIsZero(listener, REQUEST_FAILED_METER_ID);
        assertCounterIsZero(listener, RESPONSE_COUNTER_ID);
        assertRateIsZero(listener, RESPONSE_METER_ID);
        assertHistogramHasZeroStatistics(listener);

        kubernetesClient.resource(deployment).createOrReplace();
        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () -> {
                            assertCounterHasValue(listener, REQUEST_COUNTER_ID, 1);
                            assertCounterHasValue(listener, REQUEST_POST_COUNTER_ID, 1);
                            assertCounterHasValue(listener, RESPONSE_COUNTER_ID, 1);
                            assertCounterHasValue(listener, RESPONSE_201_COUNTER_ID, 1);
                            assertCounterHasValue(listener, RESPONSE_2xx_COUNTER_ID, 1);
                            assertHistogramHasStatistics(listener);
                            return true;
                        });

        kubernetesClient.resource(deployment).delete();
        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () -> {
                            assertCounterHasValue(listener, REQUEST_DELETE_COUNTER_ID, 1);
                            return true;
                        });

        kubernetesClient.resource(deployment).delete();
        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () -> {
                            assertCounterHasValue(listener, REQUEST_DELETE_COUNTER_ID, 2);
                            assertCounterHasValue(listener, RESPONSE_404_COUNTER_ID, 1);
                            assertCounterHasValue(listener, RESPONSE_404_COUNTER_ID, 1);
                            return true;
                        });

        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () -> {
                            kubernetesClient.resource(deployment).createOrReplace();
                            assertPositiveRate(listener, REQUEST_METER_ID);
                            assertPositiveRate(listener, RESPONSE_METER_ID);
                            assertPositiveRate(listener, RESPONSE_201_METER_ID);
                            assertPositiveRate(listener, RESPONSE_404_METER_ID);
                            assertPositiveRate(listener, RESPONSE_2xx_METER_ID);
                            assertPositiveRate(listener, RESPONSE_4xx_METER_ID);
                            return true;
                        });
    }

    @Test
    @Order(3)
    void testAPIServerIsDown() {
        var configuration = new Configuration();
        var listener = new TestingMetricListener(configuration);
        var kubernetesClient =
                KubernetesClientUtils.getKubernetesClient(
                        FlinkOperatorConfiguration.fromConfiguration(configuration),
                        listener.getMetricGroup(),
                        mockServer.createClient().getConfiguration());

        var deployment = TestUtils.buildApplicationCluster();
        mockServer.shutdown();

        assertCounterIsZero(listener, REQUEST_FAILED_COUNTER_ID);
        assertRateIsZero(listener, REQUEST_FAILED_METER_ID);

        await().atMost(20, TimeUnit.SECONDS)
                .until(
                        () -> {
                            assertThrows(
                                    KubernetesClientException.class,
                                    () -> kubernetesClient.resource(deployment).createOrReplace());
                            assertCounterIsPositive(listener);
                            assertPositiveRate(listener, REQUEST_FAILED_METER_ID);
                            return true;
                        });
    }

    private static void assertRateIsZero(TestingMetricListener listener, String meterId) {
        assertRate(
                listener,
                meterId,
                meter -> assertThat(meter.getRate()).isCloseTo(0.0, byLessThan(0.00001)));
    }

    private static void assertPositiveRate(TestingMetricListener listener, String meterId) {
        assertRate(
                listener,
                meterId,
                meter -> assertThat(meter.getRate()).isGreaterThanOrEqualTo(0.01));
    }

    private static void assertRate(
            TestingMetricListener listener, String meterId, Consumer<Meter> meterConsumer) {
        assertThat(listener.getMeter(listener.getMetricId(meterId)))
                .hasValueSatisfying(meterConsumer);
    }

    private static void assertCounterIsPositive(TestingMetricListener listener) {
        assertCounterHasValue(
                listener,
                KubernetesClientMetricsTest.REQUEST_FAILED_COUNTER_ID,
                counter -> assertThat(counter.getCount()).isPositive());
    }

    private static void assertCounterIsZero(TestingMetricListener listener, String counterId) {
        assertCounterHasValue(
                listener, counterId, counter -> assertThat(counter.getCount()).isZero());
    }

    private static void assertCounterHasValue(
            TestingMetricListener listener, String requestDeleteCounterId, int expected) {
        assertCounterHasValue(
                listener,
                requestDeleteCounterId,
                counter -> assertThat(counter.getCount()).isEqualTo(expected));
    }

    private static void assertCounterHasValue(
            TestingMetricListener listener,
            String requestDeleteCounterId,
            Consumer<Counter> counterConsumer) {
        assertThat(listener.getCounter(listener.getMetricId(requestDeleteCounterId)))
                .hasValueSatisfying(counterConsumer);
    }

    private static void assertHistogramHasStatistics(TestingMetricListener listener) {
        assertHistogramStatistics(
                listener,
                histogram -> {
                    assertThat(histogram.getCount()).isPositive();
                    assertThat(histogram.getStatistics())
                            .satisfies(
                                    stats -> {
                                        assertThat(stats.getMin()).isPositive();
                                        assertThat(stats.getMax()).isPositive();
                                    });
                });
    }

    private static void assertHistogramHasZeroStatistics(TestingMetricListener listener) {
        assertHistogramStatistics(
                listener,
                histogram -> {
                    assertThat(histogram.getCount()).isZero();
                    assertThat(histogram.getStatistics())
                            .satisfies(
                                    stats -> {
                                        assertThat(stats.getMin()).isZero();
                                        assertThat(stats.getMax()).isZero();
                                    });
                });
    }

    private static void assertHistogramStatistics(
            TestingMetricListener listener, Consumer<Histogram> histogramConsumer) {
        assertThat(
                        listener.getHistogram(
                                listener.getMetricId(
                                        KubernetesClientMetricsTest.RESPONSE_LATENCY_ID)))
                .hasValueSatisfying(histogramConsumer);
    }
}
