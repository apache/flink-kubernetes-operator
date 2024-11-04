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
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import io.fabric8.kubernetes.client.http.HttpRequest;
import io.fabric8.kubernetes.client.http.HttpResponse;
import io.fabric8.kubernetes.client.http.Interceptor;
import io.fabric8.kubernetes.client.http.StandardHttpRequest;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

class KubernetesClientMetricsFabric8InterceptorTest {

    private static final String NAMESPACE = "test-op-ns";
    private static final String NAME = "test-op-name";
    private static final String HOST = "test-op-host";

    private KubernetesClientMetrics kubernetesClientMetrics;
    private StandardHttpRequest.Builder builder;
    private Interceptor.RequestTags emptyTags;
    private Configuration operatorConfig;
    private TestingMetricRegistry registry;

    @BeforeEach
    void setUp() {
        registry = TestingMetricRegistry.builder().build();
        KubernetesOperatorMetricGroup metricGroup =
                KubernetesOperatorMetricGroup.create(
                        registry, new Configuration(), NAMESPACE, NAME, HOST);
        operatorConfig = new Configuration();
        operatorConfig.set(
                KubernetesOperatorMetricOptions
                        .OPERATOR_KUBERNETES_CLIENT_METRICS_HTTP_RESPONSE_CODE_GROUPS_ENABLED,
                true);
        kubernetesClientMetrics =
                new KubernetesClientMetrics(
                        metricGroup, FlinkOperatorConfiguration.fromConfiguration(operatorConfig));
        builder = new StandardHttpRequest.Builder();
        emptyTags =
                new Interceptor.RequestTags() {
                    @Override
                    public <T> T getTag(Class<T> aClass) {
                        return null;
                    }
                };
    }

    @Test
    void shouldCountPostRequest() {
        // Given
        final HttpRequest postRequest =
                builder.post("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);

        // Then
        assertThat(kubernetesClientMetrics.getRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(1L);
        assertThat(kubernetesClientMetrics.getRequestMethodCounter("POST"))
                .extracting(Counter::getCount)
                .isEqualTo(1L);
    }

    @Test
    void shouldCountDeleteRequest() {
        // Given
        final HttpRequest postRequest =
                builder.delete("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);

        // Then
        assertThat(kubernetesClientMetrics.getRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(1L);
        assertThat(kubernetesClientMetrics.getRequestMethodCounter("DELETE"))
                .extracting(Counter::getCount)
                .isEqualTo(1L);
    }

    @Test
    void shouldCountPatchRequest() {
        // Given
        final HttpRequest postRequest =
                builder.patch("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);

        // Then
        assertThat(kubernetesClientMetrics.getRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(1L);
        assertThat(kubernetesClientMetrics.getRequestMethodCounter("PATCH"))
                .extracting(Counter::getCount)
                .isEqualTo(1L);
    }

    @Test
    void shouldTrackRequestRate() {
        // Given
        final HttpRequest postRequest =
                builder.patch("application/json", "{}").uri("/random").build();
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        final OperatorMetricUtils.SynchronizedMeterView requestRateMeter =
                kubernetesClientMetrics.getRequestRateMeter();

        // When
        requestRateMeter.update();

        // Then
        assertThat(requestRateMeter)
                .extracting(OperatorMetricUtils.SynchronizedMeterView::getCount)
                .isEqualTo(3L);
        // MeterView defaults to averaging over 60s, so we expect 3 / 60
        assertThat(requestRateMeter)
                .extracting(OperatorMetricUtils.SynchronizedMeterView::getRate)
                .asInstanceOf(DOUBLE)
                .isCloseTo(0.05, Offset.offset(0.0001));
    }

    @Test
    void shouldCountPostResponses() {
        // Given
        final HttpRequest postRequest =
                builder.post("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.after(
                postRequest,
                new StubHttpResponse(postRequest, Map.of(), 200),
                (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getResponseCounter())
                .extracting(Counter::getCount)
                .isEqualTo(1L);
    }

    @Test
    void shouldCountDeleteResponse() {
        // Given
        final HttpRequest deleteRequest =
                builder.delete("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.after(
                deleteRequest,
                new StubHttpResponse(deleteRequest, Map.of(), 200),
                (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getResponseCounter())
                .extracting(Counter::getCount)
                .isEqualTo(1L);
    }

    @Test
    void shouldCountPatchResponse() {
        // Given
        final HttpRequest patchRequest =
                builder.patch("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.after(
                patchRequest,
                new StubHttpResponse(patchRequest, Map.of(), 200),
                (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getResponseCounter())
                .extracting(Counter::getCount)
                .isEqualTo(1L);
    }

    @ParameterizedTest
    @MethodSource(value = "responseCodes")
    void shouldCountEachResponseCode(int statusCode) {
        // Given
        final HttpRequest patchRequest =
                builder.patch("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.after(
                patchRequest,
                new StubHttpResponse(patchRequest, Map.of(), statusCode),
                (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getResponseCodeMeter(statusCode))
                .extracting(OperatorMetricUtils.SynchronizedMeterView::getCount)
                .isEqualTo(1L);
    }

    @ParameterizedTest
    @MethodSource(value = "responseCodes")
    void shouldCountStatusCodesByResponseGroup(int statusCode) {
        // Given
        final HttpRequest patchRequest =
                builder.patch("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.after(
                patchRequest,
                new StubHttpResponse(patchRequest, Map.of(), statusCode),
                (value, asyncBody) -> {});

        // Then
        final List<OperatorMetricUtils.SynchronizedMeterView> responseCodeGroupMeters =
                kubernetesClientMetrics.getResponseCodeGroupMeters();
        final int groupIndex =
                Integer.parseInt(String.valueOf(String.valueOf(statusCode).charAt(0))) - 1;
        assertThat(responseCodeGroupMeters.get(groupIndex))
                .extracting(OperatorMetricUtils.SynchronizedMeterView::getCount)
                .isEqualTo(1L);
    }

    @ParameterizedTest
    @MethodSource(value = "responseCodes")
    void shouldNotCountStatusCodesByResponseGroupWhenDisabled(int statusCode) {
        // Given
        operatorConfig.set(
                KubernetesOperatorMetricOptions
                        .OPERATOR_KUBERNETES_CLIENT_METRICS_HTTP_RESPONSE_CODE_GROUPS_ENABLED,
                false);
        kubernetesClientMetrics =
                new KubernetesClientMetrics(
                        KubernetesOperatorMetricGroup.create(
                                registry, new Configuration(), NAMESPACE, NAME, HOST),
                        FlinkOperatorConfiguration.fromConfiguration(operatorConfig));
        final HttpRequest patchRequest =
                builder.patch("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.after(
                patchRequest,
                new StubHttpResponse(patchRequest, Map.of(), statusCode),
                (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getResponseCodeGroupMeters()).isEmpty();
    }

    @Test
    void shouldTrackRequestLatency() {
        // Given
        long[] currentTime = {0L};
        kubernetesClientMetrics =
                new KubernetesClientMetrics(
                        KubernetesOperatorMetricGroup.create(
                                registry, new Configuration(), NAMESPACE, NAME, HOST),
                        FlinkOperatorConfiguration.fromConfiguration(operatorConfig),
                        () -> currentTime[0]);
        final HttpRequest postRequest =
                builder.post("application/json", "{}").uri("/random").build();
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        final Histogram responseLatency = kubernetesClientMetrics.getResponseLatency();
        assumeThat(responseLatency).extracting(Histogram::getCount).isEqualTo(0L);
        currentTime[0] += 1000L;

        // When
        kubernetesClientMetrics.after(
                postRequest,
                new StubHttpResponse(postRequest, Map.of(), 200),
                (value, asyncBody) -> {});

        // Then
        assertThat(responseLatency).extracting(Histogram::getCount).isEqualTo(1L);
        assertThat(responseLatency)
                .extracting(Histogram::getStatistics)
                .extracting(HistogramStatistics::getMax)
                .asInstanceOf(InstanceOfAssertFactories.LONG)
                .isEqualTo(1000L);
    }

    @Test
    void shouldTrackRequestLatencyForFailedConnection() {
        // Given
        long[] currentTime = {0L};
        kubernetesClientMetrics =
                new KubernetesClientMetrics(
                        KubernetesOperatorMetricGroup.create(
                                registry, new Configuration(), NAMESPACE, NAME, HOST),
                        FlinkOperatorConfiguration.fromConfiguration(operatorConfig),
                        () -> currentTime[0]);
        final HttpRequest postRequest =
                builder.post("application/json", "{}").uri("/random").build();
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        final Histogram responseLatency = kubernetesClientMetrics.getResponseLatency();
        assumeThat(responseLatency).extracting(Histogram::getCount).isEqualTo(0L);
        currentTime[0] += 1000L;

        // When
        kubernetesClientMetrics.afterConnectionFailure(postRequest, new RuntimeException("kaboom"));

        // Then
        assertThat(responseLatency).extracting(Histogram::getCount).isEqualTo(1L);
        assertThat(responseLatency)
                .extracting(Histogram::getStatistics)
                .extracting(HistogramStatistics::getMax)
                .asInstanceOf(InstanceOfAssertFactories.LONG)
                .isEqualTo(1000L);
    }

    @Test
    void shouldTrackResponseAsSlowResponseAboveThreshold() {
        // Given
        long[] currentTime = {0L};
        kubernetesClientMetrics =
                new KubernetesClientMetrics(
                        KubernetesOperatorMetricGroup.create(
                                registry, new Configuration(), NAMESPACE, NAME, HOST),
                        FlinkOperatorConfiguration.fromConfiguration(operatorConfig),
                        () -> currentTime[0]);
        final HttpRequest postRequest =
                builder.post("application/json", "{}").uri("/random").build();
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        assertThat(kubernetesClientMetrics.getSlowRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(0L);
        currentTime[0] += kubernetesClientMetrics.getSlowRequestThreshold().toNanos() + 1L;

        // When
        kubernetesClientMetrics.after(
                postRequest,
                new StubHttpResponse(postRequest, Map.of(), 200),
                (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getSlowRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(1L);
    }

    @Test
    void shouldNotTrackResponseAsSlowResponseBelowThreshold() {
        // Given
        long[] currentTime = {0L};
        kubernetesClientMetrics =
                new KubernetesClientMetrics(
                        KubernetesOperatorMetricGroup.create(
                                registry, new Configuration(), NAMESPACE, NAME, HOST),
                        FlinkOperatorConfiguration.fromConfiguration(operatorConfig),
                        () -> currentTime[0]);
        final HttpRequest postRequest =
                builder.post("application/json", "{}").uri("/random").build();
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        assertThat(kubernetesClientMetrics.getSlowRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(0L);
        currentTime[0] += kubernetesClientMetrics.getSlowRequestThreshold().toNanos() - 1L;

        // When
        kubernetesClientMetrics.after(
                postRequest,
                new StubHttpResponse(postRequest, Map.of(), 200),
                (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getSlowRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(0L);
    }

    @Test
    void shouldTrackResponseForFailedConnectionAsSlowResponseAboveThreshold() {
        // Given
        long[] currentTime = {0L};
        kubernetesClientMetrics =
                new KubernetesClientMetrics(
                        KubernetesOperatorMetricGroup.create(
                                registry, new Configuration(), NAMESPACE, NAME, HOST),
                        FlinkOperatorConfiguration.fromConfiguration(operatorConfig),
                        () -> currentTime[0]);
        final HttpRequest postRequest =
                builder.post("application/json", "{}").uri("/random").build();
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        assertThat(kubernetesClientMetrics.getSlowRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(0L);
        currentTime[0] += kubernetesClientMetrics.getSlowRequestThreshold().toNanos() + 1L;

        // When
        kubernetesClientMetrics.afterConnectionFailure(postRequest, new RuntimeException("kaboom"));

        // Then
        assertThat(kubernetesClientMetrics.getSlowRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(1L);
    }

    @Test
    void shouldNotTrackResponseForFailedConnectionAsSlowResponseUnderThreshold() {
        // Given
        long[] currentTime = {0L};
        kubernetesClientMetrics =
                new KubernetesClientMetrics(
                        KubernetesOperatorMetricGroup.create(
                                registry, new Configuration(), NAMESPACE, NAME, HOST),
                        FlinkOperatorConfiguration.fromConfiguration(operatorConfig),
                        () -> currentTime[0]);
        final HttpRequest postRequest =
                builder.post("application/json", "{}").uri("/random").build();
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        assertThat(kubernetesClientMetrics.getSlowRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(0L);
        currentTime[0] += kubernetesClientMetrics.getSlowRequestThreshold().toNanos() - 1L;

        // When
        kubernetesClientMetrics.afterConnectionFailure(postRequest, new RuntimeException("kaboom"));

        // Then
        assertThat(kubernetesClientMetrics.getSlowRequestCounter())
                .extracting(Counter::getCount)
                .isEqualTo(0L);
    }

    @Test
    void shouldTrackFailedRequests() {
        // Given
        final HttpRequest postRequest =
                builder.post("application/json", "{}").uri("/random").build();
        kubernetesClientMetrics.afterFailure(
                builder, new StubHttpResponse(postRequest, Map.of(), 500), emptyTags);
        kubernetesClientMetrics.afterFailure(
                builder, new StubHttpResponse(postRequest, Map.of(), 500), emptyTags);
        kubernetesClientMetrics.afterFailure(
                builder, new StubHttpResponse(postRequest, Map.of(), 500), emptyTags);
        final OperatorMetricUtils.SynchronizedMeterView requestFailedRateMeter =
                kubernetesClientMetrics.getRequestFailedRateMeter();

        // When
        requestFailedRateMeter.update();

        // Then
        assertThat(requestFailedRateMeter)
                .extracting(OperatorMetricUtils.SynchronizedMeterView::getCount)
                .isEqualTo(3L);
        // MeterView defaults to averaging over 60s, so we expect 3 / 60
        assertThat(requestFailedRateMeter)
                .extracting(OperatorMetricUtils.SynchronizedMeterView::getRate)
                .asInstanceOf(DOUBLE)
                .isCloseTo(0.05, Offset.offset(0.0001));
    }

    // Technically this is a super set of all valid HTTP status codes
    // I couldn't find a definitive list of the codes used byt kubernetes API server so thought, it
    // best to cover the full range.
    static Stream<Arguments> responseCodes() {
        return IntStream.rangeClosed(100, 505).mapToObj(Arguments::of);
    }

    private static class StubHttpResponse implements HttpResponse<String> {
        private final HttpRequest request;
        private final Map<String, List<String>> headers;
        private final int statusCode;

        public StubHttpResponse(
                HttpRequest request, Map<String, List<String>> headers, int statusCode) {
            this.request = request;
            this.headers = headers;
            this.statusCode = statusCode;
        }

        @Override
        public int code() {
            return statusCode;
        }

        @Override
        public String body() {
            return "";
        }

        @Override
        public HttpRequest request() {
            return request;
        }

        @Override
        public Optional<HttpResponse<?>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public List<String> headers(String key) {
            return headers.get(key);
        }

        @Override
        public Map<String, List<String>> headers() {
            return headers;
        }
    }
}
