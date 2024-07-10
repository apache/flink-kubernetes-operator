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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils.SynchronizedMeterView;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

import io.fabric8.kubernetes.client.http.AsyncBody;
import io.fabric8.kubernetes.client.http.HttpRequest;
import io.fabric8.kubernetes.client.http.HttpResponse;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

/** Kubernetes client metrics. */
public class KubernetesClientMetrics
        implements Interceptor, io.fabric8.kubernetes.client.http.Interceptor {

    public static final String KUBE_CLIENT_GROUP = "KubeClient";
    public static final String HTTP_REQUEST_GROUP = "HttpRequest";
    public static final String HTTP_REQUEST_FAILED_GROUP = "Failed";
    public static final String HTTP_RESPONSE_GROUP = "HttpResponse";
    public static final String HTTP_RESPONSE_1XX = "1xx";
    public static final String HTTP_RESPONSE_2XX = "2xx";
    public static final String HTTP_RESPONSE_3XX = "3xx";
    public static final String HTTP_RESPONSE_4XX = "4xx";
    public static final String HTTP_RESPONSE_5XX = "5xx";
    public static final String COUNTER = "Count";
    public static final String METER = "NumPerSecond";
    public static final String HISTO = "TimeNanos";
    private final Histogram responseLatency;

    private final MetricGroup requestMetricGroup;
    private final MetricGroup failedRequestMetricGroup;
    private final MetricGroup responseMetricGroup;

    private final Counter requestCounter;
    private final Counter failedRequestCounter;
    private final Counter responseCounter;

    private final SynchronizedMeterView requestRateMeter;
    private final SynchronizedMeterView requestFailedRateMeter;
    private final SynchronizedMeterView responseRateMeter;

    private final boolean httpResponseCodeGroupsEnabled;
    private final List<SynchronizedMeterView> responseCodeGroupMeters = new ArrayList<>(5);
    private final Map<Integer, SynchronizedMeterView> responseCodeMeters =
            new ConcurrentHashMap<>();
    private final Map<String, Counter> requestMethodCounter = new ConcurrentHashMap<>();
    private final LongSupplier nanoTimeSource;
    private final ConcurrentMap<UUID, Long> requestStartTimes;

    private final Logger logger = LoggerFactory.getLogger(KubernetesClientMetrics.class);

    public KubernetesClientMetrics(
            MetricGroup parentGroup, FlinkOperatorConfiguration flinkOperatorConfiguration) {
        this(parentGroup, flinkOperatorConfiguration, System::nanoTime);
    }

    public KubernetesClientMetrics(
            MetricGroup parentGroup,
            FlinkOperatorConfiguration flinkOperatorConfiguration,
            LongSupplier nanoTimeSource) {
        this.nanoTimeSource = nanoTimeSource;
        MetricGroup metricGroup = parentGroup.addGroup(KUBE_CLIENT_GROUP);

        this.requestMetricGroup = metricGroup.addGroup(HTTP_REQUEST_GROUP);
        this.failedRequestMetricGroup = requestMetricGroup.addGroup(HTTP_REQUEST_FAILED_GROUP);
        this.responseMetricGroup = metricGroup.addGroup(HTTP_RESPONSE_GROUP);

        this.requestCounter =
                OperatorMetricUtils.synchronizedCounter(requestMetricGroup.counter(COUNTER));
        this.failedRequestCounter =
                OperatorMetricUtils.synchronizedCounter(failedRequestMetricGroup.counter(COUNTER));
        this.responseCounter =
                OperatorMetricUtils.synchronizedCounter(responseMetricGroup.counter(COUNTER));

        this.requestRateMeter =
                OperatorMetricUtils.synchronizedMeterView(
                        requestMetricGroup.meter(METER, new MeterView(requestCounter)));
        this.requestFailedRateMeter =
                OperatorMetricUtils.synchronizedMeterView(
                        failedRequestMetricGroup.meter(METER, new MeterView(failedRequestCounter)));
        this.responseRateMeter =
                OperatorMetricUtils.synchronizedMeterView(
                        responseMetricGroup.meter(METER, new MeterView(responseCounter)));

        this.responseLatency =
                responseMetricGroup.histogram(
                        HISTO, OperatorMetricUtils.createHistogram(flinkOperatorConfiguration));

        this.httpResponseCodeGroupsEnabled =
                flinkOperatorConfiguration.isKubernetesClientMetricsHttpResponseCodeGroupsEnabled();
        if (this.httpResponseCodeGroupsEnabled) {
            this.responseCodeGroupMeters.add(
                    createMeterViewForMetricsGroup(
                            responseMetricGroup.addGroup(HTTP_RESPONSE_1XX)));
            this.responseCodeGroupMeters.add(
                    createMeterViewForMetricsGroup(
                            responseMetricGroup.addGroup(HTTP_RESPONSE_2XX)));
            this.responseCodeGroupMeters.add(
                    createMeterViewForMetricsGroup(
                            responseMetricGroup.addGroup(HTTP_RESPONSE_3XX)));
            this.responseCodeGroupMeters.add(
                    createMeterViewForMetricsGroup(
                            responseMetricGroup.addGroup(HTTP_RESPONSE_4XX)));
            this.responseCodeGroupMeters.add(
                    createMeterViewForMetricsGroup(
                            responseMetricGroup.addGroup(HTTP_RESPONSE_5XX)));
        }
        this.requestStartTimes = new ConcurrentHashMap<>();
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        updateRequestMetrics(request);
        Response response = null;
        final long startTime = nanoTimeSource.getAsLong();
        try {
            response = chain.proceed(request);
            return response;
        } finally {
            updateResponseMetrics(response, startTime);
        }
    }

    @Override
    public AsyncBody.Consumer<List<ByteBuffer>> consumer(
            AsyncBody.Consumer<List<ByteBuffer>> consumer, HttpRequest request) {
        final Long original = requestStartTimes.put(request.id(), nanoTimeSource.getAsLong());
        if (original != null) {
            logger.warn(
                    "Duplicate request ID's detected. Latency will only be tracked for the latest");
        }
        updateRequestMetrics(request);
        return consumer;
    }

    @Override
    public void after(
            HttpRequest request,
            HttpResponse<?> response,
            AsyncBody.Consumer<List<ByteBuffer>> consumer) {
        trackRequestLatency(request.id());
        updateResponseMetrics(response);
    }

    @Override
    public CompletableFuture<Boolean> afterFailure(
            HttpRequest.Builder builder, HttpResponse<?> response, RequestTags tags) {
        this.requestFailedRateMeter.markEvent();
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public void afterConnectionFailure(HttpRequest request, Throwable failure) {
        trackRequestLatency(request.id());
        this.requestFailedRateMeter.markEvent();
    }

    @VisibleForTesting
    Counter getRequestCounter() {
        return requestCounter;
    }

    @VisibleForTesting
    Counter getResponseCounter() {
        return responseCounter;
    }

    @VisibleForTesting
    Counter getRequestMethodCounter(String method) {
        return requestMethodCounter.get(method);
    }

    @VisibleForTesting
    SynchronizedMeterView getRequestRateMeter() {
        return requestRateMeter;
    }

    @VisibleForTesting
    SynchronizedMeterView getResponseCodeMeter(int statusCode) {
        return responseCodeMeters.get(statusCode);
    }

    @VisibleForTesting
    List<SynchronizedMeterView> getResponseCodeGroupMeters() {
        return responseCodeGroupMeters;
    }

    @VisibleForTesting
    Histogram getResponseLatency() {
        return responseLatency;
    }

    @VisibleForTesting
    SynchronizedMeterView getRequestFailedRateMeter() {
        return requestFailedRateMeter;
    }

    private void updateRequestMetrics(Request request) {
        this.requestRateMeter.markEvent();
        getCounterByRequestMethod(request.method()).inc();
    }

    private void updateRequestMetrics(HttpRequest request) {
        this.requestRateMeter.markEvent();
        getCounterByRequestMethod(request.method()).inc();
    }

    private void updateResponseMetrics(Response response, long startTimeNanos) {
        final long latency = nanoTimeSource.getAsLong() - startTimeNanos;
        if (response != null) {
            this.responseRateMeter.markEvent();
            this.responseLatency.update(latency);
            getMeterViewByResponseCode(response.code()).markEvent();
            if (this.httpResponseCodeGroupsEnabled) {
                responseCodeGroupMeters.get(response.code() / 100 - 1).markEvent();
            }
        } else {
            this.requestFailedRateMeter.markEvent();
        }
    }

    private void updateResponseMetrics(HttpResponse<?> response) {
        if (response != null) {
            this.responseRateMeter.markEvent();
            getMeterViewByResponseCode(response.code()).markEvent();
            if (this.httpResponseCodeGroupsEnabled) {
                responseCodeGroupMeters.get(response.code() / 100 - 1).markEvent();
            }
        } else {
            this.requestFailedRateMeter.markEvent();
        }
    }

    private void trackRequestLatency(UUID requestId) {
        final Optional<Long> computedLatency =
                Optional.ofNullable(
                        requestStartTimes.computeIfPresent(
                                requestId,
                                (uuid, startTime) -> nanoTimeSource.getAsLong() - startTime));
        final long latency = computedLatency.orElse(0L);
        this.responseLatency.update(latency);
        requestStartTimes.remove(requestId);
    }

    private Counter getCounterByRequestMethod(String method) {
        return requestMethodCounter.computeIfAbsent(
                method,
                key ->
                        OperatorMetricUtils.synchronizedCounter(
                                requestMetricGroup.addGroup(key).counter(COUNTER)));
    }

    private SynchronizedMeterView getMeterViewByResponseCode(int code) {
        return responseCodeMeters.computeIfAbsent(
                code, key -> createMeterViewForMetricsGroup(responseMetricGroup.addGroup(key)));
    }

    private SynchronizedMeterView createMeterViewForMetricsGroup(MetricGroup metricGroup) {
        return OperatorMetricUtils.synchronizedMeterView(
                metricGroup.meter(
                        METER,
                        new MeterView(
                                OperatorMetricUtils.synchronizedCounter(
                                        metricGroup.counter(COUNTER)))));
    }
}
