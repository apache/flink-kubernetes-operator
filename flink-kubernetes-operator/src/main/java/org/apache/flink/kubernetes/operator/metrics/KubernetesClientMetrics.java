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

import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils.SynchronizedMeterView;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Kubernetes client metrics. */
public class KubernetesClientMetrics implements Interceptor {

    public static final String KUBE_CLIENT_GROUP = "KubeClient";
    public static final String HTTP_REQUEST_GROUP = "HttpRequest";
    public static final String HTTP_REQUEST_FAILED_GROUP = "Failed";
    public static final String HTTP_RESPONSE_GROUP = "HttpResponse";
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

    private final Map<Integer, Counter> responseCodeCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> requestMethodCounter = new ConcurrentHashMap<>();

    public KubernetesClientMetrics(
            MetricGroup parentGroup, FlinkOperatorConfiguration flinkOperatorConfiguration) {
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

        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::updateMeters, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        updateRequestMetrics(request);
        Response response = null;
        final long startTime = System.nanoTime();
        try {
            response = chain.proceed(request);
            return response;
        } finally {
            updateResponseMetrics(response, startTime);
        }
    }

    private void updateRequestMetrics(Request request) {
        this.requestRateMeter.markEvent();
        getCounterByRequestMethod(request.method()).inc();
    }

    private void updateResponseMetrics(Response response, long startTimeNanos) {
        final long latency = System.nanoTime() - startTimeNanos;
        if (response != null) {
            this.responseRateMeter.markEvent();
            this.responseLatency.update(latency);
            getCounterByResponseCode(response.code()).inc();
        } else {
            this.requestFailedRateMeter.markEvent();
        }
    }

    private Counter getCounterByRequestMethod(String method) {
        return requestMethodCounter.computeIfAbsent(
                method,
                key ->
                        OperatorMetricUtils.synchronizedCounter(
                                requestMetricGroup.addGroup(key).counter(COUNTER)));
    }

    private Counter getCounterByResponseCode(int code) {
        return responseCodeCounters.computeIfAbsent(
                code,
                key ->
                        OperatorMetricUtils.synchronizedCounter(
                                responseMetricGroup.addGroup(key).counter(COUNTER)));
    }

    private void updateMeters() {
        this.requestRateMeter.update();
        this.requestFailedRateMeter.update();
        this.responseRateMeter.update();
    }
}
