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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.metrics.KubernetesClientMetrics;
import org.apache.flink.metrics.MetricGroup;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory;
import io.fabric8.kubernetes.client.okhttp.OkHttpClientImpl;

/** Kubernetes client utils. */
public class KubernetesClientUtils {

    public static KubernetesClient getKubernetesClient(
            FlinkOperatorConfiguration operatorConfig, MetricGroup metricGroup) {
        return getKubernetesClient(
                operatorConfig, metricGroup, new DefaultKubernetesClient().getConfiguration());
    }

    @VisibleForTesting
    public static KubernetesClient getKubernetesClient(
            FlinkOperatorConfiguration operatorConfig,
            MetricGroup metricGroup,
            Config kubernetesClientConfig) {
        var httpClientBuilder =
                new OkHttpClientFactory()
                        .createHttpClient(kubernetesClientConfig)
                        .getOkHttpClient()
                        .newBuilder();
        if (operatorConfig.isKubernetesClientMetricsEnabled()) {
            httpClientBuilder.addInterceptor(
                    new KubernetesClientMetrics(metricGroup, operatorConfig));
        }
        return new DefaultKubernetesClient(
                new OkHttpClientImpl(httpClientBuilder.build()), kubernetesClientConfig);
    }
}
