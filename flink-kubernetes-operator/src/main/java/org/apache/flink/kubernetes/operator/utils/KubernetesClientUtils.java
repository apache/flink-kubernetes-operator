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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kubernetes client utils. */
public class KubernetesClientUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesClientUtils.class);

    public static KubernetesClient getKubernetesClient(
            FlinkOperatorConfiguration operatorConfig, MetricGroup metricGroup) {
        return getKubernetesClient(operatorConfig, metricGroup, null);
    }

    @VisibleForTesting
    public static KubernetesClient getKubernetesClient(
            FlinkOperatorConfiguration operatorConfig,
            MetricGroup metricGroup,
            Config kubernetesClientConfig) {

        var clientBuilder = new KubernetesClientBuilder().withConfig(kubernetesClientConfig);

        if (operatorConfig.isKubernetesClientMetricsEnabled()) {
            clientBuilder =
                    clientBuilder.withHttpClientFactory(
                            // This logic should be replaced with a more generic solution once the
                            // fabric8 Interceptor class is improved to the point where this can be
                            // implemented.
                            new OkHttpClientFactory() {
                                @Override
                                protected void additionalConfig(OkHttpClient.Builder builder) {
                                    builder.addInterceptor(
                                            new KubernetesClientMetrics(
                                                    metricGroup, operatorConfig));
                                }
                            });
        }

        return clientBuilder.build();
    }
}
