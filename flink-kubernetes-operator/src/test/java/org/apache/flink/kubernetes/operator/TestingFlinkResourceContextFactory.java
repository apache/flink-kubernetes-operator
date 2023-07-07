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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.metrics.KubernetesResourceMetricGroup;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import java.util.Map;

/** Flink service factory mock for tests. */
public class TestingFlinkResourceContextFactory extends FlinkResourceContextFactory {
    private final FlinkService flinkService;

    public TestingFlinkResourceContextFactory(
            KubernetesClient kubernetesClient,
            FlinkConfigManager configManager,
            KubernetesOperatorMetricGroup operatorMetricGroup,
            FlinkService flinkService,
            EventRecorder eventRecorder) {
        super(kubernetesClient, configManager, operatorMetricGroup, eventRecorder);
        this.flinkService = flinkService;
    }

    @Override
    protected FlinkService getFlinkService(FlinkResourceContext<?> ctx) {
        return flinkService;
    }

    public Map<Tuple2<Class<?>, ResourceID>, KubernetesResourceMetricGroup> getMetricGroups() {
        return resourceMetricGroups;
    }
}
