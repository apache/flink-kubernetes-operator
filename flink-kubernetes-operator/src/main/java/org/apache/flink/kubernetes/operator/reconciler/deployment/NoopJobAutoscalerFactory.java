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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.Map;

/** An autoscaler implementation which does nothing. */
public class NoopJobAutoscalerFactory implements JobAutoScalerFactory, JobAutoScaler {

    @Override
    public JobAutoScaler create(KubernetesClient kubernetesClient, EventRecorder eventRecorder) {
        return this;
    }

    @Override
    public boolean scale(FlinkResourceContext<?> ctx) {
        return false;
    }

    @Override
    public void cleanup(FlinkResourceContext<?> ctx) {}

    @Override
    public Map<String, String> getParallelismOverrides(FlinkResourceContext<?> ctx) {
        return Map.of();
    }
}
