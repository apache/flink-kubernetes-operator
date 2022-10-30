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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** FlinkSessionJob metrics. */
public class FlinkSessionJobMetrics implements CustomResourceMetrics<FlinkSessionJob> {

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;
    private final Map<String, Set<String>> sessionJobs = new ConcurrentHashMap<>();
    public static final String COUNTER_NAME = "Count";

    public FlinkSessionJobMetrics(
            KubernetesOperatorMetricGroup parentMetricGroup, Configuration configuration) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
    }

    public void onUpdate(FlinkSessionJob sessionJob) {
        onRemove(sessionJob);
        sessionJobs
                .computeIfAbsent(
                        sessionJob.getMetadata().getNamespace(),
                        ns -> {
                            initNamespaceSessionJobCounts(ns);
                            return ConcurrentHashMap.newKeySet();
                        })
                .add(sessionJob.getMetadata().getName());
    }

    public void onRemove(FlinkSessionJob sessionJob) {
        if (!sessionJobs.containsKey(sessionJob.getMetadata().getNamespace())) {
            return;
        }
        sessionJobs
                .get(sessionJob.getMetadata().getNamespace())
                .remove(sessionJob.getMetadata().getName());
    }

    private void initNamespaceSessionJobCounts(String ns) {
        parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkSessionJob.class, ns)
                .gauge(COUNTER_NAME, () -> sessionJobs.get(ns).size());
    }
}
