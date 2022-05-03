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

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** FlinkDeployment metrics. */
public class FlinkDeploymentMetrics implements CustomResourceMetrics<FlinkDeployment> {

    private final Map<JobManagerDeploymentStatus, Set<String>> statuses = new HashMap<>();
    public static final String METRIC_GROUP_NAME = "FlinkDeployment";

    public FlinkDeploymentMetrics(MetricGroup parentMetricGroup) {
        MetricGroup flinkDeploymentMetrics = parentMetricGroup.addGroup(METRIC_GROUP_NAME);
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            statuses.put(status, ConcurrentHashMap.newKeySet());
        }
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            statuses.put(status, new HashSet<>());
            MetricGroup metricGroup = flinkDeploymentMetrics.addGroup(status.toString());
            metricGroup.gauge("Count", () -> statuses.get(status).size());
        }
        flinkDeploymentMetrics.gauge(
                "Count", () -> statuses.values().stream().mapToInt(Set::size).sum());
    }

    public void onUpdate(FlinkDeployment flinkApp) {
        onRemove(flinkApp);
        statuses.get(flinkApp.getStatus().getJobManagerDeploymentStatus())
                .add(flinkApp.getMetadata().getName());
    }

    public void onRemove(FlinkDeployment flinkApp) {
        statuses.values()
                .forEach(
                        deployments -> {
                            deployments.remove(flinkApp.getMetadata().getName());
                        });
    }
}
