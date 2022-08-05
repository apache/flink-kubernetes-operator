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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** FlinkDeployment metrics. */
public class FlinkDeploymentMetrics implements CustomResourceMetrics<FlinkDeployment> {

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;
    private final Map<String, Map<JobManagerDeploymentStatus, Set<String>>> deployments =
            new ConcurrentHashMap<>();
    public static final String STATUS_GROUP_NAME = "JmDeploymentStatus";
    public static final String COUNTER_NAME = "Count";

    public FlinkDeploymentMetrics(
            KubernetesOperatorMetricGroup parentMetricGroup, Configuration configuration) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
    }

    public void onUpdate(FlinkDeployment flinkApp) {
        onRemove(flinkApp);
        deployments
                .computeIfAbsent(
                        flinkApp.getMetadata().getNamespace(),
                        ns -> {
                            initNamespaceDeploymentCounts(ns);
                            initNamespaceStatusCounts(ns);
                            return createDeploymentStatusMap();
                        })
                .get(flinkApp.getStatus().getJobManagerDeploymentStatus())
                .add(flinkApp.getMetadata().getName());
    }

    public void onRemove(FlinkDeployment flinkApp) {
        if (!deployments.containsKey(flinkApp.getMetadata().getNamespace())) {
            return;
        }
        deployments
                .get(flinkApp.getMetadata().getNamespace())
                .values()
                .forEach(names -> names.remove(flinkApp.getMetadata().getName()));
    }

    private void initNamespaceDeploymentCounts(String ns) {
        parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkDeployment.class, ns)
                .gauge(
                        COUNTER_NAME,
                        () -> deployments.get(ns).values().stream().mapToInt(Set::size).sum());
    }

    private void initNamespaceStatusCounts(String ns) {
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            parentMetricGroup
                    .createResourceNamespaceGroup(configuration, FlinkDeployment.class, ns)
                    .addGroup(STATUS_GROUP_NAME)
                    .addGroup(status.toString())
                    .gauge(COUNTER_NAME, () -> deployments.get(ns).get(status).size());
        }
    }

    private Map<JobManagerDeploymentStatus, Set<String>> createDeploymentStatusMap() {
        Map<JobManagerDeploymentStatus, Set<String>> statuses = new ConcurrentHashMap<>();
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            statuses.put(status, ConcurrentHashMap.newKeySet());
        }
        return statuses;
    }
}
