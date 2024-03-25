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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.AbstractFlinkService;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.util.StringUtils;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** FlinkDeployment metrics. */
public class FlinkDeploymentMetrics implements CustomResourceMetrics<FlinkDeployment> {

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;

    // map(namespace, map(status, set(deployment))
    private final Map<String, Map<JobManagerDeploymentStatus, Set<String>>> deploymentStatuses =
            new ConcurrentHashMap<>();
    // map(namespace, map(version, set(deployment)))
    private final Map<String, Map<String, Set<String>>> deploymentFlinkVersions =
            new ConcurrentHashMap<>();
    // map(namespace, map(version, set(deployment)))
    private final Map<String, Map<String, Set<String>>> deploymentFlinkMinorVersions =
            new ConcurrentHashMap<>();
    // map(namespace, map(deployment, cpu))
    private final Map<String, Map<String, Double>> deploymentCpuUsage = new ConcurrentHashMap<>();
    // map(namespace, map(deployment, memory))
    private final Map<String, Map<String, Long>> deploymentMemoryUsage = new ConcurrentHashMap<>();
    public static final String FLINK_VERSION_GROUP_NAME = "FlinkVersion";
    public static final String FLINK_MINOR_VERSION_GROUP_NAME = "FlinkMinorVersion";
    public static final String UNKNOWN_VERSION = "UNKNOWN";
    public static final String MALFORMED_MINOR_VERSION = "MALFORMED";
    public static final String STATUS_GROUP_NAME = "JmDeploymentStatus";
    public static final String RESOURCE_USAGE_GROUP_NAME = "ResourceUsage";
    public static final String COUNTER_NAME = "Count";
    public static final String CPU_NAME = "Cpu";
    public static final String MEMORY_NAME = "Memory";

    public FlinkDeploymentMetrics(
            KubernetesOperatorMetricGroup parentMetricGroup, Configuration configuration) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
    }

    public void onUpdate(FlinkDeployment flinkApp) {
        onRemove(flinkApp);

        var namespace = flinkApp.getMetadata().getNamespace();
        var clusterInfo = flinkApp.getStatus().getClusterInfo();
        var deploymentName = flinkApp.getMetadata().getName();

        deploymentStatuses
                .computeIfAbsent(
                        namespace,
                        ns -> {
                            initNamespaceDeploymentCounts(ns);
                            initNamespaceStatusCounts(ns);
                            return createDeploymentStatusMap();
                        })
                .get(flinkApp.getStatus().getJobManagerDeploymentStatus())
                .add(deploymentName);

        // Full runtime version queried from the JobManager REST API
        var flinkVersion =
                flinkApp.getStatus()
                        .getClusterInfo()
                        .getOrDefault(DashboardConfiguration.FIELD_NAME_FLINK_VERSION, "");
        if (StringUtils.isNullOrWhitespaceOnly(flinkVersion)) {
            flinkVersion = UNKNOWN_VERSION;
        }
        deploymentFlinkVersions
                .computeIfAbsent(namespace, ns -> new ConcurrentHashMap<>())
                .computeIfAbsent(
                        flinkVersion,
                        v -> {
                            initFlinkVersions(namespace, v);
                            return ConcurrentHashMap.newKeySet();
                        })
                .add(deploymentName);

        // Minor version computed from the above
        var subVersions = flinkVersion.split("\\.");
        String minorVersion = MALFORMED_MINOR_VERSION;
        if (subVersions.length >= 2) {
            minorVersion = subVersions[0].concat(".").concat(subVersions[1]);
        }
        deploymentFlinkMinorVersions
                .computeIfAbsent(namespace, ns -> new ConcurrentHashMap<>())
                .computeIfAbsent(
                        minorVersion,
                        v -> {
                            initFlinkMinorVersions(namespace, v);
                            return ConcurrentHashMap.newKeySet();
                        })
                .add(deploymentName);

        var totalCpu =
                NumberUtils.toDouble(
                        clusterInfo.getOrDefault(AbstractFlinkService.FIELD_NAME_TOTAL_CPU, "0"));
        if (!Double.isFinite(totalCpu)) {
            totalCpu = 0;
        }
        deploymentCpuUsage
                .computeIfAbsent(
                        namespace,
                        ns -> {
                            initNamespaceCpuUsage(ns);
                            return new ConcurrentHashMap<>();
                        })
                .put(deploymentName, totalCpu);

        deploymentMemoryUsage
                .computeIfAbsent(
                        namespace,
                        ns -> {
                            initNamespaceMemoryUsage(ns);
                            return new ConcurrentHashMap<>();
                        })
                .put(
                        deploymentName,
                        NumberUtils.toLong(
                                clusterInfo.getOrDefault(
                                        AbstractFlinkService.FIELD_NAME_TOTAL_MEMORY, "0")));
    }

    public void onRemove(FlinkDeployment flinkApp) {
        var namespace = flinkApp.getMetadata().getNamespace();
        var name = flinkApp.getMetadata().getName();

        if (deploymentStatuses.containsKey(namespace)) {
            deploymentStatuses.get(namespace).values().forEach(names -> names.remove(name));
        }
        if (deploymentFlinkVersions.containsKey(namespace)) {
            deploymentFlinkVersions.get(namespace).values().forEach(names -> names.remove(name));
        }
        if (deploymentFlinkMinorVersions.containsKey(namespace)) {
            deploymentFlinkMinorVersions
                    .get(namespace)
                    .values()
                    .forEach(names -> names.remove(name));
        }
        if (deploymentCpuUsage.containsKey(namespace)) {
            deploymentCpuUsage.get(namespace).remove(name);
        }
        if (deploymentMemoryUsage.containsKey(namespace)) {
            deploymentMemoryUsage.get(namespace).remove(name);
        }
    }

    private void initNamespaceDeploymentCounts(String ns) {
        parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkDeployment.class, ns)
                .gauge(
                        COUNTER_NAME,
                        () ->
                                deploymentStatuses.get(ns).values().stream()
                                        .mapToInt(Set::size)
                                        .sum());
    }

    private void initNamespaceStatusCounts(String ns) {
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            parentMetricGroup
                    .createResourceNamespaceGroup(configuration, FlinkDeployment.class, ns)
                    .addGroup(STATUS_GROUP_NAME)
                    .addGroup(status.toString())
                    .gauge(COUNTER_NAME, () -> deploymentStatuses.get(ns).get(status).size());
        }
    }

    private void initFlinkVersions(String ns, String flinkVersion) {
        parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkDeployment.class, ns)
                .addGroup(FLINK_VERSION_GROUP_NAME, flinkVersion)
                .gauge(
                        COUNTER_NAME,
                        () -> deploymentFlinkVersions.get(ns).get(flinkVersion).size());
    }

    private void initFlinkMinorVersions(String ns, String minorVersion) {
        parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkDeployment.class, ns)
                .addGroup(FLINK_MINOR_VERSION_GROUP_NAME, minorVersion)
                .gauge(
                        COUNTER_NAME,
                        () -> deploymentFlinkMinorVersions.get(ns).get(minorVersion).size());
    }

    private void initNamespaceCpuUsage(String ns) {
        parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkDeployment.class, ns)
                .addGroup(RESOURCE_USAGE_GROUP_NAME)
                .gauge(
                        CPU_NAME,
                        () ->
                                deploymentCpuUsage.get(ns).values().stream()
                                        .reduce(0.0, Double::sum));
    }

    private void initNamespaceMemoryUsage(String ns) {
        parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkDeployment.class, ns)
                .addGroup(RESOURCE_USAGE_GROUP_NAME)
                .gauge(
                        MEMORY_NAME,
                        () ->
                                deploymentMemoryUsage.get(ns).values().stream()
                                        .reduce(0L, Long::sum));
    }

    private Map<JobManagerDeploymentStatus, Set<String>> createDeploymentStatusMap() {
        Map<JobManagerDeploymentStatus, Set<String>> statuses = new ConcurrentHashMap<>();
        for (JobManagerDeploymentStatus status : JobManagerDeploymentStatus.values()) {
            statuses.put(status, ConcurrentHashMap.newKeySet());
        }
        return statuses;
    }
}
