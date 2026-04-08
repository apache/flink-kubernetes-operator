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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Metrics for FlinkBlueGreenDeployment resources. */
public class FlinkBlueGreenDeploymentMetrics
        implements CustomResourceMetrics<FlinkBlueGreenDeployment> {

    public static final String BG_STATE_GROUP_NAME = "BlueGreenState";
    public static final String JOB_STATUS_GROUP_NAME = "JobStatus";
    public static final String COUNTER_NAME = "Count";
    public static final String FAILURES_COUNTER_NAME = "Failures";

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;

    // Tracks which deployments are in which state per namespace (for gauge metrics)
    // Map: namespace -> state -> set of deployment names
    private final Map<String, Map<FlinkBlueGreenDeploymentState, Set<String>>> deploymentStatuses =
            new ConcurrentHashMap<>();

    // Tracks which deployments are in which JobStatus per namespace (for gauge metrics)
    // Map: namespace -> JobStatus -> set of deployment names
    private final Map<String, Map<JobStatus, Set<String>>> jobStatuses = new ConcurrentHashMap<>();

    // Failure counters per namespace (historical count, never decrements)
    // Map: namespace -> Counter
    private final Map<String, Counter> failureCounters = new ConcurrentHashMap<>();

    public FlinkBlueGreenDeploymentMetrics(
            KubernetesOperatorMetricGroup parentMetricGroup, Configuration configuration) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
    }

    @Override
    public void onUpdate(FlinkBlueGreenDeployment flinkBgDep) {
        var namespace = flinkBgDep.getMetadata().getNamespace();
        var deploymentName = flinkBgDep.getMetadata().getName();
        var state = flinkBgDep.getStatus().getBlueGreenState();

        // Get current JobStatus
        var jobStatusObj = flinkBgDep.getStatus().getJobStatus();
        var currentJobStatus = jobStatusObj != null ? jobStatusObj.getState() : null;

        // Check if was in FAILING state BEFORE
        boolean wasInFailing = false;
        if (currentJobStatus != null) {
            var namespaceJobStatuses = jobStatuses.get(namespace);
            if (namespaceJobStatuses != null) {
                var failingSet = namespaceJobStatuses.get(JobStatus.FAILING);
                wasInFailing = failingSet != null && failingSet.contains(deploymentName);
            }
        }

        // Clear from all tracking (BlueGreenState and JobStatus)
        clearStateCount(flinkBgDep);

        deploymentStatuses
                .computeIfAbsent(namespace, this::initNamespaceMetrics)
                .get(state)
                .add(deploymentName);

        // Track JobStatus in gauge
        if (currentJobStatus != null) {
            jobStatuses
                    .computeIfAbsent(namespace, ns -> createJobStatusMap())
                    .get(currentJobStatus)
                    .add(deploymentName);

            // Detect transition TO FAILING for counter
            if (currentJobStatus == JobStatus.FAILING && !wasInFailing) {
                var counter = failureCounters.get(namespace);
                if (counter != null) {
                    counter.inc();
                }
            }
        }
    }

    @Override
    public void onRemove(FlinkBlueGreenDeployment flinkBgDep) {
        clearStateCount(flinkBgDep);
    }

    /** Clears the deployment from all state count sets (used before updating to new state). */
    private void clearStateCount(FlinkBlueGreenDeployment flinkBgDep) {
        var namespace = flinkBgDep.getMetadata().getNamespace();
        var deploymentName = flinkBgDep.getMetadata().getName();

        var namespaceStatuses = deploymentStatuses.get(namespace);
        if (namespaceStatuses != null) {
            namespaceStatuses
                    .values()
                    .forEach(deploymentNames -> deploymentNames.remove(deploymentName));
        }

        // Clear from JobStatus tracking
        var namespaceJobStatuses = jobStatuses.get(namespace);
        if (namespaceJobStatuses != null) {
            namespaceJobStatuses
                    .values()
                    .forEach(deploymentNames -> deploymentNames.remove(deploymentName));
        }
    }

    private Map<FlinkBlueGreenDeploymentState, Set<String>> initNamespaceMetrics(String namespace) {
        MetricGroup nsGroup =
                parentMetricGroup.createResourceNamespaceGroup(
                        configuration, FlinkBlueGreenDeployment.class, namespace);

        // Total deployment count
        nsGroup.gauge(
                COUNTER_NAME,
                () ->
                        deploymentStatuses.get(namespace).values().stream()
                                .mapToInt(Set::size)
                                .sum());

        // Historical failure counter (increments on each transition TO FAILING)
        failureCounters.put(namespace, nsGroup.counter(FAILURES_COUNTER_NAME));

        // Per-BlueGreenState counts
        Map<FlinkBlueGreenDeploymentState, Set<String>> statuses = new ConcurrentHashMap<>();
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            statuses.put(state, ConcurrentHashMap.newKeySet());
            nsGroup.addGroup(BG_STATE_GROUP_NAME)
                    .addGroup(state.toString())
                    .gauge(COUNTER_NAME, () -> deploymentStatuses.get(namespace).get(state).size());
        }

        // Per-JobStatus counts (gauges for current state)
        initJobStatusMetrics(namespace, nsGroup);

        return statuses;
    }

    private void initJobStatusMetrics(String namespace, MetricGroup nsGroup) {
        for (JobStatus status : JobStatus.values()) {
            nsGroup.addGroup(JOB_STATUS_GROUP_NAME)
                    .addGroup(status.toString())
                    .gauge(
                            COUNTER_NAME,
                            () -> {
                                var nsJobStatuses = jobStatuses.get(namespace);
                                if (nsJobStatuses == null) {
                                    return 0;
                                }
                                var statusSet = nsJobStatuses.get(status);
                                return statusSet != null ? statusSet.size() : 0;
                            });
        }
    }

    private Map<JobStatus, Set<String>> createJobStatusMap() {
        Map<JobStatus, Set<String>> statuses = new ConcurrentHashMap<>();
        for (JobStatus status : JobStatus.values()) {
            statuses.put(status, ConcurrentHashMap.newKeySet());
        }
        return statuses;
    }
}
