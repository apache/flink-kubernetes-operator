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

package org.apache.flink.kubernetes.operator.metrics.lifecycle;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.metrics.CustomResourceMetrics;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.metrics.Histogram;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;

/** Manages lifecycle metrics for FlinkBlueGreenDeployment resources. */
public class BlueGreenLifecycleMetrics implements CustomResourceMetrics<FlinkBlueGreenDeployment> {

    public static final String LIFECYCLE_GROUP_NAME = "Lifecycle";
    public static final String TRANSITION_GROUP_NAME = "Transition";
    public static final String STATE_GROUP_NAME = "State";
    public static final String TIME_SECONDS_NAME = "TimeSeconds";

    public static final String TRANSITION_INITIAL_DEPLOYMENT = "InitialDeployment";
    public static final String TRANSITION_BLUE_TO_GREEN = "BlueToGreen";
    public static final String TRANSITION_GREEN_TO_BLUE = "GreenToBlue";

    private static final List<String> TRANSITIONS =
            List.of(
                    TRANSITION_INITIAL_DEPLOYMENT,
                    TRANSITION_BLUE_TO_GREEN,
                    TRANSITION_GREEN_TO_BLUE);

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;
    private final FlinkOperatorConfiguration operatorConfig;
    private final Clock clock;
    private final boolean lifecycleMetricsEnabled;

    private final Map<String, Map<String, BlueGreenResourceLifecycleMetricTracker>>
            lifecycleTrackers = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Histogram>> namespaceTransitionHistograms =
            new ConcurrentHashMap<>();
    private final Map<FlinkBlueGreenDeploymentState, Map<String, Histogram>>
            namespaceStateTimeHistograms = new ConcurrentHashMap<>();

    private final Map<String, Histogram> systemTransitionHistograms = new ConcurrentHashMap<>();
    private final Map<FlinkBlueGreenDeploymentState, Histogram> systemStateTimeHistograms =
            new ConcurrentHashMap<>();

    public BlueGreenLifecycleMetrics(
            Configuration configuration, KubernetesOperatorMetricGroup parentMetricGroup) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
        this.operatorConfig = FlinkOperatorConfiguration.fromConfiguration(configuration);
        this.clock = Clock.systemDefaultZone();
        this.lifecycleMetricsEnabled =
                configuration.get(
                        KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED);

        TRANSITIONS.forEach(t -> namespaceTransitionHistograms.put(t, new ConcurrentHashMap<>()));
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            namespaceStateTimeHistograms.put(state, new ConcurrentHashMap<>());
        }
    }

    @Override
    public void onUpdate(FlinkBlueGreenDeployment flinkBgDep) {
        if (!lifecycleMetricsEnabled) {
            return;
        }

        var namespace = flinkBgDep.getMetadata().getNamespace();
        var deploymentName = flinkBgDep.getMetadata().getName();
        var state = flinkBgDep.getStatus().getBlueGreenState();

        getOrCreateTracker(namespace, deploymentName, flinkBgDep).onUpdate(state, clock.instant());
    }

    @Override
    public void onRemove(FlinkBlueGreenDeployment flinkBgDep) {
        var namespace = flinkBgDep.getMetadata().getNamespace();
        var deploymentName = flinkBgDep.getMetadata().getName();

        var namespaceTrackers = lifecycleTrackers.get(namespace);
        if (namespaceTrackers != null) {
            namespaceTrackers.remove(deploymentName);
        }
    }

    private BlueGreenResourceLifecycleMetricTracker getOrCreateTracker(
            String namespace, String deploymentName, FlinkBlueGreenDeployment flinkBgDep) {
        return lifecycleTrackers
                .computeIfAbsent(namespace, ns -> new ConcurrentHashMap<>())
                .computeIfAbsent(deploymentName, dn -> createTracker(namespace, flinkBgDep));
    }

    private BlueGreenResourceLifecycleMetricTracker createTracker(
            String namespace, FlinkBlueGreenDeployment flinkBgDep) {
        var initialState = flinkBgDep.getStatus().getBlueGreenState();
        var time =
                initialState == INITIALIZING_BLUE
                        ? Instant.parse(flinkBgDep.getMetadata().getCreationTimestamp())
                        : clock.instant();

        return new BlueGreenResourceLifecycleMetricTracker(
                initialState,
                time,
                buildTransitionHistograms(namespace),
                buildStateTimeHistograms(namespace));
    }

    private Map<String, List<Histogram>> buildTransitionHistograms(String namespace) {
        var histos = new HashMap<String, List<Histogram>>();
        for (String transition : TRANSITIONS) {
            histos.put(
                    transition,
                    List.of(
                            systemTransitionHistograms.computeIfAbsent(
                                    transition,
                                    t -> createSystemHistogram(TRANSITION_GROUP_NAME, t)),
                            namespaceTransitionHistograms
                                    .get(transition)
                                    .computeIfAbsent(
                                            namespace,
                                            ns ->
                                                    createNamespaceHistogram(
                                                            ns,
                                                            TRANSITION_GROUP_NAME,
                                                            transition))));
        }
        return histos;
    }

    private Map<FlinkBlueGreenDeploymentState, List<Histogram>> buildStateTimeHistograms(
            String namespace) {
        var histos = new HashMap<FlinkBlueGreenDeploymentState, List<Histogram>>();
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            histos.put(
                    state,
                    List.of(
                            systemStateTimeHistograms.computeIfAbsent(
                                    state, s -> createSystemHistogram(STATE_GROUP_NAME, s.name())),
                            namespaceStateTimeHistograms
                                    .get(state)
                                    .computeIfAbsent(
                                            namespace,
                                            ns ->
                                                    createNamespaceHistogram(
                                                            ns, STATE_GROUP_NAME, state.name()))));
        }
        return histos;
    }

    private Histogram createSystemHistogram(String groupName, String metricName) {
        return parentMetricGroup
                .addGroup(FlinkBlueGreenDeployment.class.getSimpleName())
                .addGroup(LIFECYCLE_GROUP_NAME)
                .addGroup(groupName)
                .addGroup(metricName)
                .histogram(TIME_SECONDS_NAME, OperatorMetricUtils.createHistogram(operatorConfig));
    }

    private Histogram createNamespaceHistogram(
            String namespace, String groupName, String metricName) {
        return parentMetricGroup
                .createResourceNamespaceGroup(
                        configuration, FlinkBlueGreenDeployment.class, namespace)
                .addGroup(LIFECYCLE_GROUP_NAME)
                .addGroup(groupName)
                .addGroup(metricName)
                .histogram(TIME_SECONDS_NAME, OperatorMetricUtils.createHistogram(operatorConfig));
    }
}
