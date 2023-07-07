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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.metrics.CustomResourceMetrics;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricGroup;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.CREATED;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.DEPLOYED;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.ROLLED_BACK;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.ROLLING_BACK;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.STABLE;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.SUSPENDED;
import static org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState.UPGRADING;

/**
 * Utility for tracking resource lifecycle metrics globally and per namespace.
 *
 * @param <CR> Flink resource type.
 */
public class LifecycleMetrics<CR extends AbstractFlinkResource<?, ?>>
        implements CustomResourceMetrics<CR> {

    private static final String TRANSITION_RESUME = "Resume";
    private static final String TRANSITION_UPGRADE = "Upgrade";
    private static final String TRANSITION_SUSPEND = "Suspend";
    private static final String TRANSITION_SUBMISSION = "Submission";
    private static final String TRANSITION_STABILIZATION = "Stabilization";
    private static final String TRANSITION_ROLLBACK = "Rollback";

    public static final List<Transition> TRACKED_TRANSITIONS = getTrackedTransitions();

    private final Map<Tuple2<String, String>, ResourceLifecycleMetricTracker> lifecycleTrackers =
            new ConcurrentHashMap<>();
    private final Set<String> namespaces = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Clock clock;
    private final KubernetesOperatorMetricGroup operatorMetricGroup;
    private final boolean namespaceHistosEnabled;

    private Map<String, Tuple2<Histogram, Map<String, Histogram>>> transitionMetrics;
    private Map<ResourceLifecycleState, Tuple2<Histogram, Map<String, Histogram>>> stateTimeMetrics;

    private Function<MetricGroup, MetricGroup> metricGroupFunction = mg -> mg.addGroup("Lifecycle");

    private FlinkOperatorConfiguration operatorConfig;
    private Configuration config;

    public LifecycleMetrics(
            Configuration configuration, KubernetesOperatorMetricGroup operatorMetricGroup) {
        this.clock = Clock.systemDefaultZone();
        this.operatorMetricGroup = operatorMetricGroup;
        this.config = configuration;
        this.operatorConfig = FlinkOperatorConfiguration.fromConfiguration(config);
        this.namespaceHistosEnabled =
                config.get(
                        KubernetesOperatorMetricOptions
                                .OPERATOR_LIFECYCLE_NAMESPACE_HISTOGRAMS_ENABLED);
    }

    @Override
    public void onUpdate(CR cr) {
        var status = cr.getStatus();
        getLifecycleMetricTracker(cr).onUpdate(status.getLifecycleState(), clock.instant());
    }

    @Override
    public void onRemove(CR cr) {
        lifecycleTrackers.remove(
                Tuple2.of(cr.getMetadata().getNamespace(), cr.getMetadata().getName()));
    }

    private ResourceLifecycleMetricTracker getLifecycleMetricTracker(CR cr) {
        init(cr);
        createNamespaceStateCountIfMissing(cr);
        return lifecycleTrackers.computeIfAbsent(
                Tuple2.of(cr.getMetadata().getNamespace(), cr.getMetadata().getName()),
                k -> {
                    var initialState = cr.getStatus().getLifecycleState();
                    var time =
                            initialState == CREATED
                                    ? Instant.parse(cr.getMetadata().getCreationTimestamp())
                                    : clock.instant();
                    return new ResourceLifecycleMetricTracker(
                            initialState,
                            time,
                            getTransitionHistograms(cr),
                            getStateTimeHistograms(cr));
                });
    }

    private void createNamespaceStateCountIfMissing(CR cr) {
        var namespace = cr.getMetadata().getNamespace();
        if (!namespaces.add(namespace)) {
            return;
        }

        MetricGroup lifecycleGroup =
                metricGroupFunction.apply(
                        operatorMetricGroup.createResourceNamespaceGroup(
                                config, cr.getClass(), namespace));
        for (ResourceLifecycleState state : ResourceLifecycleState.values()) {
            lifecycleGroup
                    .addGroup("State")
                    .addGroup(state.name())
                    .gauge(
                            "Count",
                            () ->
                                    lifecycleTrackers.values().stream()
                                            .map(ResourceLifecycleMetricTracker::getCurrentState)
                                            .filter(s -> s == state)
                                            .count());
        }
    }

    private synchronized void init(CR cr) {
        if (transitionMetrics != null) {
            return;
        }

        this.transitionMetrics = new ConcurrentHashMap<>();
        TRACKED_TRANSITIONS.forEach(
                t ->
                        transitionMetrics.computeIfAbsent(
                                t.metricName,
                                name ->
                                        Tuple2.of(
                                                createTransitionHistogram(
                                                        name,
                                                        operatorMetricGroup.addGroup(
                                                                cr.getClass().getSimpleName())),
                                                new ConcurrentHashMap<>())));

        this.stateTimeMetrics = new ConcurrentHashMap<>();
        for (ResourceLifecycleState state : ResourceLifecycleState.values()) {
            stateTimeMetrics.put(
                    state,
                    Tuple2.of(
                            createStateTimeHistogram(
                                    state,
                                    operatorMetricGroup.addGroup(cr.getClass().getSimpleName())),
                            new ConcurrentHashMap<>()));
        }
    }

    private Map<String, List<Histogram>> getTransitionHistograms(CR cr) {
        var histos = new HashMap<String, List<Histogram>>();
        transitionMetrics.forEach(
                (metricName, t) ->
                        histos.put(
                                metricName,
                                namespaceHistosEnabled
                                        ? List.of(
                                                t.f0,
                                                t.f1.computeIfAbsent(
                                                        cr.getMetadata().getNamespace(),
                                                        ns ->
                                                                createTransitionHistogram(
                                                                        metricName,
                                                                        operatorMetricGroup
                                                                                .createResourceNamespaceGroup(
                                                                                        config,
                                                                                        cr
                                                                                                .getClass(),
                                                                                        ns))))
                                        : List.of(t.f0)));
        return histos;
    }

    private Map<ResourceLifecycleState, List<Histogram>> getStateTimeHistograms(CR cr) {
        var histos = new HashMap<ResourceLifecycleState, List<Histogram>>();
        stateTimeMetrics.forEach(
                (state, t) ->
                        histos.put(
                                state,
                                namespaceHistosEnabled
                                        ? List.of(
                                                t.f0,
                                                t.f1.computeIfAbsent(
                                                        cr.getMetadata().getNamespace(),
                                                        ns ->
                                                                createStateTimeHistogram(
                                                                        state,
                                                                        operatorMetricGroup
                                                                                .createResourceNamespaceGroup(
                                                                                        config,
                                                                                        cr
                                                                                                .getClass(),
                                                                                        ns))))
                                        : List.of(t.f0)));
        return histos;
    }

    private Histogram createTransitionHistogram(String metricName, MetricGroup group) {
        return metricGroupFunction
                .apply(group)
                .addGroup("Transition")
                .addGroup(metricName)
                .histogram("TimeSeconds", OperatorMetricUtils.createHistogram(operatorConfig));
    }

    private Histogram createStateTimeHistogram(ResourceLifecycleState state, MetricGroup group) {
        return metricGroupFunction
                .apply(group)
                .addGroup("State")
                .addGroup(state.name())
                .histogram("TimeSeconds", OperatorMetricUtils.createHistogram(operatorConfig));
    }

    private static List<Transition> getTrackedTransitions() {
        return List.of(
                new Transition(SUSPENDED, STABLE, true, TRANSITION_RESUME),
                new Transition(STABLE, STABLE, true, TRANSITION_UPGRADE),
                new Transition(DEPLOYED, UPGRADING, true, TRANSITION_SUSPEND),
                new Transition(STABLE, UPGRADING, true, TRANSITION_SUSPEND),
                new Transition(ROLLED_BACK, UPGRADING, true, TRANSITION_SUSPEND),
                new Transition(DEPLOYED, SUSPENDED, true, TRANSITION_SUSPEND),
                new Transition(STABLE, SUSPENDED, true, TRANSITION_SUSPEND),
                new Transition(ROLLED_BACK, SUSPENDED, true, TRANSITION_SUSPEND),
                new Transition(DEPLOYED, STABLE, false, TRANSITION_STABILIZATION),
                new Transition(DEPLOYED, ROLLED_BACK, false, TRANSITION_ROLLBACK),
                new Transition(UPGRADING, DEPLOYED, true, TRANSITION_SUBMISSION),
                new Transition(ROLLING_BACK, ROLLED_BACK, true, TRANSITION_SUBMISSION));
    }

    @VisibleForTesting
    protected Map<Tuple2<String, String>, ResourceLifecycleMetricTracker> getLifecycleTrackers() {
        return lifecycleTrackers;
    }

    /**
     * Pojo for encapsulating state transitions and whether we should measure time from the
     * beginning of from or since the last update.
     */
    @ToString
    @RequiredArgsConstructor
    protected static class Transition {
        public final ResourceLifecycleState from;
        public final ResourceLifecycleState to;
        public final boolean measureFromLastUpdate;
        public final String metricName;
    }
}
