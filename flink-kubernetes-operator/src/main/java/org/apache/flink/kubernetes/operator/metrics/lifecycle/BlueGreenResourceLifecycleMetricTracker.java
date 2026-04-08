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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.metrics.Histogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_BLUE_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_GREEN_TO_BLUE;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_INITIAL_DEPLOYMENT;

/**
 * Tracks state transitions and timing for a single FlinkBlueGreenDeployment resource. Records
 * transition durations and time spent in each state.
 */
public class BlueGreenResourceLifecycleMetricTracker {

    private static final Logger LOG =
            LoggerFactory.getLogger(BlueGreenResourceLifecycleMetricTracker.class);

    private FlinkBlueGreenDeploymentState currentState;

    // map(state -> (firstEntryTime, lastUpdateTime))
    // Tracks when we entered each state and when we last saw it
    private final Map<FlinkBlueGreenDeploymentState, Tuple2<Instant, Instant>> stateTimeMap =
            new HashMap<>();

    private final Map<String, List<Histogram>> transitionHistos;
    private final Map<FlinkBlueGreenDeploymentState, List<Histogram>> stateTimeHistos;

    public BlueGreenResourceLifecycleMetricTracker(
            FlinkBlueGreenDeploymentState initialState,
            Instant time,
            Map<String, List<Histogram>> transitionHistos,
            Map<FlinkBlueGreenDeploymentState, List<Histogram>> stateTimeHistos) {
        this.currentState = initialState;
        this.transitionHistos = transitionHistos;
        this.stateTimeHistos = stateTimeHistos;
        stateTimeMap.put(initialState, Tuple2.of(time, time));
    }

    /**
     * Called on every reconciliation. Updates timestamps and records metrics on state changes.
     *
     * @param newState the current state from the resource status
     * @param time the current timestamp
     */
    public void onUpdate(FlinkBlueGreenDeploymentState newState, Instant time) {
        if (newState == currentState) {
            updateLastUpdateTime(newState, time);
            return;
        }

        // Record exit time for states that transition faster than the heartbeat interval.
        updateLastUpdateTime(currentState, time);
        recordTransitionMetrics(currentState, newState, time);

        if (newState == ACTIVE_BLUE || newState == ACTIVE_GREEN) {
            LOG.debug(
                    "Transitioned from {} to {}, recording state times for {} and clearing",
                    currentState,
                    newState,
                    stateTimeMap.keySet());

            recordStateTimeMetrics();
            clearTrackedStates();
        }

        stateTimeMap.put(newState, Tuple2.of(time, time));
        currentState = newState;
    }

    private void updateLastUpdateTime(FlinkBlueGreenDeploymentState state, Instant time) {
        var times = stateTimeMap.get(state);
        if (times != null) {
            times.f1 = time;
        }
    }

    private void recordTransitionMetrics(
            FlinkBlueGreenDeploymentState fromState,
            FlinkBlueGreenDeploymentState toState,
            Instant time) {

        if (toState == ACTIVE_BLUE && stateTimeMap.containsKey(INITIALIZING_BLUE)) {
            recordTransition(TRANSITION_INITIAL_DEPLOYMENT, INITIALIZING_BLUE, time);
        }

        if (toState == ACTIVE_GREEN && stateTimeMap.containsKey(ACTIVE_BLUE)) {
            recordTransition(TRANSITION_BLUE_TO_GREEN, ACTIVE_BLUE, time);
        }

        if (toState == ACTIVE_BLUE
                && fromState != INITIALIZING_BLUE
                && stateTimeMap.containsKey(ACTIVE_GREEN)) {
            recordTransition(TRANSITION_GREEN_TO_BLUE, ACTIVE_GREEN, time);
        }
    }

    private void recordTransition(
            String transitionName, FlinkBlueGreenDeploymentState fromState, Instant time) {
        var fromTimes = stateTimeMap.get(fromState);
        if (fromTimes == null) {
            return;
        }

        long durationSeconds = Duration.between(fromTimes.f1, time).toSeconds();

        LOG.debug(
                "Recording transition time {}s for {} (from {})",
                durationSeconds,
                transitionName,
                fromState);

        var histograms = transitionHistos.get(transitionName);
        if (histograms != null) {
            histograms.forEach(h -> h.update(durationSeconds));
        }
    }

    private void recordStateTimeMetrics() {
        stateTimeMap.forEach(
                (state, times) -> {
                    long durationSeconds = Duration.between(times.f0, times.f1).toSeconds();
                    var histograms = stateTimeHistos.get(state);
                    if (histograms != null) {
                        histograms.forEach(h -> h.update(durationSeconds));
                    }
                });
    }

    private void clearTrackedStates() {
        stateTimeMap.clear();
    }
}
