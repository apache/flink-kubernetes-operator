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
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.metrics.Histogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Lifecycle state transition tracker for a single resource. */
public class ResourceLifecycleMetricTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceLifecycleMetricTracker.class);

    private final Map<String, List<Histogram>> transitionHistos;
    private final Map<ResourceLifecycleState, List<Histogram>> stateTimeHistos;

    private final Map<ResourceLifecycleState, Tuple2<Instant, Instant>> stateTimeMap =
            new HashMap<>();

    private ResourceLifecycleState currentState;

    public ResourceLifecycleMetricTracker(
            ResourceLifecycleState initialState,
            Instant time,
            Map<String, List<Histogram>> transitionHistos,
            Map<ResourceLifecycleState, List<Histogram>> stateTimeHistos) {
        this.transitionHistos = transitionHistos;
        this.currentState = initialState;
        this.stateTimeHistos = stateTimeHistos;
        updateCurrentState(currentState, time);
    }

    public void onUpdate(ResourceLifecycleState newState, Instant time) {
        if (newState == currentState) {
            updateCurrentState(newState, time);
            return;
        }

        LifecycleMetrics.TRACKED_TRANSITIONS.stream()
                .filter(t -> t.to == newState)
                .forEach(
                        transition -> {
                            var fromTimes = stateTimeMap.get(transition.from);
                            if (fromTimes != null) {
                                var transitionTime =
                                        Duration.between(
                                                        transition.measureFromLastUpdate
                                                                ? fromTimes.f1
                                                                : fromTimes.f0,
                                                        time)
                                                .toSeconds();

                                LOG.debug(
                                        "Recording transition time {} for {} ({} -> {})",
                                        transitionTime,
                                        transition.metricName,
                                        transition.from,
                                        transition.to);
                                transitionHistos
                                        .get(transition.metricName)
                                        .forEach(h -> h.update(transitionTime));
                            }
                        });

        updateCurrentState(newState, time);
    }

    private void updateCurrentState(ResourceLifecycleState newState, Instant time) {
        stateTimeMap.compute(
                newState,
                (s, t) -> {
                    if (t == null) {
                        return Tuple2.of(time, time);
                    } else {
                        if (newState != currentState) {
                            t.f0 = time;
                        }
                        t.f1 = time;
                        return t;
                    }
                });

        if (newState == currentState) {
            return;
        }

        var toClear = newState.getClearedStatesAfterTransition(currentState);
        LOG.debug(
                "Transitioned from {} to {}, clearing times for {}",
                currentState,
                newState,
                toClear);

        toClear.forEach(
                state -> {
                    var times = stateTimeMap.remove(state);
                    if (times != null) {
                        var totalSeconds = Duration.between(times.f0, times.f1).toSeconds();
                        stateTimeHistos.get(state).forEach(h -> h.update(totalSeconds));
                    }
                });
        currentState = newState;
    }

    public ResourceLifecycleState getCurrentState() {
        return currentState;
    }

    @VisibleForTesting
    protected Map<String, List<Histogram>> getTransitionHistos() {
        return transitionHistos;
    }

    @VisibleForTesting
    protected Map<ResourceLifecycleState, List<Histogram>> getStateTimeHistos() {
        return stateTimeHistos;
    }
}
