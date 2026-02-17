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
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.metrics.Histogram;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.SAVEPOINTING_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_BLUE_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_GREEN_TO_BLUE;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_INITIAL_DEPLOYMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link BlueGreenResourceLifecycleMetricTracker}. */
public class BlueGreenResourceLifecycleMetricTrackerTest {

    private Map<String, List<Histogram>> transitionHistos;
    private Map<FlinkBlueGreenDeploymentState, List<Histogram>> stateTimeHistos;

    @BeforeEach
    void setUp() {
        transitionHistos = new ConcurrentHashMap<>();
        transitionHistos.put(TRANSITION_INITIAL_DEPLOYMENT, List.of(createHistogram()));
        transitionHistos.put(TRANSITION_BLUE_TO_GREEN, List.of(createHistogram()));
        transitionHistos.put(TRANSITION_GREEN_TO_BLUE, List.of(createHistogram()));

        stateTimeHistos = new ConcurrentHashMap<>();
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            stateTimeHistos.put(state, List.of(createHistogram()));
        }
    }

    @Test
    void testInitialDeploymentRecordsTransitionTime() {
        long ts = 0;
        var tracker = createTracker(INITIALIZING_BLUE, Instant.ofEpochSecond(ts));
        tracker.onUpdate(TRANSITIONING_TO_BLUE, Instant.ofEpochSecond(ts += 5));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochSecond(ts += 5));

        assertTransitionRecorded(TRANSITION_INITIAL_DEPLOYMENT, 5);
    }

    @Test
    void testInitialDeploymentRecordsStateTime() {
        long ts = 0;
        var tracker = createTracker(INITIALIZING_BLUE, Instant.ofEpochSecond(ts));
        tracker.onUpdate(TRANSITIONING_TO_BLUE, Instant.ofEpochSecond(ts += 2));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochSecond(ts += 5));

        assertStateTimeRecorded(INITIALIZING_BLUE, 2);
        assertStateTimeRecorded(TRANSITIONING_TO_BLUE, 5);
    }

    @Test
    void testBlueToGreenRecordsTransitionTime() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_BLUE, Instant.ofEpochSecond(ts));
        tracker.onUpdate(SAVEPOINTING_BLUE, Instant.ofEpochSecond(ts += 5));
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochSecond(ts += 10));
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochSecond(ts += 5));

        assertTransitionRecorded(TRANSITION_BLUE_TO_GREEN, 15);
    }

    @Test
    void testBlueToGreenRecordsAllIntermediateStateTimes() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_BLUE, Instant.ofEpochSecond(ts));
        tracker.onUpdate(SAVEPOINTING_BLUE, Instant.ofEpochSecond(ts += 5));
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochSecond(ts += 10));
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochSecond(ts += 3));

        assertStateTimeRecorded(ACTIVE_BLUE, 5);
        assertStateTimeRecorded(SAVEPOINTING_BLUE, 10);
        assertStateTimeRecorded(TRANSITIONING_TO_GREEN, 3);
    }

    @Test
    void testGreenToBlueRecordsTransitionTime() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_GREEN, Instant.ofEpochSecond(ts));
        tracker.onUpdate(SAVEPOINTING_GREEN, Instant.ofEpochSecond(ts += 5));
        tracker.onUpdate(TRANSITIONING_TO_BLUE, Instant.ofEpochSecond(ts += 8));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochSecond(ts += 2));

        assertTransitionRecorded(TRANSITION_GREEN_TO_BLUE, 10);
    }

    @Test
    void testGreenToBlueRecordsAllIntermediateStateTimes() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_GREEN, Instant.ofEpochSecond(ts));
        tracker.onUpdate(SAVEPOINTING_GREEN, Instant.ofEpochSecond(ts += 5));
        tracker.onUpdate(TRANSITIONING_TO_BLUE, Instant.ofEpochSecond(ts += 8));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochSecond(ts += 2));

        assertStateTimeRecorded(ACTIVE_GREEN, 5);
        assertStateTimeRecorded(SAVEPOINTING_GREEN, 8);
        assertStateTimeRecorded(TRANSITIONING_TO_BLUE, 2);
    }

    @Test
    void testSameStateUpdatesOnlyUpdateTimestamp() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_BLUE, Instant.ofEpochSecond(ts));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochSecond(ts += 1));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochSecond(ts += 1));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochSecond(ts += 1));

        assertNoTransitionRecorded(TRANSITION_BLUE_TO_GREEN);
        assertNoTransitionRecorded(TRANSITION_GREEN_TO_BLUE);
        assertNoStateTimeRecorded(ACTIVE_BLUE);
    }

    @Test
    void testIntermediateStateMetricsOnlyRecordedAtStableState() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_BLUE, Instant.ofEpochSecond(ts));
        tracker.onUpdate(SAVEPOINTING_BLUE, Instant.ofEpochSecond(ts += 5));
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochSecond(ts += 5));

        assertNoStateTimeRecorded(ACTIVE_BLUE);
        assertNoStateTimeRecorded(SAVEPOINTING_BLUE);

        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochSecond(ts += 5));

        assertStateTimeRecorded(ACTIVE_BLUE, 5);
        assertStateTimeRecorded(SAVEPOINTING_BLUE, 5);
        assertStateTimeRecorded(TRANSITIONING_TO_GREEN, 5);
    }

    @Test
    void testRecoveryFromActiveStateTracksNextTransition() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_BLUE, Instant.ofEpochSecond(ts));
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochSecond(ts += 10));
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochSecond(ts += 5));

        assertTransitionRecorded(TRANSITION_BLUE_TO_GREEN, 5);
    }

    @Test
    void testConsecutiveTransitionsEachTrackedIndependently() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_BLUE, Instant.ofEpochSecond(ts));

        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochSecond(ts += 10));
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochSecond(ts += 5));

        assertTransitionRecorded(TRANSITION_BLUE_TO_GREEN, 5);

        tracker.onUpdate(TRANSITIONING_TO_BLUE, Instant.ofEpochSecond(ts += 8));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochSecond(ts += 2));

        assertTransitionRecorded(TRANSITION_GREEN_TO_BLUE, 2);
    }

    @Test
    void testFailedBlueToGreenRollbackRecordsStateTimes() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_BLUE, Instant.ofEpochSecond(ts));

        tracker.onUpdate(SAVEPOINTING_BLUE, Instant.ofEpochSecond(ts += 10));
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochSecond(ts += 15));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochSecond(ts += 5));

        assertStateTimeRecorded(ACTIVE_BLUE, 10);
        assertStateTimeRecorded(SAVEPOINTING_BLUE, 15);
        assertStateTimeRecorded(TRANSITIONING_TO_GREEN, 5);
    }

    @Test
    void testFailedGreenToBlueRollbackRecordsStateTimes() {
        long ts = 0;
        var tracker = createTracker(ACTIVE_GREEN, Instant.ofEpochSecond(ts));

        tracker.onUpdate(SAVEPOINTING_GREEN, Instant.ofEpochSecond(ts += 8));
        tracker.onUpdate(TRANSITIONING_TO_BLUE, Instant.ofEpochSecond(ts += 12));
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochSecond(ts += 3));

        assertStateTimeRecorded(ACTIVE_GREEN, 8);
        assertStateTimeRecorded(SAVEPOINTING_GREEN, 12);
        assertStateTimeRecorded(TRANSITIONING_TO_BLUE, 3);
    }

    private BlueGreenResourceLifecycleMetricTracker createTracker(
            FlinkBlueGreenDeploymentState initialState, Instant initialTime) {
        return new BlueGreenResourceLifecycleMetricTracker(
                initialState, initialTime, transitionHistos, stateTimeHistos);
    }

    private Histogram createHistogram() {
        return OperatorMetricUtils.createHistogram(
                FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
    }

    private void assertTransitionRecorded(String transitionName, long expectedSeconds) {
        var stats = transitionHistos.get(transitionName).get(0).getStatistics();
        assertEquals(1, stats.size(), transitionName + " should have 1 sample");
        assertEquals(expectedSeconds, (long) stats.getMean(), transitionName + " duration");
    }

    private void assertNoTransitionRecorded(String transitionName) {
        var stats = transitionHistos.get(transitionName).get(0).getStatistics();
        assertEquals(0, stats.size(), transitionName + " should have no samples");
    }

    private void assertStateTimeRecorded(
            FlinkBlueGreenDeploymentState state, long expectedSeconds) {
        var stats = stateTimeHistos.get(state).get(0).getStatistics();
        assertEquals(1, stats.size(), state + " should have 1 sample");
        assertEquals(expectedSeconds, (long) stats.getMean(), state + " duration");
    }

    private void assertNoStateTimeRecorded(FlinkBlueGreenDeploymentState state) {
        var stats = stateTimeHistos.get(state).get(0).getStatistics();
        assertEquals(0, stats.size(), state + " should have no samples");
    }
}
