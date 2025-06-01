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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.ofEpochSecond;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_WINDOW;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.TOLERABLE_FAILURE_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link ClusterHealthEvaluator}. */
class ClusterHealthEvaluatorTest {

    private Configuration configuration;
    private Map<String, String> clusterInfo;
    private ClusterHealthEvaluator clusterHealthEvaluator;
    private final Instant invalidInstant = ofEpochMilli(0);
    private final Instant validInstant1 = ofEpochSecond(120);
    private final Instant validInstant2 = validInstant1.plus(2, ChronoUnit.MINUTES);
    private ClusterHealthInfo invalidClusterHealthInfo;

    @BeforeEach
    public void beforeEach() {
        configuration = new Configuration();
        configuration.set(CHECKPOINTING_TIMEOUT, Duration.ofSeconds(30));

        clusterInfo = new HashMap<>();

        var clock = Clock.fixed(invalidInstant, ZoneId.systemDefault());
        invalidClusterHealthInfo = new ClusterHealthInfo(clock);

        var now = Clock.fixed(validInstant2, ZoneId.systemDefault());
        clusterHealthEvaluator = new ClusterHealthEvaluator(now);
    }

    @Test
    public void evaluateShouldNotSetLastStateWhenInvalidObserved() {
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, invalidClusterHealthInfo);
        assertNull(ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo));
    }

    @Test
    public void evaluateShouldSetLastStateWhenValidObserved() {
        var observedClusterHealthInfo = createClusterHealthInfo(validInstant1, 0, 1);
        setLastValidClusterHealthInfo(observedClusterHealthInfo);
    }

    @Test
    public void evaluateShouldThrowExceptionWhenObservedTimestampIsOld() {
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 0, 1);

        ClusterHealthEvaluator.setLastValidClusterHealthInfo(
                clusterInfo, observedClusterHealthInfo2);
        assertThrows(
                IllegalStateException.class,
                () ->
                        clusterHealthEvaluator.evaluate(
                                configuration, clusterInfo, observedClusterHealthInfo1));
    }

    @Test
    public void evaluateShouldOverwriteRestartCountWhenLess() {
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 1, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 0, 1);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        var lastValidClusterHealthInfo =
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo);
        assertNotNull(lastValidClusterHealthInfo);
        assertEquals(
                observedClusterHealthInfo2.getNumRestarts(),
                lastValidClusterHealthInfo.getNumRestarts());
        assertEquals(
                observedClusterHealthInfo2.getTimeStamp(),
                lastValidClusterHealthInfo.getNumRestartsEvaluationTimeStamp());
    }

    @Test
    public void evaluateShouldNotOverwriteRestartCountWhenTimestampIsInWindow() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(2));
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 1, 1);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        var lastValidClusterHealthInfo =
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo);
        assertNotNull(lastValidClusterHealthInfo);
        assertEquals(
                observedClusterHealthInfo1.getNumRestarts(),
                lastValidClusterHealthInfo.getNumRestarts());
        assertEquals(
                observedClusterHealthInfo1.getTimeStamp(),
                lastValidClusterHealthInfo.getNumRestartsEvaluationTimeStamp());
    }

    @Test
    public void evaluateShouldOverwriteRestartCountWhenTimestampIsOutOfWindow() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(1));
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 1, 1);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        var lastValidClusterHealthInfo =
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo);
        assertNotNull(lastValidClusterHealthInfo);
        assertEquals(
                observedClusterHealthInfo2.getNumRestarts(),
                lastValidClusterHealthInfo.getNumRestarts());
        assertEquals(
                observedClusterHealthInfo2.getTimeStamp(),
                lastValidClusterHealthInfo.getNumRestartsEvaluationTimeStamp());
    }

    @Test
    public void evaluateShouldOverwriteCompletedCheckpointCountWhenLess() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_ENABLED, true);
        configuration.set(
                OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_WINDOW, Duration.ofMinutes(5));
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 0, 0);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        var lastValidClusterHealthInfo =
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo);
        assertNotNull(lastValidClusterHealthInfo);
        assertEquals(
                observedClusterHealthInfo2.getNumCompletedCheckpoints(),
                lastValidClusterHealthInfo.getNumCompletedCheckpoints());
        assertEquals(
                observedClusterHealthInfo2.getTimeStamp(),
                lastValidClusterHealthInfo.getNumCompletedCheckpointsIncreasedTimeStamp());
    }

    @Test
    public void evaluateShouldOverwriteCompletedCheckpointWhenIncreased() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_ENABLED, true);
        configuration.set(
                OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_WINDOW, Duration.ofMinutes(2));
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 0, 2);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        var lastValidClusterHealthInfo =
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo);
        assertNotNull(lastValidClusterHealthInfo);
        assertEquals(
                observedClusterHealthInfo2.getNumCompletedCheckpoints(),
                lastValidClusterHealthInfo.getNumCompletedCheckpoints());
        assertEquals(
                observedClusterHealthInfo2.getTimeStamp(),
                lastValidClusterHealthInfo.getNumCompletedCheckpointsIncreasedTimeStamp());
    }

    @Test
    public void evaluateShouldNotOverwriteCompletedCheckpointWhenNotIncreased() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_ENABLED, true);
        configuration.set(
                OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_WINDOW, Duration.ofMinutes(2));
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 0, 1);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        var lastValidClusterHealthInfo =
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo);
        assertNotNull(lastValidClusterHealthInfo);
        assertEquals(
                observedClusterHealthInfo1.getNumCompletedCheckpoints(),
                lastValidClusterHealthInfo.getNumCompletedCheckpoints());
        assertEquals(
                observedClusterHealthInfo1.getTimeStamp(),
                lastValidClusterHealthInfo.getNumCompletedCheckpointsIncreasedTimeStamp());
    }

    @Test
    public void evaluateShouldMarkClusterHealthyWhenNoPreviousState() {
        var observedClusterHealthInfo = createClusterHealthInfo(validInstant1, 1, 1);

        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo);
        assertClusterHealthIs(true);
    }

    @Test
    public void evaluateShouldMarkClusterHealthyWhenRestartThresholdNotHit() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(5));
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD, 100);
        ClusterHealthInfo observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 100, 1);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertClusterHealthIs(true);
    }

    @Test
    public void evaluateShouldMarkClusterUnhealthyWhenRestartThresholdHitImmediately() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(5));
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD, 100);
        ClusterHealthInfo observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 101, 1);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertClusterHealthIs(false);
    }

    @Test
    public void evaluateShouldMarkClusterUnhealthyWhenRestartThresholdHitInAverage() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(1));
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD, 100);
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 500, 1);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertClusterHealthIs(false);
    }

    @Test
    public void evaluateShouldMarkClusterHealthyWhenNoCompletedCheckpointsInsideWindow() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_ENABLED, true);
        configuration.set(
                OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_WINDOW, Duration.ofMinutes(3));
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 0);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 0, 0);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertClusterHealthIs(true);
    }

    private static Stream<Arguments> provideParametersEvaluateCheckpointing() {
        Instant tenSecInstant = ofEpochSecond(10);
        Instant twoMinInstant = ofEpochSecond(120);
        Instant fourMinInstant = twoMinInstant.plus(2, ChronoUnit.MINUTES);

        Duration oneMin = Duration.ofMinutes(1);
        return Stream.of(
                // ShouldMarkClusterUnhealthyWhenNoCompletedCheckpointsOutsideWindow
                Arguments.of(twoMinInstant, fourMinInstant, oneMin, 30L, 30L, null, false),
                // Verify checkpoint progress even if checkpointing not configured
                Arguments.of(twoMinInstant, fourMinInstant, oneMin, null, 30L, null, false),
                // Verify default window if not explicitly configured
                Arguments.of(twoMinInstant, fourMinInstant, null, 30L, 30L, null, false),
                // Verify check is off if both window and checkpointing is not configured
                Arguments.of(twoMinInstant, fourMinInstant, null, null, 30L, null, true),
                // ShouldMarkClusterHealthyWhenCompletedCheckpointsWithOutsideWindowFromCheckpointInterval
                Arguments.of(twoMinInstant, fourMinInstant, oneMin, 60L, 30L, null, true),
                // ShouldMarkClusterUnhealthyWhenNoCompletedCheckpointsWithOutsideWindowFromCheckpointInterval
                Arguments.of(tenSecInstant, fourMinInstant, oneMin, 60L, 30L, null, false),
                // ShouldMarkClusterHealthyWhenCompletedCheckpointsWithOutsideWindowFromCheckpointIntervalTimesNbTolerableFailure
                Arguments.of(twoMinInstant, fourMinInstant, oneMin, 30L, 10L, 3, true),
                // ShouldMarkClusterHealthyWhenNoCompletedCheckpointsWithOutsideWindowFromCheckpointIntervalTimesNbTolerableFailure
                Arguments.of(tenSecInstant, fourMinInstant, oneMin, 30L, 10L, 3, false),
                // ShouldMarkClusterHealthyWhenCompletedCheckpointsWithOutsideWindowFromCheckpointingTimeout
                Arguments.of(twoMinInstant, fourMinInstant, oneMin, 30L, 60L, null, true),
                // ShouldMarkClusterHealthyWhenNoCompletedCheckpointsWithOutsideWindowFromCheckpointingTimeout
                Arguments.of(tenSecInstant, fourMinInstant, oneMin, 30L, 60L, null, false),
                // ShouldMarkClusterHealthyWhenCompletedCheckpointsWithOutsideWindowFromCheckpointingTimeoutTimesNbTolerableFailure
                Arguments.of(twoMinInstant, fourMinInstant, oneMin, 10L, 30L, 3, true),
                // ShouldMarkClusterHealthyWhenNoCompletedCheckpointsWithOutsideWindowFromCheckpointingTimeoutTimesNbTolerableFailure
                Arguments.of(tenSecInstant, fourMinInstant, oneMin, 10L, 30L, 3, false));
    }

    @ParameterizedTest
    @MethodSource("provideParametersEvaluateCheckpointing")
    public void evaluateCheckpointing(
            Instant validInstant1,
            Instant validInstant2,
            Duration window,
            Long checkpointingInterval,
            long checkpointingTimeout,
            Integer tolerationFailureNumber,
            boolean expectedIsHealthy) {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_ENABLED, true);
        if (window != null) {
            configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_CHECKPOINT_PROGRESS_WINDOW, window);
        }
        if (checkpointingInterval != null) {
            configuration.set(CHECKPOINTING_INTERVAL, Duration.ofSeconds(checkpointingInterval));
        }
        configuration.set(CHECKPOINTING_TIMEOUT, Duration.ofSeconds(checkpointingTimeout));
        if (tolerationFailureNumber != null) {
            configuration.set(TOLERABLE_FAILURE_NUMBER, tolerationFailureNumber);
        }
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant1, 0, 0);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant2, 0, 0);

        setLastValidClusterHealthInfo(observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertClusterHealthIs(expectedIsHealthy);
    }

    private ClusterHealthInfo createClusterHealthInfo(
            Instant instant, int numRestarts, int numCompletedCheckpoints) {
        var clock = Clock.fixed(instant, ZoneId.systemDefault());
        var clusterHealthInfo = new ClusterHealthInfo(clock);
        clusterHealthInfo.setNumRestarts(numRestarts);
        clusterHealthInfo.setNumCompletedCheckpoints(numCompletedCheckpoints);
        return clusterHealthInfo;
    }

    private void setLastValidClusterHealthInfo(ClusterHealthInfo clusterHealthInfo) {
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, clusterHealthInfo);
        assertEquals(
                clusterHealthInfo,
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo));
    }

    private void assertClusterHealthIs(boolean healthy) {
        var lastValidClusterHealthInfo =
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo);
        assertNotNull(lastValidClusterHealthInfo);
        assertEquals(healthy, lastValidClusterHealthInfo.getHealthResult().isHealthy());
    }
}
