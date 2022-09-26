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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.ofEpochSecond;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW;
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
    private final Instant validInstant = ofEpochMilli(1);
    private ClusterHealthInfo invalidClusterHealthInfo;

    @BeforeEach
    public void beforeEach() {
        configuration = new Configuration();

        clusterInfo = new HashMap<>();

        var now = Clock.fixed(ofEpochSecond(120), ZoneId.systemDefault());
        clusterHealthEvaluator = new ClusterHealthEvaluator(now);

        var clock = Clock.fixed(invalidInstant, ZoneId.systemDefault());
        invalidClusterHealthInfo = ClusterHealthInfo.of(clock, 0);
    }

    @Test
    public void evaluateShouldNotSetLastStateWhenInvalidObserved() {
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, invalidClusterHealthInfo);
        assertNull(ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo));
    }

    @Test
    public void evaluateShouldSetLastStateWhenValidObserved() {
        var observedClusterHealthInfo = createClusterHealthInfo(validInstant, 0);

        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo);
        assertEquals(
                observedClusterHealthInfo,
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo));
    }

    @Test
    public void evaluateShouldThrowExceptionWhenObservedTimestampIsOld() {
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant, 0);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant.plusMillis(100), 0);

        ClusterHealthEvaluator.setLastValidClusterHealthInfo(
                clusterInfo, observedClusterHealthInfo2);
        assertThrows(
                IllegalStateException.class,
                () ->
                        clusterHealthEvaluator.evaluate(
                                configuration, clusterInfo, observedClusterHealthInfo1));
    }

    @Test
    public void evaluateShouldOverwriteLastStateWhenRestartCountIsLess() {
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant, 1);
        var observedClusterHealthInfo2 = createClusterHealthInfo(validInstant, 0);

        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo1);
        assertEquals(
                observedClusterHealthInfo1,
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo));
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertEquals(
                observedClusterHealthInfo2,
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo));
    }

    @Test
    public void evaluateShouldNotOverwriteLastStateWhenTimestampIsInWindow() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(2));
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant, 0);
        var observedClusterHealthInfo2 =
                createClusterHealthInfo(validInstant.plus(1, ChronoUnit.MINUTES), 0);

        ClusterHealthEvaluator.setLastValidClusterHealthInfo(
                clusterInfo, observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertEquals(
                observedClusterHealthInfo1,
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo));
    }

    @Test
    public void evaluateShouldOverwriteLastStateWhenTimestampIsOutOfWindow() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(1));
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant, 0);
        var observedClusterHealthInfo2 =
                createClusterHealthInfo(validInstant.plus(1, ChronoUnit.MINUTES), 0);

        ClusterHealthEvaluator.setLastValidClusterHealthInfo(
                clusterInfo, observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertEquals(
                observedClusterHealthInfo2,
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo));
    }

    @Test
    public void evaluateShouldMarkClusterHealthyWhenNoPreviousState() {
        var observedClusterHealthInfo = createClusterHealthInfo(validInstant, 1);

        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo);
        assertClusterHealthIs(true);
    }

    @Test
    public void evaluateShouldMarkClusterHealthyWhenThresholdNotHit() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(5));
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD, 100);
        ClusterHealthInfo observedClusterHealthInfo1 = createClusterHealthInfo(validInstant, 0);
        var observedClusterHealthInfo2 =
                createClusterHealthInfo(validInstant.plus(1, ChronoUnit.MINUTES), 100);

        ClusterHealthEvaluator.setLastValidClusterHealthInfo(
                clusterInfo, observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertClusterHealthIs(true);
    }

    @Test
    public void evaluateShouldMarkClusterUnhealthyWhenThresholdHitImmediately() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(5));
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD, 100);
        ClusterHealthInfo observedClusterHealthInfo1 = createClusterHealthInfo(validInstant, 0);
        var observedClusterHealthInfo2 =
                createClusterHealthInfo(validInstant.plus(1, ChronoUnit.MINUTES), 101);

        ClusterHealthEvaluator.setLastValidClusterHealthInfo(
                clusterInfo, observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertClusterHealthIs(false);
    }

    @Test
    public void evaluateShouldMarkClusterUnhealthyWhenThresholdHitInAverage() {
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW, Duration.ofMinutes(5));
        configuration.set(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD, 100);
        var observedClusterHealthInfo1 = createClusterHealthInfo(validInstant, 0);
        var observedClusterHealthInfo2 =
                createClusterHealthInfo(validInstant.plus(6, ChronoUnit.MINUTES), 122);

        ClusterHealthEvaluator.setLastValidClusterHealthInfo(
                clusterInfo, observedClusterHealthInfo1);
        clusterHealthEvaluator.evaluate(configuration, clusterInfo, observedClusterHealthInfo2);
        assertClusterHealthIs(false);
    }

    private ClusterHealthInfo createClusterHealthInfo(Instant instant, int numRestarts) {
        var clock = Clock.fixed(instant, ZoneId.systemDefault());
        return ClusterHealthInfo.of(clock, numRestarts);
    }

    private void assertClusterHealthIs(boolean healthy) {
        var lastValidClusterHealthInfo =
                ClusterHealthEvaluator.getLastValidClusterHealthInfo(clusterInfo);
        assertNotNull(lastValidClusterHealthInfo);
        assertEquals(healthy, lastValidClusterHealthInfo.isHealthy());
    }
}
