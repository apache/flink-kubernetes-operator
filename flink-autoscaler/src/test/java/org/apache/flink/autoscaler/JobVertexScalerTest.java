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

package org.apache.flink.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.JobVertexScaler.ParallelismChange;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.autoscaler.JobVertexScaler.INEFFECTIVE_MESSAGE_FORMAT;
import static org.apache.flink.autoscaler.JobVertexScaler.INEFFECTIVE_SCALING;
import static org.apache.flink.autoscaler.JobVertexScaler.SCALE_LIMITED_MESSAGE_FORMAT;
import static org.apache.flink.autoscaler.JobVertexScaler.SCALING_LIMITED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.OBSERVED_SCALABILITY_ENABLED;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.OBSERVED_SCALABILITY_MIN_OBSERVATIONS;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.UTILIZATION_TARGET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for vertex parallelism scaler logic. */
public class JobVertexScalerTest {

    private static final Collection<ShipStrategy> NOT_ADJUST_INPUTS =
            List.of(ShipStrategy.REBALANCE, ShipStrategy.RESCALE);

    private final JobVertexID vertex = new JobVertexID();

    private TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector;
    private JobVertexScaler<JobID, JobAutoScalerContext<JobID>> vertexScaler;
    private JobAutoScalerContext<JobID> context;
    private Configuration conf;
    private Duration restartTime;

    private static List<Collection<ShipStrategy>> adjustmentInputsProvider() {
        return List.of(
                List.of(ShipStrategy.HASH),
                List.of(ShipStrategy.REBALANCE, ShipStrategy.HASH, ShipStrategy.RESCALE));
    }

    @BeforeEach
    public void setup() {
        eventCollector = new TestingEventCollector<>();
        vertexScaler = new JobVertexScaler<>(eventCollector);
        conf = new Configuration();
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, (double) Integer.MAX_VALUE);
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ZERO);
        JobID jobID = new JobID();
        context =
                new JobAutoScalerContext<>(
                        jobID,
                        jobID,
                        JobStatus.RUNNING,
                        conf,
                        new UnregisteredMetricsGroup(),
                        null);
        restartTime = conf.get(AutoScalerOptions.RESTART_TIME);
    }

    @ParameterizedTest
    @MethodSource("adjustmentInputsProvider")
    public void testParallelismScaling(Collection<ShipStrategy> inputShipStrategies) {
        var op = new JobVertexID();
        conf.set(UTILIZATION_TARGET, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ZERO);
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismChange.build(5, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 50, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(UTILIZATION_TARGET, .8);
        assertEquals(
                ParallelismChange.build(8, false),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 50, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(UTILIZATION_TARGET, .8);
        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 80, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(UTILIZATION_TARGET, .8);
        assertEquals(
                ParallelismChange.build(8, false),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 60, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        assertEquals(
                ParallelismChange.build(8, false),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 59, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(UTILIZATION_TARGET, 0.5);
        assertEquals(
                ParallelismChange.build(10, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(2, 100, 40),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(UTILIZATION_TARGET, 0.6);
        assertEquals(
                ParallelismChange.build(4, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(2, 100, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(UTILIZATION_TARGET, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.5);
        assertEquals(
                ParallelismChange.build(5, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 10, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.6);
        assertEquals(
                ParallelismChange.build(4, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 10, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(UTILIZATION_TARGET, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.5);
        assertEquals(
                ParallelismChange.build(15, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 200, 10),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.6);
        assertEquals(
                ParallelismChange.build(16, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 200, 10),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));
    }

    @Test
    public void testParallelismComputation() {
        final int minParallelism = 1;
        final int maxParallelism = Integer.MAX_VALUE;
        final var vertex = new JobVertexID();
        assertEquals(
                1,
                JobVertexScaler.scale(
                        vertex,
                        1,
                        NOT_ADJUST_INPUTS,
                        0,
                        720,
                        0.0001,
                        minParallelism,
                        maxParallelism,
                        eventCollector,
                        context));
        assertEquals(
                1,
                JobVertexScaler.scale(
                        vertex,
                        2,
                        NOT_ADJUST_INPUTS,
                        0,
                        720,
                        0.1,
                        minParallelism,
                        maxParallelism,
                        eventCollector,
                        context));
        assertEquals(
                5,
                JobVertexScaler.scale(
                        vertex,
                        6,
                        NOT_ADJUST_INPUTS,
                        0,
                        720,
                        0.8,
                        minParallelism,
                        maxParallelism,
                        eventCollector,
                        context));
        assertEquals(
                24,
                JobVertexScaler.scale(
                        vertex,
                        16,
                        NOT_ADJUST_INPUTS,
                        0,
                        128,
                        1.5,
                        minParallelism,
                        maxParallelism,
                        eventCollector,
                        context));
        assertEquals(
                400,
                JobVertexScaler.scale(
                        vertex,
                        200,
                        NOT_ADJUST_INPUTS,
                        0,
                        720,
                        2,
                        minParallelism,
                        maxParallelism,
                        eventCollector,
                        context));
        assertEquals(
                720,
                JobVertexScaler.scale(
                        vertex,
                        200,
                        NOT_ADJUST_INPUTS,
                        0,
                        720,
                        Integer.MAX_VALUE,
                        minParallelism,
                        maxParallelism,
                        eventCollector,
                        context));
    }

    @ParameterizedTest
    @MethodSource("adjustmentInputsProvider")
    public void testParallelismComputationWithAdjustment(
            Collection<ShipStrategy> inputShipStrategies) {
        final int parallelismLowerLimit = 1;
        final int parallelismUpperLimit = Integer.MAX_VALUE;
        final var vertex = new JobVertexID();

        assertEquals(
                6,
                JobVertexScaler.scale(
                        vertex,
                        6,
                        inputShipStrategies,
                        0,
                        36,
                        0.8,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));
        assertEquals(
                32,
                JobVertexScaler.scale(
                        vertex,
                        16,
                        inputShipStrategies,
                        0,
                        128,
                        1.5,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));
        assertEquals(
                360,
                JobVertexScaler.scale(
                        vertex,
                        200,
                        inputShipStrategies,
                        0,
                        720,
                        1.3,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));
        assertEquals(
                720,
                JobVertexScaler.scale(
                        vertex,
                        200,
                        inputShipStrategies,
                        0,
                        720,
                        Integer.MAX_VALUE,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));

        int maxParallelism = 128;
        double scaleFactor = 2.5;
        int currentParallelism = 10;
        int expectedEvenly = 32;
        int expectedMaximumUtilization = 26;
        assertEquals(
                expectedEvenly,
                JobVertexScaler.scale(
                        vertex,
                        currentParallelism,
                        inputShipStrategies,
                        0,
                        maxParallelism,
                        scaleFactor,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));

        Configuration conf = context.getConfiguration();
        conf.set(
                AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE,
                JobVertexScaler.KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION);
        assertEquals(
                expectedMaximumUtilization,
                JobVertexScaler.scale(
                        vertex,
                        currentParallelism,
                        inputShipStrategies,
                        0,
                        maxParallelism,
                        scaleFactor,
                        parallelismLowerLimit,
                        maxParallelism,
                        eventCollector,
                        context));
    }

    @ParameterizedTest
    @MethodSource("adjustmentInputsProvider")
    public void testParallelismComputationWithLimit(Collection<ShipStrategy> inputShipStrategies) {
        final var vertex = new JobVertexID();

        assertEquals(
                5,
                JobVertexScaler.scale(
                        vertex,
                        6,
                        inputShipStrategies,
                        0,
                        720,
                        0.8,
                        1,
                        700,
                        eventCollector,
                        context));
        assertEquals(
                8,
                JobVertexScaler.scale(
                        vertex,
                        8,
                        inputShipStrategies,
                        0,
                        720,
                        0.8,
                        8,
                        700,
                        eventCollector,
                        context));

        assertEquals(
                32,
                JobVertexScaler.scale(
                        vertex,
                        16,
                        inputShipStrategies,
                        0,
                        128,
                        1.5,
                        1,
                        Integer.MAX_VALUE,
                        eventCollector,
                        context));
        assertEquals(
                64,
                JobVertexScaler.scale(
                        vertex,
                        16,
                        inputShipStrategies,
                        0,
                        128,
                        1.5,
                        60,
                        Integer.MAX_VALUE,
                        eventCollector,
                        context));
        assertEquals(
                240,
                JobVertexScaler.scale(
                        vertex,
                        200,
                        inputShipStrategies,
                        0,
                        720,
                        2,
                        1,
                        300,
                        eventCollector,
                        context));
        assertEquals(
                360,
                JobVertexScaler.scale(
                        vertex,
                        200,
                        inputShipStrategies,
                        0,
                        720,
                        Integer.MAX_VALUE,
                        1,
                        600,
                        eventCollector,
                        context));
    }

    @Test
    public void ensureMinParallelismDoesNotExceedMax() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                assertEquals(
                                        600,
                                        JobVertexScaler.scale(
                                                new JobVertexID(),
                                                200,
                                                NOT_ADJUST_INPUTS,
                                                0,
                                                720,
                                                Integer.MAX_VALUE,
                                                500,
                                                499,
                                                eventCollector,
                                                context)));
    }

    @Test
    public void testMinParallelismLimitIsUsed() {
        conf.setInteger(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 5);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ZERO);
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismChange.build(5, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        new JobVertexID(),
                        NOT_ADJUST_INPUTS,
                        evaluated(10, 100, 500),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        // Make sure we respect current parallelism in case it's lower
        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        new JobVertexID(),
                        NOT_ADJUST_INPUTS,
                        evaluated(4, 100, 500),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));
    }

    @Test
    public void testMaxParallelismLimitIsUsed() {
        conf.setInteger(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 10);
        conf.set(UTILIZATION_TARGET, 1.);
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        new JobVertexID(),
                        NOT_ADJUST_INPUTS,
                        evaluated(10, 500, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        // Make sure we respect current parallelism in case it's higher
        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        new JobVertexID(),
                        NOT_ADJUST_INPUTS,
                        evaluated(12, 500, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));
    }

    @Test
    public void testDisableScaleDownInterval() {
        conf.set(UTILIZATION_TARGET, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ofMinutes(0));

        var delayedScaleDown = new DelayedScaleDown();

        assertParallelismChange(10, 50, 100, ParallelismChange.build(5, true), delayedScaleDown);
    }

    @Test
    public void testScaleDownAfterInterval() {
        conf.set(UTILIZATION_TARGET, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ofMinutes(1));
        var instant = Instant.now();

        var delayedScaleDown = new DelayedScaleDown();

        // The scale down never happen when scale down is first triggered.
        vertexScaler.setClock(Clock.fixed(instant, ZoneId.systemDefault()));
        assertParallelismChange(100, 800, 1000, ParallelismChange.noChange(), delayedScaleDown);

        // The scale down never happen within scale down interval.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(10)), ZoneId.systemDefault()));
        assertParallelismChange(100, 900, 1000, ParallelismChange.noChange(), delayedScaleDown);

        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(40)), ZoneId.systemDefault()));
        assertParallelismChange(100, 720, 1000, ParallelismChange.noChange(), delayedScaleDown);

        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(59)), ZoneId.systemDefault()));
        assertParallelismChange(100, 640, 1000, ParallelismChange.noChange(), delayedScaleDown);

        // The parallelism result should be the max recommended parallelism within the scale down
        // interval.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(60)), ZoneId.systemDefault()));
        assertParallelismChange(
                100, 700, 1000, ParallelismChange.build(90, false), delayedScaleDown);
    }

    @Test
    public void testImmediateScaleUpWithinScaleDownInterval() {
        conf.set(UTILIZATION_TARGET, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ofMinutes(1));
        var instant = Instant.now();

        var delayedScaleDown = new DelayedScaleDown();

        // The scale down never happen when scale down is first triggered.
        vertexScaler.setClock(Clock.fixed(instant, ZoneId.systemDefault()));
        assertParallelismChange(100, 800, 1000, ParallelismChange.noChange(), delayedScaleDown);
        assertThat(delayedScaleDown.getDelayedVertices()).isNotEmpty();

        // The scale down never happen within scale down interval.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(10)), ZoneId.systemDefault()));
        assertParallelismChange(100, 900, 1000, ParallelismChange.noChange(), delayedScaleDown);
        assertThat(delayedScaleDown.getDelayedVertices()).isNotEmpty();

        // Allow immediate scale up
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(12)), ZoneId.systemDefault()));
        assertParallelismChange(
                100, 1700, 1000, ParallelismChange.build(170, true), delayedScaleDown);
        assertThat(delayedScaleDown.getDelayedVertices()).isEmpty();
    }

    @Test
    public void testCancelDelayedScaleDownAfterNewParallelismIsSame() {
        conf.set(UTILIZATION_TARGET, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ofMinutes(1));
        var instant = Instant.now();

        var delayedScaleDown = new DelayedScaleDown();

        // The scale down never happen when scale down is first triggered.
        vertexScaler.setClock(Clock.fixed(instant, ZoneId.systemDefault()));
        assertParallelismChange(100, 800, 1000, ParallelismChange.noChange(), delayedScaleDown);
        assertThat(delayedScaleDown.getDelayedVertices()).isNotEmpty();

        // The scale down never happen within scale down interval.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(10)), ZoneId.systemDefault()));
        assertParallelismChange(100, 900, 1000, ParallelismChange.noChange(), delayedScaleDown);
        assertThat(delayedScaleDown.getDelayedVertices()).isNotEmpty();

        // The delayed scale down is canceled when new parallelism is same with current parallelism.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(12)), ZoneId.systemDefault()));
        assertParallelismChange(100, 1000, 1000, ParallelismChange.noChange(), delayedScaleDown);
        assertThat(delayedScaleDown.getDelayedVertices()).isEmpty();
    }

    private void assertParallelismChange(
            int parallelism,
            int targetDataRate,
            int trueProcessingRate,
            ParallelismChange expectedParallelismChange,
            DelayedScaleDown delayedScaleDown) {
        assertEquals(
                expectedParallelismChange,
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        vertex,
                        NOT_ADJUST_INPUTS,
                        evaluated(parallelism, targetDataRate, trueProcessingRate),
                        new TreeMap<>(),
                        restartTime,
                        delayedScaleDown));
    }

    @Test
    public void testIneffectiveScalingDetection() {
        var op = new JobVertexID();
        conf.set(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED, true);
        conf.set(UTILIZATION_TARGET, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ZERO);

        var evaluated = evaluated(5, 100, 50);
        var history = new TreeMap<Instant, ScalingSummary>();
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismChange.build(10, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        NOT_ADJUST_INPUTS,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(100, evaluated.get(ScalingMetric.EXPECTED_PROCESSING_RATE).getCurrent());
        history.put(Instant.now(), new ScalingSummary(5, 10, evaluated));

        // Allow to scale higher if scaling was effective (80%)
        evaluated = evaluated(10, 180, 90);
        assertEquals(
                ParallelismChange.build(20, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        NOT_ADJUST_INPUTS,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(180, evaluated.get(ScalingMetric.EXPECTED_PROCESSING_RATE).getCurrent());
        history.put(Instant.now(), new ScalingSummary(10, 20, evaluated));

        // Detect ineffective scaling, less than 5% of target increase (instead of 90 -> 180, only
        // 90 -> 94. Do not try to scale above 20
        evaluated = evaluated(20, 180, 94);
        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        NOT_ADJUST_INPUTS,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));

        // Still considered ineffective (less than <10%)
        evaluated = evaluated(20, 180, 98);
        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        NOT_ADJUST_INPUTS,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));

        // Allow scale up if current parallelism doesnt match last (user rescaled manually)
        evaluated = evaluated(10, 180, 90);
        assertEquals(
                ParallelismChange.build(20, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        NOT_ADJUST_INPUTS,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));

        // Over 10%, effective
        evaluated = evaluated(20, 180, 100);
        assertEquals(
                ParallelismChange.build(36, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        NOT_ADJUST_INPUTS,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertTrue(evaluated.containsKey(ScalingMetric.EXPECTED_PROCESSING_RATE));

        // Ineffective but detection is turned off
        conf.set(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED, false);
        evaluated = evaluated(20, 180, 90);
        assertEquals(
                ParallelismChange.build(40, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        NOT_ADJUST_INPUTS,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertTrue(evaluated.containsKey(ScalingMetric.EXPECTED_PROCESSING_RATE));
        conf.set(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED, true);

        // Allow scale down even if ineffective
        evaluated = evaluated(20, 45, 90);
        assertEquals(
                ParallelismChange.build(10, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        NOT_ADJUST_INPUTS,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertTrue(evaluated.containsKey(ScalingMetric.EXPECTED_PROCESSING_RATE));
    }

    @ParameterizedTest
    @MethodSource("adjustmentInputsProvider")
    public void testSendingIneffectiveScalingEvents(Collection<ShipStrategy> inputShipStrategies) {
        var jobVertexID = new JobVertexID();
        conf.set(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED, true);
        conf.set(UTILIZATION_TARGET, 1.0);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ZERO);

        var evaluated = evaluated(5, 100, 50);
        var history = new TreeMap<Instant, ScalingSummary>();
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismChange.build(10, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        jobVertexID,
                        inputShipStrategies,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(100, evaluated.get(ScalingMetric.EXPECTED_PROCESSING_RATE).getCurrent());
        history.put(Instant.now(), new ScalingSummary(5, 10, evaluated));

        // Effective scale, no events triggered
        evaluated = evaluated(10, 180, 90);
        assertEquals(
                ParallelismChange.build(20, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        jobVertexID,
                        inputShipStrategies,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(180, evaluated.get(ScalingMetric.EXPECTED_PROCESSING_RATE).getCurrent());
        history.put(Instant.now(), new ScalingSummary(10, 20, evaluated));
        assertEquals(0, eventCollector.events.size());

        // Ineffective scale, an event is triggered
        evaluated = evaluated(20, 180, 95);
        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        jobVertexID,
                        inputShipStrategies,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(1, eventCollector.events.size());
        var event = eventCollector.events.poll();
        assertThat(event).isNotNull();
        assertThat(event.getMessage())
                .isEqualTo(
                        String.format(
                                INEFFECTIVE_MESSAGE_FORMAT, jobVertexID, 90.0, 5.0, "enabled"));
        assertThat(event.getReason()).isEqualTo(INEFFECTIVE_SCALING);
        assertEquals(1, event.getCount());

        // Repeat ineffective scale with default interval, no event is triggered
        // We slightly modify the computed tpr to simulate changing metrics
        var tpr = evaluated.get(ScalingMetric.TRUE_PROCESSING_RATE);
        evaluated.put(
                ScalingMetric.TRUE_PROCESSING_RATE,
                EvaluatedScalingMetric.avg(tpr.getAverage() + 0.01));
        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        jobVertexID,
                        inputShipStrategies,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(0, eventCollector.events.size());

        // reset tpr
        evaluated.put(ScalingMetric.TRUE_PROCESSING_RATE, tpr);

        // Repeat ineffective scale with postive interval, no event is triggered
        conf.set(AutoScalerOptions.SCALING_EVENT_INTERVAL, Duration.ofSeconds(1800));
        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        jobVertexID,
                        inputShipStrategies,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(0, eventCollector.events.size());

        // Ineffective scale with interval set to 0, an event is triggered
        conf.set(AutoScalerOptions.SCALING_EVENT_INTERVAL, Duration.ZERO);
        assertEquals(
                ParallelismChange.noChange(),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        jobVertexID,
                        inputShipStrategies,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(1, eventCollector.events.size());
        event = eventCollector.events.poll();
        assertThat(event).isNotNull();
        assertThat(event.getMessage())
                .isEqualTo(
                        String.format(
                                INEFFECTIVE_MESSAGE_FORMAT, jobVertexID, 90.0, 5.0, "enabled"));
        assertThat(event.getReason()).isEqualTo(INEFFECTIVE_SCALING);
        assertEquals(2, event.getCount());

        // Test ineffective scaling switched off
        conf.set(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED, false);
        assertEquals(
                ParallelismChange.build(40, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        jobVertexID,
                        inputShipStrategies,
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(1, eventCollector.events.size());
        event = eventCollector.events.poll();
        assertThat(event).isNotNull();
        assertThat(event.getMessage())
                .isEqualTo(
                        String.format(
                                INEFFECTIVE_MESSAGE_FORMAT, jobVertexID, 90.0, 5.0, "disabled"));
        assertThat(event.getReason()).isEqualTo(INEFFECTIVE_SCALING);
    }

    @Test
    public void testNumPartitionsAdjustment() {
        final int parallelismLowerLimit = 1;
        final int parallelismUpperLimit = Integer.MAX_VALUE;
        final var vertex = new JobVertexID();

        assertEquals(
                3,
                JobVertexScaler.scale(
                        vertex,
                        6,
                        List.of(),
                        15,
                        128,
                        0.4,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));
        assertEquals(
                15,
                JobVertexScaler.scale(
                        vertex,
                        7,
                        List.of(),
                        15,
                        128,
                        1.2,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));
        assertEquals(
                18,
                JobVertexScaler.scale(
                        vertex,
                        20,
                        List.of(),
                        35,
                        30,
                        0.9,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));

        assertEquals(
                20,
                JobVertexScaler.scale(
                        vertex,
                        22,
                        List.of(),
                        35,
                        30,
                        1.1,
                        20,
                        parallelismUpperLimit,
                        eventCollector,
                        context));

        assertEquals(
                100,
                JobVertexScaler.scale(
                        vertex,
                        80,
                        List.of(),
                        200,
                        128,
                        1.4,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));

        int partition = 199;
        double scaleFactor = 4;
        int currentParallelism = 24;
        int expectedEvenly = 199;
        // At MAXIMIZE_UTILISATION, 99 subtasks consume two partitions,
        // one subtask consumes one partition.
        int expectedMaximumUtilization = 100;

        assertEquals(
                expectedEvenly,
                JobVertexScaler.scale(
                        vertex,
                        currentParallelism,
                        List.of(),
                        partition,
                        parallelismUpperLimit,
                        scaleFactor,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));

        Configuration conf = context.getConfiguration();
        conf.set(
                AutoScalerOptions.SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE,
                JobVertexScaler.KeyGroupOrPartitionsAdjustMode.MAXIMIZE_UTILISATION);

        assertEquals(
                expectedMaximumUtilization,
                JobVertexScaler.scale(
                        vertex,
                        currentParallelism,
                        List.of(),
                        partition,
                        parallelismUpperLimit,
                        scaleFactor,
                        parallelismLowerLimit,
                        parallelismUpperLimit,
                        eventCollector,
                        context));
    }

    @Test
    public void testSendingScalingLimitedEvents() {
        var jobVertexID = new JobVertexID();
        conf.set(UTILIZATION_TARGET, 1.0);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ZERO);
        conf.set(AutoScalerOptions.SCALING_EVENT_INTERVAL, Duration.ZERO);
        var evaluated = evaluated(10, 200, 100);
        evaluated.put(ScalingMetric.NUM_SOURCE_PARTITIONS, EvaluatedScalingMetric.of(15));
        var history = new TreeMap<Instant, ScalingSummary>();
        var delayedScaleDown = new DelayedScaleDown();
        // partition limited
        assertEquals(
                ParallelismChange.build(15, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        jobVertexID,
                        List.of(),
                        evaluated,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(1, eventCollector.events.size());
        TestingEventCollector.Event<JobID, JobAutoScalerContext<JobID>> partitionLimitedEvent =
                eventCollector.events.poll();
        assertThat(partitionLimitedEvent).isNotNull();
        assertEquals(SCALING_LIMITED, partitionLimitedEvent.getReason());
        assertThat(partitionLimitedEvent.getMessage())
                .isEqualTo(
                        String.format(
                                SCALE_LIMITED_MESSAGE_FORMAT, jobVertexID, 20, 15, 15, 200, 1));
        // small changes for scaleFactor, verify that the event messageKey is the same.
        var smallChangesForScaleFactor = evaluated(10, 199, 100);
        smallChangesForScaleFactor.put(
                ScalingMetric.NUM_SOURCE_PARTITIONS, EvaluatedScalingMetric.of(15));
        assertEquals(
                ParallelismChange.build(15, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        jobVertexID,
                        List.of(),
                        smallChangesForScaleFactor,
                        history,
                        restartTime,
                        delayedScaleDown));
        assertEquals(1, eventCollector.events.size());
        TestingEventCollector.Event<JobID, JobAutoScalerContext<JobID>>
                smallChangesForScaleFactorLimitedEvent = eventCollector.events.poll();
        assertThat(partitionLimitedEvent.getMessage())
                .isEqualTo(
                        String.format(
                                SCALE_LIMITED_MESSAGE_FORMAT, jobVertexID, 20, 15, 15, 200, 1));
        assertThat(smallChangesForScaleFactorLimitedEvent).isNotNull();
        assertThat(partitionLimitedEvent.getMessageKey())
                .isEqualTo(smallChangesForScaleFactorLimitedEvent.getMessageKey());
    }

    private Map<ScalingMetric, EvaluatedScalingMetric> evaluated(
            int parallelism, double targetDataRate, double trueProcessingRate) {
        var metrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        metrics.put(ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(parallelism));
        metrics.put(ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(720));
        metrics.put(ScalingMetric.NUM_SOURCE_PARTITIONS, EvaluatedScalingMetric.of(0));
        metrics.put(
                ScalingMetric.TARGET_DATA_RATE,
                new EvaluatedScalingMetric(targetDataRate, targetDataRate));
        metrics.put(ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(0.));
        metrics.put(
                ScalingMetric.TRUE_PROCESSING_RATE,
                new EvaluatedScalingMetric(trueProcessingRate, trueProcessingRate));
        metrics.put(ScalingMetric.GC_PRESSURE, EvaluatedScalingMetric.of(Double.NaN));
        metrics.put(ScalingMetric.HEAP_MAX_USAGE_RATIO, EvaluatedScalingMetric.of(Double.NaN));
        ScalingMetricEvaluator.computeProcessingRateThresholds(metrics, conf, false, restartTime);
        return metrics;
    }

    @Test
    public void testCalculateScalingCoefficient() {
        var currentTime = Instant.now();

        var linearScalingHistory = new TreeMap<Instant, ScalingSummary>();
        var linearScalingEvaluatedData1 = evaluated(4, 100, 200);
        var linearScalingEvaluatedData2 = evaluated(2, 400, 100);
        var linearScalingEvaluatedData3 = evaluated(8, 800, 400);

        linearScalingHistory.put(
                currentTime.minusSeconds(20),
                new ScalingSummary(4, 2, linearScalingEvaluatedData1));
        linearScalingHistory.put(
                currentTime.minusSeconds(10),
                new ScalingSummary(2, 8, linearScalingEvaluatedData2));
        linearScalingHistory.put(
                currentTime, new ScalingSummary(8, 16, linearScalingEvaluatedData3));

        double linearScalingScalingCoefficient =
                JobVertexScaler.calculateObservedScalingCoefficient(linearScalingHistory, conf);

        assertEquals(1.0, linearScalingScalingCoefficient);

        var slightDiminishingReturnsScalingHistory = new TreeMap<Instant, ScalingSummary>();
        var slightDiminishingReturnsEvaluatedData1 = evaluated(4, 98, 196);
        var slightDiminishingReturnsEvaluatedData2 = evaluated(2, 396, 99);
        var slightDiminishingReturnsEvaluatedData3 = evaluated(8, 780, 390);

        slightDiminishingReturnsScalingHistory.put(
                currentTime.minusSeconds(20),
                new ScalingSummary(4, 2, slightDiminishingReturnsEvaluatedData1));
        slightDiminishingReturnsScalingHistory.put(
                currentTime.minusSeconds(10),
                new ScalingSummary(2, 8, slightDiminishingReturnsEvaluatedData2));
        slightDiminishingReturnsScalingHistory.put(
                currentTime, new ScalingSummary(8, 16, slightDiminishingReturnsEvaluatedData3));

        double slightDiminishingReturnsScalingCoefficient =
                JobVertexScaler.calculateObservedScalingCoefficient(
                        slightDiminishingReturnsScalingHistory, conf);

        assertTrue(
                slightDiminishingReturnsScalingCoefficient > 0.9
                        && slightDiminishingReturnsScalingCoefficient < 1);

        var sharpDiminishingReturnsScalingHistory = new TreeMap<Instant, ScalingSummary>();
        var sharpDiminishingReturnsEvaluatedData1 = evaluated(4, 80, 160);
        var sharpDiminishingReturnsEvaluatedData2 = evaluated(2, 384, 96);
        var sharpDiminishingReturnsEvaluatedData3 = evaluated(8, 480, 240);

        sharpDiminishingReturnsScalingHistory.put(
                currentTime.minusSeconds(20),
                new ScalingSummary(4, 2, sharpDiminishingReturnsEvaluatedData1));
        sharpDiminishingReturnsScalingHistory.put(
                currentTime.minusSeconds(10),
                new ScalingSummary(2, 8, sharpDiminishingReturnsEvaluatedData2));
        sharpDiminishingReturnsScalingHistory.put(
                currentTime, new ScalingSummary(8, 16, sharpDiminishingReturnsEvaluatedData3));

        double sharpDiminishingReturnsScalingCoefficient =
                JobVertexScaler.calculateObservedScalingCoefficient(
                        sharpDiminishingReturnsScalingHistory, conf);

        assertTrue(
                sharpDiminishingReturnsScalingCoefficient < 0.9
                        && sharpDiminishingReturnsScalingCoefficient > 0.4);

        var sharpDiminishingReturnsWithOneParallelismScalingHistory =
                new TreeMap<Instant, ScalingSummary>();
        var sharpDiminishingReturnsWithOneParallelismEvaluatedData1 = evaluated(1, 100, 50);
        var sharpDiminishingReturnsWithOneParallelismEvaluatedData2 = evaluated(2, 160, 80);
        var sharpDiminishingReturnsWithOneParallelismEvaluatedData3 = evaluated(4, 200, 100);

        sharpDiminishingReturnsWithOneParallelismScalingHistory.put(
                currentTime.minusSeconds(20),
                new ScalingSummary(1, 2, sharpDiminishingReturnsWithOneParallelismEvaluatedData1));
        sharpDiminishingReturnsWithOneParallelismScalingHistory.put(
                currentTime.minusSeconds(10),
                new ScalingSummary(2, 4, sharpDiminishingReturnsWithOneParallelismEvaluatedData2));
        sharpDiminishingReturnsWithOneParallelismScalingHistory.put(
                currentTime,
                new ScalingSummary(4, 8, sharpDiminishingReturnsWithOneParallelismEvaluatedData3));

        double sharpDiminishingReturnsWithOneParallelismScalingCoefficient =
                JobVertexScaler.calculateObservedScalingCoefficient(
                        sharpDiminishingReturnsWithOneParallelismScalingHistory, conf);

        assertTrue(
                sharpDiminishingReturnsWithOneParallelismScalingCoefficient < 0.9
                        && sharpDiminishingReturnsWithOneParallelismScalingCoefficient > 0.4);

        conf.set(OBSERVED_SCALABILITY_MIN_OBSERVATIONS, 1);

        var withOneScalingHistoryRecord = new TreeMap<Instant, ScalingSummary>();

        var withOneScalingHistoryRecordEvaluatedData1 = evaluated(4, 200, 100);

        withOneScalingHistoryRecord.put(
                currentTime, new ScalingSummary(4, 8, withOneScalingHistoryRecordEvaluatedData1));

        double withOneScalingHistoryRecordScalingCoefficient =
                JobVertexScaler.calculateObservedScalingCoefficient(
                        withOneScalingHistoryRecord, conf);

        assertEquals(1, withOneScalingHistoryRecordScalingCoefficient);

        var diminishingReturnWithTwoScalingHistoryRecord = new TreeMap<Instant, ScalingSummary>();

        var diminishingReturnWithTwoScalingHistoryRecordEvaluatedData1 = evaluated(2, 160, 80);
        var diminishingReturnWithTwoScalingHistoryRecordEvaluatedData2 = evaluated(4, 200, 100);

        diminishingReturnWithTwoScalingHistoryRecord.put(
                currentTime.minusSeconds(10),
                new ScalingSummary(
                        2, 4, diminishingReturnWithTwoScalingHistoryRecordEvaluatedData1));
        diminishingReturnWithTwoScalingHistoryRecord.put(
                currentTime,
                new ScalingSummary(
                        4, 8, diminishingReturnWithTwoScalingHistoryRecordEvaluatedData2));

        double diminishingReturnWithTwoScalingHistoryRecordScalingCoefficient =
                JobVertexScaler.calculateObservedScalingCoefficient(
                        diminishingReturnWithTwoScalingHistoryRecord, conf);

        assertTrue(
                diminishingReturnWithTwoScalingHistoryRecordScalingCoefficient < 0.9
                        && diminishingReturnWithTwoScalingHistoryRecordScalingCoefficient > 0.4);

        var linearReturnWithTwoScalingHistoryRecord = new TreeMap<Instant, ScalingSummary>();

        var linearReturnWithTwoScalingHistoryRecordEvaluatedData1 = evaluated(2, 160, 80);
        var linearReturnWithTwoScalingHistoryRecordEvaluatedData2 = evaluated(4, 320, 160);

        linearReturnWithTwoScalingHistoryRecord.put(
                currentTime.minusSeconds(10),
                new ScalingSummary(2, 4, linearReturnWithTwoScalingHistoryRecordEvaluatedData1));
        linearReturnWithTwoScalingHistoryRecord.put(
                currentTime,
                new ScalingSummary(4, 8, linearReturnWithTwoScalingHistoryRecordEvaluatedData2));

        double linearReturnWithTwoScalingHistoryRecordScalingCoefficient =
                JobVertexScaler.calculateObservedScalingCoefficient(
                        linearReturnWithTwoScalingHistoryRecord, conf);

        assertEquals(1, linearReturnWithTwoScalingHistoryRecordScalingCoefficient);
    }

    @ParameterizedTest
    @MethodSource("adjustmentInputsProvider")
    public void testParallelismScalingWithObservedScalingCoefficient(
            Collection<ShipStrategy> inputShipStrategies) {
        var op = new JobVertexID();
        var delayedScaleDown = new DelayedScaleDown();
        var currentTime = Instant.now();

        conf.set(UTILIZATION_TARGET, 0.5);
        conf.set(OBSERVED_SCALABILITY_ENABLED, true);

        var linearScalingHistory = new TreeMap<Instant, ScalingSummary>();
        var linearScalingEvaluatedData1 = evaluated(4, 100, 200);
        var linearScalingEvaluatedData2 = evaluated(2, 400, 100);
        var linearScalingEvaluatedData3 = evaluated(8, 800, 400);

        linearScalingHistory.put(
                currentTime.minusSeconds(20),
                new ScalingSummary(4, 2, linearScalingEvaluatedData1));
        linearScalingHistory.put(
                currentTime.minusSeconds(10),
                new ScalingSummary(2, 8, linearScalingEvaluatedData2));
        linearScalingHistory.put(
                currentTime, new ScalingSummary(8, 16, linearScalingEvaluatedData3));

        assertEquals(
                ParallelismChange.build(10, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(2, 100, 40),
                        linearScalingHistory,
                        restartTime,
                        delayedScaleDown));

        var diminishingReturnsScalingHistory = new TreeMap<Instant, ScalingSummary>();
        var diminishingReturnsEvaluatedData1 = evaluated(4, 80, 160);
        var diminishingReturnsEvaluatedData2 = evaluated(2, 384, 96);
        var diminishingReturnsEvaluatedData3 = evaluated(8, 480, 240);

        diminishingReturnsScalingHistory.put(
                currentTime.minusSeconds(20),
                new ScalingSummary(4, 2, diminishingReturnsEvaluatedData1));
        diminishingReturnsScalingHistory.put(
                currentTime.minusSeconds(10),
                new ScalingSummary(2, 8, diminishingReturnsEvaluatedData2));
        diminishingReturnsScalingHistory.put(
                currentTime, new ScalingSummary(8, 16, diminishingReturnsEvaluatedData3));

        assertEquals(
                ParallelismChange.build(15, true),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(2, 100, 40),
                        diminishingReturnsScalingHistory,
                        restartTime,
                        delayedScaleDown));
    }
}
