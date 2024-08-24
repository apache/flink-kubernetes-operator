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
import org.apache.flink.autoscaler.JobVertexScaler.ParallelismResult;
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
                List.of(),
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
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismResult.optional(5),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 50, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        assertEquals(
                ParallelismResult.optional(8),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 50, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        assertEquals(
                ParallelismResult.optional(10),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 80, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        assertEquals(
                ParallelismResult.optional(8),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 60, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        assertEquals(
                ParallelismResult.optional(8),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 59, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.5);
        assertEquals(
                ParallelismResult.required(10),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(2, 100, 40),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.6);
        assertEquals(
                ParallelismResult.required(4),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(2, 100, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.5);
        assertEquals(
                ParallelismResult.optional(5),
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
                ParallelismResult.optional(4),
                vertexScaler.computeScaleTargetParallelism(
                        context,
                        op,
                        inputShipStrategies,
                        evaluated(10, 10, 100),
                        Collections.emptySortedMap(),
                        restartTime,
                        delayedScaleDown));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.5);
        assertEquals(
                ParallelismResult.required(15),
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
                ParallelismResult.required(16),
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
        assertEquals(
                1,
                JobVertexScaler.scale(
                        1, NOT_ADJUST_INPUTS, 720, 0.0001, minParallelism, maxParallelism));
        assertEquals(
                1,
                JobVertexScaler.scale(
                        2, NOT_ADJUST_INPUTS, 720, 0.1, minParallelism, maxParallelism));
        assertEquals(
                5,
                JobVertexScaler.scale(
                        6, NOT_ADJUST_INPUTS, 720, 0.8, minParallelism, maxParallelism));
        assertEquals(
                24,
                JobVertexScaler.scale(
                        16, NOT_ADJUST_INPUTS, 128, 1.5, minParallelism, maxParallelism));
        assertEquals(
                400,
                JobVertexScaler.scale(
                        200, NOT_ADJUST_INPUTS, 720, 2, minParallelism, maxParallelism));
        assertEquals(
                720,
                JobVertexScaler.scale(
                        200,
                        NOT_ADJUST_INPUTS,
                        720,
                        Integer.MAX_VALUE,
                        minParallelism,
                        maxParallelism));
    }

    @ParameterizedTest
    @MethodSource("adjustmentInputsProvider")
    public void testParallelismComputationWithAdjustment(
            Collection<ShipStrategy> inputShipStrategies) {
        final int minParallelism = 1;
        final int maxParallelism = Integer.MAX_VALUE;
        assertEquals(
                6,
                JobVertexScaler.scale(
                        6, inputShipStrategies, 36, 0.8, minParallelism, maxParallelism));
        assertEquals(
                32,
                JobVertexScaler.scale(
                        16, inputShipStrategies, 128, 1.5, minParallelism, maxParallelism));
        assertEquals(
                360,
                JobVertexScaler.scale(
                        200, inputShipStrategies, 720, 1.3, minParallelism, maxParallelism));
        assertEquals(
                720,
                JobVertexScaler.scale(
                        200,
                        inputShipStrategies,
                        720,
                        Integer.MAX_VALUE,
                        minParallelism,
                        maxParallelism));
    }

    @ParameterizedTest
    @MethodSource("adjustmentInputsProvider")
    public void testParallelismComputationWithLimit(Collection<ShipStrategy> inputShipStrategies) {
        assertEquals(5, JobVertexScaler.scale(6, inputShipStrategies, 720, 0.8, 1, 700));
        assertEquals(8, JobVertexScaler.scale(8, inputShipStrategies, 720, 0.8, 8, 700));

        assertEquals(
                32, JobVertexScaler.scale(16, inputShipStrategies, 128, 1.5, 1, Integer.MAX_VALUE));
        assertEquals(
                64,
                JobVertexScaler.scale(16, inputShipStrategies, 128, 1.5, 60, Integer.MAX_VALUE));

        assertEquals(300, JobVertexScaler.scale(200, inputShipStrategies, 720, 2, 1, 300));
        assertEquals(
                600,
                JobVertexScaler.scale(200, inputShipStrategies, 720, Integer.MAX_VALUE, 1, 600));
    }

    @Test
    public void ensureMinParallelismDoesNotExceedMax() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                assertEquals(
                                        600,
                                        JobVertexScaler.scale(
                                                200,
                                                NOT_ADJUST_INPUTS,
                                                720,
                                                Integer.MAX_VALUE,
                                                500,
                                                499)));
    }

    @Test
    public void testMinParallelismLimitIsUsed() {
        conf.setInteger(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 5);
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismResult.optional(5),
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
                ParallelismResult.optional(4),
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
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismResult.optional(10),
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
                ParallelismResult.optional(12),
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
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ofMinutes(0));

        var delayedScaleDown = new DelayedScaleDown();

        assertParallelismResult(10, 50, 100, ParallelismResult.required(5), delayedScaleDown);
    }

    @Test
    public void testRequiredScaleDownAfterInterval() {
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ofMinutes(1));
        var instant = Instant.now();

        var delayedScaleDown = new DelayedScaleDown();

        // The scale down shouldn't be required.
        vertexScaler.setClock(Clock.fixed(instant, ZoneId.systemDefault()));
        assertParallelismResult(100, 800, 1000, ParallelismResult.optional(80), delayedScaleDown);

        // Within scale down interval.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(10)), ZoneId.systemDefault()));
        assertParallelismResult(100, 900, 1000, ParallelismResult.optional(90), delayedScaleDown);

        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(20)), ZoneId.systemDefault()));
        assertParallelismResult(100, 820, 1000, ParallelismResult.optional(82), delayedScaleDown);

        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(40)), ZoneId.systemDefault()));
        assertParallelismResult(100, 720, 1000, ParallelismResult.optional(72), delayedScaleDown);

        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(50)), ZoneId.systemDefault()));
        assertParallelismResult(100, 600, 1000, ParallelismResult.optional(60), delayedScaleDown);

        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(59)), ZoneId.systemDefault()));
        assertParallelismResult(100, 640, 1000, ParallelismResult.optional(64), delayedScaleDown);

        // The scale down is required after the scale down interval ends.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(60)), ZoneId.systemDefault()));
        assertParallelismResult(100, 700, 1000, ParallelismResult.required(70), delayedScaleDown);
    }

    @Test
    public void testImmediateScaleUpWithinScaleDownInterval() {
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ofMinutes(1));
        var instant = Instant.now();

        var delayedScaleDown = new DelayedScaleDown();

        // The scale down shouldn't be required.
        vertexScaler.setClock(Clock.fixed(instant, ZoneId.systemDefault()));
        assertParallelismResult(100, 800, 1000, ParallelismResult.optional(80), delayedScaleDown);
        assertThat(delayedScaleDown.getFirstTriggerTime()).isNotEmpty();

        // Within scale down interval.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(10)), ZoneId.systemDefault()));
        assertParallelismResult(100, 900, 1000, ParallelismResult.optional(90), delayedScaleDown);
        assertThat(delayedScaleDown.getFirstTriggerTime()).isNotEmpty();

        // Allow immediate scale up
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(12)), ZoneId.systemDefault()));
        assertParallelismResult(100, 1700, 1000, ParallelismResult.required(170), delayedScaleDown);
        assertThat(delayedScaleDown.getFirstTriggerTime()).isEmpty();
    }

    @Test
    public void testCancelDelayedScaleDownAfterNewParallelismIsSame() {
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ofMinutes(1));
        var instant = Instant.now();

        var delayedScaleDown = new DelayedScaleDown();

        // The scale down shouldn't be required.
        vertexScaler.setClock(Clock.fixed(instant, ZoneId.systemDefault()));
        assertParallelismResult(100, 800, 1000, ParallelismResult.optional(80), delayedScaleDown);
        assertThat(delayedScaleDown.getFirstTriggerTime()).isNotEmpty();

        // Within scale down interval.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(10)), ZoneId.systemDefault()));
        assertParallelismResult(100, 900, 1000, ParallelismResult.optional(90), delayedScaleDown);
        assertThat(delayedScaleDown.getFirstTriggerTime()).isNotEmpty();

        // The delayed scale down is canceled when new parallelism is same with current parallelism.
        vertexScaler.setClock(
                Clock.fixed(instant.plus(Duration.ofSeconds(12)), ZoneId.systemDefault()));
        assertParallelismResult(100, 1000, 1000, ParallelismResult.optional(100), delayedScaleDown);
        assertThat(delayedScaleDown.getFirstTriggerTime()).isEmpty();
    }

    private void assertParallelismResult(
            int parallelism,
            int targetDataRate,
            int trueProcessingRate,
            ParallelismResult expectedParallelismResult,
            DelayedScaleDown delayedScaleDown) {
        assertEquals(
                expectedParallelismResult,
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
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ZERO);

        var evaluated = evaluated(5, 100, 50);
        var history = new TreeMap<Instant, ScalingSummary>();
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismResult.required(10),
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
                ParallelismResult.required(20),
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
                ParallelismResult.optional(20),
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
                ParallelismResult.optional(20),
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
                ParallelismResult.required(20),
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
                ParallelismResult.required(36),
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
                ParallelismResult.required(40),
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
                ParallelismResult.required(10),
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
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.0);
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, Duration.ZERO);

        var evaluated = evaluated(5, 100, 50);
        var history = new TreeMap<Instant, ScalingSummary>();
        var delayedScaleDown = new DelayedScaleDown();

        assertEquals(
                ParallelismResult.required(10),
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
                ParallelismResult.required(20),
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
                ParallelismResult.optional(20),
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
                ParallelismResult.optional(20),
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
                ParallelismResult.optional(20),
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
                ParallelismResult.optional(20),
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
                ParallelismResult.required(40),
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

    private Map<ScalingMetric, EvaluatedScalingMetric> evaluated(
            int parallelism, double targetDataRate, double trueProcessingRate) {
        var metrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        metrics.put(ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(parallelism));
        metrics.put(ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(720));
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
}
