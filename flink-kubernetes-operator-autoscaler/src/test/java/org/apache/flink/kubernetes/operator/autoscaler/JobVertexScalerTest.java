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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.kubernetes.operator.autoscaler.JobVertexScaler.INNEFFECTIVE_MESSAGE_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for vertex parallelism scaler logic. */
@EnableKubernetesMockClient(crud = true)
public class JobVertexScalerTest {

    private JobVertexScaler vertexScaler;
    private Configuration conf;

    private KubernetesClient kubernetesClient;
    private EventCollector eventCollector;

    private FlinkDeployment flinkDep;

    @BeforeEach
    public void setup() {
        flinkDep = TestUtils.buildApplicationCluster();
        kubernetesClient.resource(flinkDep).createOrReplace();
        eventCollector = new EventCollector();
        vertexScaler = new JobVertexScaler(new EventRecorder(kubernetesClient, eventCollector));
        conf = new Configuration();
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, (double) Integer.MAX_VALUE);
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ZERO);
    }

    @Test
    public void testParallelismScaling() {
        var op = new JobVertexID();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        assertEquals(
                5,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(10, 50, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        assertEquals(
                8,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(10, 50, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(10, 80, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        assertEquals(
                8,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(10, 60, 100), Collections.emptySortedMap()));

        assertEquals(
                8,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(10, 59, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.5);
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(2, 100, 40), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.6);
        assertEquals(
                4,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(2, 100, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.5);
        assertEquals(
                5,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(10, 10, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.6);
        assertEquals(
                4,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(10, 10, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.5);
        assertEquals(
                15,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(10, 200, 10), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, 0.6);
        assertEquals(
                16,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, op, evaluated(10, 200, 10), Collections.emptySortedMap()));
    }

    @Test
    public void testParallelismComputation() {
        final int minParallelism = 1;
        final int maxParallelism = Integer.MAX_VALUE;
        assertEquals(1, JobVertexScaler.scale(1, 720, 0.0001, minParallelism, maxParallelism));
        assertEquals(1, JobVertexScaler.scale(2, 720, 0.1, minParallelism, maxParallelism));
        assertEquals(5, JobVertexScaler.scale(6, 720, 0.8, minParallelism, maxParallelism));
        assertEquals(32, JobVertexScaler.scale(16, 128, 1.5, minParallelism, maxParallelism));
        assertEquals(400, JobVertexScaler.scale(200, 720, 2, minParallelism, maxParallelism));
        assertEquals(
                720,
                JobVertexScaler.scale(200, 720, Integer.MAX_VALUE, minParallelism, maxParallelism));
    }

    @Test
    public void testParallelismComputationWithLimit() {
        assertEquals(5, JobVertexScaler.scale(6, 720, 0.8, 1, 700));
        assertEquals(8, JobVertexScaler.scale(8, 720, 0.8, 8, 700));

        assertEquals(32, JobVertexScaler.scale(16, 128, 1.5, 1, Integer.MAX_VALUE));
        assertEquals(64, JobVertexScaler.scale(16, 128, 1.5, 60, Integer.MAX_VALUE));

        assertEquals(300, JobVertexScaler.scale(200, 720, 2, 1, 300));
        assertEquals(600, JobVertexScaler.scale(200, 720, Integer.MAX_VALUE, 1, 600));
    }

    @Test
    public void ensureMinParallelismDoesNotExceedMax() {
        Assert.assertThrows(
                IllegalArgumentException.class,
                () ->
                        assertEquals(
                                600, JobVertexScaler.scale(200, 720, Integer.MAX_VALUE, 500, 499)));
    }

    @Test
    public void testMinParallelismLimitIsUsed() {
        conf.setInteger(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 5);
        assertEquals(
                5,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep,
                        conf,
                        new JobVertexID(),
                        evaluated(10, 100, 500),
                        Collections.emptySortedMap()));
    }

    @Test
    public void testMaxParallelismLimitIsUsed() {
        conf.setInteger(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 10);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep,
                        conf,
                        new JobVertexID(),
                        evaluated(10, 500, 100),
                        Collections.emptySortedMap()));
    }

    @Test
    public void testScaleDownAfterScaleUpDetection() {
        var op = new JobVertexID();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.SCALE_UP_GRACE_PERIOD, Duration.ofMinutes(1));
        var clock = Clock.systemDefaultZone();
        vertexScaler.setClock(clock);

        var evaluated = evaluated(5, 100, 50);
        var history = new TreeMap<Instant, ScalingSummary>();
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));

        history.put(clock.instant(), new ScalingSummary(5, 10, evaluated));

        // Should not allow scale back down immediately
        evaluated = evaluated(10, 50, 100);
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));

        // Pass some time...
        clock = Clock.offset(Clock.systemDefaultZone(), Duration.ofSeconds(61));
        vertexScaler.setClock(clock);

        assertEquals(
                5,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));
        history.put(clock.instant(), new ScalingSummary(10, 5, evaluated));

        // Allow immediate scale up
        evaluated = evaluated(5, 100, 50);
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));
        history.put(clock.instant(), new ScalingSummary(5, 10, evaluated));
    }

    @Test
    public void testIneffectiveScalingDetection() {
        var op = new JobVertexID();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.SCALE_UP_GRACE_PERIOD, Duration.ZERO);

        var evaluated = evaluated(5, 100, 50);
        var history = new TreeMap<Instant, ScalingSummary>();
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));
        assertEquals(100, evaluated.get(ScalingMetric.EXPECTED_PROCESSING_RATE).getCurrent());
        history.put(Instant.now(), new ScalingSummary(5, 10, evaluated));

        // Allow to scale higher if scaling was effective (80%)
        evaluated = evaluated(10, 180, 90);
        assertEquals(
                20,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));
        assertEquals(180, evaluated.get(ScalingMetric.EXPECTED_PROCESSING_RATE).getCurrent());
        history.put(Instant.now(), new ScalingSummary(10, 20, evaluated));

        // Detect ineffective scaling, less than 5% of target increase (instead of 90 -> 180, only
        // 90 -> 94. Do not try to scale above 20
        evaluated = evaluated(20, 180, 94);
        assertEquals(
                20,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));
        assertFalse(evaluated.containsKey(ScalingMetric.EXPECTED_PROCESSING_RATE));

        // Still considered ineffective (less than <10%)
        evaluated = evaluated(20, 180, 98);
        assertEquals(
                20,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));
        assertFalse(evaluated.containsKey(ScalingMetric.EXPECTED_PROCESSING_RATE));

        // Allow scale up if current parallelism doesnt match last (user rescaled manually)
        evaluated = evaluated(10, 180, 90);
        assertEquals(
                20,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));

        // Over 10%, effective
        evaluated = evaluated(20, 180, 100);
        assertEquals(
                36,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));
        assertTrue(evaluated.containsKey(ScalingMetric.EXPECTED_PROCESSING_RATE));

        // Ineffective but detection is turned off
        conf.set(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED, false);
        evaluated = evaluated(20, 180, 90);
        assertEquals(
                40,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));
        assertTrue(evaluated.containsKey(ScalingMetric.EXPECTED_PROCESSING_RATE));
        conf.set(AutoScalerOptions.SCALING_EFFECTIVENESS_DETECTION_ENABLED, true);

        // Allow scale down even if ineffective
        evaluated = evaluated(20, 45, 90);
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(flinkDep, conf, op, evaluated, history));
        assertTrue(evaluated.containsKey(ScalingMetric.EXPECTED_PROCESSING_RATE));
    }

    @Test
    public void testSendingIneffectiveScalingEvents() {
        var jobVertexID = new JobVertexID();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.0);
        conf.set(AutoScalerOptions.SCALE_UP_GRACE_PERIOD, Duration.ZERO);

        var evaluated = evaluated(5, 100, 50);
        var history = new TreeMap<Instant, ScalingSummary>();
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, jobVertexID, evaluated, history));
        assertEquals(100, evaluated.get(ScalingMetric.EXPECTED_PROCESSING_RATE).getCurrent());
        history.put(Instant.now(), new ScalingSummary(5, 10, evaluated));

        // Effective scale, no events triggered
        evaluated = evaluated(10, 180, 90);
        assertEquals(
                20,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, jobVertexID, evaluated, history));
        assertEquals(180, evaluated.get(ScalingMetric.EXPECTED_PROCESSING_RATE).getCurrent());
        history.put(Instant.now(), new ScalingSummary(10, 20, evaluated));
        assertEquals(0, eventCollector.events.size());

        // Ineffective scale, an event is triggered
        evaluated = evaluated(20, 180, 95);
        assertEquals(
                20,
                vertexScaler.computeScaleTargetParallelism(
                        flinkDep, conf, jobVertexID, evaluated, history));
        assertFalse(evaluated.containsKey(ScalingMetric.EXPECTED_PROCESSING_RATE));
        assertEquals(1, eventCollector.events.size());
        var event = eventCollector.events.poll();
        assertEquals(String.format(INNEFFECTIVE_MESSAGE_FORMAT, jobVertexID), event.getMessage());
        assertEquals(EventRecorder.Reason.IneffectiveScaling.name(), event.getReason());
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
        ScalingMetricEvaluator.computeProcessingRateThresholds(metrics, conf, false);
        return metrics;
    }
}
