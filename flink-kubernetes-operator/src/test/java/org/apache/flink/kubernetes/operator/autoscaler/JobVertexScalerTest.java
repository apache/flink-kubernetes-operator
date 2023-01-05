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
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for vertex parallelism scaler logic. */
public class JobVertexScalerTest {

    private JobVertexScaler vertexScaler;
    private Configuration conf;

    @BeforeEach
    public void setup() {
        vertexScaler = new JobVertexScaler();
        conf = new Configuration();
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ZERO);
    }

    @Test
    public void testParallelismScaling() {
        var op = new JobVertexID();
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        assertEquals(
                5,
                vertexScaler.computeScaleTargetParallelism(
                        conf, op, evaluated(10, 50, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        assertEquals(
                8,
                vertexScaler.computeScaleTargetParallelism(
                        conf, op, evaluated(10, 50, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(
                        conf, op, evaluated(10, 80, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        assertEquals(
                8,
                vertexScaler.computeScaleTargetParallelism(
                        conf, op, evaluated(10, 60, 100), Collections.emptySortedMap()));

        assertEquals(
                8,
                vertexScaler.computeScaleTargetParallelism(
                        conf, op, evaluated(10, 59, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.5);
        assertEquals(
                10,
                vertexScaler.computeScaleTargetParallelism(
                        conf, op, evaluated(2, 100, 40), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.6);
        assertEquals(
                4,
                vertexScaler.computeScaleTargetParallelism(
                        conf, op, evaluated(2, 100, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.5);
        assertEquals(
                5,
                vertexScaler.computeScaleTargetParallelism(
                        conf, op, evaluated(10, 10, 100), Collections.emptySortedMap()));

        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 0.6);
        assertEquals(
                4,
                vertexScaler.computeScaleTargetParallelism(
                        conf, op, evaluated(10, 10, 100), Collections.emptySortedMap()));
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
                        conf,
                        new JobVertexID(),
                        evaluated(10, 500, 100),
                        Collections.emptySortedMap()));
    }

    private Map<ScalingMetric, EvaluatedScalingMetric> evaluated(
            int parallelism, double target, double procRate, double catchupRate) {
        var metrics = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        metrics.put(ScalingMetric.PARALLELISM, EvaluatedScalingMetric.of(parallelism));
        metrics.put(ScalingMetric.MAX_PARALLELISM, EvaluatedScalingMetric.of(720));
        metrics.put(ScalingMetric.TARGET_DATA_RATE, new EvaluatedScalingMetric(target, target));
        metrics.put(ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(catchupRate));
        metrics.put(
                ScalingMetric.TRUE_PROCESSING_RATE, new EvaluatedScalingMetric(procRate, procRate));
        ScalingMetricEvaluator.computeProcessingRateThresholds(metrics, conf);
        return metrics;
    }

    private Map<ScalingMetric, EvaluatedScalingMetric> evaluated(
            int parallelism, double target, double procRate) {
        return evaluated(parallelism, target, procRate, 0.);
    }
}
