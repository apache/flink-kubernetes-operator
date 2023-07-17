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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.autoscaler.AutoscalerTestUtils.getOrCreateInfo;
import static org.apache.flink.kubernetes.operator.autoscaler.ScalingExecutor.SCALING_SUMMARY_ENTRY;
import static org.apache.flink.kubernetes.operator.autoscaler.ScalingExecutor.SCALING_SUMMARY_HEADER_SCALING_DISABLED;
import static org.apache.flink.kubernetes.operator.autoscaler.ScalingExecutor.SCALING_SUMMARY_HEADER_SCALING_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for scaling execution logic. */
@EnableKubernetesMockClient(crud = true)
public class ScalingExecutorTest {

    private ScalingExecutor scalingDecisionExecutor;

    private EventCollector eventCollector;
    private Configuration conf;
    private KubernetesClient kubernetesClient;
    private FlinkDeployment flinkDep;

    @BeforeEach
    public void setup() {
        eventCollector = new EventCollector();
        scalingDecisionExecutor =
                new ScalingExecutor(new EventRecorder(kubernetesClient, eventCollector));
        conf = new Configuration();
        conf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ZERO);
        conf.set(AutoScalerOptions.SCALING_ENABLED, true);
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, (double) Integer.MAX_VALUE);
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ZERO);

        flinkDep = TestUtils.buildApplicationCluster();
        kubernetesClient.resource(flinkDep).createOrReplace();
        var jobStatus = flinkDep.getStatus().getJobStatus();
        jobStatus.setStartTime(String.valueOf(System.currentTimeMillis()));
        jobStatus.setUpdateTime(String.valueOf(System.currentTimeMillis()));
        jobStatus.setState(JobStatus.RUNNING.name());
    }

    @Test
    public void testUtilizationBoundaries() {
        // Restart time should not affect utilization boundary
        conf.set(AutoScalerOptions.RESTART_TIME, Duration.ZERO);
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ZERO);

        var flinkDep = TestUtils.buildApplicationCluster();
        kubernetesClient.resource(flinkDep).createOrReplace();

        var op1 = new JobVertexID();

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.6);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.);

        var evaluated = Map.of(op1, evaluated(1, 70, 100));
        var scalingSummary = Map.of(op1, new ScalingSummary(2, 1, evaluated.get(op1)));
        assertFalse(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.2);
        evaluated = Map.of(op1, evaluated(1, 70, 100));
        scalingSummary = Map.of(op1, new ScalingSummary(2, 1, evaluated.get(op1)));
        assertTrue(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));
        assertTrue(getScaledParallelism(kubernetesClient, flinkDep).isEmpty());

        var op2 = new JobVertexID();
        evaluated =
                Map.of(
                        op1, evaluated(1, 70, 100),
                        op2, evaluated(1, 85, 100));
        scalingSummary =
                Map.of(
                        op1,
                        new ScalingSummary(1, 2, evaluated.get(op1)),
                        op2,
                        new ScalingSummary(1, 2, evaluated.get(op2)));

        assertFalse(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));

        evaluated =
                Map.of(
                        op1, evaluated(1, 70, 100),
                        op2, evaluated(1, 70, 100));
        scalingSummary =
                Map.of(
                        op1,
                        new ScalingSummary(1, 2, evaluated.get(op1)),
                        op2,
                        new ScalingSummary(1, 2, evaluated.get(op2)));
        assertTrue(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));

        // Test with backlog based scaling
        evaluated = Map.of(op1, evaluated(1, 70, 100, 15));
        scalingSummary = Map.of(op1, new ScalingSummary(1, 2, evaluated.get(op1)));
        assertFalse(ScalingExecutor.allVerticesWithinUtilizationTarget(evaluated, scalingSummary));
    }

    @Test
    public void testVertexesExclusionForScaling() {
        var sourceHexString = "0bfd135746ac8efb3cce668b12e16d3a";
        var source = JobVertexID.fromHexString(sourceHexString);
        var filterOperatorHexString = "869fb403873411306404e9f2e4438c0e";
        var filterOperator = JobVertexID.fromHexString(filterOperatorHexString);
        var sinkHexString = "a6b7102b8d3e3a9564998c1ffeb5e2b7";
        var sink = JobVertexID.fromHexString(sinkHexString);

        var scalingInfo = new AutoScalerInfo(new HashMap<>());
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, .8);
        var metrics =
                Map.of(
                        source,
                        evaluated(10, 80, 100),
                        filterOperator,
                        evaluated(10, 30, 100),
                        sink,
                        evaluated(10, 80, 100));
        // filter operator should not scale
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, List.of(filterOperatorHexString));
        assertFalse(scalingDecisionExecutor.scaleResource(flinkDep, scalingInfo, conf, metrics));
        // filter operator should scale
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, List.of());
        assertTrue(scalingDecisionExecutor.scaleResource(flinkDep, scalingInfo, conf, metrics));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testScalingEvents(boolean scalingEnabled) {
        var jobVertexID = new JobVertexID();
        conf.set(AutoScalerOptions.SCALING_ENABLED, scalingEnabled);
        var metrics = Map.of(jobVertexID, evaluated(1, 110, 100));
        var scalingInfo = new AutoScalerInfo(new HashMap<>());
        assertEquals(
                scalingEnabled,
                scalingDecisionExecutor.scaleResource(flinkDep, scalingInfo, conf, metrics));
        assertEquals(1, eventCollector.events.size());
        var event = eventCollector.events.poll();
        assertTrue(
                event.getMessage()
                        .contains(
                                String.format(
                                        SCALING_SUMMARY_ENTRY,
                                        jobVertexID,
                                        1,
                                        2,
                                        100.0,
                                        157.0,
                                        110.0)));
        assertTrue(
                event.getMessage()
                        .contains(
                                scalingEnabled
                                        ? SCALING_SUMMARY_HEADER_SCALING_ENABLED
                                        : SCALING_SUMMARY_HEADER_SCALING_DISABLED));
        assertEquals(EventRecorder.Reason.ScalingReport.name(), event.getReason());

        metrics = Map.of(jobVertexID, evaluated(1, 110, 101));
        assertEquals(
                scalingEnabled,
                scalingDecisionExecutor.scaleResource(flinkDep, scalingInfo, conf, metrics));
        var event2 = eventCollector.events.poll();
        assertEquals(event.getMetadata().getUid(), event2.getMetadata().getUid());
        assertEquals(2, event2.getCount());
        assertEquals(!scalingEnabled, scalingInfo.getCurrentOverrides().isEmpty());
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
        ScalingMetricEvaluator.computeProcessingRateThresholds(metrics, conf, false);
        return metrics;
    }

    private Map<ScalingMetric, EvaluatedScalingMetric> evaluated(
            int parallelism, double target, double procRate) {
        return evaluated(parallelism, target, procRate, 0.);
    }

    protected static Map<JobVertexID, Integer> getScaledParallelism(
            KubernetesClient client, AbstractFlinkResource<?, ?> cr) {
        return getScaledParallelism(getOrCreateInfo(cr, client));
    }

    protected static Map<JobVertexID, Integer> getScaledParallelism(AutoScalerInfo info) {
        return info.getCurrentOverrides().entrySet().stream()
                .collect(
                        Collectors.toMap(
                                e -> JobVertexID.fromHexString(e.getKey()),
                                e -> Integer.valueOf(e.getValue())));
    }
}
