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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.FlinkMetric;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.kubernetes.operator.autoscaler.topology.VertexInfo;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for scaling metrics collection logic. */
@EnableKubernetesMockClient(crud = true)
public class MetricsCollectionAndEvaluationTest {

    private ScalingMetricEvaluator evaluator;
    private TestingFlinkService service;
    private TestingMetricsCollector metricsCollector;
    private ScalingExecutor scalingExecutor;

    private FlinkDeployment app;
    private Configuration conf;
    private JobVertexID source1, source2, map, sink;
    private JobTopology topology;

    private KubernetesClient kubernetesClient;

    private Clock clock;

    private Instant startTime;

    @BeforeEach
    public void setup() {
        evaluator = new ScalingMetricEvaluator();
        scalingExecutor = new ScalingExecutor(kubernetesClient);
        service = new TestingFlinkService();

        app = TestUtils.buildApplicationCluster();
        app.getMetadata().setGeneration(1L);
        app.getStatus().getJobStatus().setJobId(new JobID().toHexString());
        kubernetesClient.resource(app).createOrReplace();

        source1 = new JobVertexID();
        source2 = new JobVertexID();
        map = new JobVertexID();
        sink = new JobVertexID();

        topology =
                new JobTopology(
                        new VertexInfo(source1, Set.of(), 2, 720),
                        new VertexInfo(source2, Set.of(), 2, 720),
                        new VertexInfo(map, Set.of(source1, source2), 12, 720),
                        new VertexInfo(sink, Set.of(map), 8, 24));

        metricsCollector = new TestingMetricsCollector(topology);

        var confManager = new FlinkConfigManager(new Configuration());
        conf = confManager.getDeployConfig(app.getMetadata(), app.getSpec());
        conf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ofSeconds(10));
        conf.set(AutoScalerOptions.METRICS_WINDOW, Duration.ofSeconds(100));
        conf.set(AutoScalerOptions.RESTART_TIME, Duration.ZERO);
        conf.set(AutoScalerOptions.SCALING_ENABLED, true);
        conf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        ReconciliationUtils.updateStatusForDeployedSpec(app, conf);
        clock = Clock.fixed(Instant.ofEpochSecond(0), ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        startTime = clock.instant();
        app.getStatus().getJobStatus().setStartTime(String.valueOf(startTime.toEpochMilli()));
        app.getStatus().getJobStatus().setUpdateTime(String.valueOf(startTime.toEpochMilli()));
        app.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
    }

    @Test
    public void testEndToEnd() throws Exception {
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.);

        var scalingInfo = new AutoScalerInfo(new HashMap<>());

        setDefaultMetrics(metricsCollector);

        // We haven't left the stabilization period => no metrics reporting and collection should
        // take place
        var collectedMetrics = metricsCollector.getMetricsHistory(app, scalingInfo, service, conf);
        assertTrue(collectedMetrics.getMetricHistory().isEmpty());

        // We haven't collected a full window yet, no metrics should be reported but metrics should
        // still get collected.
        clock =
                Clock.fixed(
                        clock.instant().plus(conf.get(AutoScalerOptions.STABILIZATION_INTERVAL)),
                        ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        collectedMetrics = metricsCollector.getMetricsHistory(app, scalingInfo, service, conf);
        assertTrue(collectedMetrics.getMetricHistory().isEmpty());

        // We haven't collected a full window yet, no metrics should be reported but metrics should
        // still get collected.
        clock = Clock.fixed(clock.instant().plus(Duration.ofSeconds(1)), ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        collectedMetrics = metricsCollector.getMetricsHistory(app, scalingInfo, service, conf);
        assertTrue(collectedMetrics.getMetricHistory().isEmpty());

        // Advance time to stabilization period + full window => metrics should be present
        clock =
                Clock.fixed(
                        startTime
                                .plus(conf.get(AutoScalerOptions.STABILIZATION_INTERVAL))
                                .plus(conf.get(AutoScalerOptions.METRICS_WINDOW)),
                        ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        collectedMetrics = metricsCollector.getMetricsHistory(app, scalingInfo, service, conf);
        assertEquals(3, collectedMetrics.getMetricHistory().size());

        // Test resetting the collector and make sure we can deserialize the scalingInfo correctly
        metricsCollector = new TestingMetricsCollector(topology);
        metricsCollector.setClock(clock);
        setDefaultMetrics(metricsCollector);
        collectedMetrics = metricsCollector.getMetricsHistory(app, scalingInfo, service, conf);
        assertEquals(3, collectedMetrics.getMetricHistory().size());

        evaluator.setClock(clock);
        var evaluation = evaluator.evaluate(conf, collectedMetrics);
        scalingExecutor.scaleResource(app, scalingInfo, conf, evaluation);

        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(app);
        assertEquals(4, scaledParallelism.size());

        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(source2));
        assertEquals(6, scaledParallelism.get(map));
        assertEquals(4, scaledParallelism.get(sink));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.5);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.);

        evaluation = evaluator.evaluate(conf, collectedMetrics);
        scalingExecutor.scaleResource(app, scalingInfo, conf, evaluation);

        scaledParallelism = ScalingExecutorTest.getScaledParallelism(app);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(source2));
        assertEquals(12, scaledParallelism.get(map));
        assertEquals(8, scaledParallelism.get(sink));

        var resourceID = ResourceID.fromResource(app);
        assertNotNull(metricsCollector.getHistories().get(resourceID));
        assertNotNull(metricsCollector.getTopologies().get(resourceID));

        metricsCollector.cleanup(app);
        scalingExecutor.cleanup(app);
        assertNull(metricsCollector.getHistories().get(resourceID));
        assertNull(metricsCollector.getAvailableVertexMetricNames().get(resourceID));
        assertNull(metricsCollector.getTopologies().get(resourceID));
        assertNull(metricsCollector.getTopologies().get(resourceID));
    }

    private void setDefaultMetrics(TestingMetricsCollector metricsCollector) {
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, 1000., Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.)),
                        source2,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, 1000., Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.)),
                        map,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, 500., Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 1000.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, 500., Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 2000.))));
    }

    @Test
    public void testKafkaPartitionMaxParallelism() throws Exception {
        var scalingInfo = new AutoScalerInfo(new HashMap<>());

        setDefaultMetrics(metricsCollector);
        metricsCollector.getMetricsHistory(app, scalingInfo, service, conf);

        var clock = Clock.fixed(Instant.now().plus(Duration.ofSeconds(3)), ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        evaluator.setClock(clock);

        var collectedMetrics = metricsCollector.getMetricsHistory(app, scalingInfo, service, conf);

        assertEquals(720, collectedMetrics.getJobTopology().getMaxParallelisms().get(source1));
        assertEquals(720, collectedMetrics.getJobTopology().getMaxParallelisms().get(source2));

        clock = Clock.fixed(Instant.now().plus(Duration.ofSeconds(3)), ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        evaluator.setClock(clock);

        metricsCollector.setMetricNames(
                Map.of(
                        source1,
                        List.of(
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.0.currentOffset"),
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.1.currentOffset"),
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.2.currentOffset"),
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.3.currentOffset"))));

        collectedMetrics = metricsCollector.getMetricsHistory(app, scalingInfo, service, conf);
        assertEquals(4, collectedMetrics.getJobTopology().getMaxParallelisms().get(source1));
        assertEquals(720, collectedMetrics.getJobTopology().getMaxParallelisms().get(source2));
    }

    @Test
    public void testJobDetailsRestCompatibility() throws JsonProcessingException {
        String flink15Response =
                "{\"jid\":\"068d4a00e4592099e94bb7a45f5bbd95\",\"name\":\"State machine job\",\"isStoppable\":false,\"state\":\"RUNNING\",\"start-time\":1667487397898,\"end-time\":-1,\"duration\":82350,\"maxParallelism\":-1,\"now\":1667487480248,\"timestamps\":{\"RUNNING\":1667487398514,\"FAILING\":0,\"CANCELLING\":0,\"FINISHED\":0,\"FAILED\":0,\"RESTARTING\":0,\"SUSPENDED\":0,\"INITIALIZING\":1667487397898,\"CANCELED\":0,\"RECONCILING\":0,\"CREATED\":1667487398210},\"vertices\":[{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"name\":\"Source: Custom Source\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1667487404820,\"end-time\":-1,\"duration\":75428,\"tasks\":{\"FINISHED\":0,\"CANCELING\":0,\"RUNNING\":2,\"SCHEDULED\":0,\"RECONCILING\":0,\"CANCELED\":0,\"FAILED\":0,\"INITIALIZING\":0,\"CREATED\":0,\"DEPLOYING\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":true,\"write-bytes\":1345204,\"write-bytes-complete\":true,\"read-records\":0,\"read-records-complete\":true,\"write-records\":99268,\"write-records-complete\":true}},{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"name\":\"Flat Map -> Sink: Print to Std. Out\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1667487405294,\"end-time\":-1,\"duration\":74954,\"tasks\":{\"FINISHED\":0,\"CANCELING\":0,\"RUNNING\":2,\"SCHEDULED\":0,\"RECONCILING\":0,\"CANCELED\":0,\"FAILED\":0,\"INITIALIZING\":0,\"CREATED\":0,\"DEPLOYING\":0},\"metrics\":{\"read-bytes\":1386967,\"read-bytes-complete\":true,\"write-bytes\":0,\"write-bytes-complete\":true,\"read-records\":99205,\"read-records-complete\":true,\"write-records\":0,\"write-records-complete\":true}}],\"status-counts\":{\"FINISHED\":0,\"CANCELING\":0,\"RUNNING\":2,\"SCHEDULED\":0,\"RECONCILING\":0,\"CANCELED\":0,\"FAILED\":0,\"INITIALIZING\":0,\"CREATED\":0,\"DEPLOYING\":0},\"plan\":{\"jid\":\"068d4a00e4592099e94bb7a45f5bbd95\",\"name\":\"State machine job\",\"type\":\"STREAMING\",\"nodes\":[{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Flat Map<br/>+- Sink: Print to Std. Out<br/>\",\"inputs\":[{\"num\":0,\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"ship_strategy\":\"HASH\",\"exchange\":\"pipelined_bounded\"}],\"optimizer_properties\":{}},{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Source: Custom Source<br/>\",\"optimizer_properties\":{}}]}}";
        String flink16Response =
                "{\"jid\":\"2667c218edfecda90ba9b4b23e8e14e1\",\"name\":\"State machine job\",\"isStoppable\":false,\"state\":\"RUNNING\",\"start-time\":1667487688693,\"end-time\":-1,\"duration\":36646,\"maxParallelism\":-1,\"now\":1667487725339,\"timestamps\":{\"RESTARTING\":0,\"RECONCILING\":0,\"INITIALIZING\":1667487688693,\"FAILED\":0,\"CANCELED\":0,\"SUSPENDED\":0,\"RUNNING\":1667487689116,\"FAILING\":0,\"FINISHED\":0,\"CREATED\":1667487688912,\"CANCELLING\":0},\"vertices\":[{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"name\":\"Source: Custom Source\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1667487695274,\"end-time\":-1,\"duration\":30065,\"tasks\":{\"INITIALIZING\":0,\"CREATED\":0,\"RUNNING\":2,\"FAILED\":0,\"SCHEDULED\":0,\"DEPLOYING\":0,\"CANCELING\":0,\"RECONCILING\":0,\"FINISHED\":0,\"CANCELED\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":true,\"write-bytes\":417562,\"write-bytes-complete\":true,\"read-records\":0,\"read-records-complete\":true,\"write-records\":33254,\"write-records-complete\":true,\"accumulated-backpressured-time\":0,\"accumulated-idle-time\":0,\"accumulated-busy-time\":\"NaN\"}},{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"name\":\"Flat Map -> Sink: Print to Std. Out\",\"maxParallelism\":128,\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1667487695288,\"end-time\":-1,\"duration\":30051,\"tasks\":{\"INITIALIZING\":0,\"CREATED\":0,\"RUNNING\":2,\"FAILED\":0,\"SCHEDULED\":0,\"DEPLOYING\":0,\"CANCELING\":0,\"RECONCILING\":0,\"FINISHED\":0,\"CANCELED\":0},\"metrics\":{\"read-bytes\":464603,\"read-bytes-complete\":true,\"write-bytes\":0,\"write-bytes-complete\":true,\"read-records\":33233,\"read-records-complete\":true,\"write-records\":0,\"write-records-complete\":true,\"accumulated-backpressured-time\":0,\"accumulated-idle-time\":37846,\"accumulated-busy-time\":171.0}}],\"status-counts\":{\"INITIALIZING\":0,\"CREATED\":0,\"RUNNING\":2,\"FAILED\":0,\"SCHEDULED\":0,\"DEPLOYING\":0,\"CANCELING\":0,\"RECONCILING\":0,\"FINISHED\":0,\"CANCELED\":0},\"plan\":{\"jid\":\"2667c218edfecda90ba9b4b23e8e14e1\",\"name\":\"State machine job\",\"type\":\"STREAMING\",\"nodes\":[{\"id\":\"20ba6b65f97481d5570070de90e4e791\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Flat Map<br/>+- Sink: Print to Std. Out<br/>\",\"inputs\":[{\"num\":0,\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"ship_strategy\":\"HASH\",\"exchange\":\"pipelined_bounded\"}],\"optimizer_properties\":{}},{\"id\":\"bc764cd8ddf7a0cff126f51c16239658\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Source: Custom Source<br/>\",\"optimizer_properties\":{}}]}}";

        var flinkObjectMapper = new ObjectMapper();
        flinkObjectMapper.readValue(flink15Response, JobDetailsInfo.class);
        flinkObjectMapper.readValue(flink16Response, JobDetailsInfo.class);
    }
}
