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
import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.FlinkMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.kubernetes.operator.autoscaler.topology.VertexInfo;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for scaling metrics collection logic. */
@EnableKubernetesMockClient(crud = true)
public class MetricsCollectionAndEvaluationTest {

    private final AutoScalerInfo scalingInfo = new AutoScalerInfo(new HashMap<>());

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
        scalingExecutor =
                new ScalingExecutor(new EventRecorder(kubernetesClient, new EventCollector()));
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
        conf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, (double) Integer.MAX_VALUE);
        ReconciliationUtils.updateStatusForDeployedSpec(app, conf);
        clock = Clock.fixed(Instant.ofEpochSecond(0), ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        startTime = clock.instant();
        metricsCollector.setJobUpdateTs(startTime);
        app.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
    }

    @Test
    public void testEndToEnd() throws Exception {
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.);

        setDefaultMetrics(metricsCollector);

        // We haven't left the stabilization period
        // => no metrics reporting and collection should take place
        var collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertTrue(collectedMetrics.getMetricHistory().isEmpty());

        // We haven't collected a full window yet, no metrics should be reported but metrics should
        // still get collected.
        clock = Clock.offset(clock, conf.get(AutoScalerOptions.STABILIZATION_INTERVAL));
        metricsCollector.setClock(clock);
        collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(1, collectedMetrics.getMetricHistory().size());
        assertFalse(collectedMetrics.isFullyCollected());

        // We haven't collected a full window yet
        // => no metrics should be reported but metrics should still get collected.
        clock = Clock.offset(clock, Duration.ofSeconds(1));
        metricsCollector.setClock(clock);
        collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(2, collectedMetrics.getMetricHistory().size());
        assertFalse(collectedMetrics.isFullyCollected());

        // Advance time to stabilization period + full window => metrics should be present
        clock =
                Clock.fixed(
                        startTime
                                .plus(conf.get(AutoScalerOptions.STABILIZATION_INTERVAL))
                                .plus(conf.get(AutoScalerOptions.METRICS_WINDOW)),
                        ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(3, collectedMetrics.getMetricHistory().size());
        assertTrue(collectedMetrics.isFullyCollected());

        // Test resetting the collector and make sure we can deserialize the scalingInfo correctly
        metricsCollector = new TestingMetricsCollector(topology);
        metricsCollector.setJobUpdateTs(startTime);
        metricsCollector.setClock(clock);
        setDefaultMetrics(metricsCollector);
        collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(3, collectedMetrics.getMetricHistory().size());
        assertTrue(collectedMetrics.isFullyCollected());

        var evaluation = evaluator.evaluate(conf, collectedMetrics);
        scalingExecutor.scaleResource(app, scalingInfo, conf, evaluation);

        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(scalingInfo);
        assertEquals(4, scaledParallelism.size());

        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(source2));
        assertEquals(6, scaledParallelism.get(map));
        assertEquals(4, scaledParallelism.get(sink));

        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.5);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.);

        evaluation = evaluator.evaluate(conf, collectedMetrics);
        scalingExecutor.scaleResource(app, scalingInfo, conf, evaluation);

        scaledParallelism = ScalingExecutorTest.getScaledParallelism(scalingInfo);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(source2));
        assertEquals(12, scaledParallelism.get(map));
        assertEquals(8, scaledParallelism.get(sink));

        var resourceID = ResourceID.fromResource(app);
        assertNotNull(metricsCollector.getHistories().get(resourceID));

        metricsCollector.cleanup(app);
        assertNull(metricsCollector.getHistories().get(resourceID));
        assertNull(metricsCollector.getAvailableVertexMetricNames().get(resourceID));
    }

    private void setDefaultMetrics(TestingMetricsCollector metricsCollector) {
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.)),
                        source2,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.)),
                        map,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 500., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 2000.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 1000.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 500., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 2000.))));
    }

    @Test
    public void testKafkaPartitionMaxParallelism() throws Exception {
        var scalingInfo = new AutoScalerInfo(new HashMap<>());

        setDefaultMetrics(metricsCollector);
        metricsCollector.updateMetrics(app, scalingInfo, service, conf);

        var clock = Clock.fixed(Instant.now().plus(Duration.ofSeconds(3)), ZoneId.systemDefault());
        metricsCollector.setClock(clock);

        var collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);

        assertEquals(720, collectedMetrics.getJobTopology().getMaxParallelisms().get(source1));
        assertEquals(720, collectedMetrics.getJobTopology().getMaxParallelisms().get(source2));

        clock = Clock.fixed(Instant.now().plus(Duration.ofSeconds(3)), ZoneId.systemDefault());
        metricsCollector.setClock(clock);

        metricsCollector.setMetricNames(
                Map.of(
                        source1,
                        List.of(
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.0.anotherMetric"),
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.anotherTopic.partition.0.currentOffset"),
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.0.currentOffset"),
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.1.currentOffset"),
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.2.currentOffset"),
                                new AggregatedMetric(
                                        "1.Source__Kafka_Source_(testTopic).KafkaSourceReader.topic.testTopic.partition.3.currentOffset"))));

        collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(5, collectedMetrics.getJobTopology().getMaxParallelisms().get(source1));
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

    @Test
    public void testMetricCollectorWindow() throws Exception {
        setDefaultMetrics(metricsCollector);
        var metricsHistory = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(0, metricsHistory.getMetricHistory().size());

        // Not stable, nothing should be collected
        metricsCollector.setClock(Clock.offset(clock, Duration.ofSeconds(1)));
        metricsHistory = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(0, metricsHistory.getMetricHistory().size());

        // Update clock to stable time
        clock = Clock.offset(clock, conf.get(AutoScalerOptions.STABILIZATION_INTERVAL));
        metricsCollector.setClock(clock);

        // This call will lead to metric collection but we haven't reached the window size yet
        // which will hold back metrics
        metricsHistory = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(1, metricsHistory.getMetricHistory().size());
        assertFalse(metricsHistory.isFullyCollected());

        // Collect more values in window
        metricsCollector.setClock(Clock.offset(clock, Duration.ofSeconds(1)));
        metricsHistory = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(2, metricsHistory.getMetricHistory().size());
        assertFalse(metricsHistory.isFullyCollected());

        // Window size reached
        metricsCollector.setClock(Clock.offset(clock, conf.get(AutoScalerOptions.METRICS_WINDOW)));
        metricsHistory = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(3, metricsHistory.getMetricHistory().size());
        assertTrue(metricsHistory.isFullyCollected());

        // Window size + 1 will invalidate the first metric
        metricsCollector.setClock(
                Clock.offset(clock, conf.get(AutoScalerOptions.METRICS_WINDOW).plusSeconds(1)));
        metricsHistory = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(3, metricsHistory.getMetricHistory().size());

        // Complete new metric window with just the currently collected metric
        metricsCollector.setClock(
                Clock.offset(clock, conf.get(AutoScalerOptions.METRICS_WINDOW).plusDays(1)));
        metricsHistory = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(1, metricsHistory.getMetricHistory().size());

        // Existing metrics should be cleared on job updates
        metricsCollector.setJobUpdateTs(clock.instant().plus(Duration.ofDays(10)));
        metricsHistory = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(0, metricsHistory.getMetricHistory().size());
    }

    @Test
    public void testClearHistoryOnTopoChange() throws Exception {
        conf.set(AutoScalerOptions.TARGET_UTILIZATION, 1.);
        conf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.);

        setDefaultMetrics(metricsCollector);

        // We haven't left the stabilization period
        // => no metrics reporting and collection should take place
        var collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertTrue(collectedMetrics.getMetricHistory().isEmpty());
    }

    @Test
    public void testTolerateAbsenceOfPendingRecordsMetric() throws Exception {
        var topology = new JobTopology(new VertexInfo(source1, Set.of(), 5, 720));

        metricsCollector = new TestingMetricsCollector(topology);
        metricsCollector.setJobUpdateTs(startTime);
        metricsCollector.setCurrentMetrics(
                Map.of(
                        // Set source1 metrics without the PENDING_RECORDS metric
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 100., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 500.))));

        var collectedMetrics = collectMetrics();

        Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluation =
                evaluator.evaluate(conf, collectedMetrics);
        assertEquals(
                500., evaluation.get(source1).get(ScalingMetric.TARGET_DATA_RATE).getCurrent());
        assertEquals(
                5000.,
                evaluation.get(source1).get(ScalingMetric.TRUE_PROCESSING_RATE).getCurrent());
        assertEquals(
                1667.,
                evaluation.get(source1).get(ScalingMetric.SCALE_DOWN_RATE_THRESHOLD).getCurrent());
        assertEquals(
                500.,
                evaluation.get(source1).get(ScalingMetric.SCALE_UP_RATE_THRESHOLD).getCurrent());

        scalingExecutor.scaleResource(app, scalingInfo, conf, evaluation);
        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(scalingInfo);
        assertEquals(1, scaledParallelism.get(source1));
    }

    @Test
    public void testFinishedVertexMetricsCollection() throws Exception {
        var s1 = new JobVertexID();
        var finished = new JobVertexID();
        var topology =
                new JobTopology(
                        new VertexInfo(s1, Set.of(), 10, 720),
                        new VertexInfo(finished, Set.of(), 10, 720, true));

        metricsCollector = new TestingMetricsCollector(topology);
        metricsCollector.setJobUpdateTs(startTime);
        metricsCollector.setCurrentMetrics(
                Map.of(
                        // Set source1 metrics without the PENDING_RECORDS metric
                        s1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 0.1, Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1000.),
                                FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 500.))));

        var collectedMetrics = collectMetrics().getMetricHistory();
        var finishedMetrics =
                collectedMetrics.get(collectedMetrics.lastKey()).getVertexMetrics().get(finished);
        assertEquals(
                Map.of(
                        ScalingMetric.LAG,
                        0.,
                        ScalingMetric.CURRENT_PROCESSING_RATE,
                        0.,
                        ScalingMetric.SOURCE_DATA_RATE,
                        0.,
                        ScalingMetric.TRUE_PROCESSING_RATE,
                        Double.POSITIVE_INFINITY,
                        ScalingMetric.LOAD,
                        0.),
                finishedMetrics);
    }

    @Test
    public void testScaleDownWithZeroProcessingRate() throws Exception {
        var topology = new JobTopology(new VertexInfo(source1, Set.of(), 10, 720));

        metricsCollector = new TestingMetricsCollector(topology);
        metricsCollector.setJobUpdateTs(startTime);
        metricsCollector.setCurrentMetrics(
                Map.of(
                        // Set source1 metrics without the PENDING_RECORDS metric
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 0.1, Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.),
                                FlinkMetric.SOURCE_TASK_NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.))));

        var collectedMetrics = collectMetrics();

        Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluation =
                evaluator.evaluate(conf, collectedMetrics);
        assertEquals(0, evaluation.get(source1).get(ScalingMetric.TARGET_DATA_RATE).getCurrent());
        assertEquals(
                Double.POSITIVE_INFINITY,
                evaluation.get(source1).get(ScalingMetric.TRUE_PROCESSING_RATE).getCurrent());
        assertEquals(
                0.,
                evaluation.get(source1).get(ScalingMetric.SCALE_DOWN_RATE_THRESHOLD).getCurrent());
        assertEquals(
                0.,
                evaluation.get(source1).get(ScalingMetric.SCALE_UP_RATE_THRESHOLD).getCurrent());

        scalingExecutor.scaleResource(app, scalingInfo, conf, evaluation);
        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(scalingInfo);
        assertEquals(1, scaledParallelism.get(source1));
    }

    private CollectedMetricHistory collectMetrics() throws Exception {
        conf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ZERO);
        conf.set(AutoScalerOptions.METRICS_WINDOW, Duration.ofSeconds(2));

        metricsCollector.setClock(clock);

        var collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(1, collectedMetrics.getMetricHistory().size());
        assertFalse(collectedMetrics.isFullyCollected());

        metricsCollector.setClock(Clock.offset(clock, Duration.ofSeconds(2)));

        collectedMetrics = metricsCollector.updateMetrics(app, scalingInfo, service, conf);
        assertEquals(2, collectedMetrics.getMetricHistory().size());
        assertTrue(collectedMetrics.isFullyCollected());

        return collectedMetrics;
    }
}
