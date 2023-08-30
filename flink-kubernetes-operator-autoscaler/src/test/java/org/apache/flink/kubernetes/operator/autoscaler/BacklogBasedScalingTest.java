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
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.FlinkMetric;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.kubernetes.operator.autoscaler.topology.VertexInfo;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EventCollector;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.operator.autoscaler.AutoscalerTestUtils.getOrCreateInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for scaling metrics collection logic. */
@EnableKubernetesMockClient(crud = true)
public class BacklogBasedScalingTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;

    private ScalingMetricEvaluator evaluator;
    private TestingMetricsCollector metricsCollector;
    private ScalingExecutor scalingExecutor;

    private FlinkDeployment app;
    private JobVertexID source1, sink;

    private JobAutoScalerImpl autoscaler;

    private EventCollector eventCollector = new EventCollector();

    @BeforeEach
    public void setup() {
        evaluator = new ScalingMetricEvaluator();
        scalingExecutor =
                new ScalingExecutor(new EventRecorder(kubernetesClient, new EventCollector()));

        app = TestUtils.buildApplicationCluster();
        app.getMetadata().setGeneration(1L);
        app.getStatus().getJobStatus().setJobId(new JobID().toHexString());
        kubernetesClient.resource(app).createOrReplace();

        source1 = new JobVertexID();
        sink = new JobVertexID();

        metricsCollector =
                new TestingMetricsCollector(
                        new JobTopology(
                                new VertexInfo(source1, Set.of(), 1, 720),
                                new VertexInfo(sink, Set.of(source1), 1, 720)));

        var defaultConf = new Configuration();
        defaultConf.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        defaultConf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ZERO);
        defaultConf.set(AutoScalerOptions.RESTART_TIME, Duration.ofSeconds(1));
        defaultConf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ofSeconds(2));
        defaultConf.set(AutoScalerOptions.SCALING_ENABLED, true);
        defaultConf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        defaultConf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, (double) Integer.MAX_VALUE);
        defaultConf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.8);
        defaultConf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.1);
        defaultConf.set(AutoScalerOptions.SCALE_UP_GRACE_PERIOD, Duration.ZERO);

        configManager = new FlinkConfigManager(defaultConf);
        ReconciliationUtils.updateStatusForDeployedSpec(
                app, configManager.getDeployConfig(app.getMetadata(), app.getSpec()));
        app.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
        app.getStatus().getReconciliationStatus().markReconciledSpecAsStable();

        autoscaler =
                new JobAutoScalerImpl(
                        kubernetesClient,
                        metricsCollector,
                        evaluator,
                        scalingExecutor,
                        new EventRecorder(kubernetesClient, eventCollector));

        // Reset custom window size to default
        metricsCollector.setTestMetricWindowSize(null);
    }

    @Test
    public void test() throws Exception {
        var ctx = createAutoscalerTestContext();

        /* Test scaling up. */
        var now = Instant.ofEpochMilli(0);
        setClocksTo(now);
        metricsCollector.setJobUpdateTs(now);
        // Adjust metric window size, so we can fill the metric window with two metrics
        metricsCollector.setTestMetricWindowSize(Duration.ofSeconds(1));
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 850., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 2000.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 850., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 500.))));

        autoscaler.scale(getResourceContext(app, ctx));
        assertEquals(1, getOrCreateInfo(app, kubernetesClient).getMetricHistory().size());
        assertFlinkMetricsCount(0, 0, ctx);

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(getResourceContext(app, ctx));
        assertEquals(2, getOrCreateInfo(app, kubernetesClient).getMetricHistory().size());

        var scaledParallelism = ScalingExecutorTest.getScaledParallelism(kubernetesClient, app);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));
        assertFlinkMetricsCount(1, 0, ctx);

        /* Test stability while processing pending records. */

        // Update topology to reflect updated parallelisms
        metricsCollector.setJobTopology(
                new JobTopology(
                        new VertexInfo(source1, Set.of(), 4, 24),
                        new VertexInfo(sink, Set.of(source1), 4, 720)));
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1800.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1800.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 2500.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 1800.))));
        now = now.plusSeconds(1);
        setClocksTo(now);
        // Redeploying which erases metric history
        metricsCollector.setJobUpdateTs(now);
        // Adjust metric window size, so we can fill the metric window with three metrics
        metricsCollector.setTestMetricWindowSize(Duration.ofSeconds(2));

        autoscaler.scale(getResourceContext(app, ctx));
        assertFlinkMetricsCount(1, 0, ctx);
        assertEquals(1, getOrCreateInfo(app, kubernetesClient).getMetricHistory().size());
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(kubernetesClient, app);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));

        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1800.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 1800.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 1200.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 1800.))));

        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(getResourceContext(app, ctx));
        assertFlinkMetricsCount(1, 0, ctx);
        assertEquals(2, getOrCreateInfo(app, kubernetesClient).getMetricHistory().size());
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(kubernetesClient, app);
        assertEquals(4, scaledParallelism.get(source1));
        assertEquals(4, scaledParallelism.get(sink));

        /* Test scaling down. */

        // We have finally caught up to our original lag, time to scale down
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 600., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 800.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 800.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 600., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 800.))));
        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(getResourceContext(app, ctx));
        assertFlinkMetricsCount(2, 0, ctx);
        assertEquals(3, getOrCreateInfo(app, kubernetesClient).getMetricHistory().size());

        scaledParallelism = ScalingExecutorTest.getScaledParallelism(kubernetesClient, app);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));

        metricsCollector.setJobUpdateTs(now);
        metricsCollector.setJobTopology(
                new JobTopology(
                        new VertexInfo(source1, Set.of(), 2, 24),
                        new VertexInfo(sink, Set.of(source1), 2, 720)));

        /* Test stability while processing backlog. */

        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 900.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 900.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 900.))));
        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(getResourceContext(app, ctx));
        assertFlinkMetricsCount(2, 0, ctx);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(kubernetesClient, app);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));

        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 900.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 900.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 100.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 1000., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 900.))));
        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(getResourceContext(app, ctx));
        assertFlinkMetricsCount(2, 0, ctx);

        scaledParallelism = ScalingExecutorTest.getScaledParallelism(kubernetesClient, app);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));

        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 500., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.PENDING_RECORDS,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 0.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 500., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 500.))));
        now = now.plus(Duration.ofSeconds(1));
        setClocksTo(now);
        autoscaler.scale(getResourceContext(app, ctx));
        assertFlinkMetricsCount(2, 1, ctx);
        scaledParallelism = ScalingExecutorTest.getScaledParallelism(kubernetesClient, app);
        assertEquals(2, scaledParallelism.get(source1));
        assertEquals(2, scaledParallelism.get(sink));
    }

    @Test
    public void testMetricsPersistedAfterRedeploy() {
        var ctx = createAutoscalerTestContext();
        var now = Instant.ofEpochMilli(0);
        setClocksTo(now);
        metricsCollector.setJobUpdateTs(now);
        metricsCollector.setCurrentMetrics(
                Map.of(
                        source1,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 850., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_OUT_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric("", Double.NaN, Double.NaN, Double.NaN, 500.)),
                        sink,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("", Double.NaN, 850., Double.NaN, Double.NaN),
                                FlinkMetric.NUM_RECORDS_IN_PER_SEC,
                                new AggregatedMetric(
                                        "", Double.NaN, Double.NaN, Double.NaN, 500.))));

        autoscaler.scale(getResourceContext(app, ctx));
        assertFalse(getOrCreateInfo(app, kubernetesClient).getMetricHistory().isEmpty());
    }

    @Test
    public void testEventOnError() {
        // Invalid config
        app.getSpec()
                .getFlinkConfiguration()
                .put("kubernetes.operator.job.autoscaler.enabled", "3");
        autoscaler.scale(getResourceContext(app, createAutoscalerTestContext()));

        var event = eventCollector.events.poll();
        assertTrue(eventCollector.events.isEmpty());
        assertEquals(EventRecorder.Reason.AutoscalerError.toString(), event.getReason());
        assertTrue(event.getMessage().startsWith("Could not parse"));
    }

    @Test
    public void testNoEvaluationDuringStabilization() {
        var conf = configManager.getDefaultConfig();
        conf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ofMinutes(1));
        configManager.updateDefaultConfig(conf);

        var ctx = createAutoscalerTestContext();
        var now = Instant.ofEpochMilli(0);
        setClocksTo(now);
        metricsCollector.setJobUpdateTs(now);
        assertFalse(autoscaler.scale(getResourceContext(app, ctx)));
        assertTrue(getOrCreateInfo(app, kubernetesClient).getMetricHistory().isEmpty());
        assertTrue(eventCollector.events.isEmpty());
    }

    private void setClocksTo(Instant time) {
        var clock = Clock.fixed(time, ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        scalingExecutor.setClock(clock);
    }

    @NotNull
    private TestUtils.TestingContext<HasMetadata> createAutoscalerTestContext() {
        return new TestUtils.TestingContext<>() {
            public <T1> Set<T1> getSecondaryResources(Class<T1> aClass) {
                return (Set)
                        kubernetesClient.configMaps().inAnyNamespace().list().getItems().stream()
                                .collect(Collectors.toSet());
            }
        };
    }

    private void assertFlinkMetricsCount(
            int scalingCount, int balancedCount, TestUtils.TestingContext<HasMetadata> ctx) {
        AutoscalerFlinkMetrics autoscalerFlinkMetrics =
                autoscaler.flinkMetrics.get(
                        ResourceID.fromResource(getResourceContext(app, ctx).getResource()));
        assertEquals(scalingCount, autoscalerFlinkMetrics.numScalings.getCount());
        assertEquals(balancedCount, autoscalerFlinkMetrics.numBalanced.getCount());
    }
}
