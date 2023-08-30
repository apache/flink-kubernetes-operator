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
import org.apache.flink.kubernetes.operator.autoscaler.metrics.FlinkMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.topology.JobTopology;
import org.apache.flink.kubernetes.operator.autoscaler.topology.VertexInfo;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.SCALING_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for JobAutoScalerImpl. */
@EnableKubernetesMockClient(crud = true)
public class JobAutoScalerImplTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;

    KubernetesMockServer mockWebServer;

    private FlinkDeployment app;

    @BeforeEach
    public void setup() {
        app = TestUtils.buildApplicationCluster();
        app.getMetadata().setGeneration(1L);
        app.getStatus().getJobStatus().setJobId(new JobID().toHexString());
        kubernetesClient.resource(app).createOrReplace();

        var defaultConf = new Configuration();
        defaultConf.set(AUTOSCALER_ENABLED, true);
        configManager = new FlinkConfigManager(defaultConf);
        ReconciliationUtils.updateStatusForDeployedSpec(
                app, configManager.getDeployConfig(app.getMetadata(), app.getSpec()));
        app.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
        app.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
    }

    @Test
    void testMetricReporting() {
        JobVertexID jobVertexID = new JobVertexID();
        JobTopology jobTopology = new JobTopology(new VertexInfo(jobVertexID, Set.of(), 1, 10));

        TestingMetricsCollector metricsCollector = new TestingMetricsCollector(jobTopology);
        metricsCollector.setCurrentMetrics(
                Map.of(
                        jobVertexID,
                        Map.of(
                                FlinkMetric.BUSY_TIME_PER_SEC,
                                new AggregatedMetric("load", 0., 420., 0., 0.))));
        metricsCollector.setJobUpdateTs(Instant.ofEpochMilli(0));

        ScalingMetricEvaluator evaluator = new ScalingMetricEvaluator();
        ScalingExecutor scalingExecutor = new ScalingExecutor(eventRecorder);

        var autoscaler =
                new JobAutoScalerImpl(
                        kubernetesClient,
                        metricsCollector,
                        evaluator,
                        scalingExecutor,
                        eventRecorder);
        FlinkResourceContext<FlinkDeployment> resourceContext = getResourceContext(app);
        ResourceID resourceId = ResourceID.fromResource(app);

        autoscaler.scale(resourceContext);

        MetricGroup metricGroup = autoscaler.flinkMetrics.get(resourceId).metricGroup;
        assertEquals(
                0.42,
                getGaugeValue(
                        metricGroup,
                        AutoscalerFlinkMetrics.CURRENT,
                        AutoscalerFlinkMetrics.JOB_VERTEX_ID,
                        jobVertexID.toHexString(),
                        ScalingMetric.LOAD.name()),
                "Expected scaling metric LOAD was not reported. Reporting is broken");
    }

    @SuppressWarnings("unchecked")
    private static double getGaugeValue(
            MetricGroup metricGroup, String gaugeName, String... nestedMetricGroupNames) {
        for (String nestedMetricGroupName : nestedMetricGroupNames) {
            metricGroup =
                    ((Map<String, GenericMetricGroup>)
                                    Whitebox.getInternalState(metricGroup, "groups"))
                            .get(nestedMetricGroupName);
        }
        var metrics = (Map<String, Metric>) Whitebox.getInternalState(metricGroup, "metrics");
        return ((Gauge<Double>) metrics.get(gaugeName)).getValue();
    }

    @Test
    void testErrorReporting() {
        var autoscaler = new JobAutoScalerImpl(kubernetesClient, null, null, null, eventRecorder);
        FlinkResourceContext<FlinkDeployment> resourceContext = getResourceContext(app);
        ResourceID resourceId = ResourceID.fromResource(app);

        autoscaler.scale(resourceContext);
        Assertions.assertEquals(1, autoscaler.flinkMetrics.get(resourceId).numErrors.getCount());

        autoscaler.scale(resourceContext);
        Assertions.assertEquals(2, autoscaler.flinkMetrics.get(resourceId).numErrors.getCount());

        assertEquals(0, autoscaler.flinkMetrics.get(resourceId).numScalings.getCount());
    }

    @Test
    void testParallelismOverrides() throws Exception {
        var autoscaler = new JobAutoScalerImpl(kubernetesClient, null, null, null, eventRecorder);
        var ctx = getResourceContext(app);

        // Initially we should return empty overrides, do not crate any CM
        assertEquals(Map.of(), autoscaler.getParallelismOverrides(ctx));
        assertFalse(autoscaler.infoManager.getInfoFromKubernetes(app).isPresent());

        var autoscalerInfo = autoscaler.infoManager.getOrCreateInfo(app);

        var v1 = new JobVertexID().toString();
        var v2 = new JobVertexID().toString();
        autoscalerInfo.setCurrentOverrides(Map.of(v1, "1", v2, "2"));
        autoscalerInfo.replaceInKubernetes(kubernetesClient);

        assertEquals(Map.of(v1, "1", v2, "2"), autoscaler.getParallelismOverrides(ctx));

        // Disabling autoscaler should clear overrides
        app.getSpec().getFlinkConfiguration().put(AUTOSCALER_ENABLED.key(), "false");
        ctx = getResourceContext(app);
        assertEquals(Map.of(), autoscaler.getParallelismOverrides(ctx));
        // But not clear the autoscaler info
        assertTrue(autoscaler.infoManager.getInfoFromKubernetes(app).isPresent());

        int requestCount = mockWebServer.getRequestCount();
        // Make sure we don't update in kubernetes once removed
        autoscaler.getParallelismOverrides(ctx);
        assertEquals(requestCount, mockWebServer.getRequestCount());

        app.getSpec().getFlinkConfiguration().put(AUTOSCALER_ENABLED.key(), "true");
        ctx = getResourceContext(app);
        assertEquals(Map.of(), autoscaler.getParallelismOverrides(ctx));

        autoscalerInfo.setCurrentOverrides(Map.of(v1, "1", v2, "2"));
        autoscalerInfo.replaceInKubernetes(kubernetesClient);
        assertEquals(Map.of(v1, "1", v2, "2"), autoscaler.getParallelismOverrides(ctx));

        app.getSpec().getFlinkConfiguration().put(SCALING_ENABLED.key(), "false");
        ctx = getResourceContext(app);
        assertEquals(Map.of(v1, "1", v2, "2"), autoscaler.getParallelismOverrides(ctx));

        // Test error handling
        // Invalid config
        app.getSpec().getFlinkConfiguration().put(AUTOSCALER_ENABLED.key(), "asd");
        ctx = getResourceContext(app);
        assertEquals(Map.of(), autoscaler.getParallelismOverrides(ctx));
    }
}
