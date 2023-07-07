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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.kubernetes.operator.metrics.FlinkSessionJobMetrics.COUNTER_NAME;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link FlinkSessionJobMetrics tests. */
public class FlinkSessionJobMetricsTest {

    private final Configuration configuration = new Configuration();
    private TestingMetricListener listener;
    private MetricManager<FlinkSessionJob> metricManager;

    @BeforeEach
    public void init() {
        listener = new TestingMetricListener(configuration);
        metricManager =
                MetricManager.createFlinkSessionJobMetricManager(
                        configuration, listener.getMetricGroup());
    }

    @Test
    public void testMetricsSameNamespace() {
        var namespace = "test-ns";
        var job1 = TestUtils.buildSessionJob("job1", namespace);
        var job2 = TestUtils.buildSessionJob("job2", namespace);
        var metricId =
                listener.getNamespaceMetricId(FlinkSessionJob.class, namespace, COUNTER_NAME);
        assertTrue(listener.getGauge(namespace).isEmpty());

        metricManager.onUpdate(job1);
        metricManager.onUpdate(job2);
        assertEquals(2, listener.getGauge(metricId).get().getValue());

        metricManager.onUpdate(job1);
        metricManager.onUpdate(job2);
        assertEquals(2, listener.getGauge(metricId).get().getValue());

        metricManager.onRemove(job1);
        metricManager.onRemove(job2);
        assertEquals(0, listener.getGauge(metricId).get().getValue());

        metricManager.onRemove(job1);
        metricManager.onRemove(job2);
        assertEquals(0, listener.getGauge(metricId).get().getValue());
    }

    @Test
    public void testMetricsMultiNamespace() {
        var namespace1 = "ns1";
        var namespace2 = "ns2";
        var job1 = TestUtils.buildSessionJob("job", namespace1);
        var job2 = TestUtils.buildSessionJob("job", namespace2);

        var metricId1 =
                listener.getNamespaceMetricId(FlinkSessionJob.class, namespace1, COUNTER_NAME);
        var metricId2 =
                listener.getNamespaceMetricId(FlinkSessionJob.class, namespace2, COUNTER_NAME);

        assertTrue(listener.getGauge(metricId1).isEmpty());
        assertTrue(listener.getGauge(metricId2).isEmpty());

        metricManager.onUpdate(job1);
        metricManager.onUpdate(job2);
        assertEquals(1, listener.getGauge(metricId1).get().getValue());
        assertEquals(1, listener.getGauge(metricId2).get().getValue());

        metricManager.onUpdate(job1);
        metricManager.onUpdate(job2);
        assertEquals(1, listener.getGauge(metricId1).get().getValue());
        assertEquals(1, listener.getGauge(metricId2).get().getValue());

        metricManager.onRemove(job1);
        metricManager.onRemove(job2);
        assertEquals(0, listener.getGauge(metricId1).get().getValue());
        assertEquals(0, listener.getGauge(metricId2).get().getValue());

        metricManager.onRemove(job1);
        metricManager.onRemove(job2);
        assertEquals(0, listener.getGauge(metricId1).get().getValue());
        assertEquals(0, listener.getGauge(metricId2).get().getValue());
    }

    @Test
    public void testMetricsDisabled() {
        var flinkSessionJob = TestUtils.buildSessionJob();

        var conf = new Configuration();
        conf.set(OPERATOR_RESOURCE_METRICS_ENABLED, false);
        var listener = new TestingMetricListener(conf);
        var metricManager =
                MetricManager.createFlinkSessionJobMetricManager(conf, listener.getMetricGroup());

        var metricId =
                listener.getNamespaceMetricId(
                        FlinkSessionJob.class,
                        flinkSessionJob.getMetadata().getNamespace(),
                        COUNTER_NAME);

        assertTrue(listener.getGauge(metricId).isEmpty());

        metricManager.onUpdate(flinkSessionJob);
        assertTrue(listener.getGauge(metricId).isEmpty());

        metricManager.onRemove(flinkSessionJob);
        assertTrue(listener.getGauge(metricId).isEmpty());
    }
}
